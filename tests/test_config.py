from __future__ import annotations

import pytest

from collector.config import Config, ConfigError, load, secret_values

SAMPLE = """
[paths]
data_root = "/tmp/collector"

[storage]
compression_level = 15

[source]
calls_per_minute = 5

[collection]
extended_hours = false
"""


def write_config(tmp_path, text=SAMPLE):
    path = tmp_path / "collector.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_loads_values_and_defaults(tmp_path):
    config = load(write_config(tmp_path))
    assert str(config.data_root) == "/tmp/collector"
    assert config.storage.compression_level == 15
    assert config.collection.extended_hours is False
    # Untouched sections fall back to the documented defaults.
    assert config.universe.size == 1000
    assert config.health.max_status_age_seconds == 93_600


def test_the_shipped_config_file_is_valid():
    """The config mounted into the container must actually load."""
    from pathlib import Path

    shipped = Path(__file__).resolve().parents[1] / "config" / "collector.toml"
    config = load(shipped)
    assert config.source.calls_per_minute == 5  # the free plan's limit
    assert config.scheduler.timezone == "America/New_York"


def test_env_overrides_the_data_root(tmp_path, monkeypatch):
    monkeypatch.setenv("COLLECTOR_DATA_ROOT", "/elsewhere")
    assert str(load(write_config(tmp_path)).data_root) == "/elsewhere"


def test_missing_file_is_fine_when_not_requested_explicitly(tmp_path, monkeypatch):
    """A config path from the environment that does not exist falls back to defaults."""
    monkeypatch.setenv("COLLECTOR_CONFIG", str(tmp_path / "absent.toml"))
    monkeypatch.setenv("COLLECTOR_DATA_ROOT", str(tmp_path))
    config = load()
    assert config.config_path is None
    assert config.source.calls_per_minute == 5
    assert str(config.data_root) == str(tmp_path)


def test_an_explicitly_named_missing_file_is_an_error(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load(tmp_path / "nope.toml")


def test_invalid_toml_is_an_error(tmp_path):
    with pytest.raises(ConfigError, match="invalid TOML"):
        load(write_config(tmp_path, "this is not = = toml"))


def test_a_typo_in_a_key_fails_loudly(tmp_path):
    """Silently ignoring it would mean running with an unintended default."""
    with pytest.raises(ConfigError, match="unknown key"):
        load(write_config(tmp_path, "[storage]\ncompresion_level = 9\n"))


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("[storage]\ncompression_level = 30\n", "compression_level"),
        ("[source]\ncalls_per_minute = 0\n", "calls_per_minute"),
        ("[universe]\nsize = 0\n", "size"),
        ("[collection]\nbackfill_days = 0\n", "backfill_days"),
        ("[checks]\nrun_failure_abort_ratio = 0\n", "run_failure_abort_ratio"),
        ('[scheduler]\ntimezone = "Mars/Olympus"\n', "timezone"),
    ],
)
def test_invalid_values_are_rejected(tmp_path, text, message):
    with pytest.raises(ConfigError, match=message):
        load(write_config(tmp_path, text))


def test_soft_tolerance_above_hard_tolerance_is_rejected(tmp_path):
    text = "[checks]\neod_count_soft_tolerance = 0.5\neod_count_hard_tolerance = 0.3\n"
    with pytest.raises(ConfigError, match="soft tolerance"):
        load(write_config(tmp_path, text))


# --------------------------------------------------------------------------- #
# Secrets
# --------------------------------------------------------------------------- #


def test_api_key_comes_from_the_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-live-abcdef123456")
    config = load(write_config(tmp_path))
    assert config.api_key == "sk-live-abcdef123456"
    assert config.has_api_key() is True


def test_polygon_env_var_also_works(monkeypatch, tmp_path):
    """The provider was Polygon.io before rebranding; accept both names."""
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.setenv("POLYGON_API_KEY", "sk-polygon-999")
    assert load(write_config(tmp_path)).api_key == "sk-polygon-999"


def test_missing_api_key_raises_with_a_pointer_to_the_env_file(monkeypatch, tmp_path):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)
    config = load(write_config(tmp_path))
    assert config.has_api_key() is False
    with pytest.raises(ConfigError, match=r"\.env\.example"):
        _ = config.api_key


def test_the_key_is_never_stored_on_the_config_object(monkeypatch, tmp_path):
    """A repr or dump of the config must not be able to leak it."""
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-live-SUPERSECRET")
    config = load(write_config(tmp_path))
    assert "SUPERSECRET" not in repr(config)
    assert "SUPERSECRET" not in str(vars(config.source))
    assert not any("SUPERSECRET" in str(value) for value in config.__dict__.values())


def test_secret_values_lists_whatever_is_set(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-live-abcdef")
    monkeypatch.setenv("COLLECTOR_ALERT_WEBHOOK", "https://hooks.example/T/abc123")
    values = secret_values()
    assert "sk-live-abcdef" in values
    assert "https://hooks.example/T/abc123" in values


def test_blank_and_trivially_short_values_are_not_treated_as_secrets(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "   ")
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)
    monkeypatch.delenv("COLLECTOR_ALERT_WEBHOOK", raising=False)
    assert secret_values() == []
    # A blank key is "no key", not an empty key.
    assert Config(data_root="/data").has_api_key() is False
