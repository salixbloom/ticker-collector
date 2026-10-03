from __future__ import annotations

import pytest

from collector import __version__
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


TYPO_CONFIG = """[storage]
compresion_level = 9
"""


def test_a_typo_in_a_key_fails_loudly(tmp_path):
    """Silently ignoring it would mean running with an unintended default."""
    with pytest.raises(ConfigError, match="unknown key"):
        load(write_config(tmp_path, "[storage]\ncompresion_level = 9\n"))


def test_unknown_key_error_names_the_section_and_the_accepted_keys(tmp_path):
    with pytest.raises(ConfigError) as excinfo:
        load(write_config(tmp_path, TYPO_CONFIG))
    message = str(excinfo.value)
    assert "[storage]" in message
    assert "compresion_level" in message
    assert "compression_level" in message  # the key they meant, from `known:`


def test_unknown_key_error_points_at_a_stale_image(tmp_path):
    """The other cause of this error is a container older than its bind-mounted
    config -- it looks identical but is not a config fault at all."""
    with pytest.raises(ConfigError) as excinfo:
        load(write_config(tmp_path, TYPO_CONFIG))
    message = str(excinfo.value)
    assert __version__ in message
    assert "--build" in message


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


# --------------------------------------------------------------------------- #
# The key from a file: survives a stack manager rewriting the stack directory
# --------------------------------------------------------------------------- #


def test_the_key_can_come_from_a_file(tmp_path, monkeypatch):
    """Dockge manages the stack directory, so a `.env` there can be cleared.

    Every other setting has a default and degrades quietly; a lost key leaves the
    container healthy, idle and collecting nothing. A file outside that directory
    is not something the stack manager touches.
    """
    key_file = tmp_path / "api_key"
    key_file.write_text("sk-live-fromfile123\n", encoding="utf-8")
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.setenv("MASSIVE_API_KEY_FILE", str(key_file))

    config = load(write_config(tmp_path))
    assert config.api_key == "sk-live-fromfile123"  # trailing newline stripped
    assert config.has_api_key() is True


def test_the_file_wins_over_the_environment(tmp_path, monkeypatch):
    """The file is the deliberate configuration; a stale env var should not win."""
    key_file = tmp_path / "api_key"
    key_file.write_text("sk-live-fromfile", encoding="utf-8")
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-live-fromenv")
    monkeypatch.setenv("MASSIVE_API_KEY_FILE", str(key_file))
    assert load(write_config(tmp_path)).api_key == "sk-live-fromfile"


def test_an_unreadable_key_file_says_so_rather_than_falling_back(tmp_path, monkeypatch):
    """Silently using a different key would be worse than refusing."""
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-live-fromenv")
    monkeypatch.setenv("MASSIVE_API_KEY_FILE", str(tmp_path / "missing"))
    config = load(write_config(tmp_path))
    with pytest.raises(ConfigError, match="could not be read"):
        _ = config.api_key


def test_an_empty_key_file_is_an_error(tmp_path, monkeypatch):
    """The exact Dockge symptom: the file is there but its contents are gone."""
    key_file = tmp_path / "api_key"
    key_file.write_text("   \n", encoding="utf-8")
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.setenv("MASSIVE_API_KEY_FILE", str(key_file))
    config = load(write_config(tmp_path))
    with pytest.raises(ConfigError, match="is empty"):
        _ = config.api_key


def test_a_file_supplied_key_is_still_redacted_from_logs(tmp_path, monkeypatch):
    """Otherwise the safer configuration would be the one that leaks."""
    key_file = tmp_path / "api_key"
    key_file.write_text("sk-live-SECRETFROMFILE", encoding="utf-8")
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.setenv("MASSIVE_API_KEY_FILE", str(key_file))

    assert "sk-live-SECRETFROMFILE" in secret_values()

    from collector.logging_setup import redact

    assert "SECRETFROMFILE" not in redact(
        "GET https://api.massive.com/v2/aggs?apiKey=sk-live-SECRETFROMFILE"
    )


def test_the_key_file_path_is_not_treated_as_a_secret(tmp_path, monkeypatch):
    """Redacting the path would make every log line mentioning it unreadable."""
    key_file = tmp_path / "api_key"
    key_file.write_text("sk-live-abcdefgh", encoding="utf-8")
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.setenv("MASSIVE_API_KEY_FILE", str(key_file))
    assert str(key_file) not in secret_values()


def test_the_key_source_is_reportable_without_the_key(tmp_path, monkeypatch):
    key_file = tmp_path / "api_key"
    key_file.write_text("sk-live-SECRET", encoding="utf-8")
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.setenv("MASSIVE_API_KEY_FILE", str(key_file))

    source = load(write_config(tmp_path)).api_key_source
    assert "MASSIVE_API_KEY_FILE" in source
    assert str(key_file) in source
    assert "SECRET" not in source.replace(str(key_file), "")


def test_no_key_anywhere_names_both_ways_to_set_it(tmp_path, monkeypatch):
    for name in ("MASSIVE_API_KEY", "POLYGON_API_KEY", "MASSIVE_API_KEY_FILE"):
        monkeypatch.delenv(name, raising=False)
    config = load(write_config(tmp_path))
    assert config.api_key_source == "not set"
    with pytest.raises(ConfigError) as caught:
        _ = config.api_key
    message = str(caught.value)
    assert "MASSIVE_API_KEY" in message
    assert "MASSIVE_API_KEY_FILE" in message
