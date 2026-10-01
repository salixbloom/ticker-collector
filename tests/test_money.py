from __future__ import annotations

from decimal import Decimal

import pytest

from collector.money import (
    DECIMALS,
    SCALE,
    PriceConversionError,
    column_to_scaled,
    format_scaled,
    from_scaled,
    to_scaled,
    to_scaled_checked,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("123.4567", 123_456_700),
        (123.4567, 123_456_700),
        (Decimal("123.4567"), 123_456_700),
        ("0.000001", 1),
        ("0.0011", 1_100),
        (0, 0),
        (None, None),
        # Real values pulled from the provider on 2026-09-29.
        (337.8049, 337_804_900),
        (7491.363143, 7_491_363_143),
        (85227.9653, 85_227_965_300),
        (256569048.870547, 256_569_048_870_547),
        (1000000.0, 1_000_000_000_000),
    ],
)
def test_to_scaled(value, expected):
    assert to_scaled(value) == expected


def test_scale_is_six_decimals():
    """Measured against real data: 4 decimals silently rounds 276 prices a day."""
    assert DECIMALS == 6
    assert SCALE == 1_000_000


def test_fractional_volume_is_representable_exactly():
    """The provider reports fractional share volume, so volume cannot be a count."""
    assert from_scaled(to_scaled(85227.9653)) == Decimal("85227.9653")
    assert from_scaled(to_scaled(7491.363143)) == Decimal("7491.363143")


def test_largest_observed_values_stay_far_inside_int64():
    biggest = to_scaled(256569048.870547)  # largest EOD volume observed
    assert biggest == 256_569_048_870_547
    assert biggest < 2**63 - 1
    # Four and a half orders of magnitude of headroom.
    assert (2**63 - 1) / biggest > 10_000


def test_float_uses_shortest_repr_not_binary_expansion():
    # Decimal(0.1) is 0.1000000000000000055511151231257827; the source meant "0.1".
    assert to_scaled(0.1) == 100_000
    assert to_scaled(2.675) == 2_675_000  # binary expansion is 2.67499...


def test_rounding_is_half_even_and_stable():
    assert to_scaled("1.0000005") == 1_000_000  # ties to even
    assert to_scaled("1.0000015") == 1_000_002  # ties to even
    # Repeated conversion is a fixed point, which is what idempotency needs.
    once = to_scaled("99.12345678")
    assert to_scaled(from_scaled(once)) == once


def test_round_trip_is_exact_within_the_scale():
    for text in ("0.000001", "7.5", "254.1234", "337.804900", "99999.999999"):
        assert from_scaled(to_scaled(text)) == Decimal(text)


# --------------------------------------------------------------------------- #
# Precision loss is reported, not hidden
# --------------------------------------------------------------------------- #


def test_values_inside_the_scale_are_not_flagged():
    scaled, rounded = to_scaled_checked("337.804900")
    assert (scaled, rounded) == (337_804_900, False)


def test_a_value_needing_more_precision_is_flagged():
    """Published rounded, but recorded as a soft flag -- never silently lost."""
    scaled, rounded = to_scaled_checked("0.0000005678")
    assert rounded is True
    assert scaled == 1  # half-even on the 7th decimal


def test_none_is_never_flagged():
    assert to_scaled_checked(None) == (None, False)


def test_column_to_scaled_counts_the_rounded_values():
    values, rounded = column_to_scaled(["1.5", "2.0000001", None, "3.25", "4.0000009"])
    assert values == [1_500_000, 2_000_000, None, 3_250_000, 4_000_001]
    assert rounded == 2


def test_column_to_scaled_on_clean_data_reports_nothing():
    values, rounded = column_to_scaled([337.8049, 7491.363143, None])
    assert rounded == 0
    assert values == [337_804_900, 7_491_363_143, None]


# --------------------------------------------------------------------------- #
# Rejections
# --------------------------------------------------------------------------- #


def test_format_scaled():
    assert format_scaled(337_804_900) == "337.804900"
    assert format_scaled(337_804_900, places=4) == "337.8049"
    assert format_scaled(None) == ""


@pytest.mark.parametrize("value", ["", "abc", float("nan"), float("inf"), object()])
def test_rejects_non_numbers(value):
    with pytest.raises(PriceConversionError):
        to_scaled(value)


def test_rejects_a_value_too_large_for_int64():
    with pytest.raises(PriceConversionError, match="int64"):
        to_scaled("1e14")
