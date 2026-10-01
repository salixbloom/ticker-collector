"""Scaled-integer quantities.

Prices and volumes are stored as int64 in units of 1/1,000,000 (337.8049 ->
337804900). Conversion goes through ``Decimal`` rather than float arithmetic so
that the same input always produces the same integer: checksums and the
idempotency guarantee both depend on bit-identical output across runs.

**Why six decimals, not the four the plan first specified.** Measured against a
full grouped-daily response (2026-09-29, 12,605 tickers): 276 of 62,235 price
values carry 5 or 6 decimal places, and volume is fractional in 92% of rows with
up to 6 decimals. A 1/10,000 scale would silently round 276 prices and 8,381
volumes every day, which is the same class of error the plan rejects ``float32``
for. At 1/1,000,000 nothing in that sample rounds, and the largest resulting
integer is 2.57e14 against int64's 9.22e18 ceiling -- four and a half orders of
magnitude of headroom.

**Why volume is scaled too.** The provider reports fractional share volume
(``"v": 85227.9653``), so an integer share count cannot represent it.
"""

from __future__ import annotations

from collections.abc import Iterable
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation

DECIMALS = 6
SCALE = 10**DECIMALS
"""Multiplier for prices and volumes."""

_SCALE_DEC = Decimal(SCALE)
_QUANTUM = Decimal(1).scaleb(-DECIMALS)

#: int64 bounds, so a nonsense input is caught here rather than by Arrow.
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


class PriceConversionError(ValueError):
    """Raised when a value cannot be represented as a scaled integer."""


def _to_decimal(value: float | int | str | Decimal) -> Decimal:
    """Parse a quantity to ``Decimal``.

    Floats are routed via ``repr`` so the shortest decimal that round-trips to
    the float is used: ``Decimal(0.1)`` is 0.1000000000000000055511..., whereas
    ``Decimal("0.1")`` is exactly 0.1, which is what the source meant.
    """
    try:
        dec = Decimal(repr(value)) if isinstance(value, float) else Decimal(value)
    except (InvalidOperation, TypeError) as exc:
        raise PriceConversionError(f"not a number: {value!r}") from exc
    if not dec.is_finite():
        raise PriceConversionError(f"not a finite number: {value!r}")
    return dec


def to_scaled(value: float | int | str | Decimal | None) -> int | None:
    """Convert a quantity to scaled integer units. ``None`` passes through."""
    scaled, _ = to_scaled_checked(value)
    return scaled


def to_scaled_checked(
    value: float | int | str | Decimal | None,
) -> tuple[int | None, bool]:
    """Convert, and report whether precision was lost.

    Returns ``(scaled, rounded)``. Callers surface ``rounded`` as a soft flag
    naming the affected ticker and field, so a provider that one day widens its
    precision shows up in ``status.json`` instead of degrading data silently.
    """
    if value is None:
        return None, False
    dec = _to_decimal(value)
    quantized = dec.quantize(_QUANTUM, rounding=ROUND_HALF_EVEN)
    scaled = int((quantized * _SCALE_DEC).to_integral_exact())
    if not _INT64_MIN <= scaled <= _INT64_MAX:
        raise PriceConversionError(f"{value!r} does not fit in int64 at scale 1e{DECIMALS}")
    return scaled, quantized != dec


def from_scaled(value: int | None) -> Decimal | None:
    """Convert scaled integer units back to a ``Decimal``."""
    if value is None:
        return None
    return Decimal(value) / _SCALE_DEC


def format_scaled(value: int | None, places: int = DECIMALS) -> str:
    """Render a scaled quantity for logs and reports, e.g. ``337.804900``."""
    if value is None:
        return ""
    return f"{from_scaled(value):.{places}f}"


def column_to_scaled(
    values: Iterable[float | int | str | Decimal | None],
) -> tuple[list[int | None], int]:
    """Convert a whole column, returning the values and how many were rounded."""
    out: list[int | None] = []
    rounded = 0
    for value in values:
        scaled, was_rounded = to_scaled_checked(value)
        out.append(scaled)
        rounded += was_rounded
    return out, rounded
