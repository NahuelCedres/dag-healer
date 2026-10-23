"""Deterministic synthetic data for the dag-healer example cases.

Writes the day-1 files (what the pipelines processed yesterday; ``dag-healer init`` infers the
contracts from them) and the day-2 files (today's drop, which makes each pipeline fail in its
own way):

==============  ===================  =========  ==============================================
file            pipeline             category   what changed on day 2
==============  ===================  =========  ==============================================
payments.csv    payments_settlement  DATA       a new provider sends decimal-comma amounts
                                                ("1.234,50") and some rows lose their currency
orders.csv      orders_enrichment    CODE       the order system now exports cancelled orders
                                                (items 0, total 0.00), valid under the contract
fx_rates.csv    fx_rates_publish     AMBIGUOUS  nothing: the data is fine, but the pricing
                                                service the job publishes to is down
==============  ===================  =========  ==============================================

Everything is synthetic: people come from short fixed name lists, e-mail addresses use the
reserved example.com/.org/.net domains and phone numbers the fictional 555-01xx range. The same
seed always writes byte-identical files.

Usage: python examples/synthetic/generate.py --day1 DIR --day2 DIR [--seed N]
"""

from __future__ import annotations

import argparse
import csv
import random
from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TypeVar

T = TypeVar("T")

SEED = 20261022
DAY1 = date(2026, 10, 21)
DAY2 = date(2026, 10, 22)

FIRST_NAMES = (
    "Ana", "Bruno", "Carla", "Diego", "Elena", "Felix", "Grace", "Hugo", "Ivy", "Jonas",
    "Kara", "Liam", "Maya", "Noah", "Olga", "Pablo", "Quinn", "Rosa", "Sam", "Tara",
)  # fmt: skip
LAST_NAMES = (
    "Acosta", "Becker", "Costa", "Dubois", "Evans", "Ferreira", "Garcia", "Hansen", "Ito", "Jensen",
    "Kowalski", "Lopez", "Moreau", "Nakamura", "Okafor", "Petrov", "Quintana", "Rossi", "Silva", "Tanaka",
)  # fmt: skip
DOMAINS = ("example.com", "example.org", "example.net")

PAYMENT_COLUMNS = (
    "payment_id", "order_id", "payer_name", "payer_email", "payer_phone",
    "amount", "currency", "method", "psp", "status", "created_at",
)  # fmt: skip
PAYMENT_CURRENCIES = (("USD", 55), ("EUR", 25), ("GBP", 12), ("BRL", 8))
PAYMENT_METHODS = (("card", 70), ("bank_transfer", 15), ("wallet", 15))
PAYMENT_PROVIDERS = (("alpha-pay", 60), ("beta-gateway", 40))
PAYMENT_STATUSES = (("captured", 93), ("refunded", 7))
NEW_PROVIDER = "euro-pay"  # day 2: amounts with a decimal comma

ORDER_COLUMNS = (
    "order_id", "customer_id", "customer_email", "items", "total", "currency", "status", "created_at",
)  # fmt: skip
ORDER_ITEMS = ((1, 30), (2, 25), (3, 15), (4, 10), (5, 8), (6, 5), (7, 4), (8, 3))
ORDER_CURRENCIES = (("USD", 60), ("EUR", 28), ("GBP", 12))
ORDER_STATUSES = (("completed", 85), ("shipped", 15))

FX_COLUMNS = ("rate_date", "base", "quote", "rate", "source")
FX_START = (
    ("EUR", 0.92), ("GBP", 0.79), ("BRL", 5.45), ("MXN", 18.2),
    ("JPY", 151.3), ("CAD", 1.37), ("CLP", 948.0), ("ARS", 985.0),
)  # fmt: skip
FX_WINDOW_DAYS = 30

Row = dict[str, object]


def _pick(rng: random.Random, weighted: Sequence[tuple[T, int]]) -> T:
    values, weights = zip(*weighted, strict=True)
    return rng.choices(values, weights=weights)[0]


def _person(rng: random.Random) -> tuple[str, str, str]:
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    email = f"{first}.{last}{rng.randint(1, 99)}@{rng.choice(DOMAINS)}".lower()
    phone = f"+1-555-01{rng.randint(0, 99):02d}"
    return f"{first} {last}", email, phone


def _timestamp(rng: random.Random, day: date) -> str:
    midnight = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return (midnight + timedelta(seconds=rng.randint(0, 86_399))).strftime("%Y-%m-%dT%H:%M:%SZ")


def decimal_comma(value: float) -> str:
    """``1234.5`` -> ``"1.234,50"``: how the new payment provider formats amounts."""
    whole, cents = f"{value:,.2f}".split(".")
    return f"{whole.replace(',', '.')},{cents}"


def payments(rng: random.Random, day: date, rows: int, first_id: int, *, drift: bool) -> list[Row]:
    """Payment provider exports. With ``drift``, every 8th row comes from the new provider
    (decimal-comma amounts) and every 47th row of the others has no currency."""
    out: list[Row] = []
    for n in range(first_id, first_id + rows):
        name, email, phone = _person(rng)
        amount = min(2500.0, max(1.0, round(rng.lognormvariate(3.6, 0.8), 2)))
        currency = _pick(rng, PAYMENT_CURRENCIES)
        provider = _pick(rng, PAYMENT_PROVIDERS)
        amount_text = f"{amount:.2f}"
        if drift and n % 8 == 3:
            provider, currency, amount_text = NEW_PROVIDER, "EUR", decimal_comma(amount)
        elif drift and n % 47 == 11:
            currency = ""
        out.append(
            {
                "payment_id": f"PAY-{n:06d}",
                "order_id": f"ORD-{rng.randint(100_000, 999_999)}",
                "payer_name": name,
                "payer_email": email,
                "payer_phone": phone,
                "amount": amount_text,
                "currency": currency,
                "method": _pick(rng, PAYMENT_METHODS),
                "psp": provider,
                "status": _pick(rng, PAYMENT_STATUSES),
                "created_at": _timestamp(rng, day),
            }
        )
    return out


def orders(rng: random.Random, day: date, rows: int, first_id: int, *, drift: bool) -> list[Row]:
    """Order system exports. With ``drift``, every 29th order is a cancelled one: its items are
    released, so ``items`` is 0 and ``total`` 0.00 (the contract allows both)."""
    out: list[Row] = []
    for n in range(first_id, first_id + rows):
        _, email, _ = _person(rng)
        items = _pick(rng, ORDER_ITEMS)
        total = round(sum(rng.uniform(4.99, 129.99) for _ in range(items)), 2)
        status = _pick(rng, ORDER_STATUSES)
        if drift and n % 29 == 7:
            items, total, status = 0, 0.0, "cancelled"
        out.append(
            {
                "order_id": f"ORD-2026-{n:06d}",
                "customer_id": f"CUS-{rng.randint(1, 4_000):05d}",
                "customer_email": email,
                "items": items,
                "total": f"{total:.2f}",
                "currency": _pick(rng, ORDER_CURRENCIES),
                "status": status,
                "created_at": _timestamp(rng, day),
            }
        )
    return out


def fx_history(rng: random.Random, first_day: date, days: int) -> dict[date, list[Row]]:
    """Daily rates (USD base) as a random walk of about 0.4% a day, keyed by day."""
    rates = dict(FX_START)
    history: dict[date, list[Row]] = {}
    for offset in range(days):
        day = first_day + timedelta(days=offset)
        history[day] = []
        for quote, _ in FX_START:
            rates[quote] *= 1 + rng.uniform(-0.004, 0.004)
            history[day].append(
                {
                    "rate_date": day.isoformat(),
                    "base": "USD",
                    "quote": quote,
                    "rate": f"{rates[quote]:.4f}",
                    "source": "central-bank-mirror",
                }
            )
    return history


def fx_window(history: dict[date, list[Row]], last_day: date) -> list[Row]:
    """The rolling 30-day window of ``history`` that ends on ``last_day``."""
    days = [last_day - timedelta(days=back) for back in range(FX_WINDOW_DAYS - 1, -1, -1)]
    return [row for day in days for row in history[day]]


def write_csv(path: Path, columns: Sequence[str], rows: list[Row]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return path


def generate(day1_dir: Path, day2_dir: Path, seed: int = SEED) -> list[Path]:
    """Write the day-1 and day-2 files of the three cases; returns the written paths."""
    rng = random.Random(seed)
    history = fx_history(rng, DAY1 - timedelta(days=FX_WINDOW_DAYS - 1), FX_WINDOW_DAYS + 1)
    return [
        write_csv(day1_dir / "payments.csv", PAYMENT_COLUMNS, payments(rng, DAY1, 600, 1, drift=False)),
        write_csv(day2_dir / "payments.csv", PAYMENT_COLUMNS, payments(rng, DAY2, 640, 601, drift=True)),
        write_csv(day1_dir / "orders.csv", ORDER_COLUMNS, orders(rng, DAY1, 500, 1, drift=False)),
        write_csv(day2_dir / "orders.csv", ORDER_COLUMNS, orders(rng, DAY2, 520, 501, drift=True)),
        write_csv(day1_dir / "fx_rates.csv", FX_COLUMNS, fx_window(history, DAY1)),
        write_csv(day2_dir / "fx_rates.csv", FX_COLUMNS, fx_window(history, DAY2)),
    ]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write the synthetic day-1 and day-2 files.")
    parser.add_argument("--day1", type=Path, required=True, help="directory for the day-1 files")
    parser.add_argument("--day2", type=Path, required=True, help="directory for the day-2 files")
    parser.add_argument("--seed", type=int, default=SEED, help=f"random seed (default {SEED})")
    args = parser.parse_args(argv)
    for path in generate(args.day1, args.day2, args.seed):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
