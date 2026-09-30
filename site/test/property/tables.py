"""
Random tables for the Explore rules' invariant tests (site/test/explore-invariants.test.ts).

Seeded, so a failure reproduces: each table is written as CSV, profiled by the real catalog
builder (``build_dataset``), and printed as JSON with its text, for the TypeScript test to
draw every chart the rules choose. The tables mix what has broken the rules before:

- categories with empty cells, integer codes, a value that is the sum of the others
  ("Total"), and a value that only happens to equal a sum;
- measures with negatives, zeros and duplicates, missing-value markers, and names such as
  ``_sum``, ``count[0]``, ``a.b``, ``deaths_per100k`` and ``Rate (%)``;
- time columns as ISO dates (some cells empty), datetimes with and without offsets, or
  integer years; panels (one row per time and series) and event logs.

Usage: uv run --group site python site/test/property/tables.py --seed 7 --count 200
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import random
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scripts.build_site_catalog import build_dataset

MEASURE_NAMES = [
    "value",
    "_sum",
    "count[0]",
    "a.b",
    "deaths_per100k",
    "Rate (%)",
    "population",
    "count",
    "people",
    "score",
]
CATEGORY_NAMES = ["region", "cause", "group.name", "sex", "code"]
URL = "https://cdn.jsdelivr.net/npm/vega-datasets@3/data/"


def time_values(rng: random.Random, kind: str, n: int) -> list[str]:
    if kind == "year":
        start = rng.randint(1950, 2000)
        return [str(start + i) for i in range(n)]
    if kind == "date":
        # Month ends and starts: a zone that shifts a date moves it to another month.
        days = ["01-31", "02-01", "03-31", "04-01", "06-30", "07-01"]
        return [f"{2019 + i // len(days)}-{days[i % len(days)]}" for i in range(n)]
    if kind == "datetime":
        return [f"2020-0{1 + i % 9}-1{i % 10} 0{i % 10}:30:00" for i in range(n)]
    return [
        f"2020-0{1 + i % 9}-28T23:30:00{rng.choice(['Z', '+09:00', '-05:00'])}"
        for i in range(n)
    ]


def times_of(rng: random.Random) -> tuple[str, list[str]]:
    """A time column's kind and values; "none" for a table without one."""
    kind = rng.choice(["year", "date", "datetime", "zoned", "none", "daily", "daily"])
    if kind == "daily":
        # Long enough (over 1,000 rows) for the chart to bucket its dates by month.
        start = date(2019 + rng.randint(0, 3), 1, 1)
        return "date", [
            (start + timedelta(days=i)).isoformat()
            for i in range(rng.randint(520, 800))
        ]
    return kind, time_values(rng, kind, rng.randint(3, 8)) if kind != "none" else []


def cell(rng: random.Random, marker: str | None) -> str:
    """A measure's cell: zeros, duplicates, negatives, decimals, sometimes a missing marker."""
    v = rng.choice([
        0,
        0,
        1,
        5,
        5,
        10,
        20,
        -3,
        rng.randint(-50, 500),
        round(rng.uniform(-1, 1000), 2),
    ])
    return marker if marker and rng.random() < 0.1 else str(v)


def totals(
    rows: list[dict[str, str]],
    times: list[str],
    cats: list[str],
    measures: list[str],
    code: str,
    marker: str | None,
) -> list[dict[str, str]]:
    """A total row per time: the sum of the others (as disasters' "All natural disasters")."""
    out = []
    for t in times:
        parts = [r for r in rows if r.get("when") == t and r[cats[0]] not in {"", code}]
        total = {"when": t, cats[0]: code, **({cats[1]: "a"} if len(cats) > 1 else {})}
        for m in measures:
            total[m] = str(sum(float(r[m]) for r in parts if r[m] not in {"", marker}))
        out.append(total)
    return out


def table(rng: random.Random) -> tuple[list[dict], list[dict[str, str]]]:
    """A random table: its schema fields and its rows as text, as a CSV file holds them."""
    kind, times = times_of(rng)
    cats = rng.sample(CATEGORY_NAMES, rng.randint(1, 2))
    measures = rng.sample(MEASURE_NAMES, rng.randint(1, 3))
    integer_cat = rng.random() < 0.3
    # Sometimes more series than the palette has colors (eleven to thirteen).
    width = rng.choice([rng.randint(2, 5), rng.randint(2, 5), rng.randint(11, 13)])
    series = (
        [str(i) for i in range(1, width + 1)]
        if integer_cat
        else [f"s{i:02}" for i in range(width)]
    )
    with_total = rng.random() < 0.3
    marker = "-99" if rng.random() < 0.3 else None
    panel = bool(times) and rng.random() < 0.7
    points = (
        [(t, s) for t in times for s in series]
        if panel
        else [
            (rng.choice(times) if times else "", rng.choice(series))
            for _ in range(rng.randint(8, 40))
        ]
    )
    rows: list[dict[str, str]] = []
    for t, s in points:
        row = (
            {"when": "" if kind == "date" and rng.random() < 0.05 else t}
            if times
            else {}
        )
        row[cats[0]] = "" if rng.random() < 0.05 else s
        if len(cats) > 1:
            row[cats[1]] = rng.choice(["a", "b", "c"])
        row.update({m: cell(rng, marker) for m in measures})
        rows.append(row)
    if with_total and panel:
        rows += totals(
            rows, times, cats, measures, "99" if integer_cat else "Total", marker
        )
    time_type = {"year": "integer", "date": "date"}.get(kind, "datetime")
    fields = [{"name": "when", "type": time_type}] if times else []
    fields.append({"name": cats[0], "type": "integer" if integer_cat else "string"})
    if len(cats) > 1:
        fields.append({"name": cats[1], "type": "string"})
    missing = {"missingValues": [marker, ""]} if marker else {}
    fields += [{"name": m, "type": "number", **missing} for m in measures]
    return fields, rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--count", type=int, default=200)
    args = parser.parse_args()
    rng = random.Random(args.seed)
    out = []
    with tempfile.TemporaryDirectory() as tmp:
        for i in range(args.count):
            fields, rows = table(rng)
            columns = [f["name"] for f in fields]
            text = io.StringIO()
            writer = csv.DictWriter(text, fieldnames=columns, lineterminator="\n")
            writer.writeheader()
            writer.writerows([{c: r.get(c, "") for c in columns} for r in rows])
            name = f"t{i:03}"
            path = Path(tmp) / f"{name}.csv"
            path.write_text(text.getvalue(), "utf-8")
            resource = {
                "name": name,
                "path": str(path),
                "format": ".csv",
                "type": "table",
                "bytes": path.stat().st_size,
                "schema": {"fields": fields},
            }
            entry = build_dataset(resource, [], f"{URL}{name}.csv", Path(tmp))
            out.append({"dataset": entry, "csv": text.getvalue()})
    json.dump(out, sys.stdout)


if __name__ == "__main__":
    main()
