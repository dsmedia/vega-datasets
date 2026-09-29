"""Offline tests for the Field Guide catalog builder (scripts/build_site_catalog.py)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from io import BytesIO
from typing import TYPE_CHECKING

import polars as pl
from PIL import Image

from scripts.build_site_catalog import (
    HIST_BINS,
    data_url,
    editor_spec_url,
    example_slug,
    parse_dates,
    preview_rows,
    profile_field,
    read_table,
    readme_markdown,
    thumbnail_urls,
    write_thumbnail,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_example_slug() -> None:
    assert (
        example_slug("https://vega.github.io/vega/examples/bar-chart/") == "bar-chart"
    )
    assert (
        example_slug("https://altair-viz.github.io/gallery/area_chart.html")
        == "area_chart"
    )
    assert example_slug("https://vega.github.io/vega-lite/examples/bar.html") == "bar"


def test_thumbnail_urls_follow_the_registry_commit() -> None:
    vl = "https://cdn.jsdelivr.net/gh/vega/vega-lite@abc123/examples/specs/bar.vl.json"
    assert thumbnail_urls("vega-lite", "bar", vl) == [
        "https://cdn.jsdelivr.net/gh/vega/vega-lite@abc123/examples/compiled/bar.png"
    ]
    vg = "https://cdn.jsdelivr.net/gh/vega/vega@def456/docs/examples/bar-chart.vg.json"
    assert thumbnail_urls("vega", "bar-chart", vg) == [
        "https://cdn.jsdelivr.net/gh/vega/vega@def456/docs/examples/img/bar-chart.png"
    ]
    py = "https://cdn.jsdelivr.net/gh/vega/altair@0a1b2c/tests/examples_arguments_syntax/x.py"
    assert thumbnail_urls("altair", "x", py) == [
        "https://altair-viz.github.io/_static/x-thumb.png",
        "https://altair-viz.github.io/_static/x-thumb.svg",
    ]


def test_editor_spec_url() -> None:
    assert editor_spec_url("vega", "bar-chart") == (
        "https://vega.github.io/editor/spec/vega/bar-chart.vg.json"
    )
    assert editor_spec_url("vega-lite", "bar") == (
        "https://vega.github.io/editor/spec/vega-lite/bar.vl.json"
    )
    assert editor_spec_url("altair", "bar") is None


def test_data_url_prefers_the_released_cdn_file() -> None:
    released = {"cars.json"}
    assert data_url("cars.json", "3", released) == (
        "https://cdn.jsdelivr.net/npm/vega-datasets@3/data/cars.json"
    )
    assert data_url("new.json", "3", released) == (
        "https://vega.github.io/vega-datasets/data/new.json"
    )


def test_profile_quantitative() -> None:
    s = pl.Series("x", ["1", "2", "2", "10", None, "n/a"])
    p = profile_field(s, "number")
    assert p["kind"] == "quantitative"
    assert (p["min"], p["max"], p["missing"]) == (1.0, 10.0, 2)
    assert len(p["bins"]) == HIST_BINS
    assert sum(p["bins"]) == 4
    assert p["bins"][0] == 1
    assert p["bins"][-1] == 1


def test_profile_constant_and_empty_numbers() -> None:
    assert profile_field(pl.Series("x", ["5", "5"]), "integer")["bins"] == [2]
    assert profile_field(pl.Series("x", [None, "?"]), "number") == {
        "kind": "empty",
        "missing": 2,
    }


def test_profile_temporal_mixed_formats() -> None:
    s = pl.Series("d", ["2020-01-01", "2020-06-30", None])
    p = profile_field(s, "date")
    assert p["kind"] == "temporal"
    assert p["min"].startswith("2020-01-01")
    assert p["max"].startswith("2020-06-30")
    assert p["missing"] == 1
    assert sum(p["bins"]) == 2


def test_parse_dates_picks_the_best_format() -> None:
    parsed = parse_dates(pl.Series("d", ["Jan 05 2001", "Feb 11 2002", "Mar 30 2003"]))
    assert parsed.null_count() == 0


def test_profile_nominal() -> None:
    s = pl.Series("c", ["a", "b", "a", "", None, "c"])
    p = profile_field(s, "string")
    assert (p["kind"], p["distinct"], p["missing"]) == ("nominal", 3, 2)
    assert p["top"][0] == ["a", 2]
    assert sorted(p["top"][1:]) == [["b", 1], ["c", 1]]


def test_read_table_json(tmp_path: Path) -> None:
    rows = tmp_path / "rows.json"
    rows.write_text(json.dumps([{"a": 1, "b": [1, 2]}, {"a": 2, "c": "x"}]), "utf-8")
    df = read_table(rows, "json")
    assert df is not None
    assert df.columns == ["a", "b", "c"]
    assert df["b"].to_list() == ["[1, 2]", None]
    other = tmp_path / "tree.json"
    other.write_text(json.dumps({"name": "root"}), "utf-8")
    assert read_table(other, "json") is None


def test_preview_rows_truncates_long_cells() -> None:
    df = pl.DataFrame({"a": ["x" * 100], "b": [None]})
    [[a, b]] = preview_rows(df, ["a", "b"])
    assert len(a) == 48
    assert a.endswith("…")
    assert not b


def test_write_thumbnail_png(tmp_path: Path) -> None:
    buf = BytesIO()
    Image.new("RGBA", (960, 480), (0, 0, 0, 0)).save(buf, "PNG")
    name, size = write_thumbnail(buf.getvalue(), ".png", tmp_path / "thumbs" / "bar")
    assert name == "bar.webp"
    assert size == [480, 240]
    with Image.open(tmp_path / "thumbs" / name) as im:
        # Transparent pixels are flattened onto white, so thumbnails read on dark cards.
        assert im.convert("RGB").getpixel((0, 0)) == (255, 255, 255)


def test_write_thumbnail_svg_is_kept(tmp_path: Path) -> None:
    svg = b'<svg xmlns="http://www.w3.org/2000/svg" width="1" height="1"/>'
    name, size = write_thumbnail(svg, ".svg", tmp_path / "emoji")
    assert (name, size) == ("emoji.svg", None)
    assert (tmp_path / name).read_bytes() == svg


def test_profile_nominal_ties_are_ordered_by_value() -> None:
    # A column named "count" must not collide with the tally column.
    p = profile_field(pl.Series("count", ["b", "a", "c", "b", "a"]), "string")
    assert p["top"] == [["a", 2], ["b", 2], ["c", 1]]


def test_profile_native_datetimes() -> None:
    s = pl.Series("d", ["2020-01-01", "2021-01-01"]).str.to_datetime(time_zone="UTC")
    p = profile_field(s, "datetime")
    assert p["kind"] == "temporal"
    assert p["min"].startswith("2020-01-01")


def test_profile_temporal_names_its_zone() -> None:
    # Browsers read a date-time without a zone as local time: a day early east of UTC.
    p = profile_field(pl.Series("d", ["2012-01-01", "2015-12-31"]), "date")
    assert (p["min"], p["max"]) == ("2012-01-01T00:00:00Z", "2015-12-31T00:00:00Z")
    aware = pl.Series("d", ["2000-01-01T08:00:00"]).str.to_datetime(time_zone="UTC")
    value = profile_field(aware, "datetime")["min"]
    assert value.endswith(("Z", "+00:00"))
    assert datetime.fromisoformat(value) == datetime(2000, 1, 1, 8, tzinfo=UTC)


def test_readme_markdown() -> None:
    lines = [
        "# Vega Datasets",
        "[![npm](https://img.shields.io/npm/v/vega-datasets.svg)](https://npmjs.com)",
        "Intro with [cars](datapackage.md#carsjson) and [rules](CONTRIBUTING.md).",
        "Browse the [Field Guide](https://vega.github.io/vega-datasets/).",
        "> [!IMPORTANT]",
        "> **Licensing**: see [the metadata](datapackage.md).",
        "Unknown anchor: [x](datapackage.md#nopejson), external [y](https://x.org/a.md).",
        "```js",
        "const [a](b) = 1;",
        "```",
    ]
    out = readme_markdown("\n".join(lines), {"cars.json": "cars"})
    assert out.splitlines() == [
        "Intro with [cars](datasets/cars/) and [rules](https://github.com/vega/vega-datasets/blob/main/CONTRIBUTING.md).",
        "Browse the [Field Guide](./).",
        "> **Licensing**: see [the metadata](https://github.com/vega/vega-datasets/blob/main/datapackage.md).",
        "Unknown anchor: [x](https://github.com/vega/vega-datasets/blob/main/datapackage.md#nopejson), external [y](https://x.org/a.md).",
        "```js",
        "const [a](b) = 1;",
        "```",
    ]
