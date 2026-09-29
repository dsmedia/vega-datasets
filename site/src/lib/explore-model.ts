/**
 * The Explore section's charts, as Vega-Lite specs (no DOM, so they are unit-tested):
 * a scatter plot of two measures picked with input bindings, and the starter chart
 * (starter.ts) for everything else — "Over Time" when it is a time series.
 */
import { dsvFormat } from "d3-dsv";
import type { Dataset, Field } from "./catalog";
import { formatCount } from "./format";
import { fieldRef, isMeasure, isYear, nominal, starterSpec } from "./starter";

type Spec = Record<string, unknown>;

export type Mode = "scatter" | "time" | "starter";
export const MODE_LABEL: Record<Mode, string> = { scatter: "Scatter", time: "Over Time", starter: "Chart" };

const SCHEMA = "https://vega.github.io/schema/vega-lite/v6.json";

export interface ScatterFields {
  measures: Field[];
  /** Colored, and isolated from the legend: a category with at most 10 values. */
  color: Field | undefined;
  /** Names the point in the tooltip: a category with many values (a car's name). */
  label: Field | undefined;
  /** A date or year column, shown in the tooltip. */
  time: Field | undefined;
}

export function scatterFields(d: Dataset): ScatterFields | null {
  if (d.kind !== "table" || d.format === "parquet" || d.format === "arrow") return null;
  const fields = d.fields.filter((f) => f.type !== "array");
  const measures = fields.filter(isMeasure);
  if (measures.length < 2) return null;
  return {
    measures,
    color: fields.find((f) => nominal(f, 10)),
    label: fields.find((f) => f.profile.kind === "nominal" && f.profile.distinct > 10),
    time: fields.find((f) => f.profile.kind === "temporal" || isYear(f)),
  };
}

/** A starter chart that is a line over dates or years. */
function isTimeSeries(spec: Spec | null): boolean {
  const mark = spec?.mark as { type?: string } | undefined;
  return mark?.type === "line";
}

/** The charts Explore offers for a dataset, in switch order; empty when none is possible. */
export function exploreModes(d: Dataset): Mode[] {
  const starter = starterSpec(d);
  // Maps (geographic files, latitude/longitude columns) show the map.
  if (starter?.projection) return ["starter"];
  if (scatterFields(d)) return isTimeSeries(starter) ? ["scatter", "time"] : ["scatter"];
  return starter ? ["starter"] : [];
}

export interface ScatterOptions {
  x: string;
  y: string;
  /** Scroll to zoom and drag to pan (not on phones, where it would trap page scrolling). */
  zoom: boolean;
  height: number;
}

/** The measures to start with: fields named x and y when there are both, else the first two, the first on y. */
export function defaultAxes(f: ScatterFields): { x: string; y: string } {
  const named = (axis: string) => f.measures.find((m) => m.name.toLowerCase() === axis);
  const [x, y] = [named("x"), named("y")];
  if (x && y) return { x: x.name, y: y.name };
  return { x: f.measures[1]!.name, y: f.measures[0]!.name };
}

/**
 * Two measures against each other. The x and y pickers are input bindings on the
 * xField and yField params, so they work the same in the Vega Editor; the axis titles
 * are text marks that read those params (an axis title can't).
 */
export function scatterSpec(d: Dataset, f: ScatterFields, o: ScatterOptions): Spec {
  const options = f.measures.map((m) => m.name);
  const title = (param: string, place: Spec) => ({
    data: { values: [{}] },
    // Zoom (scale binding) clips every mark in the view; the titles sit outside the plot.
    mark: { type: "text", text: { expr: param }, fontWeight: "bold", fontSize: 11, clip: false, ...place },
  });
  const rows = d.rows ?? 0;
  const params: Spec[] = [];
  if (o.zoom) params.push({ name: "zoom", select: "interval", bind: "scales" });
  if (f.color) params.push({ name: "pick", select: { type: "point", fields: [f.color.name] }, bind: "legend" });
  // The plotted values need names no field of the file has: a calculate `as: "x"` would overwrite a
  // field called x (platformer_terrain) before the next calculate reads it.
  const taken = new Set(d.fields.map((m) => m.name));
  const free = (name: string): string => (taken.has(name) ? free(`_${name}`) : name);
  const [px, py] = [free("x"), free("y")];
  return {
    $schema: SCHEMA,
    description: `Two measures of ${d.name} from vega-datasets, picked with the x and y menus.`,
    width: "container",
    height: o.height,
    autosize: { type: "fit-x", contains: "padding" },
    data: { url: d.url },
    params: [
      { name: "xField", value: o.x, bind: { input: "select", options, name: "x " } },
      { name: "yField", value: o.y, bind: { input: "select", options, name: "y " } },
    ],
    layer: [
      {
        // The field is picked at run time, so Vega-Lite can't parse it up front (CSV values are strings).
        transform: [
          { calculate: "toNumber(datum[xField])", as: px },
          { calculate: "toNumber(datum[yField])", as: py },
          { filter: `isValid(datum.${px}) && isValid(datum.${py}) && isFinite(datum.${px}) && isFinite(datum.${py})` },
        ],
        params,
        mark: { type: "point", opacity: rows > 5000 ? 0.35 : 0.8 },
        encoding: {
          x: { field: px, type: "quantitative", scale: { zero: false }, axis: { title: null } },
          y: { field: py, type: "quantitative", scale: { zero: false }, axis: { title: null } },
          ...(f.color
            ? {
                color: { field: fieldRef(f.color.name), type: "nominal" },
                opacity: { condition: { param: "pick", empty: true, value: rows > 5000 ? 0.35 : 0.8 }, value: 0.08 },
              }
            : {}),
          tooltip: [
            ...(f.label ? [{ field: fieldRef(f.label.name), type: "nominal" }] : []),
            { field: px, type: "quantitative", title: "x" },
            { field: py, type: "quantitative", title: "y" },
            ...(f.color ? [{ field: fieldRef(f.color.name), type: "nominal" }] : []),
            ...(f.time
              ? [{ field: fieldRef(f.time.name), type: f.time.profile.kind === "temporal" ? "temporal" : "quantitative", ...(isYear(f.time) ? { format: "d" } : {}) }]
              : []),
          ],
        },
      },
      title("xField", { x: { expr: "width / 2" }, y: { expr: "height + 32" }, align: "center", baseline: "top" }),
      title("yField", { x: 0, y: -10, align: "left", baseline: "bottom" }),
    ],
  };
}

/** Tables longer than this open on a density overview, binned when the site is built. */
export const DENSITY_ROWS = 20_000;
/** The overview's resolution: at most this many bins across and up. */
export const DENSITY_BINS = { x: 60, y: 40 };

/** Does this dataset open on the density overview (a long table with a scatter plot)? */
export function hasDensity(d: Dataset): boolean {
  return (d.rows ?? 0) > DENSITY_ROWS && scatterFields(d) !== null;
}

/** One bin of the overview: its ranges on x and y and how many rows fall in it. */
export interface DensityBin {
  x0: number;
  x1: number;
  y0: number;
  y1: number;
  count: number;
}

const DENSITY_COLOR = { type: "symlog", scheme: "blues" };

/**
 * How the rows spread over two measures: a 2D histogram (rect marks, rows per bin).
 * This is the spec the Editor opens: it loads the whole file and bins it there.
 */
export function densitySpec(d: Dataset, axes: { x: string; y: string }, height: number): Spec {
  const x = { field: fieldRef(axes.x), type: "quantitative", bin: { maxbins: DENSITY_BINS.x } };
  const y = { field: fieldRef(axes.y), type: "quantitative", bin: { maxbins: DENSITY_BINS.y } };
  return {
    $schema: SCHEMA,
    description: `How the rows of ${d.name} spread over ${axes.x} and ${axes.y}: rows per bin.`,
    width: "container",
    height,
    autosize: { type: "fit-x", contains: "padding" },
    data: { url: d.url },
    mark: "rect",
    encoding: {
      x,
      y,
      color: { aggregate: "count", type: "quantitative", title: "Rows", scale: DENSITY_COLOR },
      tooltip: [x, y, { aggregate: "count", type: "quantitative", title: "Rows", format: "," }],
    },
  };
}

/** The same overview drawn from bins computed when the site was built (no file to load). */
export function densityFromBins(d: Dataset, axes: { x: string; y: string }, height: number, bins: DensityBin[]): Spec {
  const range = (lo: string, hi: string) => `format(datum.${lo}, ",") + " – " + format(datum.${hi}, ",")`;
  return {
    ...densitySpec(d, axes, height),
    description: `How the ${bins.reduce((s, b) => s + b.count, 0).toLocaleString("en-US")} rows of ${d.name} spread over ${axes.x} and ${axes.y}: rows per bin, binned when the site was built.`,
    data: { values: bins },
    transform: [
      { calculate: range("x0", "x1"), as: "x range" },
      { calculate: range("y0", "y1"), as: "y range" },
    ],
    encoding: {
      x: { field: "x0", type: "quantitative", bin: { binned: true }, title: axes.x },
      x2: { field: "x1" },
      y: { field: "y0", type: "quantitative", bin: { binned: true }, title: axes.y },
      y2: { field: "y1" },
      color: { field: "count", type: "quantitative", title: "Rows", scale: DENSITY_COLOR },
      tooltip: [
        { field: "x range", title: axes.x },
        { field: "y range", title: axes.y },
        { field: "count", type: "quantitative", title: "Rows", format: "," },
      ],
    },
  };
}

/** Maps with this many bytes or more open on a picture drawn when the site was built. */
export const MAP_PREVIEW_BYTES = 500_000;

/** Is the starter chart a map heavy enough to open on its preview (us_10m's 3,641 counties, say)? */
export function hasMapPreview(d: Dataset): boolean {
  return (d.bytes ?? 0) >= MAP_PREVIEW_BYTES && Boolean(starterSpec(d)?.projection);
}

/** The starter chart, sized to its column. */
export function starterChart(d: Dataset): Spec | null {
  const spec = starterSpec(d);
  if (!spec) return null;
  return { ...spec, width: "container", autosize: { type: "fit-x", contains: "padding" } };
}

/**
 * The page loads data from its own origin (its CSP allows nothing else); the Editor
 * link keeps the public URL.
 */
export function withDataUrl(spec: Spec, url: string): Spec {
  const data = spec.data as { url?: string } | undefined;
  return data?.url ? { ...spec, data: { ...data, url } } : spec;
}

/** The same chart drawing rows the page has already read (Vega-Lite parses their strings). */
export function withValues(spec: Spec, values: Record<string, unknown>[]): Spec {
  return { ...spec, data: { values } };
}

export function siteDataUrl(d: Dataset): string {
  return `data/${d.file}`;
}

/** Tables the page reads itself (see parseTable); maps and other files load by URL. */
export function readsRows(d: Dataset): boolean {
  return d.kind === "table" && (d.format === "csv" || d.format === "tsv" || d.format === "json");
}

/**
 * Rows of a CSV, TSV or JSON table, as Vega would read them but without `new Function`:
 * d3-dsv's csvParse (which Vega's CSV reader uses) compiles its row converter, and the
 * page's CSP forbids that. `parseRows` doesn't, so objects are built here.
 */
export function parseTable(text: string, format: string): Record<string, unknown>[] {
  if (format === "json") return JSON.parse(text) as Record<string, unknown>[];
  const [columns = [], ...rows] = dsvFormat(format === "tsv" ? "\t" : ",").parseRows(text);
  return rows.map((r) => Object.fromEntries(columns.map((c, i) => [c, r[i] ?? ""])));
}

/** Rows where both fields hold numbers. */
function countBoth(rows: Record<string, unknown>[], x: string, y: string): number {
  const ok = (v: unknown) => v !== null && v !== "" && Number.isFinite(Number(v));
  return rows.filter((r) => ok(r[x]) && ok(r[y])).length;
}

/**
 * "Both fields have values in 398 of 406 rows." for the scatter caption, from the rows already read
 * to draw the chart; null until then, so the caption never downloads a file on its own
 * (a large file waits for its Draw Chart button).
 */
export function bothValuesNote(d: Dataset, rows: Record<string, unknown>[] | null, x: string, y: string): string | null {
  if (!rows || d.rows === null) return null;
  return `Both fields have values in ${formatCount(countBoth(rows, x, y))} of ${formatCount(d.rows)} rows.`;
}

/** The Vega-Lite features a spec uses, for the line under the chart. */
export function chartFeatures(spec: Spec): string[] {
  const out = ["Vega-Lite"];
  const text = JSON.stringify(spec);
  const has = (s: string) => text.includes(s);
  if (has('"bind":{"input"')) out.push("input binding");
  if (has('"bind":"scales"')) out.push("scale binding");
  if (has('"bind":"legend"')) out.push("legend binding");
  if (out.length > 1) return out;
  const mark = spec.mark as { type?: string } | string | undefined;
  const type = typeof mark === "string" ? mark : mark?.type;
  if (type) out.push(`${type} mark`);
  const projection = spec.projection as { type?: string } | undefined;
  if (projection?.type) out.push(`${projection.type} projection`);
  if (has('"timeUnit"')) out.push("time unit");
  for (const agg of ["mean", "sum", "count"]) if (has(`"aggregate":"${agg}"`)) out.push(`${agg} aggregate`);
  if (has('"bin"')) out.push("binning");
  if (has('"tooltip":true')) out.push("tooltip");
  return out;
}
