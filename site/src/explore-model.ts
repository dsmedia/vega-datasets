/**
 * The Explore section's charts, as Vega-Lite specs (no DOM, so they are unit-tested):
 * a scatter plot of two measures picked with input bindings, and the starter chart
 * (starter.ts) for everything else — "Over time" when it is a time series.
 */
import { dsvFormat } from "d3-dsv";
import type { Dataset, Field } from "./catalog";
import { fieldRef, isMeasure, isYear, nominal, starterSpec } from "./starter";

type Spec = Record<string, unknown>;

export type Mode = "scatter" | "time" | "starter";
export const MODE_LABEL: Record<Mode, string> = { scatter: "Scatter", time: "Over time", starter: "Chart" };

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

/** The measures to start with: the first two, the first on y. */
export function defaultAxes(f: ScatterFields): { x: string; y: string } {
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
          { calculate: "toNumber(datum[xField])", as: "x" },
          { calculate: "toNumber(datum[yField])", as: "y" },
          { filter: "isValid(datum.x) && isValid(datum.y) && isFinite(datum.x) && isFinite(datum.y)" },
        ],
        params,
        mark: { type: "point", opacity: rows > 5000 ? 0.35 : 0.8 },
        encoding: {
          x: { field: "x", type: "quantitative", scale: { zero: false }, axis: { title: null } },
          y: { field: "y", type: "quantitative", scale: { zero: false }, axis: { title: null } },
          ...(f.color
            ? {
                color: { field: fieldRef(f.color.name), type: "nominal" },
                opacity: { condition: { param: "pick", empty: true, value: rows > 5000 ? 0.35 : 0.8 }, value: 0.08 },
              }
            : {}),
          tooltip: [
            ...(f.label ? [{ field: fieldRef(f.label.name), type: "nominal" }] : []),
            { field: "x", type: "quantitative", title: "x" },
            { field: "y", type: "quantitative", title: "y" },
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
