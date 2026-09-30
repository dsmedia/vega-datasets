/**
 * The Explore section's charts, as Vega-Lite specs (no DOM, so they are unit-tested):
 * a scatter plot of two measures picked with input bindings, and the starter chart
 * (starter.ts) for everything else — "Over Time" when it is a time series, "Small
 * Multiples" when it is one panel per group.
 */
import { type Dataset, documentedRange, effectiveMissing, type Field, fieldTitle } from "./catalog";
import { correlation, distinctValues, sampled, type ScaleType, scaleFor, scaleType, summable, totalsOf, withScale } from "./chart-rules";
import { BAND_POLICY, rowBand } from "./large-data";
import { formatCount } from "./format";
import { category, categoryAsText, colorable, defaultPair, fieldRef, isMeasure, isYear, markerForms, missingFilter, PANEL_SIZE, starterSpec, tag, tagged, timeField, timeKeyOf, timeYear, titled, TOP, untag } from "./starter";

type Spec = Record<string, unknown>;

export type Mode = "scatter" | "time" | "panels" | "starter";
export const MODE_LABEL: Record<Mode, string> = { scatter: "Scatter", time: "Over Time", panels: "Small Multiples", starter: "Chart" };

const SCHEMA = "https://vega.github.io/schema/vega-lite/v6.json";

export interface ScatterFields {
  measures: Field[];
  /** Colored, and isolated from the legend: an informative category with at most 10 values (G-5). */
  color: Field | undefined;
  /** Names the point in the tooltip: a category with many values (a car's name). */
  label: Field | undefined;
  /** A date or year column, shown in the tooltip. */
  time: Field | undefined;
}

export function scatterFields(d: Dataset): ScatterFields | null {
  if (d.kind !== "table" || d.format === "parquet" || d.format === "arrow") return null;
  const fields = d.fields.filter((f) => f.type !== "array");
  const measures = fields.filter((f) => isMeasure(f, fields));
  // No scatter plot when every pair is a near-duplicate (G-3): it would draw one line.
  if (measures.length < 2 || !defaultPair(d, measures)) return null;
  return {
    measures,
    color: fields.find((f) => colorable(f, 10, fields, d.rows ?? 0)),
    label: fields.find((f) => f.profile.kind === "nominal" && f.profile.distinct > 10),
    time: fields.find((f) => f.profile.kind === "temporal" || isYear(f)),
  };
}

/** A starter chart that is a line over dates or years. */
function isTimeSeries(spec: Spec | null): boolean {
  const mark = spec?.mark as { type?: string } | undefined;
  return mark?.type === "line";
}

/**
 * Does "Over Time" open first (G-1)? When the time field is the table's axis: evenly
 * sampled (days, months, years, not the times of events) and indexing the rows, alone or
 * with the series the line keeps apart. Then a scatter of two of its measures mostly plots
 * their shared trend against itself (high against open, CO2 against adjusted CO2). The
 * scatter stays first when a category outside that key can color it (seattle_weather's
 * weather type: the scatter shows how the measures separate by it), unless its opening
 * pair moves together (|r| ≥ 0.9: the shared trend again, whatever the colors); and when
 * the line would average over more entities than it can tell apart (countries), unless it
 * counts things, which add up to a total.
 */
export function timeFirst(d: Dataset, f: ScatterFields): boolean {
  const fields = d.fields.filter((x) => x.type !== "array");
  const rows = d.rows ?? 0;
  const t = timeField(fields);
  if (!t || !sampled(t)) return false;
  const key = timeKeyOf(d, t);
  if (key === null) return false;
  const pair = defaultPair(d, f.measures)!;
  const trend = Math.abs(correlation(d, pair.x.name, pair.y.name) ?? 0) >= 0.9;
  if (!trend && fields.some((c) => !key.includes(c) && colorable(c, 10, fields, rows))) return false;
  return key.every((g) => colorable(g, 12, fields, rows) || timeYear(g)) || summable(f.measures[0]!);
}

/** The charts Explore offers for a dataset, in switch order; empty when none is possible. */
export function exploreModes(d: Dataset): Mode[] {
  const starter = starterSpec(d);
  // Maps (geographic files, latitude/longitude columns) show the map.
  if (starter?.projection) return ["starter"];
  const scatter = scatterFields(d);
  if (starter?.facet) return scatter ? ["panels", "scatter"] : ["panels"];
  // A table long enough for the density overview opens on it, and the overview is of the scatter plot.
  const overview = rowBand(d.rows ?? 0) === "density";
  if (scatter) return isTimeSeries(starter) ? (!overview && timeFirst(d, scatter) ? ["time", "scatter"] : ["scatter", "time"]) : ["scatter"];
  return starter ? ["starter"] : [];
}

export interface ScatterOptions {
  x: string;
  y: string;
  /** Scroll to zoom and drag to pan (not on phones, where it would trap page scrolling). */
  zoom: boolean;
  height: number;
}

/** The measures to start with (starter.ts `defaultPair`: named axes, no near-duplicates, a log axis on x). */
export function defaultAxes(d: Dataset, f: ScatterFields): { x: string; y: string } {
  const pair = defaultPair(d, f.measures)!;
  return { x: pair.x.name, y: pair.y.name };
}

/**
 * The scale a picked measure is drawn on (G-2). A scale's type can't follow a param, so the
 * page draws the chart again when a pick changes it; the Editor's copy keeps the types it opened with.
 */
export function pickScale(f: ScatterFields, name: string): ScaleType {
  const m = f.measures.find((x) => x.name === name);
  return m ? scaleType(m) : "linear";
}

function pickedScale(f: ScatterFields, name: string): { scale: Spec; axis: Spec } {
  const m = f.measures.find((x) => x.name === name);
  return m ? scaleFor(m) : { scale: {}, axis: {} };
}

/**
 * What the measures' metadata adds to the scatter plot, looked up by the picked field's
 * position among the measures in expressions (arrays, which the CSP-safe interpreter reads,
 * and which take any field name): their titles, and the documented ranges that every value
 * lies inside. A measure without a range keeps Vega-Lite's own "nice" domain, as it has
 * without metadata. Empty when no measure has either, so the spec stays as it was.
 */
function measureLookups(d: Dataset, measures: Field[]) {
  const at = (param: string) => `indexof(${tagged(measures.map((m) => m.name))}, ${tag(param)})`;
  const ranges = measures.map((m) => documentedRange(m)).map((r) => (r?.fits ? r : null));
  const mins = ranges.map((r) => r?.min ?? null);
  const maxs = ranges.map((r) => r?.max ?? null);
  const lookup = (values: unknown[], param: string) => `${JSON.stringify(values)}[${at(param)}]`;
  const titled = measures.some((m) => m.title);
  const markers = measures.map((m) => markerForms(m, effectiveMissing(d, m) ?? []));
  const drop = (param: string) => `indexof([${markers.map(tagged).join(", ")}][${at(param)}], ${tag(`datum[${param}]`)}) < 0`;
  return {
    /** Leaves out the rows whose picked measures hold a documented missing-value marker. */
    missing: markers.some((m) => m.length) ? { filter: `${drop("xField")} && ${drop("yField")}` } : null,
    labels: titled ? measures.map(fieldTitle) : null,
    title: (param: string) => (titled ? untag(`${tagged(measures.map(fieldTitle))}[${at(param)}]`) : param),
    bounds: (param: string): Spec => ({
      ...(mins.some((v) => v !== null) ? { domainMin: { expr: lookup(mins, param) } } : {}),
      ...(maxs.some((v) => v !== null) ? { domainMax: { expr: lookup(maxs, param) } } : {}),
      // Setting a bound turns Vega-Lite's default nice off for every pick; keep it for the unbounded.
      ...(ranges.some((r) => r) ? { nice: { expr: `!${lookup(ranges.map((r) => r !== null), param)}` } } : {}),
    }),
  };
}

/**
 * Two measures against each other. The x and y pickers are input bindings on the
 * xField and yField params, so they work the same in the Vega Editor; the axis titles
 * are text marks that read those params (an axis title can't).
 */
export function scatterSpec(d: Dataset, f: ScatterFields, o: ScatterOptions): Spec {
  const options = f.measures.map((m) => m.name);
  const meta = measureLookups(d, f.measures);
  const prepare = [meta.missing, f.color ? missingFilter(d, [f.color]) : null, f.color ? categoryAsText(f.color) : null].filter((t) => t !== null);
  const labels = meta.labels ? { labels: meta.labels } : {};
  const title = (param: string, place: Spec) => ({
    data: { values: [{}] },
    // Zoom (scale binding) clips every mark in the view; the titles sit outside the plot.
    mark: { type: "text", text: { expr: meta.title(param) }, fontWeight: "bold", fontSize: 11, clip: false, ...place },
  });
  const { opacity } = BAND_POLICY[rowBand(d.rows ?? 0)];
  const params: Spec[] = [];
  if (o.zoom) params.push({ name: "zoom", select: "interval", bind: "scales" });
  if (f.color) params.push({ name: "pick", select: { type: "point", fields: [fieldRef(f.color.name)] }, bind: "legend" });
  // The plotted values need names no field of the file has: a calculate `as: "x"` would overwrite a
  // field called x (platformer_terrain) before the next calculate reads it.
  const taken = new Set(d.fields.map((m) => m.name));
  const free = (name: string): string => (taken.has(name) ? free(`_${name}`) : name);
  const [px, py] = [free("x"), free("y")];
  const [sx, sy] = [pickedScale(f, o.x), pickedScale(f, o.y)];
  return {
    $schema: SCHEMA,
    description: `Two measures of ${d.name} from vega-datasets, picked with the x and y menus.`,
    width: "container",
    height: o.height,
    autosize: { type: "fit-x", contains: "padding" },
    data: { url: d.url },
    params: [
      { name: "xField", value: o.x, bind: { input: "select", options, ...labels, name: "x " } },
      { name: "yField", value: o.y, bind: { input: "select", options, ...labels, name: "y " } },
    ],
    layer: [
      {
        // The field is picked at run time, so Vega-Lite can't parse it up front (CSV values are strings).
        transform: [
          ...prepare,
          { calculate: "toNumber(datum[xField])", as: px },
          { calculate: "toNumber(datum[yField])", as: py },
          { filter: `isValid(datum.${px}) && isValid(datum.${py}) && isFinite(datum.${px}) && isFinite(datum.${py})` },
        ],
        params,
        mark: { type: "point", opacity: opacity },
        encoding: {
          x: { field: px, type: "quantitative", scale: withScale({ zero: false, ...meta.bounds("xField") }, sx), axis: { title: null, ...sx.axis } },
          y: { field: py, type: "quantitative", scale: withScale({ zero: false, ...meta.bounds("yField") }, sy), axis: { title: null, ...sy.axis } },
          ...(f.color
            ? {
                color: category(f.color),
                opacity: { condition: { param: "pick", empty: true, value: opacity }, value: 0.08 },
              }
            : {}),
          tooltip: [
            ...(f.label ? [{ field: fieldRef(f.label.name), type: "nominal", ...titled(f.label) }] : []),
            { field: px, type: "quantitative", title: "x" },
            { field: py, type: "quantitative", title: "y" },
            ...(f.color ? [{ field: fieldRef(f.color.name), type: "nominal", ...titled(f.color) }] : []),
            ...(f.time
              ? [{ field: fieldRef(f.time.name), type: f.time.profile.kind === "temporal" ? "temporal" : "quantitative", ...(isYear(f.time) ? { format: "d" } : {}), ...titled(f.time) }]
              : []),
          ],
        },
      },
      title("xField", { x: { expr: "width / 2" }, y: { expr: "height + 32" }, align: "center", baseline: "top" }),
      title("yField", { x: 0, y: -10, align: "left", baseline: "bottom" }),
    ],
  };
}

/** Does this dataset open on the density overview (a table past the points bands, with a scatter plot)? */
export function hasDensity(d: Dataset): boolean {
  return rowBand(d.rows ?? 0) === "density" && scatterFields(d) !== null;
}

/**
 * What a map leaves out, said under it: Albers USA has no place for points outside the 50
 * states (Puerto Rico, Guam), so the starter drops them; a world map fitted to the middle
 * 98% of the points clips the rest. The page says how many; null when none is left out.
 */
export function mapNote(d: Dataset): string | null {
  const projection = starterSpec(d)?.projection as Spec | undefined;
  if (!projection || !d.points || d.rows === null) return null;
  if (projection.type === "albersUsa") {
    const n = d.points.outsideUs ?? 0;
    return n ? `The map leaves out ${formatCount(n)} of ${formatCount(d.rows)} rows, outside the 50 states: the Albers USA projection has no place for them.` : null;
  }
  // A world map fitted to the middle 98% of the points clips the rest.
  const n = projection.fit ? (d.points.outsideBox ?? 0) : 0;
  return n ? `The map frames the middle 98% of the points; ${formatCount(n)} of ${formatCount(d.rows)} rows ${n === 1 ? "lies" : "lie"} outside the frame.` : null;
}

/** The starter chart, sized to its column; small multiples keep two columns of fixed panels, smaller on a phone. */
export function starterChart(d: Dataset, phone = false): Spec | null {
  const spec = starterSpec(d);
  if (!spec) return null;
  if (spec.facet) {
    const size = phone ? PANEL_SIZE.phone : PANEL_SIZE.wide;
    return { ...spec, spec: { ...(spec.spec as Spec), width: size, height: size } };
  }
  const height = discreteHeight(d, spec);
  return { ...spec, width: "container", ...(height ? { height } : {}), autosize: { type: "fit-x", contains: "padding" } };
}

/** Vega-Lite's default step (px) per value of a discrete axis. */
const STEP = 20;

/**
 * A chart with categories down its y axis (bars, a dot plot, a heatmap) gets its height as
 * a number: the default step times the values the axis shows, which is the height a step
 * would draw (20 px each, with Vega-Lite's default paddings). A step height beside a width
 * that fits its container makes Vega-Lite warn that it drops a "fit-y" it was never asked
 * for (upstream, DOSSIER §16.1). Null when the axis isn't discrete or its values can't be counted.
 */
export function discreteHeight(d: Dataset, spec: Spec): number | null {
  const y = (spec.encoding as Record<string, Spec> | undefined)?.y;
  if (!y || spec.height !== undefined || (y.type !== "nominal" && y.type !== "ordinal")) return null;
  const name = String(y.field ?? "").replace(/\\(.)/g, "$1");
  const f = d.fields.find((x) => x.name === name);
  const counted = f ? distinctValues(f) : null;
  if (!f || counted === null) return null;
  // Rows with a missing value of the measure are left out, and categories with them.
  const x = (spec.encoding as Record<string, Spec>).x;
  const transforms = (spec.transform as Spec[] | undefined) ?? [];
  const summed = transforms.flatMap((t) => ((t.aggregate as Spec[] | undefined) ?? []).map((a) => String(a.field)));
  const measures = [String(x?.field ?? ""), ...summed].map((n) => n.replace(/\\(.)/g, "$1"));
  const present = measures.map((m) => d.presentCategories?.[f.name]?.[m]).find((n) => n !== undefined);
  const all = present ?? counted;
  // The top twenty leave out totals, then keep twenty at most.
  const top = transforms.some((t) => t.window);
  const shown = top ? Math.min(TOP, all - totalsOf(d, f).length) : all;
  return shown > 0 ? shown * STEP : null;
}

/**
 * "Both fields have values in 398 of 406 rows." for the scatter caption, from the rows the
 * chart plots (read from the view after it runs); null until then, so the caption never
 * loads a file on its own (a large file waits for its button).
 */
export function bothValuesNote(d: Dataset, plotted: number | null): string | null {
  if (plotted === null || d.rows === null) return null;
  return `Both fields have values in ${formatCount(plotted)} of ${formatCount(d.rows)} rows.`;
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
  // A map's marks are its last layer (over the basemap); small multiples', their inner spec.
  const layers = spec.layer as Spec[] | undefined;
  const unit = (spec.spec as Spec | undefined) ?? layers?.at(-1) ?? spec;
  const mark = unit.mark as { type?: string } | string | undefined;
  const type = typeof mark === "string" ? mark : mark?.type;
  if (type) out.push(`${type} mark`);
  const projection = spec.projection as { type?: string } | undefined;
  if (projection?.type) out.push(`${projection.type} projection`);
  if (layers) out.push("layers");
  if (spec.facet) out.push("facet");
  if (has('"timeUnit"')) out.push("time unit");
  for (const agg of ["mean", "sum", "count"]) if (has(`"aggregate":"${agg}"`)) out.push(`${agg} aggregate`);
  if (has('"bin"')) out.push("binning");
  if (has('"tooltip":true')) out.push("tooltip");
  return out;
}
