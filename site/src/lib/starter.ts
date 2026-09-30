/**
 * "Try this dataset": build a small, sensible Vega-Lite starter chart from a dataset's
 * schema and field profiles, and open it in the Vega Editor.
 *
 * The rules, in order: maps for geographic files and for latitude/longitude columns;
 * start/end ranges as a timeline; time series as lines; two measures as a scatter
 * plot; a measure by category as bars; then a histogram or category counts.
 * Identifier and code columns are never plotted as measurements.
 */
import LZString from "lz-string";
import { categoryLabels, categoryValues, type Dataset, documentedRange, effectiveMissing, type Field, fieldTitle, orderedCategories } from "./catalog";
import { balanced, distinctValues, idName, informative, inUs, namedAxes, nearDuplicate, scaleFor, scaleType, summable, timeKey, totalsOf, unique, withScale } from "./chart-rules";

type Spec = Record<string, unknown>;
type Enc = Record<string, unknown>;

const EDITOR = "https://vega.github.io/editor/#/url/vega-lite/";
const SCHEMA = "https://vega.github.io/schema/vega-lite/v6.json";

/** Vega-Lite treats `.` and `[ ]` in field names as nested access; escape them. */
export function fieldRef(name: string): string {
  return name.replace(/([.[\]])/g, "\\$1");
}

// What a field's metadata adds to an encoding of it. Each addition needs its property,
// so an undescribed field encodes exactly as before.

/** The field's title for an axis, legend or tooltip; Vega-Lite's own "Mean of …" or "Sum of …" for an aggregate. */
export function titled(f: Field, enc: Enc = {}): Enc {
  if (!f.title) return {};
  return { title: enc.aggregate === "mean" ? `Mean of ${f.title}` : enc.aggregate === "sum" ? `Sum of ${f.title}` : f.title };
}

// Text from the metadata inside a Vega expression. Vega refuses a string literal that names a
// JavaScript object property ("toString", "__proto__", "constructor"), so each one goes in
// behind a prefix that no such name has, and is compared with the prefixed text of a value.
const TAG = "v:";

/** Texts as an array literal of prefixed strings, for `indexof(…, tag(expr))`. */
export function tagged(texts: string[]): string {
  return JSON.stringify(texts.map((t) => TAG + t));
}

/** An expression's value as prefixed text (numbers too: -99 becomes "v:-99"). */
export function tag(expr: string): string {
  return `"${TAG}" + ${expr}`;
}

/** A prefixed text back as the text. */
export function untag(expr: string): string {
  return `slice(${expr}, ${TAG.length})`;
}

/**
 * An expression that shows each category's label in place of its value, looked up in
 * arrays (no object literal), so any value is safe and the CSP-safe interpreter reads it.
 */
export function labelExpr(labels: [string, string][]): string {
  // Parenthesized: a facet header's Vega-Lite puts an expression of its own in place of datum.label.
  const at = `indexof(${tagged(labels.map(([v]) => v))}, ${tag("(datum.label)")})`;
  return `${at} < 0 ? (datum.label) : ${untag(`${tagged(labels.map(([, l]) => l))}[${at}]`)}`;
}

type Range = ReturnType<typeof documentedRange>;

/** The bounds of `r` on zero's side (a minimum at or below it, a maximum at or above it); null when none is. */
function zeroSide(r: Range): Range {
  if (!r) return null;
  const min = r.min !== undefined && r.min <= 0 ? r.min : undefined;
  const max = r.max !== undefined && r.max >= 0 ? r.max : undefined;
  if (min === undefined && max === undefined) return null;
  return { ...(min !== undefined ? { min } : {}), ...(max !== undefined ? { max } : {}), fits: r.fits };
}

/**
 * A number field on a channel. A documented range that every value lies inside becomes
 * the scale's bounds (a histogram's bin extent, when both ends are documented). A bar's
 * length starts at zero, so for `bars` a bound on the far side of zero is left out: the
 * bars would start off the plot.
 */
export function measure(f: Field, enc: Enc = {}, bars = false): Enc {
  const out: Enc = { field: fieldRef(f.name), type: "quantitative", ...enc };
  // A documented range bounds each row's value, not a sum of rows.
  const own = enc.aggregate === "sum" ? null : documentedRange(f);
  const range = bars ? zeroSide(own) : own;
  if (range?.fits && out.bin) {
    if (range.min !== undefined && range.max !== undefined) out.bin = { ...(out.bin as Enc), extent: [range.min, range.max] };
  } else if (range?.fits) {
    out.scale = {
      ...(out.scale as Enc | undefined),
      ...(range.min !== undefined ? { domainMin: range.min } : {}),
      ...(range.max !== undefined ? { domainMax: range.max } : {}),
    };
  }
  return { ...out, ...titled(f, out) };
}

/**
 * The documented order as a Vega-Lite `sort` array of text, or null. Text that names an
 * object property can't go in (Vega refuses the literal in the sort expression Vega-Lite
 * writes), so such categories keep the default order. Numbered categories are sorted as
 * text, and `categoryAsText` makes the field's values text to match: a file may hold them
 * as numbers, as text (CSV), or both, and the sort compares strictly.
 */
function sortOrder(f: Field): string[] | null {
  const order = orderedCategories(f)?.map(String);
  return order && !order.some((v) => v in Object.prototype) ? order : null;
}

/** The transform that makes an ordered, numbered category's values text (null stays null), or null when none is needed. */
export function categoryAsText(f: Field): Enc | null {
  const numbered = orderedCategories(f)?.some((v) => typeof v === "number");
  return numbered && sortOrder(f) ? { calculate: `toString(datum[${JSON.stringify(f.name)}])`, as: f.name } : null;
}

/**
 * A category on a channel, with labels on its axis or legend. When the metadata says the
 * order matters, the documented order replaces `enc.sort` (and so sets the color domain's
 * order), and an axis reads it as ordinal. Colors stay nominal: an ordinal ramp would fade
 * the first category into the background.
 */
export function category(f: Field, enc: Enc = {}, guide: "axis" | "legend" | "header" = "legend"): Enc {
  const order = sortOrder(f);
  const labels = categoryLabels(f);
  return {
    field: fieldRef(f.name),
    type: order && guide === "axis" ? "ordinal" : "nominal",
    ...enc,
    ...(order ? { sort: order } : {}),
    ...titled(f),
    ...(labels ? { [guide]: { labelExpr: labelExpr(labels) } } : {}),
  };
}

/** A marker as a value may hold it: its text, and for a number the number's own text ("-99.0" is -99 once parsed). */
export function markerForms(f: Field, markers: string[]): string[] {
  const numeric = f.type === "integer" || f.type === "number";
  return [...new Set(markers.flatMap((m) => (numeric && m.trim() !== "" && Number.isFinite(Number(m)) ? [m, String(Number(m))] : [m])))];
}


/**
 * A filter that leaves out the rows where any of `fields` holds one of its documented
 * missing-value markers, as the fields table's profile does; null when no field has
 * markers declared, so an undescribed chart stays as it was.
 */
export function missingFilter(d: Dataset, fields: Field[]): Enc | null {
  const tests = fields.flatMap((f) => {
    const markers = effectiveMissing(d, f);
    if (!markers?.length) return [];
    return [`indexof(${tagged(markerForms(f, markers))}, ${tag(`datum[${JSON.stringify(f.name)}]`)}) < 0`];
  });
  return tests.length ? { filter: tests.join(" && ") } : null;
}

/**
 * The starter chart with what its encoded fields' metadata asks of the data: the rows they
 * mark as missing left out, and ordered numbered categories made text. Markers are matched
 * on a value's text, as Table Schema says, so a date field with markers is read as text
 * (`parse: null`) and parsed after the filter (`toDate`, as Vega-Lite's own parse does).
 * These go before the chart's own transforms. Without such metadata the spec is unchanged.
 */
/** The source fields a spec's transforms read (aggregate and window fields, groupings, sorts), not the ones they compute. */
function transformFields(spec: Spec): string[] {
  const computed = new Set<string>();
  const read: string[] = [];
  for (const t of (spec.transform as Enc[] | undefined) ?? []) {
    const ops = [...((t.aggregate as Enc[] | undefined) ?? []), ...((t.window as Enc[] | undefined) ?? [])];
    for (const op of ops) if (typeof op.field === "string" && !computed.has(op.field)) read.push(op.field);
    for (const g of (t.groupby as string[] | undefined) ?? []) if (!computed.has(g)) read.push(g);
    for (const s of (t.sort as Enc[] | undefined) ?? []) if (typeof s.field === "string" && !computed.has(s.field)) read.push(s.field);
    for (const op of ops) if (typeof op.as === "string") computed.add(op.as);
    if (typeof t.as === "string") computed.add(t.as);
  }
  return read;
}

function withMetadata(d: Dataset, spec: Spec | null): Spec | null {
  if (!spec) return spec;
  // A map's points are its last layer (the basemap has its own data); small multiples encode in their inner spec.
  const layers = spec.layer as Spec[] | undefined;
  if (layers) return { ...spec, layer: layers.map((l, i) => (i === layers.length - 1 ? withMetadata(d, l)! : l)) };
  const encoding = ((spec.spec as Spec | undefined)?.encoding ?? spec.encoding) as Record<string, Enc> | undefined;
  if (!encoding) return spec;
  // Fields the encodings show, the facet splits by, and the chart's own transforms read (a sum per group).
  const used = new Set([...Object.values(encoding).map((e) => e.field), (spec.facet as Enc | undefined)?.field, ...transformFields(spec).map(fieldRef)]);
  const fields = d.fields.filter((f) => used.has(fieldRef(f.name)));
  const dates = fields.filter((f) => f.profile.kind === "temporal" && effectiveMissing(d, f)?.length);
  const transform = [
    missingFilter(d, fields),
    ...dates.map((f) => ({ calculate: `toDate(datum[${JSON.stringify(f.name)}])`, as: f.name })),
    ...fields.map(categoryAsText),
  ].filter((t) => t !== null);
  if (!transform.length) return spec;
  const data = spec.data as Spec;
  const parse = dates.length ? { format: { ...(data.format as Spec | undefined), parse: Object.fromEntries(dates.map((f) => [f.name, null])) } } : {};
  return { ...spec, data: { ...data, ...parse }, transform: [...transform, ...((spec.transform as Spec[] | undefined) ?? [])] };
}

const ID_DESC = /\b(identifier|unique id|fips|code for|index of)\b/i;
const YEAR_NAME = /^(year|yr)$|year$/i;
const TIME_PART = /^(month|day|hour|minute|weekday)$/i;
/** Integer columns that group rows (age bands, sex codes) rather than measure them. */
const GROUPING = /^(age|sex|rank|level|grade)$/i;
const LAT = /^(lat|latitude)$/i;
const LON = /^(lon|lng|long|longitude)$/i;
const SERIES = /^(symbol|source|location|country|region|series|sex|gender|division|entity|variety|site|origin|species|type|category)$/i;
/** A direction in degrees (wind, heading), drawn as the angle of a wedge on a map. */
const DIRECTION = /^(dir|direction|bearing|heading|wind_?dir(ection)?)$/i;

/** Coordinates, by name or (metadata first) by title. */
export const isLat = (f: Field): boolean => LAT.test(f.name) || LAT.test(f.title ?? "");
export const isLon = (f: Field): boolean => LON.test(f.name) || LON.test(f.title ?? "");

/** Numeric identifiers and codes: plotting them as measurements is meaningless. */
function isId(f: Field, fields: Field[]): boolean {
  if (idName(f, fields)) return true;
  return f.profile.kind === "quantitative" && (/categor/i.test(f.name) || ID_DESC.test(f.description ?? ""));
}

export function isYear(f: Field): boolean {
  const p = f.profile;
  return p.kind === "quantitative" && !integerCategory(f) && Number.isInteger(p.min) && p.min >= 1000 && p.max <= 2200 && (YEAR_NAME.test(f.name) || f.type === "integer");
}

/** A year field that makes a time axis: three or more years (two are a comparison, not a trend). */
export function timeYear(f: Field): boolean {
  return isYear(f) && (distinctValues(f) ?? 3) >= 3;
}

/** Small-range integers (cylinders, ratings, ages in bands) behave like categories, not measures. */
function isOrdinalInt(f: Field): boolean {
  const p = f.profile;
  return p.kind === "quantitative" && f.type === "integer" && p.max - p.min <= 12;
}

/** Integers the metadata documents as categories (grades 1–3, with labels): groups, not measures. */
function integerCategory(f: Field): boolean {
  return f.type === "integer" && f.profile.kind === "quantitative" && categoryValues(f) !== null;
}

/**
 * Quantities worth plotting on an axis (not identifiers, years, codes, coordinates or
 * documented categories). `fields` is the table's, for names that depend on their
 * neighbors (`source` is an identifier only beside a `target`).
 */
export function isMeasure(f: Field, fields: Field[] = [f]): boolean {
  return (
    f.profile.kind === "quantitative" &&
    !integerCategory(f) &&
    !isId(f, fields) &&
    !isYear(f) &&
    !isOrdinalInt(f) &&
    !TIME_PART.test(f.name) &&
    !(f.type === "integer" && GROUPING.test(f.name)) &&
    !isLat(f) &&
    !isLon(f)
  );
}

/**
 * A category with between 2 and `max` values (identifiers excluded); an integer category
 * counts its documented values. Documented categories count even under an identifier's
 * name (gapminder's `cluster`, whose six values are labeled regions): the metadata says so.
 */
export function nominal(f: Field, max: number, fields: Field[] = [f]): boolean {
  const n = f.profile.kind === "nominal" ? f.profile.distinct : integerCategory(f) ? categoryValues(f)!.length : 0;
  return n >= 2 && n <= max && (!idName(f, fields) || categoryValues(f) !== null);
}

/** A category worth a color: at most `max` values, and informative (G-5: not a helper, not nearly all one value). */
export function colorable(f: Field, max: number, fields: Field[], rows: number): boolean {
  return nominal(f, max, fields) && informative(f, rows);
}

/** Fields that can group a table's rows: categories (not one value per row) and integers that aren't measures. */
function groupings(fields: Field[], rows: number): Field[] {
  return fields.filter((f) => {
    const n = distinctValues(f);
    if (n === null || n < 2 || unique(f, rows)) return false;
    return f.profile.kind === "nominal" || (f.profile.kind === "quantitative" && f.type === "integer" && !isMeasure(f, fields));
  });
}

/** The field a time series runs along: the first date field, else the first year field with three or more years. */
export function timeField(fields: Field[]): Field | undefined {
  return fields.find((f) => f.profile.kind === "temporal") ?? fields.find(timeYear);
}

/** The fields that, with the time, identify every row (see `timeKey`); null when the time doesn't index the rows. */
export function timeKeyOf(d: Dataset, t: Field): Field[] | null {
  return timeKey(d, t);
}

function spanYears(f: Field): number {
  const p = f.profile;
  if (p.kind !== "temporal") return 0;
  return (new Date(p.max).getTime() - new Date(p.min).getTime()) / (365.25 * 864e5);
}

/** The unit a long daily or hourly series is averaged into: days for up to two years, months up to forty, else years. */
function timeUnit(t: Field): string {
  const span = spanYears(t);
  return span <= 2 ? "yearmonthdate" : span <= 40 ? "yearmonth" : "year";
}

/** A measure's scale and axis for a position channel (G-2), as encoding properties. */
function scaled(f: Field, scale: Enc = {}): Enc {
  const s = scaleFor(f);
  return { scale: withScale(scale, s), ...(Object.keys(s.axis).length ? { axis: s.axis } : {}) };
}

/**
 * The two measures a scatter plot opens on (G-3, G-7): fields named for their axes (`x`
 * and `y`, `cx` and `cy`) when there are such; else the first pair, in field order, that
 * isn't a near-duplicate (|r| > 0.97: one line, the same thing measured twice), its first
 * field on `first` (Explore's scatter puts it on y; the starter chart, as it always has, on x).
 * A measure that wants a log axis goes on x when the other doesn't (income, population:
 * orders of magnitude read left to right, as in the Preston curve). Null when every pair
 * is a near-duplicate.
 */
export function defaultPair(d: Dataset, measures: Field[], first: "x" | "y" = "y"): { x: Field; y: Field } | null {
  const named = namedAxes(measures);
  if (named) return named;
  for (const [i, a] of measures.entries()) {
    for (const b of measures.slice(i + 1)) {
      if (nearDuplicate(d, a.name, b.name)) continue;
      const pair = first === "y" ? { x: b, y: a } : { x: a, y: b };
      return scaleType(pair.y) === "log" && scaleType(pair.x) === "linear" ? { x: pair.y, y: pair.x } : pair;
    }
  }
  return null;
}

/** Up to this many rows a table is small enough for small multiples of its points. */
const SMALL_TABLE = 200;
/** A small multiple's size (px): two columns fit a phone's Explore column (358 px). */
export const PANEL_SIZE = { wide: 180, phone: 130 };

/** The small multiples' category: a small table's category of two to four equal groups (Anscombe's four series). */
export function panelsBy(d: Dataset, fields: Field[]): Field | undefined {
  const rows = d.rows ?? 0;
  if (rows === 0 || rows > SMALL_TABLE) return undefined;
  return fields.find((f) => nominal(f, 4, fields) && balanced(f, rows));
}

const PROJECTION: Record<string, string> = {
  us_10m: "albersUsa",
  world_110m: "equalEarth",
  london_boroughs: "mercator",
  london_tube_lines: "mercator",
  earthquakes: "equalEarth",
};

/** vega-datasets' own world map, beside the dataset's file (jsDelivr or GitHub Pages alike). */
export function basemapUrl(d: Dataset): string {
  return d.url.replace(/[^/]+$/, "world-110m.json");
}

/**
 * Countries under a map's points, from vega-datasets' world_110m (119 KB, 45 KB over the
 * wire, and shared by every page that draws it); only the United States under Albers USA.
 * A quiet gray that reads on light and dark grounds; decorative, so screen readers skip it.
 */
function basemap(d: Dataset, usOnly: boolean): Spec {
  return {
    data: { url: basemapUrl(d), format: { type: "topojson", feature: "countries" } },
    ...(usOnly ? { transform: [{ filter: "datum.id == 840" }] } : {}),
    mark: { type: "geoshape", fill: "#8a8f98", fillOpacity: 0.14, stroke: "#8a8f98", strokeOpacity: 0.5, strokeWidth: 0.5, clip: true, aria: false },
  };
}

/**
 * A sequential ramp for marks over the basemap: both ends at mid luminance (blue to orange),
 * so the weakest values stay visible on the gray countries and on light and dark grounds;
 * the default ramp starts near the background color.
 */
const BASEMAP_RAMP = ["#3b82c4", "#e0662a"];

/** The 1:110m basemap goes only under points spread over at least this many degrees: closer in, its coarse coast would mislead. */
const BASEMAP_MIN_DEGREES = 10;

/** A box of longitude and latitude as a GeoJSON feature to fit a projection to (corners and edge midpoints: edges curve). */
function boxFeature(box: { longitude: [number, number]; latitude: [number, number] }): Spec {
  const [w, e] = box.longitude;
  const [s, n] = box.latitude;
  const [mx, my] = [(w + e) / 2, (s + n) / 2];
  return { type: "Feature", properties: {}, geometry: { type: "MultiPoint", coordinates: [[w, s], [mx, s], [e, s], [e, my], [e, n], [mx, n], [w, n], [w, my]] } };
}

function lineGeometry(types: string[] | undefined): boolean {
  return !!types?.length && types.every((t) => /LineString$/.test(t));
}

function geoFile(d: Dataset, base: Spec): Spec | null {
  if (d.format === "topojson") {
    const feature = d.objects?.[0];
    if (!feature) return null;
    // Lines (tube lines) are strokes: a fill would close each one into a shape. Each line's id colors it.
    const lines = lineGeometry(d.objectGeometryTypes?.[feature]);
    return {
      ...base,
      width: 600,
      height: 400,
      data: { url: d.url, format: { type: "topojson", feature } },
      projection: { type: PROJECTION[d.name] ?? "equalEarth" },
      // Thousands of shapes: screen readers get the chart's description, not each one.
      mark: lines
        ? { type: "geoshape", filled: false, strokeWidth: 1.5, aria: false }
        : { type: "geoshape", stroke: "white", strokeWidth: 0.5, aria: false },
      ...(lines ? { encoding: { color: { field: "id", type: "nominal", title: "id", scale: { scheme: "tableau20" } } } } : {}),
    };
  }
  const lines = lineGeometry(d.geometryTypes);
  const points = !!d.geometryTypes?.length && d.geometryTypes.every((t) => /Point$/.test(t));
  const shapes: Spec = {
    data: { url: d.url, format: { type: "json", property: "features" } },
    mark: lines ? { type: "geoshape", filled: false, strokeWidth: 1.5, aria: false } : { type: "geoshape", aria: false },
  };
  const { $schema, description } = base;
  const frame: Spec = { $schema, description, width: 600, height: 360, projection: { type: PROJECTION[d.name] ?? "equalEarth" } };
  // Points (earthquakes) sit on the world's countries; shapes and lines carry their own geography.
  if (points) return { ...frame, layer: [basemap(d, false), shapes] };
  return { ...frame, ...shapes };
}

function pointMap(d: Dataset, base: Spec, lat: Field, lon: Field, color: Field | undefined, measures: Field[]): Spec {
  const la = lat.profile;
  const lo = lon.profile;
  // Where the points lie, as the builder recorded it for these two columns; else their range.
  const where = d.points?.latitude === lat.name && d.points.longitude === lon.name ? d.points : null;
  const box =
    where?.box ??
    (la.kind === "quantitative" && lo.kind === "quantitative" ? { longitude: [lo.min, lo.max] as [number, number], latitude: [la.min, la.max] as [number, number] } : null);
  // Nearly all in the US (95% or more): Albers USA, which draws Alaska and Hawaii beside the
  // rest and leaves out the few points elsewhere (airports' Pacific islands), instead of a
  // world map with the US in a corner. Without the builder's share, the whole range must fit.
  const us = where
    ? where.us >= 0.95
    : !!box && box.longitude[0] >= -180 && box.longitude[1] <= -60 && box.latitude[0] >= 15 && box.latitude[1] <= 72;
  const wide = !!box && Math.max(box.longitude[1] - box.longitude[0], box.latitude[1] - box.latitude[0]) >= BASEMAP_MIN_DEGREES;
  // A world projection fits the middle 98% of the points, so a few far ones don't shrink the
  // rest; Albers USA fits the country (or the points themselves, when they are close together).
  const fit = !us && where && wide ? { fit: boxFeature(where.box) } : {};
  const direction = measures.find((m) => DIRECTION.test(m.name) && m.profile.kind === "quantitative" && m.profile.min >= 0 && m.profile.max <= 360);
  const strength = direction && measures.find((m) => m !== direction && !DIRECTION.test(m.name) && !nearDuplicate(d, m.name, direction.name));
  const clip = wide ? { clip: true } : {};
  const points: Spec = {
    // Albers USA has no place for points outside the US (they would pile up in a corner).
    ...(us ? { transform: [{ filter: inUs(lon.name, lat.name) }] } : {}),
    mark: direction
      ? { type: "point", shape: "wedge", filled: true, size: (d.rows ?? 0) > 2000 ? 40 : 80, tooltip: true, ...clip }
      : { type: "circle", size: (d.rows ?? 0) > 5000 ? 4 : 16, opacity: 0.7, tooltip: true, ...clip },
    encoding: {
      longitude: { field: fieldRef(lon.name), type: "quantitative", ...titled(lon) },
      latitude: { field: fieldRef(lat.name), type: "quantitative", ...titled(lat) },
      // A direction turns each wedge; the other measure (wind speed) colors it.
      ...(direction ? { angle: { field: fieldRef(direction.name), type: "quantitative", scale: { domain: [0, 360], range: [0, 360] }, ...titled(direction) } } : {}),
      ...(strength
        ? { color: { field: fieldRef(strength.name), type: "quantitative", scale: { range: BASEMAP_RAMP, interpolate: "hcl" }, ...titled(strength) } }
        : color ? { color: category(color) } : {}),
    },
  };
  // Points either side of 180° (the box's east edge past it): the world turns to put them in the middle.
  const across = !us && !!box && box.longitude[1] > 180;
  const rotate = across ? { rotate: [-(box!.longitude[0] + box!.longitude[1]) / 2, 0, 0] } : {};
  const frame: Spec = { ...base, width: 600, height: 380, projection: { type: us ? "albersUsa" : "equalEarth", ...rotate, ...fit } };
  if (!wide) return { ...frame, ...points };
  const { data, ...rest } = frame;
  return { ...rest, layer: [basemap(d, us), { data, ...points }] };
}

export function starterSpec(d: Dataset): Spec | null {
  return withMetadata(d, starterRule(d));
}

/**
 * A line over time (G-1, G-4). The line never joins rows of different series: when each
 * time holds one row per series, the series colors the line (up to 12 of them); two year
 * fields that index the rows together make one line per vintage (budgets: each budget
 * year's forecasts). Rows it can't keep apart are aggregated: counts summed (the total is
 * what they mean), other measures averaged. A series value that is the total of the others
 * is left out, and an unaggregated heavy-tailed measure gets a log axis (G-2).
 */
function timeSeries(d: Dataset, base: Spec, t: Field, m: Field, fields: Field[]): Spec {
  const rows = d.rows ?? 0;
  const date = t.profile.kind === "temporal";
  const key = timeKeyOf(d, t);
  const unit = date && rows > 1000 ? timeUnit(t) : undefined;
  const series =
    key?.find((f) => colorable(f, 12, fields, rows)) ?? fields.find((f) => colorable(f, 12, fields, rows) && SERIES.test(f.name));
  const vintage = !series && key?.length === 1 && timeYear(key[0]!) ? key[0] : undefined;
  const exact = key !== null && key.every((f) => f === series || f === vintage);
  // Sum only across the separate groups of one time (people of each age in a year), never
  // across times merged into one bucket (a daily population is not a monthly one); totals
  // among the groups are left out, or they'd count everything twice.
  const merges = !!unit && !(d.timeKeyBuckets?.[t.name] ?? []).includes(unit);
  const sums = !exact && key !== null && !merges && summable(m);
  const aggregate = unit || !exact ? (sums ? "sum" : "mean") : undefined;
  // Two year fields: the one with more years runs along x, and the other draws a line each.
  const [along, lines] = vintage && (distinctValues(vintage) ?? 0) > (distinctValues(t) ?? 0) ? [vintage, t] : [t, vintage];
  // A series (colored) never shows its total among the parts; a sum leaves out every key's totals.
  const splits = [...new Set([...(series ? [series] : []), ...(sums ? key! : [])])];
  const leaveOut = splits.flatMap((f) => {
    const totals = totalsOf(d, f);
    const left = (distinctValues(f) ?? 0) - totals.length;
    return totals.length && (f !== series || sums || left >= 2)
      ? [{ filter: `indexof(${tagged(totals)}, ${tag(`datum[${JSON.stringify(f.name)}]`)}) < 0` }]
      : [];
  });
  // Values in a narrow band far from zero (CO2 in ppm, air pressure) leave zero off the axis, or the line is flat.
  const q = m.profile;
  const band = aggregate !== "sum" && q.kind === "quantitative" && q.min > 0 && q.min >= q.max / 2;
  return {
    ...base,
    width: 640,
    height: 300,
    ...(leaveOut.length ? { transform: leaveOut } : {}),
    mark: date ? { type: "line", interpolate: "monotone", tooltip: true } : { type: "line", point: rows <= 60, tooltip: true },
    encoding: {
      x: date
        ? { field: fieldRef(along.name), type: "temporal", ...(unit ? { timeUnit: unit } : {}), axis: { ...TIME_AXIS, format: dateFormat(along) }, ...titled(along) }
        : measure(along, { scale: { zero: false }, axis: { format: "d", ...TIME_AXIS } }),
      y: measure(m, aggregate ? { aggregate, ...(band ? { scale: { zero: false } } : {}) } : scaled(m, band ? { zero: false } : {})),
      ...(series ? { color: category(series) } : {}),
      ...(lines
        ? {
            color: { field: fieldRef(lines.name), type: "quantitative", legend: { format: "d" }, ...titled(lines) },
            detail: { field: fieldRef(lines.name), type: "quantitative" },
          }
        : {}),
    },
  };
}

/**
 * A time axis: about one tick per 90 px of plot (so a 60-year series doesn't label every
 * year at 880 px, nor collide at 358), and any labels that still overlap dropped.
 */
const TIME_AXIS = { tickCount: { expr: "ceil(width / 90)" }, labelOverlap: "greedy" };

/** Date labels as short as the span allows: years over four years, months and years up to four, days within a year and a half. */
function dateFormat(t: Field): string {
  const span = spanYears(t);
  return span > 4 ? "%Y" : span > 1.5 ? "%b %Y" : "%b %d";
}

/** Bars for the largest groups of a category with many values. */
export const TOP = 20;

function starterRule(d: Dataset): Spec | null {
  const base: Spec = {
    $schema: SCHEMA,
    description: `Starter chart for ${d.name} from vega-datasets. Edit freely.`,
    data: { url: d.url },
  };
  if (d.format === "topojson" || d.format === "geojson") return geoFile(d, base);
  if (d.kind !== "table" || d.fields.length === 0) return null;
  if (d.format === "parquet" || d.format === "arrow") return null; // Vega-Lite needs extra loaders for these.

  const fields = d.fields.filter((f) => f.type !== "array");
  const rows = d.rows ?? 0;
  const measures = fields.filter((f) => isMeasure(f, fields));
  // Color only by informative categories (G-5); count and compare by categories that group rows, not by labels.
  const smallCat = fields.find((f) => colorable(f, 10, fields, rows));
  const cat = fields.find((f) => nominal(f, 60, fields));
  const colorBy = (f: Field | undefined): Enc => (f ? { color: category(f) } : {});

  // 1. Latitude/longitude columns → a point map.
  const lat = fields.find((f) => isLat(f) && f.profile.kind === "quantitative");
  const lon = fields.find((f) => isLon(f) && f.profile.kind === "quantitative");
  if (lat && lon) return pointMap(d, base, lat, lon, smallCat, measures);

  // 2. start/end columns → a timeline of ranges.
  const start = fields.find((f) => /^start$/i.test(f.name) && f.profile.kind === "quantitative");
  const end = fields.find((f) => /^end$/i.test(f.name) && f.profile.kind === "quantitative");
  const label = fields.find((f) => nominal(f, 80, fields));
  if (start && end && label) {
    return {
      ...base,
      width: 520,
      mark: { type: "bar", tooltip: true },
      encoding: {
        // Ranges read in time order, whatever order the labels are documented in.
        y: { field: fieldRef(label.name), type: "nominal", sort: { field: fieldRef(start.name) }, ...titled(label) },
        x: { field: fieldRef(start.name), type: "quantitative", scale: { zero: false }, axis: { format: "d" }, ...titled(start) },
        x2: { field: fieldRef(end.name) },
      },
    };
  }

  const [m1] = measures;

  // 3. Dates, or three or more years, with a measure → a time series.
  const t = timeField(fields);
  if (t && m1) return timeSeries(d, base, t, m1, fields);

  // 4. A small table of equal groups with two measures → small multiples, a panel per group (G-7).
  const pair = defaultPair(d, measures, "x");
  const by = pair ? panelsBy(d, fields) : undefined;
  if (pair && by) {
    return {
      ...base,
      columns: 2,
      facet: category(by, {}, "header"),
      spec: {
        width: PANEL_SIZE.wide,
        height: PANEL_SIZE.wide,
        mark: { type: "point", tooltip: true },
        encoding: {
          x: measure(pair.x, { scale: { zero: false } }),
          y: measure(pair.y, { scale: { zero: false } }),
        },
      },
    };
  }

  // 5. Two measures → a scatter plot, of the pair the Explore scatter opens on.
  if (pair) {
    return {
      ...base,
      width: 480,
      height: 360,
      mark: { type: "point", tooltip: true, opacity: rows > 5000 ? 0.3 : 0.8 },
      encoding: {
        x: measure(pair.x, scaled(pair.x, { zero: false })),
        y: measure(pair.y, scaled(pair.y, { zero: false })),
        ...colorBy(smallCat),
      },
    };
  }

  // 6. One measure, a category, and a two- or three-way grouping (two years) → a dot plot
  //    comparing the groups across the category with the fewest values (barley: its sites).
  const across = fields
    .filter((f) => nominal(f, 60, fields) && !unique(f, rows) && (distinctValues(f) ?? 0) > 3)
    .sort((a, b) => (distinctValues(a) ?? 0) - (distinctValues(b) ?? 0))[0];
  const compare = across && groupings(fields, rows).find((g) => g !== across && (distinctValues(g) ?? 0) <= 3);
  if (m1 && across && compare) {
    return {
      ...base,
      width: 480,
      mark: { type: "point", filled: true, size: 60, tooltip: true },
      encoding: {
        y: category(across, { sort: "-x" }, "axis"),
        x: measure(m1, { aggregate: summable(m1) ? "sum" : "mean", scale: { zero: false } }),
        color: category(compare, { type: "nominal" }),
      },
    };
  }

  // 7. A measure by category → sorted bars.
  if (m1 && cat) {
    return {
      ...base,
      width: 480,
      mark: { type: "bar", tooltip: true },
      encoding: {
        y: category(cat, { sort: "-x" }, "axis"),
        x: measure(m1, { aggregate: "mean" }, true),
      },
    };
  }

  // 8. A count by a category with many values → the largest twenty, summed (the busiest airports).
  const many = fields.find((f) => f.profile.kind === "nominal" && f.profile.distinct > 60 && !unique(f, rows) && !idName(f, fields));
  if (m1 && many && summable(m1)) {
    const taken = new Set(fields.map((f) => f.name));
    const free = (name: string): string => (taken.has(name) ? free(`_${name}`) : name);
    const [total, rank] = [free("total"), free("rank")];
    return {
      ...base,
      width: 480,
      transform: [
        ...(totalsOf(d, many).length ? [{ filter: `indexof(${tagged(totalsOf(d, many))}, ${tag(`datum[${JSON.stringify(many.name)}]`)}) < 0` }] : []),
        { aggregate: [{ op: "sum", field: m1.name, as: total }], groupby: [many.name] },
        { window: [{ op: "row_number", as: rank }], sort: [{ field: total, order: "descending" }] },
        { filter: `datum[${JSON.stringify(rank)}] <= ${TOP}` },
      ],
      mark: { type: "bar", tooltip: true },
      encoding: {
        y: { field: fieldRef(many.name), type: "nominal", sort: "-x", ...titled(many) },
        x: { field: total, type: "quantitative", title: `Sum of ${fieldTitle(m1)}` },
      },
    };
  }

  // 9. One measure → a histogram.
  if (m1) {
    return {
      ...base,
      width: 480,
      height: 260,
      mark: { type: "bar", tooltip: true },
      encoding: {
        x: measure(m1, { bin: { maxbins: 30 } }),
        y: { aggregate: "count", type: "quantitative" },
      },
    };
  }

  // 10. Two small-range integers (scores) → how often each pair of values occurs.
  const grid = fields.filter((f) => isOrdinalInt(f) && !isId(f, fields) && !isYear(f) && !TIME_PART.test(f.name) && (distinctValues(f) ?? 0) >= 3);
  if (grid.length >= 2) {
    const [gx, gy] = grid as [Field, Field];
    return {
      ...base,
      width: 480,
      transform: [{ filter: `isValid(datum[${JSON.stringify(gx.name)}]) && isValid(datum[${JSON.stringify(gy.name)}])` }],
      mark: { type: "rect", tooltip: true },
      encoding: {
        x: { field: fieldRef(gx.name), type: "ordinal", ...titled(gx) },
        y: { field: fieldRef(gy.name), type: "ordinal", sort: "descending", ...titled(gy) },
        color: { aggregate: "count", type: "quantitative" },
      },
    };
  }

  // 11. Only categories → counts, by a category that groups rows (a label unique to each row would make every bar one).
  const groupedBy = fields.find((f) => nominal(f, 60, fields) && !unique(f, rows));
  if (groupedBy) {
    return {
      ...base,
      width: 480,
      mark: { type: "bar", tooltip: true },
      encoding: {
        y: category(groupedBy, { sort: "-x" }, "axis"),
        x: { aggregate: "count", type: "quantitative" },
      },
    };
  }
  return null;
}

/** A Vega Editor link that opens `spec` (Vega-Lite). */
export function editorUrl(spec: Spec): string {
  return EDITOR + LZString.compressToEncodedURIComponent(JSON.stringify(spec, null, 2));
}

export function starterEditorUrl(d: Dataset): string | null {
  const spec = starterSpec(d);
  return spec ? editorUrl(spec) : null;
}
