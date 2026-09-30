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
import { balanced, distinctValues, idName, informative, inUs, JAGGED, namedAxes, mostlyZero, nearDuplicate, PALETTE_SIZE, POWER_LABEL, scaleFor, scaleType, SERIES_LIMIT, summable, TABLEAU10, timeKey, totalsOf, unique, withScale } from "./chart-rules";

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
/** The source fields a spec's transforms read (aggregate and window fields, groupings, sorts), as field references, not the ones they compute. */
function transformFields(spec: Spec): string[] {
  const computed = new Set<string>();
  const read: string[] = [];
  for (const t of (spec.transform as Enc[] | undefined) ?? []) {
    const ops = [...((t.aggregate as Enc[] | undefined) ?? []), ...((t.window as Enc[] | undefined) ?? [])];
    for (const op of ops) if (typeof op.field === "string" && !computed.has(op.field)) read.push(op.field);
    for (const g of (t.groupby as string[] | undefined) ?? []) if (!computed.has(g)) read.push(g);
    for (const s of (t.sort as Enc[] | undefined) ?? []) if (typeof s.field === "string" && !computed.has(s.field)) read.push(s.field);
    for (const f of (t.fold as string[] | undefined) ?? []) if (!computed.has(f)) read.push(f);
    for (const op of ops) if (typeof op.as === "string") computed.add(op.as);
    if (typeof t.as === "string") computed.add(t.as);
    if (Array.isArray(t.as)) for (const a of t.as) computed.add(String(a));
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
  const used = new Set([...Object.values(encoding).map((e) => e.field), (spec.facet as Enc | undefined)?.field, ...transformFields(spec)]);
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
  // Empty cells reach the color domain as a value of their own (Codex round 5, #3).
  const empty = f.profile.kind === "nominal" && f.profile.missing > 0 ? 1 : 0;
  return nominal(f, max - empty, fields) && informative(f, rows);
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

/** The unit a long daily or hourly series is averaged into (the builder's `timeSteps` unit): days, months or years by its span; none for a smaller table. */
function timeUnit(d: Dataset, t: Field): string | undefined {
  const unit = d.timeSteps?.[t.name]?.unit;
  return unit && unit !== "none" ? unit : undefined;
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
/** A small multiple's size (px): two columns fit the narrowest phone's Explore column (288 px at 320). */
export const PANEL_SIZE = { wide: 180, phone: 100 };

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
  return outline(basemapUrl(d), "countries", usOnly ? [{ filter: "datum.id == 840" }] : []);
}

/** A basemap layer: a TopoJSON file of vega-datasets (beside the dataset's own) and its feature, in the quiet gray. */
function outline(url: string, feature: string, transform: Spec[] = []): Spec {
  return {
    data: { url, format: { type: "topojson", feature } },
    ...(transform.length ? { transform } : {}),
    mark: { type: "geoshape", fill: "#8a8f98", fillOpacity: 0.14, stroke: "#8a8f98", strokeOpacity: 0.5, strokeWidth: 0.5, clip: true, aria: false },
  };
}

type Box = { longitude: [number, number]; latitude: [number, number] };

/**
 * The detailed basemaps vega-datasets has for points close together (S9), and where they
 * reach: Greater London's boroughs; the United States' counties (for points nearly all in
 * the US, the builder's share). Past them, the world's countries.
 */
const DETAILED_BASEMAPS: { file: string; feature: string; box?: Box; us?: true }[] = [
  { file: "londonBoroughs.json", feature: "boroughs", box: { longitude: [-0.52, 0.34], latitude: [51.28, 51.7] } },
  { file: "us-10m.json", feature: "counties", us: true },
];

/** The most detailed basemap that holds a box of points (the first in DETAILED_BASEMAPS), else the world's countries. */
function basemapFor(d: Dataset, box: Box, us: boolean): Spec {
  const inside = (b: Box) => box.longitude[0] >= b.longitude[0] && box.longitude[1] <= b.longitude[1] && box.latitude[0] >= b.latitude[0] && box.latitude[1] <= b.latitude[1];
  const map = DETAILED_BASEMAPS.find((m) => (m.box ? inside(m.box) : !!m.us && us));
  return map ? outline(d.url.replace(/[^/]+$/, map.file), map.feature) : basemap(d, false);
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
      // Colored by id within the palette's ten colors, as every categorical color is (S1, S14):
      // more lines than that (the tube's 13) are one color, named by their tooltip.
      ...(lines
        ? (d.objectIds?.[feature] ?? Infinity) <= PALETTE_SIZE
          ? { encoding: { color: { field: "id", type: "nominal", title: "id" }, tooltip: { field: "id", type: "nominal" } } }
          : { mark: { type: "geoshape", filled: false, strokeWidth: 1.5, stroke: TABLEAU10[0], aria: false }, encoding: { tooltip: { field: "id", type: "nominal", title: "id" } } }
        : {}),
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
  // Close together (a city): a Mercator map fitted to the middle 98% of the points, over the
  // most detailed basemap that holds them (S9). The rest of the points are left out, and said so.
  const close = !!box && !wide;
  // A world projection fits the middle 98% of the points, so a few far ones don't shrink the
  // rest; Albers USA fits the country.
  const fit = box && (close || (!us && where && wide)) ? { fit: boxFeature(where?.box ?? box) } : {};
  const direction = measures.find((m) => DIRECTION.test(m.name) && m.profile.kind === "quantitative" && m.profile.min >= 0 && m.profile.max <= 360);
  const strength = direction && measures.find((m) => m !== direction && !DIRECTION.test(m.name) && !nearDuplicate(d, m.name, direction.name));
  const clip = { clip: true };
  const points: Spec = {
    // Albers USA has no place for points outside the US (they would pile up in a corner).
    ...(us && !close ? { transform: [{ filter: inUs(lon.name, lat.name) }] } : {}),
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
  const type = close ? "mercator" : us ? "albersUsa" : "equalEarth";
  const frame: Spec = { ...base, width: 600, height: 380, projection: { type, ...rotate, ...fit } };
  const { data, ...rest } = frame;
  // Every point map has a basemap (S9): without one, points are a scatter with no place.
  const under = close ? basemapFor(d, box, us) : basemap(d, us);
  return { ...rest, layer: [under, { data, ...points }] };
}

/** The starter chart; `phone` for a phone's narrow column (fewer series per chart, S1). */
export function starterSpec(d: Dataset, phone = false): Spec | null {
  return withMetadata(d, starterRule(d, phone));
}

/** The "Total" mode's chart: a detected total's own line (CHART-STANDARDS.md S2); null without one. */
export function totalSpec(d: Dataset, phone = false): Spec | null {
  const fields = d.fields.filter((f) => f.type !== "array");
  const t = timeField(fields);
  const m = fields.find((f) => isMeasure(f, fields));
  if (!t || !m || !totalOf(d)) return null;
  const base: Spec = { $schema: SCHEMA, description: `The total of ${d.name} over time, from vega-datasets. Edit freely.`, data: { url: d.url } };
  return withMetadata(d, timeSeries(d, base, t, m, fields, phone, "total"));
}

/** The series a time chart splits by, and what to do with them (CHART-STANDARDS.md S1, S2). */
interface Split {
  series: Field | undefined;
  /** The series' values that are totals of the others for the measure: drawn in their own "Total" mode, never with the parts. */
  totals: string[];
  /** More parts than lines can tell apart: a heatmap of time by series. */
  heatmap: boolean;
}

/** Series values a heatmap can still label down its y axis. */
const HEATMAP_MAX = 40;

function splitOf(d: Dataset, t: Field, m: Field, fields: Field[], phone: boolean): Split {
  const rows = d.rows ?? 0;
  const key = timeKeyOf(d, t);
  const limit = phone ? SERIES_LIMIT.phone : SERIES_LIMIT.wide;
  // Empty cells are a value of their own on a chart (a line, a row of a heatmap).
  const parts = (f: Field) => (distinctValues(f) ?? 0) + (f.profile.kind === "nominal" && f.profile.missing ? 1 : 0) - totalsOf(d, f, m).length;
  // A key's category the palette (or a heatmap) can show part by part.
  const inKey = key?.find((f) => nominal(f, HEATMAP_MAX + totalsOf(d, f, m).length, fields) && informative(f, rows) && parts(f) >= 2);
  const series = inKey ?? fields.find((f) => colorable(f, limit, fields, rows) && SERIES.test(f.name));
  if (!series) return { series: undefined, totals: [], heatmap: false };
  const totals = totalsOf(d, series, m);
  const heatmap = parts(series) > limit;
  // Beyond the palette with no heatmap to go to (a named series outside the key): no color.
  if (!inKey && heatmap) return { series: undefined, totals: [], heatmap: false };
  return { series, totals, heatmap };
}

/** Rows without a series' totals: the parts, which a chart shows without their sum. */
function withoutTotals(f: Field, totals: string[]): Spec[] {
  return totals.length ? [{ filter: `indexof(${tagged(totals)}, ${tag(`datum[${JSON.stringify(f.name)}]`)}) < 0` }] : [];
}

/** Only a series' totals: the "Total" mode's line. */
function onlyTotals(f: Field, totals: string[]): Spec[] {
  return [{ filter: `indexof(${tagged(totals)}, ${tag(`datum[${JSON.stringify(f.name)}]`)}) >= 0` }];
}

/**
 * The detected total a time series shows in a mode of its own (disasters' "All natural
 * disasters"), or null when its series has none.
 */
export function totalOf(d: Dataset): { series: Field; totals: string[] } | null {
  const fields = d.fields.filter((f) => f.type !== "array");
  const t = timeField(fields);
  const m = fields.find((f) => isMeasure(f, fields));
  if (!t || !m || d.kind !== "table" || d.format === "parquet" || d.format === "arrow") return null;
  if (fields.some((f) => isLat(f)) && fields.some((f) => isLon(f))) return null;
  const { series, totals } = splitOf(d, t, m, fields, false);
  return series && totals.length ? { series, totals } : null;
}

/**
 * Where a line would cross a gap in its times (CHART-STANDARDS.md S3): a segment number per
 * run of times without a gap, per series, for `detail`, so the line breaks there. Built from
 * the builder's widest regular gap (1.5 times the 90th percentile); null when there is none.
 */
function segments(d: Dataset, t: Field, series: Field | undefined, fields: Field[]): { transform: Spec[]; field: string } | null {
  const steps = d.timeSteps?.[t.name];
  if (!steps) return null;
  const taken = new Set(fields.map((f) => f.name));
  const free = (name: string): string => (taken.has(name) ? free(`_${name}`) : name);
  const [prev, jump, run] = [free("previous"), free("gap"), free("segment")];
  // Times as numbers: years as they are, dates in milliseconds (the builder's step is in seconds).
  const ms = t.profile.kind === "temporal" ? 1000 : 1;
  const limit = steps.breakAt * ms;
  const value = (expr: string) => `toNumber(${expr})`;
  const groupby = series ? [fieldRef(series.name)] : [];
  return {
    field: run,
    transform: [
      { window: [{ op: "lag", field: fieldRef(t.name), as: prev }], sort: [{ field: fieldRef(t.name) }], groupby },
      { calculate: `isValid(datum[${JSON.stringify(prev)}]) && ${value(`datum[${JSON.stringify(t.name)}]`)} - ${value(`datum[${JSON.stringify(prev)}]`)} > ${limit} ? 1 : 0`, as: jump },
      { window: [{ op: "sum", field: jump, as: run }], sort: [{ field: fieldRef(t.name) }], groupby },
    ],
  };
}

/**
 * How the builder saw a line drawn this way, bucketed by the time's unit: its jaggedness
 * and gaps. Its lines are split by the series ("series"), by the time's key, one row each
 * (""), or not at all, one line of all rows, a mean or sum over the key ("*").
 */
function shapeOf(d: Dataset, t: Field, m: Field, series: Field | undefined, exactKey: boolean) {
  const shapes = d.lineShapes?.[t.name]?.[m.name];
  return shapes?.[series ? series.name : exactKey ? "" : "*"] ?? shapes?.[""];
}

/** Is a measure too jagged along the time for lines (CHART-STANDARDS.md S5), on the scale it's drawn with? */
function jagged(d: Dataset, t: Field, m: Field, series: Field | undefined, exactKey: boolean): boolean {
  const shape = shapeOf(d, t, m, series, exactKey);
  const j = scaleType(m) === "log" && shape?.jagLog !== undefined ? shape.jagLog : shape?.jag;
  return j !== undefined && j > JAGGED;
}

/**
 * A line over time (G-1, G-4) under the chart standards. The line never joins rows of
 * different series: when each time holds one row per series, the series colors the line
 * (up to SERIES_LIMIT of them: six, four on a phone, S1); with more, a heatmap of time by
 * series. Two year fields that index the rows together make one line per vintage (budgets).
 * A series value that is the total of the others is left out, into a "Total" mode of its
 * own (S2). Rows it can't keep apart are aggregated: counts summed, other measures averaged.
 * Unaggregated lines break at gaps in the times (S3) and give way to points when the
 * measure jumps too much from one time to the next (S5); a heavy tail gets a log axis (S4).
 */
function timeSeries(d: Dataset, base: Spec, t: Field, m: Field, fields: Field[], phone: boolean, mode: "chart" | "total" = "chart"): Spec {
  const date = t.profile.kind === "temporal";
  const key = timeKeyOf(d, t);
  const unit = timeUnit(d, t);
  const split = splitOf(d, t, m, fields, phone);
  const total = mode === "total" && split.series && split.totals.length;
  // The Total mode draws one line per total; the chart's series never holds its totals.
  const series = total ? undefined : split.series;
  const vintage = !split.series && key?.length === 1 && timeYear(key[0]!) ? key[0] : undefined;
  const splitBy = total ? split.series : series;
  const exact = key !== null && key.every((f) => f === splitBy || f === vintage);
  // Sum only across the separate groups of one time (people of each age in a year), never
  // across times merged into one bucket (a daily population is not a monthly one).
  const merges = !!unit && !(d.timeKeyBuckets?.[t.name] ?? []).includes(unit);
  // Never across a category where a value looks like the total of the others (it would count
  // everything twice): average there.
  const addsTotal = key?.some((g) => g !== splitBy && g !== vintage && totalsOf(d, g, m).length > 0) ?? false;
  const sums = !exact && key !== null && !merges && !addsTotal && summable(m);
  const aggregate = unit || !exact ? (sums ? "sum" : "mean") : undefined;
  const [along, lines] = vintage && (distinctValues(vintage) ?? 0) > (distinctValues(t) ?? 0) ? [vintage, t] : [t, vintage];
  const unitName = unit ? (along.profile.kind === "temporal" && along.profile.utc ? `utc${unit}` : unit) : undefined;
  const q = m.profile;
  // Values in a narrow band far from zero (CO2 in ppm, air pressure) leave zero off the axis, or the line is flat.
  const band = aggregate !== "sum" && q.kind === "quantitative" && q.min > 0 && q.min >= q.max / 2;
  const keep = split.series && split.totals.length ? (total ? onlyTotals(split.series, split.totals) : withoutTotals(split.series, split.totals)) : [];
  const x = date
    ? { field: fieldRef(along.name), type: "temporal", ...(unitName ? { timeUnit: unitName } : {}), axis: { ...TIME_AXIS, format: dateFormat(along) }, ...titled(along) }
    : // S11: the axis ends at the data, not at a rounded year past it (1880-2023 ran to 2040).
      measure(along, { scale: { zero: false, nice: false }, axis: { format: "d", ...TIME_AXIS } });

  if (!total && series && split.heatmap) {
    // S1: more series than lines can tell apart. Time across, series down, the measure as a
    // sequential color (log when it spans three decades or more, S4). S13: a count colors
    // rows by their size (the largest industry darkest in every month); a rate of the same
    // rows shows change over time instead, so it colors when the table has one.
    const hue = rateOf(m, fields) ?? m;
    const s = scaleFor(hue);
    const heatX = date
      ? unitName || !heatUnit(along).endsWith("yearmonthdate")
        ? // Buckets of a unit across the span: a time axis (a cell as wide as its bucket), ticks and labels as the line chart's, not one per column.
          { field: fieldRef(along.name), type: "temporal", timeUnit: unitName ?? heatUnit(along), axis: { ...TIME_AXIS, format: dateFormat(along) }, ...titled(along) }
        : // A few dates, irregular: a column each (on a time axis a day's cell would be a sliver).
          { field: fieldRef(along.name), type: "ordinal", timeUnit: heatUnit(along), axis: { labelOverlap: "greedy", labelSeparation: 8, labelAngle: 0, format: spanYears(along) > 1.5 ? "%b %d, %Y" : "%b %d", ...titled(along) }, ...titled(along) }
      : // S12: years labeled at round steps (decades), not every fifth column from wherever it starts.
        { field: fieldRef(along.name), type: "ordinal", axis: { labelOverlap: "greedy", labelSeparation: 8, labelAngle: 0, ticks: false, labelExpr: roundYears(along, phone) }, ...titled(along) };
    const parts = (distinctValues(series) ?? 0) - split.totals.length;
    return {
      ...base,
      width: 640,
      height: parts * 20,
      usermeta: { chart: "time" },
      ...(keep.length ? { transform: keep } : {}),
      mark: { type: "rect", tooltip: true },
      encoding: {
        x: heatX,
        y: (() => {
          const y = category(series, {}, "axis");
          return phone ? { ...y, axis: { ...(y.axis as Enc | undefined), labelLimit: PHONE_ROW_LABELS } } : y;
        })(),
        color: (() => {
          const c = measure(hue, aggregate ? { aggregate: hue === m ? aggregate : "mean" } : {});
          // S10: a log color's legend labels its decades, not only its ends.
          const decades = s.scale.type ? ((s.axis.values as number[] | undefined) ?? []).filter((v) => v === 0 || Number.isInteger(Math.log10(Math.abs(v)))) : [];
          const legend = decades.length >= 3 ? { legend: { values: decades, labelExpr: POWER_LABEL, gradientLength: 200, labelOverlap: "greedy", titlePadding: 10 } } : {};
          return { ...c, scale: { ...(c.scale as Enc | undefined), ...(s.scale.type ? { type: s.scale.type } : {}), ...(s.scale.type === "symlog" ? { constant: s.scale.constant } : {}), scheme: "blues" }, ...legend };
        })(),
      },
    };
  }

  // S16: a measure mostly zero (bird strikes' costs) has no mean worth charting: its records
  // per bucket instead, when the line isn't split (a split needs the measure per series).
  const counted = !series && !lines && !!unit && mostlyZero(m);
  const lined = series ? seriesLines(series) : null;
  // S3, S5: a line breaks at gaps in its times, and gives way to points when it's too jagged.
  // (Rows aggregated per time, without a time unit, share their time's segment, so the sum or
  // mean is unchanged. Buckets of a time unit that skip one draw as points instead.)
  const skips = !counted && (shapeOf(d, along, m, series, exact)?.gaps ?? false);
  const gaps = !unit && !lines && skips ? segments(d, along, splitBy, fields) : null;
  const points = (!!unit && skips) || (!counted && jagged(d, along, m, series, exact));
  const mark = points
    ? { type: "point", filled: true, size: 24, tooltip: true }
    : // S8: straight segments between the values (a curve would invent peaks and troughs), and
      // the values marked where there are few enough to see (or a gap splits the line).
      { type: "line", tooltip: true, ...(gaps || timesAlong(along, unit) <= FEW_POINTS ? { point: { size: date ? 16 : 24 } } : {}) };
  const transform = [...keep, ...(gaps && !points ? gaps.transform : [])];
  return {
    ...base,
    width: 640,
    height: 300,
    usermeta: { chart: total ? "total" : "time" },
    ...(transform.length ? { transform } : {}),
    ...(lined ? { params: lined.params } : {}),
    mark,
    encoding: {
      x,
      // A mean stays within the measure's values, so it keeps the measure's scale (S4: a heavy
      // tail's mean is still heavy-tailed); a sum's range is not the measure's.
      y: counted
        ? { aggregate: "count", type: "quantitative" }
        : measure(m, aggregate === "sum" ? { aggregate, ...(band ? { scale: { zero: false } } : {}) } : { ...(aggregate ? { aggregate } : {}), ...(keep.length ? fitted(m, band) : scaled(m, band ? { zero: false } : {})) }),
      ...(lined ? lined.encoding : {}),
      ...(gaps && !points ? { detail: { field: gaps.field, type: "nominal" } } : {}),
      ...(lines
        ? {
            color: { field: fieldRef(lines.name), type: "quantitative", legend: { format: "d" }, ...titled(lines) },
            detail: { field: fieldRef(lines.name), type: "quantitative" },
          }
        : {}),
    },
  };
}

/** A series' lines: colored by it, with legend isolation (click a series; the others fade). */
function seriesLines(f: Field): { encoding: Enc; params: Spec[] } {
  return {
    params: [{ name: "series", select: { type: "point", fields: [fieldRef(f.name)] }, bind: "legend" }],
    encoding: { color: category(f), opacity: { condition: { param: "series", empty: true, value: 1 }, value: 0.15 } },
  };
}

/**
 * A time axis: about one tick per 90 px of plot (so a 60-year series doesn't label every
 * year at 880 px, nor collide at 358), and any labels that still overlap dropped.
 */
const TIME_AXIS = { tickCount: { expr: "ceil(width / 90)" }, labelOverlap: "greedy", labelSeparation: 8, labelFlush: true };

/** Date labels as short as the span allows: years over four years, months and years up to four, days within a year and a half. */
function dateFormat(t: Field): string {
  const span = spanYears(t);
  return span > 4 ? "%Y" : span > 1.5 ? "%b %Y" : "%b %d";
}

/**
 * A measure's scale fitted to the rows a chart draws, when it draws some of them (the Total
 * mode's totals, the parts without them): a log scale spans whole decades around those rows
 * (S4), not the whole column's range (the totals' 300 to 3.7 million, not 1 to 3.7 million).
 */
function fitted(m: Field, band: boolean): Enc {
  if (scaleType(m) !== "log") return scaled(m, band ? { zero: false } : {});
  // Labels at the decades only (Vega's log ticks also mark 2 to 9 of each, which crowd).
  const decade = "abs(log(datum.value) / LN10 - round(log(datum.value) / LN10)) < 1e-6";
  return { scale: { type: "log", nice: true }, axis: { labelExpr: `${decade} ? (${POWER_LABEL}) : ''` } };
}

/** A rate among a count's fields (the same rows as a percentage): what a heatmap colors by (S13). */
function rateOf(m: Field, fields: Field[]): Field | undefined {
  if (!summable(m)) return undefined;
  return fields.find((f) => f !== m && f.profile.kind === "quantitative" && isMeasure(f, fields) && RATE_NAME.test(`${f.name} ${f.title ?? ""} ${f.description ?? ""}`));
}
const RATE_NAME = /\b(rate|percent|percentage|share|ratio)\b/i;

/**
 * Labels for a band axis of years at a round step (S12): 1, 2, 5, 10, 20, 25, 50 or 100
 * years, whichever leaves at most 12 labels (5 on a phone). An expression, not a list of
 * values: a CSV file's years reach the band scale as text, a JSON file's as numbers.
 */
function roundYears(t: Field, phone: boolean): string | undefined {
  const p = t.profile;
  if (p.kind !== "quantitative") return undefined;
  const most = phone ? 5 : 12;
  const step = [1, 2, 5, 10, 20, 25, 50, 100, 200, 500].find((k) => Math.floor(p.max / k) - Math.ceil(p.min / k) + 1 <= most) ?? 1000;
  return `toNumber(datum.value) % ${step} === 0 ? datum.label : ''`;
}

/**
 * Whether a time chart of `m` along `t` has something to show (S15): its lines (or heatmap
 * rows) have three or more times each, as the builder counted them (the median series).
 * Candidates each reporting on a date or two (political contributions) make a grid of
 * isolated cells, not a change over time.
 */
function timeSeriesHolds(d: Dataset, t: Field, m: Field, fields: Field[], phone: boolean): boolean {
  const { series } = splitOf(d, t, m, fields, phone);
  if (!series) return true;
  const per = d.lineShapes?.[t.name]?.[m.name]?.[series.name]?.perSeries;
  return per === undefined || per >= 3;
}

/** The words that name a measure's kind ("Deaths from Wounds", "Deaths from Disease"): its description's first two words. */
function kindOf(f: Field): string | null {
  const words = (f.description ?? "").toLowerCase().match(/[a-z]+/g) ?? [];
  return words.length >= 3 ? words.slice(0, 2).join(" ") : null;
}

/**
 * Measures of one kind, drawn together (S17): two to SERIES_LIMIT of them (four on a phone)
 * whose descriptions open with the same two words ("Deaths from …"), all non-negative
 * integers, in a table with one row per time (crimea: deaths from disease, wounds and other
 * causes each month, Nightingale's classic view). The first measure's kind decides; the
 * rule is conservative on purpose: a shared unit is claimed only where the metadata says so.
 */
function sameKind(d: Dataset, t: Field, measures: Field[], phone: boolean): Field[] | null {
  const kind = measures[0] ? kindOf(measures[0]) : null;
  if (!kind || timeKeyOf(d, t)?.length !== 0) return null;
  const kin = measures.filter((m) => m !== t && kindOf(m) === kind && m.type === "integer" && m.profile.kind === "quantitative" && m.profile.min >= 0);
  return kin.length >= 2 && kin.length <= (phone ? SERIES_LIMIT.phone : SERIES_LIMIT.wide) ? kin : null;
}

/** Several measures of one kind over time (S17): folded into one colored line each, on one axis, with legend isolation. */
function foldedSeries(d: Dataset, base: Spec, t: Field, kin: Field[]): Spec {
  const taken = new Set(d.fields.map((f) => f.name));
  const free = (name: string): string => (taken.has(name) ? free(`_${name}`) : name);
  const [key, value] = [free("measure"), free("value")];
  const date = t.profile.kind === "temporal";
  const x = date
    ? { field: fieldRef(t.name), type: "temporal", axis: { ...TIME_AXIS, format: dateFormat(t) }, ...titled(t) }
    : measure(t, { scale: { zero: false, nice: false }, axis: { format: "d", ...TIME_AXIS } });
  const title = (f: Field) => fieldTitle(f);
  // The axis names the kind by its first word ("Deaths").
  const kind = (kindOf(kin[0]!) ?? "value").split(" ")[0]!;
  return {
    ...base,
    width: 640,
    height: 300,
    usermeta: { chart: "time" },
    transform: [
      { fold: kin.map((m) => fieldRef(m.name)), as: [key, value] },
      // The legend names each measure by its title.
      ...(kin.some((m) => title(m) !== m.name) ? [{ calculate: `${JSON.stringify(Object.fromEntries(kin.map((m) => [m.name, title(m)])))}[datum[${JSON.stringify(key)}]]`, as: key }] : []),
    ],
    params: [{ name: "series", select: { type: "point", fields: [key] }, bind: "legend" }],
    mark: { type: "line", tooltip: true, ...(timesAlong(t, undefined) <= FEW_POINTS ? { point: { size: date ? 16 : 24 } } : {}) },
    encoding: {
      x,
      y: { field: value, type: "quantitative", title: kind.charAt(0).toUpperCase() + kind.slice(1) },
      color: { field: key, type: "nominal", title: null, scale: { domain: kin.map(title) } },
      opacity: { condition: { param: "series", empty: true, value: 1 }, value: 0.15 },
    },
  };
}

/** A line marks its values when it has at most this many (S8). */
export const FEW_POINTS = 60;

/** How many times a line along `t` draws: its buckets of `unit` across the span, else its distinct values. */
function timesAlong(t: Field, unit: string | undefined): number {
  const perYear: Record<string, number> = { yearmonthdate: 365.25, yearmonth: 12, year: 1 };
  return unit ? Math.floor(spanYears(t) * (perYear[unit.replace(/^utc/, "")] ?? 1)) + 1 : (distinctValues(t) ?? Infinity);
}

/** A heatmap's columns, most: a date drawn as it is (no time unit) gets a column per day up to this many distinct dates. */
const HEATMAP_COLUMNS = 80;

/**
 * The columns of a heatmap along a date the line chart would draw as it is: a column per
 * day while the dates are few (a year of twenty report dates keeps its twenty), else per
 * year (a column per date would be slivers). In UTC when the dates are.
 */
function heatUnit(t: Field): string {
  const unit = (distinctValues(t) ?? Infinity) <= HEATMAP_COLUMNS ? "yearmonthdate" : "year";
  return t.profile.kind === "temporal" && t.profile.utc ? `utc${unit}` : unit;
}

/** A phone heatmap's row labels (px, then cut with an ellipsis): about a third of the narrowest column (288 px), so the cells and the legend above keep the rest. */
const PHONE_ROW_LABELS = 100;

/** Bars for the largest groups of a category with many values. */
export const TOP = 20;

function starterRule(d: Dataset, phone = false): Spec | null {
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
  const smallCat = fields.find((f) => colorable(f, PALETTE_SIZE, fields, rows));
  const cat = fields.find((f) => nominal(f, 60, fields));
  const colorBy = (f: Field | undefined): Enc => (f ? { color: category(f) } : {});

  // 1. Latitude/longitude columns → a point map: the pair the builder found (by name, a
  //    centroid's cx/cy, or x/y described as longitude and latitude, all values in range), else by name.
  const byName = (test: (f: Field) => boolean) => fields.find((f) => test(f) && f.profile.kind === "quantitative");
  const lat = d.points ? fields.find((f) => f.name === d.points!.latitude) : byName(isLat);
  const lon = d.points ? fields.find((f) => f.name === d.points!.longitude) : byName(isLon);
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

  // 3. Dates, or three or more years, with a measure → a time series: several measures of
  //    one kind on one chart (S17), else the first measure, when its series have times enough (S15).
  const t = timeField(fields);
  const kin = t && m1 ? sameKind(d, t, measures, phone) : null;
  if (t && kin) return foldedSeries(d, base, t, kin);
  if (t && m1 && timeSeriesHolds(d, t, m1, fields, phone)) return timeSeries(d, base, t, m1, fields, phone);

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
        { aggregate: [{ op: "sum", field: fieldRef(m1.name), as: total }], groupby: [fieldRef(many.name)] },
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
        // S10: an integer's bins step by whole numbers, labeled as integers (4, not 4.0).
        x: measure(m1, { bin: { maxbins: 30, ...(m1.type === "integer" ? { minstep: 1 } : {}) }, ...(m1.type === "integer" ? { axis: { format: "d" } } : {}) }),
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
        // S10: short labels upright (Vega-Lite turns an ordinal x axis's labels on their side).
        x: { field: fieldRef(gx.name), type: "ordinal", axis: { labelAngle: 0 }, ...titled(gx) },
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
