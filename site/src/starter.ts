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
import type { Dataset, Field } from "./catalog";

type Spec = Record<string, unknown>;
type Enc = Record<string, unknown>;

const EDITOR = "https://vega.github.io/editor/#/url/vega-lite/";
const SCHEMA = "https://vega.github.io/schema/vega-lite/v6.json";

/** Vega-Lite treats `.` and `[ ]` in field names as nested access; escape them. */
function fieldRef(name: string): string {
  return name.replace(/([.[\]])/g, "\\$1");
}

const ID_NAME = /(^id$|_id$|^id_|code$|^code|^zip|zip_code|^key$|^index$|^cluster$|^source$|^target$|^group$|^fips)/i;
const ID_DESC = /\b(identifier|unique id|fips|code for|index of)\b/i;
const YEAR_NAME = /^(year|yr)$|year$/i;
const TIME_PART = /^(month|day|hour|minute|weekday)$/i;
/** Integer columns that group rows (age bands, sex codes) rather than measure them. */
const GROUPING = /^(age|sex|rank|level|grade)$/i;
const LAT = /^(lat|latitude)$/i;
const LON = /^(lon|lng|long|longitude)$/i;
const SERIES = /^(symbol|source|location|country|region|series|sex|gender|division|entity|variety|site|origin|species|type|category)$/i;

/** Numeric identifiers and codes: plotting them as measurements is meaningless. */
function isId(f: Field): boolean {
  if (ID_NAME.test(f.name)) return true;
  return f.profile.kind === "quantitative" && (/categor/i.test(f.name) || ID_DESC.test(f.description ?? ""));
}

function isYear(f: Field): boolean {
  const p = f.profile;
  return p.kind === "quantitative" && Number.isInteger(p.min) && p.min >= 1000 && p.max <= 2200 && (YEAR_NAME.test(f.name) || f.type === "integer");
}

/** Small-range integers (cylinders, ratings, ages in bands) behave like categories, not measures. */
function isOrdinalInt(f: Field): boolean {
  const p = f.profile;
  return p.kind === "quantitative" && f.type === "integer" && p.max - p.min <= 12;
}

function isMeasure(f: Field): boolean {
  return (
    f.profile.kind === "quantitative" &&
    !isId(f) &&
    !isYear(f) &&
    !isOrdinalInt(f) &&
    !TIME_PART.test(f.name) &&
    !(f.type === "integer" && GROUPING.test(f.name)) &&
    !LAT.test(f.name) &&
    !LON.test(f.name)
  );
}

function nominal(f: Field, max: number): boolean {
  return f.profile.kind === "nominal" && f.profile.distinct >= 2 && f.profile.distinct <= max && !ID_NAME.test(f.name);
}

function spanYears(f: Field): number {
  const p = f.profile;
  if (p.kind !== "temporal") return 0;
  return (new Date(p.max).getTime() - new Date(p.min).getTime()) / (365.25 * 864e5);
}

const PROJECTION: Record<string, string> = {
  us_10m: "albersUsa",
  world_110m: "equalEarth",
  london_boroughs: "mercator",
  london_tube_lines: "mercator",
  earthquakes: "equalEarth",
};

function geoFile(d: Dataset, base: Spec): Spec | null {
  if (d.format === "topojson") {
    const feature = d.objects?.[0];
    if (!feature) return null;
    return {
      ...base,
      width: 600,
      height: 400,
      data: { url: d.url, format: { type: "topojson", feature } },
      projection: { type: PROJECTION[d.name] ?? "equalEarth" },
      mark: { type: "geoshape", stroke: "white", strokeWidth: 0.5 },
    };
  }
  return {
    ...base,
    width: 600,
    height: 360,
    data: { url: d.url, format: { type: "json", property: "features" } },
    projection: { type: PROJECTION[d.name] ?? "equalEarth" },
    mark: { type: "geoshape" },
  };
}

function pointMap(d: Dataset, base: Spec, lat: Field, lon: Field, color: Field | undefined): Spec {
  const la = lat.profile;
  const lo = lon.profile;
  // Mostly-US coordinates read best on the Albers USA projection.
  const us =
    la.kind === "quantitative" && lo.kind === "quantitative" &&
    lo.min >= -180 && lo.max <= -60 && la.min >= 15 && la.max <= 72;
  return {
    ...base,
    width: 600,
    height: 380,
    projection: { type: us ? "albersUsa" : "equalEarth" },
    mark: { type: "circle", size: (d.rows ?? 0) > 5000 ? 4 : 16, opacity: 0.7, tooltip: true },
    encoding: {
      longitude: { field: fieldRef(lon.name), type: "quantitative" },
      latitude: { field: fieldRef(lat.name), type: "quantitative" },
      ...(color ? { color: { field: fieldRef(color.name), type: "nominal" } } : {}),
    },
  };
}

export function starterSpec(d: Dataset): Spec | null {
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
  const measures = fields.filter(isMeasure);
  const temporal = fields.filter((f) => f.profile.kind === "temporal");
  const years = fields.filter(isYear);
  const smallCat = fields.find((f) => nominal(f, 10));
  const seriesCat = fields.find((f) => nominal(f, 12) && SERIES.test(f.name));
  const cat = fields.find((f) => nominal(f, 60));
  const colorBy = (f: Field | undefined): Enc => (f ? { color: { field: fieldRef(f.name), type: "nominal" } } : {});

  // 1. Latitude/longitude columns → a point map.
  const lat = fields.find((f) => LAT.test(f.name) && f.profile.kind === "quantitative");
  const lon = fields.find((f) => LON.test(f.name) && f.profile.kind === "quantitative");
  if (lat && lon) return pointMap(d, base, lat, lon, smallCat);

  // 2. start/end columns → a timeline of ranges.
  const start = fields.find((f) => /^start$/i.test(f.name) && f.profile.kind === "quantitative");
  const end = fields.find((f) => /^end$/i.test(f.name) && f.profile.kind === "quantitative");
  const label = fields.find((f) => nominal(f, 80));
  if (start && end && label) {
    return {
      ...base,
      width: 520,
      mark: { type: "bar", tooltip: true },
      encoding: {
        y: { field: fieldRef(label.name), type: "nominal", sort: { field: fieldRef(start.name) } },
        x: { field: fieldRef(start.name), type: "quantitative", scale: { zero: false }, axis: { format: "d" } },
        x2: { field: fieldRef(end.name) },
      },
    };
  }

  const [m1, m2] = measures;

  // 3. Dates or years with a measure → a time series.
  const t = temporal[0];
  if (t && m1) {
    const unit = rows > 1000 ? (spanYears(t) > 20 ? "year" : "yearmonth") : undefined;
    return {
      ...base,
      width: 640,
      height: 300,
      mark: { type: "line", interpolate: "monotone", tooltip: true },
      encoding: {
        x: { field: fieldRef(t.name), type: "temporal", ...(unit ? { timeUnit: unit } : {}) },
        y: { field: fieldRef(m1.name), type: "quantitative", ...(unit || seriesCat ? { aggregate: "mean" } : {}) },
        ...colorBy(seriesCat),
      },
    };
  }
  const y = years[0];
  if (y && m1) {
    return {
      ...base,
      width: 640,
      height: 300,
      mark: { type: "line", point: rows <= 60, tooltip: true },
      encoding: {
        x: { field: fieldRef(y.name), type: "quantitative", scale: { zero: false }, axis: { format: "d" } },
        y: { field: fieldRef(m1.name), type: "quantitative", aggregate: "mean" },
        ...colorBy(seriesCat),
      },
    };
  }

  // 4. Two measures → a scatter plot.
  if (m1 && m2) {
    return {
      ...base,
      width: 480,
      height: 360,
      mark: { type: "point", tooltip: true, opacity: rows > 5000 ? 0.3 : 0.8 },
      encoding: {
        x: { field: fieldRef(m1.name), type: "quantitative", scale: { zero: false } },
        y: { field: fieldRef(m2.name), type: "quantitative", scale: { zero: false } },
        ...colorBy(smallCat),
      },
    };
  }

  // 5. A measure by category → sorted bars.
  if (m1 && cat) {
    return {
      ...base,
      width: 480,
      mark: { type: "bar", tooltip: true },
      encoding: {
        y: { field: fieldRef(cat.name), type: "nominal", sort: "-x" },
        x: { field: fieldRef(m1.name), type: "quantitative", aggregate: "mean" },
      },
    };
  }

  // 6. One measure → a histogram.
  if (m1) {
    return {
      ...base,
      width: 480,
      height: 260,
      mark: { type: "bar", tooltip: true },
      encoding: {
        x: { field: fieldRef(m1.name), type: "quantitative", bin: { maxbins: 30 } },
        y: { aggregate: "count", type: "quantitative" },
      },
    };
  }

  // 7. Only categories → counts.
  if (cat) {
    return {
      ...base,
      width: 480,
      mark: { type: "bar", tooltip: true },
      encoding: {
        y: { field: fieldRef(cat.name), type: "nominal", sort: "-x" },
        x: { aggregate: "count", type: "quantitative" },
      },
    };
  }
  return null;
}

export function starterEditorUrl(d: Dataset): string | null {
  const spec = starterSpec(d);
  if (!spec) return null;
  return EDITOR + LZString.compressToEncodedURIComponent(JSON.stringify(spec, null, 2));
}
