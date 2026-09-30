/**
 * What the Explore rules (DOSSIER §6.1, G-1 to G-7) read from a dataset: the metadata
 * first (documented categories and their order, titles, descriptions), then the field
 * profiles and the pairwise correlations the catalog builder records. No dataset names:
 * every rule reads structure, so a table added tomorrow gets the same treatment.
 *
 * The visual standards the rules meet (S1 to S17: series limits, totals, gaps, log axes, lines,
 * maps, labels, time axes), with their rationale and the tests that hold them: site/CHART-STANDARDS.md.
 */
import { categoryValues, type Dataset, documentedRange, type Field, orderedCategories } from "./catalog";

type Enc = Record<string, unknown>;

// --- G-2: log and symlog scales for heavy tails ----------------------------------------------

export type ScaleType = "linear" | "log" | "symlog";

/**
 * How many decades (powers of ten) a measure's positive values span: from its minimum, or
 * from its smallest positive value when it also holds zero or less; 0 when that can't be told.
 */
export function decades(f: Field): number {
  const p = f.profile;
  if (p.kind !== "quantitative" || p.max <= 0) return 0;
  const low = p.min > 0 ? p.min : p.minPositive;
  return low && low > 0 ? Math.log10(p.max / low) : 0;
}

/** The share of values in the first of the profile's histogram bins. */
function firstBinShare(f: Field): number {
  const bins = f.profile.kind === "quantitative" ? f.profile.bins : [];
  const total = bins.reduce((a, b) => a + b, 0);
  return total ? bins[0]! / total : 0;
}

/**
 * The scale a measure reads best on. A heavy tail is positive values spanning three or more
 * decades, or two or more with over a third of them piled into the histogram's first bin (a
 * linear axis then squeezes most points against zero): `log`. Values with zeros or negatives,
 * or a documented range reaching zero (a log axis can't), get `symlog` under a stricter test.
 * The first-bin share alone isn't enough: a count from 1 to 50 with a long tail gains little.
 */
export function scaleType(f: Field): ScaleType {
  const p = f.profile;
  if (p.kind !== "quantitative") return "linear";
  const span = decades(f);
  const piled = firstBinShare(f) > 1 / 3;
  const documentedMin = documentedRange(f)?.min;
  if (p.min > 0) {
    if (span < 3 && !(span >= 2 && piled)) return "linear";
    // The documented range reaches zero: an axis that must show it can't be log.
    return documentedMin !== undefined && documentedMin <= 0 ? "symlog" : "log";
  }
  // With zeros (or values around zero: delays, returns), both: a symlog axis reads less
  // easily, so it is kept for values that pile up near zero and still span three decades
  // (costs, contributions), not for rainfall in millimetres.
  return span >= 3 && piled ? "symlog" : "linear";
}

/** Powers of ten across a symlog axis (and their negatives), at most about six of them. */
function symlogTicks(f: Field): number[] {
  const p = f.profile;
  if (p.kind !== "quantitative") return [];
  const low = p.min > 0 ? p.min : (p.minPositive ?? 1);
  const powers = (to: number) => {
    const out: number[] = [];
    for (let k = Math.ceil(Math.log10(low)); 10 ** k <= to * 1.0001; k++) out.push(10 ** k);
    const step = Math.ceil(out.length / 6);
    return out.filter((_, i) => (out.length - 1 - i) % step === 0);
  };
  const negative = p.min < 0 ? powers(-p.min).map((v) => -v).reverse() : [];
  return [...negative, ...(p.min <= 0 ? [0] : []), ...powers(p.max)];
}

/**
 * A log or symlog tick's label, each on its own: SI prefixes from a thousand up ("10k", "1M"),
 * plain numbers below ("0.001", not "1m"). A shared axis format would give every label one prefix ("0.1M").
 */
export const POWER_LABEL = "abs(datum.value) >= 1000 ? format(datum.value, '~s') : format(datum.value, '~g')";

/**
 * A log axis's steps from just below `min` to just above `max`: 1, 2 and 5 times each power of
 * ten over up to three decades, whole powers beyond (thinned to at most ten), never the minor
 * 3, 4, 6 … ticks that crowd a log axis.
 */
export function logTicks(min: number, max: number): number[] {
  const lo = Math.floor(Math.log10(min));
  const hi = Math.ceil(Math.log10(max));
  const steps = hi - lo <= 3 ? [1, 2, 5] : [1];
  const all: number[] = [];
  for (let k = lo; k <= hi; k++) for (const m of steps) all.push(Number((m * 10 ** k).toPrecision(12)));
  const below = all.filter((v) => v <= min * 1.0001).at(-1) ?? all[0]!;
  const above = all.find((v) => v >= max / 1.0001) ?? all.at(-1)!;
  const inside = all.filter((v) => v >= below && v <= above);
  const every = Math.ceil(inside.length / 10);
  // Thin from the top, keeping both ends.
  return inside.filter((_, i) => (inside.length - 1 - i) % every === 0 || i === 0);
}

/**
 * The scale and axis properties for a measure on a position channel: nothing for a linear
 * one, `log`, or `symlog` with its constant at the smallest positive value (below it the
 * axis is linear) and ticks at powers of ten, which symlog doesn't place by itself.
 */
export function scaleFor(f: Field): { scale: Enc; axis: Enc } {
  const type = scaleType(f);
  if (type === "linear") return { scale: {}, axis: {} };
  if (type === "log") {
    const p = f.profile as { min: number; max: number };
    // Metadata first: documented bounds that the data fits set the domain (a log scale's
    // bounds are positive here: a documented minimum at or below zero made it symlog).
    const r = documentedRange(f);
    const [docMin, docMax] = [r?.fits ? r.min : undefined, r?.fits ? r.max : undefined];
    const steps = logTicks(docMin ?? p.min, docMax ?? p.max);
    // Documented bounds are the domain exactly; the steps around the data otherwise.
    const [lo, hi] = [docMin ?? steps[0]!, docMax ?? steps.at(-1)!];
    const ticks = steps.filter((v) => v >= lo && v <= hi);
    // The domain ends at the steps around the data (5 to 1,000 for prices of 6 to 800), not at a
    // power of ten far below it; ticks, and so grid lines, only at those steps.
    return {
      scale: { type: "log", domainMin: lo, domainMax: hi, nice: false },
      axis: { values: ticks, labelExpr: POWER_LABEL },
    };
  }
  const p = f.profile as { min: number; minPositive?: number };
  return {
    scale: { type: "symlog", constant: p.min > 0 ? p.min : (p.minPositive ?? 1) },
    axis: { values: symlogTicks(f), labelExpr: POWER_LABEL },
  };
}

/** `scale` with a measure's scale type laid over it; a log scale never includes zero, so `zero` goes. */
export function withScale(scale: Enc, s: { scale: Enc }): Enc {
  const out = { ...scale, ...s.scale };
  if (out.type === "log") delete out.zero;
  return out;
}

/**
 * The United States as boxes of longitude and latitude (west, east, south, north): the lower
 * 48, Alaska (and the Aleutians past 180°) and Hawaii, as the catalog builder counts them.
 */
export const US_BOXES: readonly (readonly [number, number, number, number])[] = [
  [-125, -66.5, 24.3, 49.5],
  [-180, -129.9, 51, 71.5],
  [172, 180, 51, 53.5],
  [-161, -154.5, 18.8, 22.4],
];

/** An expression that is true for rows whose coordinates lie in the US boxes (Albers USA draws nothing sensible elsewhere). */
export function inUs(lon: string, lat: string): string {
  const [x, y] = [`toNumber(datum[${JSON.stringify(lon)}])`, `toNumber(datum[${JSON.stringify(lat)}])`];
  return US_BOXES.map(([w, e, s, n]) => `(${x} >= ${w} && ${x} <= ${e} && ${y} >= ${s} && ${y} <= ${n})`).join(" || ");
}

// --- G-3: near-duplicate pairs ------------------------------------------------------------

/** Above this correlation (in size) two measures are the same thing measured twice. */
export const DUPLICATE_R = 0.97;

/** The recorded correlation of two fields, or null when it is below the builder's cut (0.9). */
export function correlation(d: Dataset, a: string, b: string): number | null {
  const hit = d.correlated?.find(([x, y]) => (x === a && y === b) || (x === b && y === a));
  return hit ? hit[2] : null;
}

/** Do two measures move together so closely that a scatter plot of them is one line? */
export function nearDuplicate(d: Dataset, a: string, b: string): boolean {
  return Math.abs(correlation(d, a, b) ?? 0) > DUPLICATE_R;
}

// --- G-4: identifiers and series ----------------------------------------------------------

const ID_NAME = /(^id$|_id$|^id_|code$|^code|^zip|zip_code|^key$|^index$|^cluster$|^target$|^group$|^fips)/i;

/**
 * Is this field's name an identifier's? `source` is one only in a table of edges, where a
 * `target` goes with it (flare_dependencies); elsewhere it is a series (an energy source).
 */
export function idName(f: Field, fields: Field[]): boolean {
  if (/^source$/i.test(f.name)) return fields.some((g) => /^target$/i.test(g.name));
  return ID_NAME.test(f.name);
}

// --- Colors -------------------------------------------------------------------------------------

/**
 * Vega's default category scheme (tableau10), which the site's --chart-* tokens mirror: a
 * color encoding may show at most this many values, or two of them share a color. Every
 * "can this be colored" limit reads it.
 */
export const TABLEAU10 = ["#4c78a8", "#f58518", "#e45756", "#72b7b2", "#54a24b", "#eeca3b", "#b279a2", "#ff9da6", "#9d755d", "#bab0ac"] as const;
export const PALETTE_SIZE = TABLEAU10.length;
/** Colored lines on one set of axes, at most (CHART-STANDARDS.md S1): beyond, a heatmap. */
export const SERIES_LIMIT = { wide: 6, phone: 4 };
/** Past this jaggedness (median step over the range), points instead of lines (S5). */
export const JAGGED = 0.2;
/** A line that is the total of the others: gray, so it reads as the sum, not as one more part (on light and dark grounds). */
export const TOTAL_COLOR = "#8a8f98";

// --- G-5: informative categories -----------------------------------------------------------

/** Fields that place labels or order marks for a particular chart rather than describe the data. */
const HELPER = /^(side|label|labels|key|order|anchor|align|offset|placement|position)$/i;
/** A category whose most common value covers more of its rows than this says little by color. */
export const DOMINANT_SHARE = 0.8;

/**
 * Is a category worth coloring by? A documented, ordered category is (the metadata says its
 * values mean something, in order); a helper field (`side`, whose values say where a label
 * goes) is not, even with its values listed; otherwise a documented category is, and so is
 * one whose values are spread out, but not one that is nearly all one value (airports'
 * `country`: almost all USA).
 */
export function informative(f: Field, rows: number): boolean {
  if (orderedCategories(f)) return true;
  if (HELPER.test(f.name)) return false;
  if (categoryValues(f)) return true;
  const p = f.profile;
  if (p.kind !== "nominal") return true;
  const present = rows - p.missing;
  return present <= 0 || (p.top[0]?.[1] ?? 0) / present <= DOMINANT_SHARE;
}

/** Are the category's groups all the same size (a designed comparison, such as Anscombe's four series)? */
export function balanced(f: Field, rows: number): boolean {
  const p = f.profile;
  if (p.kind !== "nominal" || p.distinct > p.top.length || p.missing) return false;
  return p.top.every(([, n]) => n * p.distinct === rows);
}

/** Does every row have its own value (a name or label, not a group)? */
export function unique(f: Field, rows: number): boolean {
  const p = f.profile;
  return p.kind === "nominal" && p.distinct === rows - p.missing;
}

// --- G-1: the time axis ----------------------------------------------------------------------

/** How many distinct values a field has, from its profile; null when unknown. */
export function distinctValues(f: Field): number | null {
  const p = f.profile;
  if (p.kind === "nominal") return p.distinct;
  if (p.kind === "quantitative" || p.kind === "temporal") return p.distinct ?? null;
  return null;
}

/**
 * The fields that, with time field `t`, identify every row, as the catalog builder found
 * them: [] when the time alone does, one or two series fields when each time holds one row
 * per series (a panel), null when the rows don't line up with the time (event times repeat).
 */
export function timeKey(d: Dataset, t: Field): Field[] | null {
  const names = d.timeKeys?.[t.name];
  if (!names) return null;
  const fields = names.map((n) => d.fields.find((f) => f.name === n));
  return fields.every((f) => f !== undefined) ? (fields as Field[]) : null;
}

/** Is `t` a sampled time axis (evenly spaced: days, months, years), rather than event times? */
export function sampled(t: Field): boolean {
  return t.profile.kind !== "nominal" && t.profile.kind !== "empty" && t.profile.evenlySpaced === true;
}

// --- Summing where totals are meant ---------------------------------------------------------

/** Name parts of a count of things (read with `nameTokens`, like the rate parts). */
const COUNT_TOKEN = /^(count|counts|people|population|pop|deaths|cases|number|total|votes|visitors|passengers|jobs)$/i;
const COUNT_DESC = /^(the )?(total )?(number|count) of\b/i;
/** Name parts that make a measure a rate or a summary (`deaths_per100k`, `avgPrice`, `pct`, `%`), whatever else the name says. */
const RATE_TOKEN = /^(per|rate|rates|ratio|ratios|pct|percent|percentage|share|shares|avg|average|mean|median|index|perc|proportion|density|%)$/i;

/** A field name's parts: split at underscores, hyphens, spaces, dots, camelCase humps, and between letters, digits and symbols (`per100k`: per, 100, k). */
export function nameTokens(name: string): string[] {
  return name
    .replace(/([a-z\d])([A-Z])/g, "$1 $2")
    .split(/[_\-\s.]+/)
    .flatMap((part) => part.match(/[A-Za-z]+|\d+|[^A-Za-z\d\s]/g) ?? []);
}

/**
 * Is a measure a count of things (people, deaths, jobs), so that rows for separate groups
 * add up to a total? Read from its description first ("Number of …", but not "… per …",
 * a rate), else its name or title; never for negative values.
 */
export function summable(f: Field): boolean {
  const p = f.profile;
  if (p.kind !== "quantitative" || p.min < 0) return false;
  // One reading for name, title and description: "Deaths per100k" in any of them is a rate.
  if ([f.name, f.title ?? "", f.description ?? ""].some((text) => nameTokens(text).some((t) => RATE_TOKEN.test(t)))) return false;
  if (f.description && COUNT_DESC.test(f.description.trim())) return true;
  return [f.name, f.title ?? ""].some((text) => nameTokens(text).some((t) => COUNT_TOKEN.test(t)));
}

// --- G-7: obvious axes -----------------------------------------------------------------------

/**
 * Measures whose names say which axis they belong on: `x` and `y` (any case), or a pair that
 * differs only in a leading or trailing x and y (`cx`/`cy`, `pos_x`/`pos_y`); null otherwise.
 */
export function namedAxes(measures: Field[]): { x: Field; y: Field } | null {
  // The rest of the name is one letter or ends at a separator, so `max` and `may` don't pair.
  const short = (rest: string) => rest.length <= 1 || /[_\-\s]$|^[_\-\s]/.test(rest);
  const key = (name: string, axis: "x" | "y") => {
    const n = name.toLowerCase();
    if (n === axis) return "";
    if (n.endsWith(axis) && short(n.slice(0, -1))) return `${n.slice(0, -1)}|`;
    if (n.startsWith(axis) && short(n.slice(1))) return `|${n.slice(1)}`;
    return null;
  };
  for (const x of measures) {
    const kx = key(x.name, "x");
    if (kx === null) continue;
    const y = measures.find((m) => m !== x && key(m.name, "y") === kx);
    if (y) return { x, y };
  }
  return null;
}

// --- Totals ----------------------------------------------------------------------------------

/**
 * A category's values that look like totals of its other values for measure `m`, as the
 * catalog builder found them in the data (each value's row equals the others' sum, time
 * after time). A signal for choosing a chart, never a filter: a chart that would sum
 * across this category averages instead, and a colored line per value shows the total as
 * what it is. None for a table the builder didn't read.
 */
export function totalsOf(d: Dataset, f: Field, m: Field): string[] {
  return d.totalValues?.[f.name]?.[m.name] ?? [];
}

// --- Phone legends ---------------------------------------------------------------------------

/** Encoding channels that draw a legend. */
const LEGEND_CHANNELS = ["color", "strokeDash", "shape", "size"] as const;

/** A phone legend's room across (px): the Explore column (288 px at 320) past a y axis's labels. */
const LEGEND_ROOM = 230;
/** A legend label's width per character (px, at the theme's 11 px), and an entry's symbol, gap and column padding. */
const LEGEND_CHAR = 6.5;
const LEGEND_ENTRY = 36;

/**
 * A spec with its legends above the plot, in columns, for a phone's narrow column: on the
 * right a legend takes half of 358 px. `labels` (the labels the legend shows, when known)
 * set the columns: as many as fit the room at the longest label's width, up to four. Each legend keeps its own properties
 * (labels, formats); channels sharing a field get the same placement, so Vega-Lite still
 * merges them into one legend. Layers and small multiples' inner specs alike. `offset`
 * (px above the plot) clears anything drawn there (the scatter plot's y title).
 */
export function legendsOnTop(spec: Record<string, unknown>, labels: string[] = [], offset = 6): Record<string, unknown> {
  const longest = Math.max(0, ...labels.map((l) => l.length));
  const columns = Math.max(1, Math.min(4, Math.floor(LEGEND_ROOM / (LEGEND_ENTRY + longest * LEGEND_CHAR))));
  // Room for a few labels besides the ends (S10), and no more than the narrowest column leaves beside a heatmap's row labels.
  const top = { orient: "top", direction: "horizontal", columns, columnPadding: 10, offset, gradientLength: 150, tickCount: 5 };
  const unit = (u: Record<string, unknown>): Record<string, unknown> => {
    const encoding = u.encoding as Record<string, Record<string, unknown>> | undefined;
    if (!encoding) return u;
    const moved = Object.fromEntries(
      Object.entries(encoding).map(([channel, e]) => {
        if (!(LEGEND_CHANNELS as readonly string[]).includes(channel) || !e?.field || e.legend === null) return [channel, e];
        return [channel, { ...e, legend: { ...((e.legend as Record<string, unknown> | undefined) ?? {}), ...top } }];
      }),
    );
    return { ...u, encoding: moved };
  };
  const layers = spec.layer as Record<string, unknown>[] | undefined;
  if (layers) return { ...spec, layer: layers.map(unit) };
  if (spec.spec) return { ...spec, spec: unit(spec.spec as Record<string, unknown>) };
  return unit(spec);
}

// --- S16: mostly-zero measures -------------------------------------------------------------

/**
 * More than half a measure's values are exactly zero and the rest span decades (the costs of
 * bird strikes: most cost nothing, a few millions). Its mean over a bucket is then a few rare
 * rows, a spike train. Rain (zero on most days, millimetres on the rest) is not: its monthly
 * mean is the month's average daily rainfall, a quantity worth a line.
 */
export function mostlyZero(f: Field): boolean {
  const p = f.profile;
  if (p.kind !== "quantitative" || !p.zeros) return false;
  const values = p.bins.reduce((a, b) => a + b, 0);
  return values > 0 && p.zeros / values > 0.5 && scaleType(f) !== "linear";
}

// --- Names Vega can't read ------------------------------------------------------------------

/**
 * A field Vega can read by name. A column named for one of Object's own properties
 * (`constructor`, `toString`, `__proto__`) breaks Vega's dataflow wherever it is read ("Operator
 * not defined"), in an encoding or an expression alike: charts leave such a field out
 * (Codex round 6, #9), and draw the rest.
 */
export function readable(f: Field): boolean {
  // A backslash or a double quote, too: Vega-Lite reads a backslash as an escape in a field
  // reference however many are written (another field: nothing drawn), and writes a double
  // quote unescaped into its tooltip's expression (a parse error).
  return !UNREADABLE.has(f.name) && !/[\\"]/.test(f.name);
}
const UNREADABLE = new Set([...Object.getOwnPropertyNames(Object.prototype), "__proto__"]);
