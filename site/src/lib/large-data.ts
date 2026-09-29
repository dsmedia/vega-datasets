/**
 * The large-data policy: how Explore draws a table or a map by its size, so that no page
 * blocks a phone for seconds on a chart nobody asked for.
 *
 * | Rows          | Default view                                   | Interaction                        |
 * |---------------|------------------------------------------------|------------------------------------|
 * | ≤ 5,000       | points, SVG                                    | zoom and pan, legend isolation     |
 * | ≤ 20,000      | points on canvas, drawn by itself only on a    | zoom on desktop-class devices only |
 * |               | desktop-class device (device.ts), else a button|                                    |
 * | ≤ 50,000      | points on canvas, drawn on request             | no scale-bound zoom                |
 * | > 50,000      | a density heatmap binned from every row when   | "Draw All N Points" opts in, on    |
 * |               | the site is built                              | canvas, no scale-bound zoom        |
 *
 * Maps with more than MAP_PREVIEW_MARKS shapes or points open on a picture drawn when the
 * site is built, with "Draw the Live Map".
 */
import type { Dataset } from "./catalog";
import { formatBytes, formatCount } from "./format";
import { fieldRef, starterSpec } from "./starter";

type Spec = Record<string, unknown>;

/** Up to this many rows, points are drawn in SVG, by themselves. */
export const SVG_MAX_ROWS = 5_000;
/** Up to this many rows, points are drawn on canvas, by themselves on a desktop-class device. */
export const AUTO_DRAW_MAX_ROWS = 20_000;
/** Up to this many rows, points are drawn on canvas when asked; above it, the density overview. */
export const POINTS_MAX_ROWS = 50_000;
/** Maps drawing more shapes or points than this open on a picture drawn when the site was built. */
export const MAP_PREVIEW_MARKS = 1_000;

export type Band = "svg" | "canvas" | "on-request" | "density";

export function rowBand(rows: number): Band {
  if (rows <= SVG_MAX_ROWS) return "svg";
  if (rows <= AUTO_DRAW_MAX_ROWS) return "canvas";
  if (rows <= POINTS_MAX_ROWS) return "on-request";
  return "density";
}

/** "always", "desktop" (only on a desktop-class device), or "never". */
export type When = "always" | "desktop" | "never";

export interface BandPolicy {
  renderer: "svg" | "canvas";
  /** Whether the points draw when Explore comes into view (otherwise a button draws them). */
  autoDraw: When;
  /** Scroll to zoom and drag to pan (never on a narrow screen, where it would trap page scrolling). */
  zoom: When;
  /** The points' opacity: lower where they overlap by the thousand. */
  opacity: number;
}

export const BAND_POLICY: Readonly<Record<Band, BandPolicy>> = {
  svg: { renderer: "svg", autoDraw: "always", zoom: "always", opacity: 0.8 },
  canvas: { renderer: "canvas", autoDraw: "desktop", zoom: "desktop", opacity: 0.35 },
  "on-request": { renderer: "canvas", autoDraw: "never", zoom: "never", opacity: 0.35 },
  // Once the reader asks for every point.
  density: { renderer: "canvas", autoDraw: "never", zoom: "never", opacity: 0.35 },
};

export function allowed(when: When, desktopClass: boolean): boolean {
  return when === "always" || (when === "desktop" && desktopClass);
}

/** The band of a table's chart; null for files without rows (maps and other JSON). */
export function tableBand(d: Dataset): Band | null {
  return d.rows === null ? null : rowBand(d.rows);
}

/** The label of the button that draws the points of a table that doesn't draw by itself. */
export function drawPointsLabel(d: Dataset): string {
  const all = rowBand(d.rows ?? 0) === "density" ? "All " : "";
  return `Draw ${all}${formatCount(d.rows)} Points (${formatBytes(d.bytes)})`;
}

/**
 * How many shapes or points the starter map draws, from the counts the catalog records
 * (the first TopoJSON object's features, the GeoJSON features, or the rows of a table
 * with latitude and longitude); null when the starter chart isn't a map.
 */
export function mapMarks(d: Dataset): number | null {
  if (!starterSpec(d)?.projection) return null;
  if (d.format === "topojson") {
    const object = d.objects?.[0];
    return object === undefined ? null : (d.objectFeatures?.[object] ?? null);
  }
  if (d.format === "geojson") return d.features ?? null;
  return d.rows;
}

/** Is the starter chart a map heavy enough to open on its picture? */
export function hasMapPreview(d: Dataset): boolean {
  return (mapMarks(d) ?? 0) > MAP_PREVIEW_MARKS;
}

// --- The density overview ---------------------------------------------------------------------

/** The overview's resolution: at most this many bins across and up. */
export const DENSITY_BINS = { x: 60, y: 40 } as const;
/** Bin between these quantiles of each axis, so a handful of outliers don't flatten the view. */
export const DENSITY_CLIP = [0.005, 0.995] as const;

/** A 2D histogram of two measures, binned when the site is built. */
export interface DensityGrid {
  x: string;
  y: string;
  /** The rows of the table, and those where both fields hold numbers. */
  rows: number;
  complete: number;
  /** Complete rows outside the box (beyond the clip quantiles on either axis), not binned. */
  outside: number;
  /** The box: the clip quantiles of each axis. */
  box: { x: [number, number]; y: [number, number] };
  /** Where the first bin starts, each bin's width, and how many bins, on each axis. */
  xstart: number;
  xstep: number;
  nx: number;
  ystart: number;
  ystep: number;
  ny: number;
  /** The non-empty bins, as [column, row, rows in the bin]. */
  cells: [number, number, number][];
}

/** A 1, 2 or 5 × 10^k step giving at most `maxbins` bins, as Vega's bin transform chooses. */
export function niceStep(lo: number, hi: number, maxbins: number): number {
  const raw = (hi - lo) / maxbins;
  if (!(raw > 0)) return 1;
  const base = 10 ** Math.floor(Math.log10(raw));
  return [1, 2, 5, 10].map((m) => m * base).find((s) => s >= raw * (1 - 1e-12))!;
}

/** The p-quantile of sorted values, interpolated linearly between the two nearest ranks. */
export function quantile(sorted: Float64Array, p: number): number {
  const h = (sorted.length - 1) * p;
  const lo = Math.floor(h);
  const hi = Math.min(lo + 1, sorted.length - 1);
  return sorted[lo]! + (h - lo) * (sorted[hi]! - sorted[lo]!);
}

/** As Vega's toNumber: null, undefined and "" are no value. */
function toNumber(v: unknown): number {
  return v === null || v === undefined || v === "" ? NaN : Number(v);
}

// Vega's bin transform adds this before flooring, so a value on a bin edge lands in the upper bin.
const EPSILON = 1e-14;

/**
 * Bin every row with numbers in both fields into at most `maxbins` bins across and up,
 * between the DENSITY_CLIP quantiles of each field; count the rest as outside.
 */
export function densityGrid(rows: readonly Record<string, unknown>[], x: string, y: string, maxbins: { x: number; y: number } = DENSITY_BINS): DensityGrid {
  const xs: number[] = [];
  const ys: number[] = [];
  for (const r of rows) {
    const a = toNumber(r[x]);
    const b = toNumber(r[y]);
    if (Number.isFinite(a) && Number.isFinite(b)) {
      xs.push(a);
      ys.push(b);
    }
  }
  const box = (values: number[]): [number, number] => {
    if (!values.length) return [0, 0];
    const sorted = Float64Array.from(values).sort();
    return [quantile(sorted, DENSITY_CLIP[0]), quantile(sorted, DENSITY_CLIP[1])];
  };
  const [bx, by] = [box(xs), box(ys)];
  const axis = ([lo, hi]: [number, number], maxb: number) => {
    const step = niceStep(lo, hi, maxb);
    const start = Math.floor(lo / step) * step;
    return { start, step, n: Math.max(1, Math.ceil((hi - start) / step - EPSILON)) };
  };
  const [ax, ay] = [axis(bx, maxbins.x), axis(by, maxbins.y)];
  const counts = new Uint32Array(ax.n * ay.n);
  let inside = 0;
  const index = (v: number, a: { start: number; step: number; n: number }) => Math.min(a.n - 1, Math.floor(EPSILON + (v - a.start) / a.step));
  for (let k = 0; k < xs.length; k++) {
    const a = xs[k]!;
    const b = ys[k]!;
    if (a < bx[0] || a > bx[1] || b < by[0] || b > by[1]) continue;
    inside++;
    counts[index(a, ax) * ay.n + index(b, ay)]!++;
  }
  const cells: [number, number, number][] = [];
  counts.forEach((n, k) => {
    if (n) cells.push([Math.floor(k / ay.n), k % ay.n, n]);
  });
  return {
    x,
    y,
    rows: rows.length,
    complete: xs.length,
    outside: xs.length - inside,
    box: { x: bx, y: by },
    xstart: ax.start,
    xstep: ax.step,
    nx: ax.n,
    ystart: ay.start,
    ystep: ay.step,
    ny: ay.n,
    cells,
  };
}

const SCHEMA = "https://vega.github.io/schema/vega-lite/v6.json";
/** Rows per bin on a log scale. The page takes its ramp from the theme (config.range.heatmap, lib/vega-theme.ts); the Editor, on white, from the blues scheme. */
const DENSITY_COLOR = { type: "log" };
const EDITOR_COLOR = { ...DENSITY_COLOR, scheme: "blues" };
/** Bin edges without floating-point noise (0.30000000000000004). */
const edge = (start: number, step: number, i: number) => Number((start + i * step).toPrecision(12));

/**
 * The overview as the Editor opens it: the whole file from its public URL, filtered to the
 * box and binned on the same edges, so it draws the same bins as the page.
 */
export function densitySpec(d: Dataset, g: DensityGrid, height: number): Spec {
  const bin = (start: number, step: number, n: number) => ({ extent: [start, edge(start, step, n)], step });
  const x = { field: fieldRef(g.x), type: "quantitative", bin: bin(g.xstart, g.xstep, g.nx), title: g.x };
  const y = { field: fieldRef(g.y), type: "quantitative", bin: bin(g.ystart, g.ystep, g.ny), title: g.y };
  return {
    $schema: SCHEMA,
    description: `How the rows of ${d.name} spread over ${g.x} and ${g.y}: rows per bin, between the 0.5th and 99.5th percentiles of each.`,
    width: "container",
    height,
    autosize: { type: "fit-x", contains: "padding" },
    data: { url: d.url },
    transform: [
      { filter: { field: fieldRef(g.x), range: g.box.x } },
      { filter: { field: fieldRef(g.y), range: g.box.y } },
    ],
    mark: "rect",
    encoding: {
      x,
      y,
      color: { aggregate: "count", type: "quantitative", title: "Rows", scale: EDITOR_COLOR },
      tooltip: [x, y, { aggregate: "count", type: "quantitative", title: "Rows", format: "," }],
    },
  };
}

/** The same overview drawn from the bins in the page (pre-binned data: no file to load). */
export function densityPageSpec(d: Dataset, g: DensityGrid, height: number): Spec {
  const range = (lo: string, hi: string) => `format(datum.${lo}, ",") + " – " + format(datum.${hi}, ",")`;
  const values = g.cells.map(([i, j, n]) => ({
    x0: edge(g.xstart, g.xstep, i),
    x1: edge(g.xstart, g.xstep, i + 1),
    y0: edge(g.ystart, g.ystep, j),
    y1: edge(g.ystart, g.ystep, j + 1),
    n,
  }));
  return {
    $schema: SCHEMA,
    description: `How the ${formatCount(g.complete - g.outside)} rows of ${d.name} inside the 0.5th–99.5th percentiles spread over ${g.x} and ${g.y}: rows per bin, binned when the site was built.`,
    width: "container",
    height,
    autosize: { type: "fit-x", contains: "padding" },
    data: { values },
    transform: [
      { calculate: range("x0", "x1"), as: "x range" },
      { calculate: range("y0", "y1"), as: "y range" },
    ],
    mark: "rect",
    encoding: {
      x: { field: "x0", type: "quantitative", bin: { binned: true, step: g.xstep }, title: g.x },
      x2: { field: "x1" },
      y: { field: "y0", type: "quantitative", bin: { binned: true, step: g.ystep }, title: g.y },
      y2: { field: "y1" },
      color: { field: "n", type: "quantitative", title: "Rows", scale: DENSITY_COLOR },
      tooltip: [
        { field: "x range", title: g.x },
        { field: "y range", title: g.y },
        { field: "n", type: "quantitative", title: "Rows", format: "," },
      ],
    },
  };
}

/** The caption under the overview: what was binned and what was left out. */
export function densityCaption(g: DensityGrid): string {
  const parts = [`Rows per bin, from all ${formatCount(g.rows)} rows, binned when the site was built.`];
  if (g.outside) parts.push(`The chart leaves out ${formatCount(g.outside)} ${g.outside === 1 ? "row" : "rows"} beyond the 0.5th or 99.5th percentile of either field.`);
  const missing = g.rows - g.complete;
  if (missing) parts.push(`Another ${formatCount(missing)} ${missing === 1 ? "lacks" : "lack"} a value in one of them.`);
  parts.push("Draw all points to pick the fields.");
  return parts.join(" ");
}
