/**
 * What the home page shows, as plain data: counts, the catalog chart's rows, and
 * the card list after search, filters and sort. No DOM, so it is unit-tested.
 */
import { Catalog, type CatalogFile, type Dataset, type Example, type Gallery, GALLERIES } from "./catalog";

/** The chart's color groups: the three common formats, everything else together. */
export const FORMAT_GROUPS = ["JSON", "CSV", "TopoJSON", "Other"] as const;
export type FormatGroup = (typeof FORMAT_GROUPS)[number];

/** tableau10 slots (the --chart-* tokens) that keep the four groups apart for every reader. */
export const FORMAT_COLORS: Record<FormatGroup, string> = {
  JSON: "#54a24b",
  CSV: "#b279a2",
  TopoJSON: "#9d755d",
  Other: "#bab0ac",
};

export function formatGroup(d: Dataset): FormatGroup {
  if (d.format === "json") return "JSON";
  if (d.format === "csv") return "CSV";
  if (d.format === "topojson") return "TopoJSON";
  return "Other";
}

export type Sort = "used" | "az" | "size";
export const SORT_LABEL: Record<Sort, string> = { used: "Most Used", az: "A to Z", size: "Largest" };
export const SORT_NOTE: Record<Sort, string> = { used: "Most used first", az: "A to Z", size: "Largest first" };

/** A chart brush, in data units: file size in bytes and gallery examples. */
export interface Brush {
  bytes: [number, number];
  examples: [number, number];
}

export interface Filters {
  query: string;
  formats: ReadonlySet<FormatGroup>;
  galleries: ReadonlySet<Gallery>;
  brush: Brush | null;
  sort: Sort;
}

export const NO_FILTERS: Filters = { query: "", formats: new Set(), galleries: new Set(), brush: null, sort: "used" };

export function isFiltered(f: Filters): boolean {
  return f.query.trim() !== "" || f.formats.size > 0 || f.galleries.size > 0 || f.brush !== null;
}

function within(v: number, [lo, hi]: [number, number]): boolean {
  return v >= Math.min(lo, hi) && v <= Math.max(lo, hi);
}

/** Search matches a dataset's name, its field names and its description. */
function matches(d: Dataset, needle: string): boolean {
  return d.name.toLowerCase().includes(needle)
    || d.fields.some((f) => f.name.toLowerCase().includes(needle))
    || d.description.toLowerCase().includes(needle);
}

/** The datasets to list: every active filter must match (within a chip group, any chip). */
export function listDatasets(c: Catalog, f: Filters): Dataset[] {
  const needle = f.query.trim().toLowerCase();
  const list = c.datasets.filter((d) => {
    if (needle && !matches(d, needle)) return false;
    if (f.formats.size && !f.formats.has(formatGroup(d))) return false;
    if (f.galleries.size) {
      const usage = c.usage(d);
      if (![...f.galleries].some((g) => usage[g] > 0)) return false;
    }
    if (f.brush && !(within(d.bytes ?? 0, f.brush.bytes) && within(d.usedBy.length, f.brush.examples))) return false;
    return true;
  });
  // c.datasets is A to Z, and sort() is stable, so ties stay alphabetical.
  if (f.sort === "used") list.sort((a, b) => b.usedBy.length - a.usedBy.length);
  if (f.sort === "size") list.sort((a, b) => (b.bytes ?? 0) - (a.bytes ?? 0));
  return list;
}

/**
 * The datasets the search and chips match, ignoring the brush: the chart shows these at
 * full strength, and the brush then narrows the cards within them. The chart never drops
 * points, so its axes don't move under a brush.
 */
export function baseMatches(c: Catalog, f: Filters): Dataset[] {
  return listDatasets(c, { ...f, brush: null });
}

export interface HomeCounts {
  datasets: number;
  examples: number;
  /** Examples that load at least one vega-datasets file. */
  examplesWithData: number;
  formats: Record<FormatGroup, number>;
  /** Datasets that each gallery uses at least once. */
  galleries: Record<Gallery, number>;
}

/** Datasets per format group (the chart legend's and the chips' counts). */
export function formatCounts(c: Catalog): Record<FormatGroup, number> {
  const formats = Object.fromEntries(FORMAT_GROUPS.map((g) => [g, 0])) as Record<FormatGroup, number>;
  for (const d of c.datasets) formats[formatGroup(d)]++;
  return formats;
}

export function homeCounts(c: Catalog): HomeCounts {
  const formats = formatCounts(c);
  const galleries: Record<Gallery, number> = { vega: 0, "vega-lite": 0, altair: 0 };
  for (const d of c.datasets) {
    const usage = c.usage(d);
    for (const g of GALLERIES) if (usage[g] > 0) galleries[g]++;
  }
  return {
    datasets: c.datasets.length,
    examples: c.examples.length,
    examplesWithData: c.examples.filter((e) => e.datasets.length > 0).length,
    formats,
    galleries,
  };
}

export interface ChartRow {
  name: string;
  bytes: number;
  size: string;
  examples: number;
  format: FormatGroup;
  href: string;
}

/** Each dataset's point on the catalog chart; `href` opens its page (relative to the home page). */
export function chartRows(c: Catalog, size: (bytes: number) => string): ChartRow[] {
  return c.datasets
    .filter((d) => d.bytes !== null && d.bytes > 0)
    .map((d) => ({
      name: d.name,
      bytes: d.bytes!,
      size: size(d.bytes!),
      examples: d.usedBy.length,
      format: formatGroup(d),
      href: `datasets/${encodeURIComponent(d.name)}/`,
    }));
}

/**
 * The catalog, cut down to what the home page's search, chips, sort and chart read
 * (served as home-index.json): each dataset's name, format, size, row count, description,
 * field names and the examples that use it, and each such example's gallery.
 */
export interface HomeIndex {
  package: CatalogFile["package"];
  datasets: (Pick<Dataset, "name" | "format" | "kind" | "bytes" | "rows" | "description" | "usedBy"> & { fields: { name: string }[] })[];
  examples: Pick<Example, "id" | "gallery">[];
}

export function homeIndex(c: Catalog): HomeIndex {
  const used = new Set(c.datasets.flatMap((d) => d.usedBy));
  return {
    package: c.package,
    datasets: c.datasets.map((d) => ({
      name: d.name,
      format: d.format,
      kind: d.kind,
      bytes: d.bytes,
      rows: d.rows,
      description: d.description,
      usedBy: d.usedBy,
      fields: d.fields.map((f) => ({ name: f.name })),
    })),
    examples: c.examples.filter((e) => used.has(e.id)).map((e) => ({ id: e.id, gallery: e.gallery })),
  };
}

/**
 * A Catalog over a HomeIndex: enough for listDatasets, baseMatches, usage, formatCounts
 * and chartRows (the functions the home page's script runs; not homeCounts, which needs
 * every example).
 */
export function indexCatalog(index: HomeIndex): Catalog {
  return new Catalog({ ...index, readme: "" } as unknown as CatalogFile);
}

/** Thumbnails at least this much wider than tall fill the strip's 180 by 100 slots. */
const LANDSCAPE = 1.25;

function landscape(e: Example): boolean {
  return e.thumb !== null && e.thumbSize !== null && e.thumbSize[0] >= LANDSCAPE * e.thumbSize[1];
}

/**
 * Thumbnails for the strip under the title: the most used datasets' landscape
 * examples, taking the galleries in turn so all three show, one per dataset per pass.
 */
export function showcase(c: Catalog, n: number): Example[] {
  const queues = GALLERIES.map((g) =>
    [...c.datasets]
      .sort((a, b) => b.usedBy.length - a.usedBy.length)
      .flatMap((d) => c.examplesFor(d).filter((e) => e.gallery === g && landscape(e)).slice(0, 1)));
  const seen = new Set<string>();
  const out: Example[] = [];
  for (let i = 0; out.length < n && queues.some((q) => i < q.length); i++) {
    for (const q of queues) {
      const e = q[i];
      if (e && !seen.has(e.id) && out.length < n) {
        seen.add(e.id);
        out.push(e);
      }
    }
  }
  return out;
}

/** The first paragraph of a Markdown description as plain text, for a card. */
export function plainSummary(markdown: string): string {
  const first = markdown.trim().split(/\n\s*\n/)[0] ?? "";
  return first
    .replace(/!?\[([^\]]*)\]\([^)]*\)/g, "$1")
    .replace(/[*_`]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

/** One `## Heading` section of the README (without the heading), or null if it is gone. */
export function readmeSection(readme: string, heading: string): string | null {
  const lines = readme.split("\n");
  const start = lines.findIndex((l) => l.trim() === `## ${heading}`);
  if (start < 0) return null;
  const end = lines.findIndex((l, i) => i > start && l.startsWith("## "));
  return lines.slice(start + 1, end < 0 ? undefined : end).join("\n").trim();
}
