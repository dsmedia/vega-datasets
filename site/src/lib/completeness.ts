/**
 * How fully a dataset is described (DECISIONS D6, METADATA-TODO.md "Fully described"),
 * shared by the dataset page (a quiet "Add" beside each undescribed field, one footer line)
 * and the status page for contributors (every dataset against the checklist).
 *
 * Counted as gaps: the dataset's `title`, `description`, a source, a known license, and a
 * `description` for every field of a table with a schema. Properties that apply only to
 * some datasets (`categories`, `constraints`, `missingValues`, `primaryKey`, `foreignKeys`,
 * a field `title` for a cryptic name) can't be judged automatically: they are reported as
 * documented when present, never as gaps.
 */
import type { Dataset, Field } from "./catalog";
import { licenseFamily } from "./catalog";

/** The dataset-level checklist, in the order the status page shows it. */
export type Check = "title" | "description" | "source" | "license";
export const CHECKS: readonly Check[] = ["title", "description", "source", "license"];
export const CHECK_LABEL: Record<Check, string> = {
  title: "Title",
  description: "Description",
  source: "Source",
  license: "License",
};

export interface Completeness {
  /** Whether each dataset-level item is filled in. */
  has: Record<Check, boolean>;
  /** The table's fields and which lack a description; null when there is no schema (maps, images, JSON trees). */
  fields: { total: number; undescribed: string[] } | null;
  /** Missing dataset-level items plus undescribed fields. */
  gaps: number;
  /** Properties that apply only where they fit, as present: shown for information, never counted. */
  documented: string[];
}

const filled = (text: string | null | undefined): boolean => typeof text === "string" && text.trim() !== "";

/** Whether a field has a description (whitespace alone doesn't count). */
export function hasDescription(f: Field): boolean {
  return filled(f.description);
}

const count = (n: number, one: string) => (n === 1 ? `${one} (1 field)` : `${one} (${n} fields)`);

/** The "where they apply" properties the dataset documents, in words. */
export function documented(d: Dataset): string[] {
  const n = (p: (f: Field) => boolean) => d.fields.filter(p).length;
  const out: string[] = [];
  const titles = n((f) => filled(f.title));
  const categories = n((f) => (f.categories?.length ?? 0) > 0);
  const constraints = n((f) => Object.keys(f.constraints ?? {}).length > 0);
  const missing = n((f) => f.missingValues !== undefined);
  if (titles) out.push(count(titles, "Field titles"));
  if (categories) out.push(count(categories, "Categories"));
  if (constraints) out.push(count(constraints, "Constraints"));
  if (d.missingValues !== undefined || missing) out.push(d.missingValues !== undefined ? "Missing values" : count(missing, "Missing values"));
  if (d.primaryKey !== undefined && d.primaryKey.length > 0) out.push("Primary key");
  if (d.foreignKeys?.length) out.push(d.foreignKeys.length === 1 ? "Foreign key" : `Foreign keys (${d.foreignKeys.length})`);
  return out;
}

export function completeness(d: Dataset): Completeness {
  const has: Record<Check, boolean> = {
    title: filled(d.title),
    description: filled(d.description),
    // A source counts when one is recorded (METADATA-TODO's baseline); the status page
    // notes sources recorded without a link.
    source: d.sources.some((s) => filled(s.title) || filled(s.path)),
    license: licenseFamily(d) !== "Not specified",
  };
  const fields = d.fields.length ? { total: d.fields.length, undescribed: d.fields.filter((f) => !hasDescription(f)).map((f) => f.name) } : null;
  const gaps = CHECKS.filter((c) => !has[c]).length + (fields?.undescribed.length ?? 0);
  return { has, fields, gaps, documented: documented(d) };
}

/** Whether the dataset records sources but none with a link (information on the status page, not a gap). */
export function sourcesUnlinked(d: Dataset): boolean {
  return d.sources.length > 0 && !d.sources.some((s) => filled(s.path));
}

/** Datasets by number of gaps, most first, ties by name. */
export function byGaps<T extends { dataset: Dataset; status: Completeness }>(rows: T[]): T[] {
  return [...rows].sort((a, b) => b.status.gaps - a.status.gaps || a.dataset.name.localeCompare(b.dataset.name));
}

export interface Summary {
  datasets: number;
  /** Datasets with no gaps. */
  complete: number;
  /** Datasets missing each dataset-level item. */
  missing: Record<Check, number>;
  /** Fields of tables with a schema, and how many have a description. */
  fields: { total: number; described: number };
  /** Tables with a schema but no field description at all. */
  tablesWithoutDescriptions: number;
}

export function summarize(datasets: Dataset[]): Summary {
  const all = datasets.map(completeness);
  const missing = Object.fromEntries(CHECKS.map((c) => [c, all.filter((s) => !s.has[c]).length])) as Record<Check, number>;
  const tables = all.flatMap((s) => (s.fields ? [s.fields] : []));
  const total = tables.reduce((sum, f) => sum + f.total, 0);
  const undescribed = tables.reduce((sum, f) => sum + f.undescribed.length, 0);
  return {
    datasets: datasets.length,
    complete: all.filter((s) => s.gaps === 0).length,
    missing,
    fields: { total, described: total - undescribed },
    tablesWithoutDescriptions: tables.filter((f) => f.undescribed.length === f.total).length,
  };
}

// --- Where a dataset's metadata lives ----------------------------------------------------------

/** The metadata file contributors edit, relative to the repository root. */
export const ADDITIONS_FILE = "_data/datapackage_additions.toml";

/**
 * The line of each `[[resources]]` block in the metadata TOML, by the file it describes:
 * named by the header's `# Path: <file>` comment, else by the block's own `path = "…"`.
 */
export function resourceLines(toml: string): Map<string, number> {
  const lines = new Map<string, number>();
  let open: number | null = null;
  toml.split(/\r?\n/).forEach((line, i) => {
    const header = /^\s*\[\[resources\]\]\s*(?:#\s*Path:\s*(\S+))?/.exec(line);
    if (header) {
      const file = header[1];
      open = file ? null : i + 1;
      if (file && !lines.has(file)) lines.set(file, i + 1);
      return;
    }
    // Only the block's own keys: a sub-table (sources, licenses, schema) has paths of its own.
    if (/^\s*\[/.test(line)) open = null;
    const path = open !== null ? /^\s*path\s*=\s*["']([^"']+)["']/.exec(line) : null;
    if (path && open !== null) {
      if (!lines.has(path[1]!)) lines.set(path[1]!, open);
      open = null;
    }
  });
  return lines;
}

/** A link to a dataset's entry in the metadata file (its line when known, else the file). */
export function entryUrl(fileUrl: string, lines: ReadonlyMap<string, number>, file: string): string {
  const n = lines.get(file);
  return n ? `${fileUrl}#L${n}` : fileUrl;
}
