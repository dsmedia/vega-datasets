/** Types and indexes for catalog.json (built by scripts/build_site_catalog.py). */

export type Gallery = "vega" | "vega-lite" | "altair";
export const GALLERIES: readonly Gallery[] = ["vega-lite", "vega", "altair"];

export interface License {
  name: string;
  title?: string;
  path?: string;
}

export interface Source {
  title: string;
  path?: string;
}

export interface QuantProfile {
  kind: "quantitative";
  min: number;
  max: number;
  mean: number;
  missing: number;
  bins: number[];
}

export interface TemporalProfile {
  kind: "temporal";
  min: string;
  max: string;
  missing: number;
  bins?: number[];
}

export interface NominalProfile {
  kind: "nominal";
  distinct: number;
  top: [string, number][];
  missing: number;
}

export interface EmptyProfile {
  kind: "empty";
  missing: number;
}

export type Profile = QuantProfile | TemporalProfile | NominalProfile | EmptyProfile;

/** A category's documented value, with an optional label (Table Schema v2 `categories`). */
export interface LabeledValue {
  value: string | number;
  label?: string;
}

/** Table Schema `constraints` (the ones the site reads; others pass through untyped). */
export interface Constraints {
  required?: boolean;
  unique?: boolean;
  /** Numbers for number fields; dates and times are strings. */
  minimum?: number | string;
  maximum?: number | string;
  enum?: (string | number | boolean)[];
  [other: string]: unknown;
}

/** Values that mean "missing": strings, or `{value, label}` (Table Schema v2 `missingValues`). */
export type MissingValues = string[] | { value: string; label?: string }[];

/** Table Schema v2 `foreignKeys`: a string names one field; no resource (or "", as v1 wrote it) is the table itself. */
export interface ForeignKey {
  fields: string | string[];
  reference: { resource?: string; fields: string | string[] };
}

/**
 * A field. `description` is always present (null when not written); the other schema
 * properties only when the metadata fills them in.
 */
export interface Field {
  name: string;
  type: string;
  description: string | null;
  title?: string;
  categories?: string[] | number[] | LabeledValue[];
  categoriesOrdered?: boolean;
  constraints?: Constraints;
  format?: string;
  missingValues?: MissingValues;
  profile: Profile;
}

export interface Dataset {
  name: string;
  /** A short summary (Data Package `title`), when the metadata has one. */
  title?: string;
  file: string;
  /** Where to load the file from: jsDelivr for released files, GitHub Pages otherwise. */
  url: string;
  format: string;
  kind: "table" | "json" | "file";
  bytes: number | null;
  description: string;
  licenses: License[];
  sources: Source[];
  usedBy: string[];
  fields: Field[];
  rows: number | null;
  preview: { columns: string[]; rows: string[][] } | null;
  /** TopoJSON: the objects, and how many features each one becomes. */
  objects?: string[];
  objectFeatures?: Record<string, number>;
  /** GeoJSON: how many features the file holds. */
  features?: number;
  image?: string;
  /** The table schema's keys and missing-value markers, when the metadata has them. */
  primaryKey?: string | string[];
  foreignKeys?: ForeignKey[];
  missingValues?: MissingValues;
}

export interface Example {
  id: string;
  gallery: Gallery;
  slug: string;
  name: string;
  url: string;
  source: string;
  categories: string[];
  description: string | null;
  datasets: string[];
  thumb: string | null;
  thumbSize: [number, number] | null;
  editor: string | null;
}

export interface CatalogFile {
  package: { name: string; version: string; commit: string };
  /** README.md, adapted for the home page (see readme_markdown in the builder). */
  readme: string;
  datasets: Dataset[];
  examples: Example[];
}

export class Catalog {
  readonly datasets: Dataset[];
  readonly examples: Example[];
  readonly package: CatalogFile["package"];
  readonly readme: string;
  private readonly byName = new Map<string, Dataset>();
  private readonly byId = new Map<string, Example>();

  constructor(file: CatalogFile) {
    this.package = file.package;
    this.readme = file.readme;
    this.datasets = [...file.datasets].sort((a, b) => a.name.localeCompare(b.name));
    this.examples = file.examples;
    for (const d of this.datasets) this.byName.set(d.name, d);
    for (const e of this.examples) this.byId.set(e.id, e);
  }

  dataset(name: string): Dataset | undefined {
    return this.byName.get(name);
  }

  examplesFor(d: Dataset): Example[] {
    return d.usedBy.map((id) => this.byId.get(id)).filter((e): e is Example => e !== undefined);
  }

  usage(d: Dataset): Record<Gallery, number> {
    const counts: Record<Gallery, number> = { vega: 0, "vega-lite": 0, altair: 0 };
    for (const e of this.examplesFor(d)) counts[e.gallery]++;
    return counts;
  }
}

export const GALLERY_LABEL: Record<Gallery, string> = {
  vega: "Vega",
  "vega-lite": "Vega-Lite",
  altair: "Altair",
};

/** Collapse the Data Package license identifiers into a few readable families. */
export function licenseFamily(d: Dataset): string {
  const names = d.licenses.map((l) => l.name);
  if (names.length === 0 || names.every((n) => n === "notspecified")) return "Not specified";
  if (names.some((n) => n === "other-pd" || n === "CC0-1.0" || n === "PDDL-1.0")) return "Public domain";
  if (names.some((n) => n.startsWith("CC-BY") || n === "ODC-By-1.0" || n === "OGL-UK-3.0")) return "Attribution";
  if (names.some((n) => n.startsWith("BSD") || n === "MIT" || n === "ISC")) return "Permissive";
  if (names.some((n) => n.startsWith("ODbL") || n.includes("GPL") || n.includes("-SA"))) return "Share-alike";
  return "Other open";
}

// --- Schema metadata, normalized -------------------------------------------------------------

const list = (x: string | string[] | undefined): string[] => (x === undefined ? [] : Array.isArray(x) ? x : [x]);

/** The field's title (which may carry units, "Horsepower (hp)"), else its name. */
export function fieldTitle(f: Field): string {
  return f.title ?? f.name;
}

/** The documented categories as `{value, label?}`, or null when there are none. */
export function categoryValues(f: Field): LabeledValue[] | null {
  if (!f.categories?.length) return null;
  return (f.categories as (string | number | LabeledValue)[]).map((c) => (typeof c === "object" ? c : { value: c }));
}

/** The categories' values in their documented order, when the metadata says the order matters. */
export function orderedCategories(f: Field): (string | number)[] | null {
  const values = f.categoriesOrdered ? categoryValues(f) : null;
  return values ? values.map((c) => c.value) : null;
}

/** `[value (as text), label]` for the categories that have a label, or null when none does. */
export function categoryLabels(f: Field): [string, string][] | null {
  const labeled = (categoryValues(f) ?? []).filter((c) => c.label !== undefined);
  return labeled.length ? labeled.map((c) => [String(c.value), c.label!]) : null;
}

/**
 * A number field's documented `minimum` and `maximum`, and whether every value lies
 * inside them (the profile's range); null when neither is documented as a number.
 */
export function documentedRange(f: Field): { min?: number; max?: number; fits: boolean } | null {
  const { minimum, maximum } = f.constraints ?? {};
  const min = typeof minimum === "number" ? minimum : undefined;
  const max = typeof maximum === "number" ? maximum : undefined;
  if (min === undefined && max === undefined) return null;
  const fits = insideDocumented(f) !== false;
  return { ...(min !== undefined ? { min } : {}), ...(max !== undefined ? { max } : {}), fits };
}

/**
 * Whether the data's range (the profile's) lies inside the documented `minimum` and
 * `maximum`: numbers for a number field, dates and times (as text) for a temporal one;
 * null when that can't be told (no bounds, or bounds that don't match the data's kind).
 */
export function insideDocumented(f: Field): boolean | null {
  const { minimum, maximum } = f.constraints ?? {};
  const p = f.profile;
  const value = (v: number | string | undefined): number | undefined =>
    p.kind === "quantitative" ? (typeof v === "number" ? v : undefined)
    : p.kind === "temporal" && typeof v === "string" && !Number.isNaN(Date.parse(v)) ? Date.parse(v)
    : undefined;
  const [min, max] = [value(minimum), value(maximum)];
  if (min === undefined && max === undefined) return null;
  const [lo, hi] = p.kind === "quantitative" ? [p.min, p.max] : p.kind === "temporal" ? [Date.parse(p.min), Date.parse(p.max)] : [NaN, NaN];
  return (min === undefined || lo >= min) && (max === undefined || hi <= max);
}

/** The primary key's fields (empty when there is none). */
export function primaryKey(d: Dataset): string[] {
  return list(d.primaryKey);
}

/** A foreign key with its string-or-array forms normalized; `resource` is the dataset's own name for a self-reference. */
export interface Join {
  fields: string[];
  resource: string;
  referenceFields: string[];
  self: boolean;
}

export function joins(d: Dataset): Join[] {
  return (d.foreignKeys ?? []).map((k) => {
    const resource = k.reference.resource || d.name;
    return { fields: list(k.fields), resource, referenceFields: list(k.reference.fields), self: resource === d.name };
  });
}

/** The values a `missingValues` list marks as missing, as text (empty when there is no list). */
export function missingMarkers(values: MissingValues | undefined): string[] {
  return (values ?? []).map((m) => (typeof m === "object" ? m.value : m));
}

/**
 * The markers that count as missing in a field: its own list, else the schema's (Table
 * Schema v2); null when neither has one, and the default (empty cells) applies.
 */
export function effectiveMissing(d: Dataset, f: Field): string[] | null {
  const values = f.missingValues ?? d.missingValues;
  return values ? missingMarkers(values) : null;
}
