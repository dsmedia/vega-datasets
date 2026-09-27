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

export interface Field {
  name: string;
  type: string;
  description: string | null;
  profile: Profile;
}

export interface Dataset {
  name: string;
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
  objects?: string[];
  image?: string;
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

export async function loadCatalog(): Promise<Catalog> {
  const res = await fetch("catalog.json");
  if (!res.ok) throw new Error(`Could not load catalog.json (HTTP ${res.status})`);
  return new Catalog((await res.json()) as CatalogFile);
}

export const GALLERY_LABEL: Record<Gallery, string> = {
  vega: "Vega",
  "vega-lite": "Vega-Lite",
  altair: "Altair",
};

export function githubSource(d: Dataset): string {
  return `https://github.com/vega/vega-datasets/blob/main/data/${d.file}`;
}

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
