/**
 * How the page runs the exact spec it shows, public data URL included, under its CSP
 * (connect-src 'self', no 'unsafe-eval'). Two documented Vega hooks, with no DOM here so
 * they are unit-tested; client/embed.ts installs them:
 * - `vega.formats()`: CSV and TSV readers that build rows without compiling code. Vega's
 *   own readers use d3-dsv's `parse`, which compiles a row converter with `new Function`.
 * - a Vega loader that fetches a public vega-datasets URL from the site's own `data/`.
 */
import { dsvFormat } from "d3-dsv";

type Row = Record<string, string>;

/** A reader for `vega.formats(name, reader)`: rows as objects, without `new Function`. */
export function dsvReader(delimiter: string): (text: string) => Row[] {
  const format = dsvFormat(delimiter);
  return (text) => {
    const [columns = [], ...rows] = format.parseRows(text);
    return rows.map((r) => Object.fromEntries(columns.map((c, i) => [c, r[i] ?? ""])));
  };
}

/** The readers to register: CSV, TSV, and Vega's "dsv" type with its own delimiter. */
export function readers(): Record<string, (text: string, format?: { delimiter?: string }) => Row[]> {
  const csv = dsvReader(",");
  const tsv = dsvReader("\t");
  return {
    csv: (text) => csv(text),
    tsv: (text) => tsv(text),
    dsv: (text, format) => dsvReader(format?.delimiter ?? ",")(text),
  };
}

/** A vega-datasets data URL as specs publish it: a jsDelivr release, or GitHub Pages. */
export const PUBLIC_DATA = /^https:\/\/(?:cdn\.jsdelivr\.net\/npm\/vega-datasets@[^/]+|vega\.github\.io\/vega-datasets)\/data\//;

/**
 * Where the page fetches `uri` from: a public vega-datasets data URL becomes the same file
 * under `siteData` (the site's own `data/`, an absolute URL ending in a slash); any other
 * URI is left as it is.
 */
export function siteDataUri(uri: string, siteData: string): string {
  return uri.replace(PUBLIC_DATA, siteData);
}

interface VegaMark {
  type?: string;
  from?: { data?: string; facet?: unknown };
  marks?: VegaMark[];
}

/**
 * The dataset the scatter plot's points draw from, in the compiled Vega spec: the rows
 * left after its filters (both values present), whose length is the caption's count.
 */
export function pointSource(vg: { marks?: VegaMark[] }): string | null {
  for (const m of vg.marks ?? []) {
    if (m.type === "symbol" && m.from?.data) return m.from.data;
    const inner = pointSource(m);
    if (inner) return inner;
  }
  return null;
}
