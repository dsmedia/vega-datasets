/** What a dataset page says about loading and describing a file, as plain data (unit-tested). */
import type { Dataset, Example, Gallery } from "./catalog";
import { GALLERIES } from "./catalog";
import { formatBytes, formatCount, FORMAT_LABEL } from "./format";
import type { Snippet } from "./components";

/** Released files are on jsDelivr (npm); newer ones are only on GitHub Pages so far. */
export function isReleased(d: Dataset): boolean {
  const url = new URL(d.url);
  return url.hostname === "cdn.jsdelivr.net" && url.pathname.startsWith("/npm/vega-datasets@");
}

/** A variable name for the loaded data. */
function variable(d: Dataset): string {
  const id = d.name.replace(/[^A-Za-z0-9_$]/g, "_");
  return /^[0-9]/.test(id) ? `_${id}` : id;
}

/** The files Vega-Lite reads with a plain `data.url`. */
const VL_FORMATS = new Set(["csv", "tsv", "json", "topojson", "geojson"]);

/**
 * How to load the file: its URL; the npm package and Altair (released files only,
 * since both follow npm releases); and a Vega-Lite `data` block (formats it reads).
 */
export function useSnippets(d: Dataset): Snippet[] {
  const out: Snippet[] = [{ name: "URL", code: d.url }];
  const v = variable(d);
  const parsed = d.format === "json" || d.format === "csv";
  if (isReleased(d)) {
    out.push({
      name: "JavaScript",
      code: `import data from 'vega-datasets';\n\n${parsed ? `const ${v} = await data['${d.file}']();` : `const url = data['${d.file}'].url;`}`,
    });
  }
  if (VL_FORMATS.has(d.format)) {
    const data: Record<string, unknown> = { url: d.url };
    if (d.format === "topojson" && d.objects?.[0]) data.format = { type: "topojson", feature: d.objects[0] };
    if (d.format === "geojson") data.format = { type: "json", property: "features" };
    out.push({ name: "Vega-Lite", code: `"data": ${JSON.stringify(data, null, 2)}` });
  }
  if (isReleased(d)) {
    out.push({
      name: "Python",
      code: `from altair.datasets import data\n\n${d.kind === "table" ? `${v} = data.${d.name}()` : `url = data.${d.name}.url`}`,
    });
  }
  return out;
}

export function fileName(d: Dataset): string {
  return d.file.split("/").pop() ?? d.file;
}

export function formatDescription(d: Dataset): string {
  const label = FORMAT_LABEL[d.format] ?? d.format.toUpperCase();
  if (d.format === "json" && d.kind === "table") return "JSON, array of records";
  if (d.kind === "file") return `${label} image`;
  return label;
}

export function sizeDescription(d: Dataset): string {
  return [formatBytes(d.bytes), d.rows !== null ? `${formatCount(d.rows)} rows` : null].filter(Boolean).join(" · ");
}

/** Link text for a URL: its file name if it has one, else its host. */
export function linkText(url: string): string {
  try {
    const u = new URL(url);
    const last = u.pathname.split("/").filter(Boolean).pop() ?? "";
    return last.includes(".") ? last : u.hostname;
  } catch {
    return url;
  }
}

/** Examples in gallery turns (Vega-Lite, Vega, Altair, Vega-Lite, …) so "All" shows every gallery up front. */
export function interleave(examples: Example[], galleries: readonly Gallery[] = GALLERIES): Example[] {
  const queues = galleries.map((g) => examples.filter((e) => e.gallery === g));
  const out: Example[] = [];
  for (let i = 0; out.length < examples.length; i++) {
    for (const q of queues) if (q[i]) out.push(q[i]!);
  }
  return out;
}
