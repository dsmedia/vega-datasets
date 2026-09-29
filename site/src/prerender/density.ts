/**
 * The density overview of a long table (lib/large-data.ts), binned when the site is built
 * from every row of the local file, so the page shows where the rows fall without
 * downloading the file.
 */
import * as vega from "vega";
import type { Dataset } from "../lib/catalog";
import { type DensityGrid, densityGrid } from "../lib/large-data";
import { readData } from "./repo";

/** Every row of a table file, as Vega reads it (CSV and TSV values stay strings). */
export function readRows(d: Dataset): Record<string, unknown>[] {
  return vega.read(readData(d.file), { type: d.format as "csv" | "tsv" | "json" }) as Record<string, unknown>[];
}

export function densityOf(d: Dataset, axes: { x: string; y: string }): DensityGrid {
  return densityGrid(readRows(d), axes.x, axes.y);
}
