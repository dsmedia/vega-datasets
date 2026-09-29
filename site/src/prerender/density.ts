/**
 * The density overview of a long table (explore-model.ts), binned when the site is built:
 * headless Vega runs the same Vega-Lite 2D histogram the Editor link opens, on the local
 * file, so the page shows every row's place without downloading the file.
 */
import * as vega from "vega";
import { compile, type TopLevelSpec } from "vega-lite";
import type { Dataset } from "../lib/catalog";
import { type DensityBin, densitySpec } from "../lib/explore-model";
import { readDataUrl } from "./repo";

export async function densityBins(d: Dataset, axes: { x: string; y: string }): Promise<DensityBin[]> {
  const spec = { ...densitySpec(d, axes, 300), width: 600 } as unknown as TopLevelSpec;
  const { spec: vg } = compile(spec);
  const loader = vega.loader();
  loader.load = async (uri: string) => readDataUrl(uri);
  const view = new vega.View(vega.parse(vg), { renderer: "none", loader });
  try {
    await view.runAsync();
    // The rect marks' own data: one row per non-empty bin, with Vega-Lite's bin fields and the count.
    const marks = (vg.marks ?? []).find((m) => m.type === "rect");
    const source = (marks?.from as { data?: string } | undefined)?.data;
    if (!source) throw new Error(`No rect marks in the density spec for ${d.name}`);
    const rows = view.data(source) as Record<string, number>[];
    const key = (name: string, end = false) => Object.keys(rows[0] ?? {}).find((k) => k.startsWith("bin_") && k.includes(name.replace(/[^\w]/g, "_")) && k.endsWith("_end") === end)!;
    const [x0, x1, y0, y1] = [key(axes.x), key(axes.x, true), key(axes.y), key(axes.y, true)];
    return rows.map((r) => ({ x0: r[x0]!, x1: r[x1]!, y0: r[y0]!, y1: r[y1]!, count: r.__count! }));
  } finally {
    view.finalize();
  }
}
