/**
 * The home page's catalog chart, live. The page arrives with the chart drawn to SVG at
 * build time (links on its points work without scripts); the first time the reader
 * points at it, focuses it or filters the list, the live view (brush, tooltips, menu)
 * replaces the static drawing in the same box, so nothing below moves.
 */
import { type ChartOptions, catalogSpec, toBrush } from "../lib/catalog-chart";
import type { Brush, ChartRow, FormatGroup } from "../lib/home-model";
import { embedOptions, labelActions, loadVega } from "./embed";

export interface MountedChart {
  /** Draw again (clears the brush). */
  redraw(): Promise<void>;
  /**
   * Fade the points not in `names` (null: none). Sets a signal on the live view, so the
   * brush and the axes stay as they are; kept across redraws.
   */
  setMatches(names: string[] | null): void;
  destroy(): void;
}

/**
 * Embed the live chart in `host`, replacing its static drawing. `options` is read on
 * every draw, so it can follow the host's width; `onBrush` hears every brush change
 * (null when cleared or redrawn).
 */
export async function mountCatalogChart(
  host: HTMLElement,
  rows: ChartRow[],
  counts: Record<FormatGroup, number>,
  options: () => ChartOptions,
  onBrush: (b: Brush | null) => void,
): Promise<MountedChart> {
  const v = await loadVega();
  // Drawn out of sight over the static chart, then swapped in, so the page never shows both.
  const live = document.createElement("div");
  live.className = "chart-live pending";
  host.append(live);
  let result: Awaited<ReturnType<typeof v.vegaEmbed>> | undefined;
  let queue: Promise<void> = Promise.resolve();
  let destroyed = false;
  let matched: string[] | null = null;
  const draw = async () => {
    if (destroyed) return;
    result?.finalize();
    const o = options();
    result = await v.vegaEmbed(live, catalogSpec(rows, counts, o) as never, embedOptions(v, "svg", { export: true, source: false, compiled: false, editor: true }));
    if (destroyed) {
      result.finalize();
      return;
    }
    labelActions(host);
    // Vega-Lite gives an interval brush's marks an ARIA role but no name; they're decoration.
    live.querySelectorAll('[class*="brush_brush"]').forEach((g) => g.setAttribute("aria-hidden", "true"));
    // The live view is drawn: the static drawing goes.
    host.querySelectorAll(".chart-static").forEach((el) => el.remove());
    live.classList.remove("pending");
    if (matched) await result.view.signal("matched", matched).runAsync();
    onBrush(null);
    if (o.brush) result.view.addSignalListener("brush", (_name, value) => onBrush(toBrush(value)));
  };
  const redraw = () => (queue = queue.then(draw));
  await redraw();
  return {
    redraw,
    setMatches: (names) => {
      matched = names;
      void result?.view.signal("matched", names).runAsync();
    },
    destroy: () => {
      destroyed = true;
      result?.finalize();
    },
  };
}
