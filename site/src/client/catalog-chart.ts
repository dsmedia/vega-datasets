/**
 * The home page's catalog chart, live. The page arrives with the chart drawn to SVG at
 * build time (links on its points work without scripts); the first time the reader
 * uses a mouse, keyboard or filters the list, the live view (brush, tooltips, menu)
 * replaces the static drawing in the same box, so nothing below moves.
 */
import { type ChartOptions, catalogSpec, toBrush } from "../lib/catalog-chart";
import type { Brush, ChartRow } from "../lib/home-model";
import type { Gallery } from "../lib/catalog";
import type { View } from "vega";
import { embedOptions, labelActions, loadVega } from "./embed";

export interface MountedChart {
  /** Draw again for a layout change (clears the brush). */
  redraw(): Promise<void>;
  /** Recount through Vega's galleries parameter, without replacing the view. */
  setGalleries(galleries: Gallery[]): void;
  clearBrush(): void;
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
  let galleries = options().galleries ?? [];
  let brush: Brush | null = null;
  const enqueue = (work: () => Promise<void>) => (queue = queue.then(work));
  const standalone = () => ({
    ...catalogSpec(rows.map((r) => ({ ...r, href: new URL(r.href, document.baseURI).href })), {
      ...options(), galleries, matched, initialBrush: brush, legend: true,
    }),
    width: Math.max(host.clientWidth - 38, 240),
  });
  // Embed's Editor action serializes its public input spec. Keep that snapshot current,
  // with a native legend: the external Editor has none of this page's format buttons.
  const syncEditor = () => { if (result) Object.assign(result.spec, standalone()); };
  // Embed's documented viewClass hook keeps its own PNG/SVG actions. The export is drawn
  // by a normal Vega view with a legend; exporting never changes the chart on the page.
  class ExportableView extends v.View {
    override async toImageURL(...args: Parameters<View["toImageURL"]>): Promise<string> {
      await queue;
      const output = await v.vegaEmbed(document.createElement("div"), standalone() as never, {
        ...embedOptions(v, "svg", false), tooltip: false,
      });
      try { return await output.view.toImageURL(...args); }
      finally { output.finalize(); }
    }
  }
  const draw = async () => {
    if (destroyed) return;
    result?.finalize();
    const o = options();
    brush = null;
    result = await v.vegaEmbed(live, catalogSpec(rows, { ...o, galleries, matched }) as never, {
      ...embedOptions(v, "svg", { export: true, source: false, compiled: false, editor: true }),
      viewClass: ExportableView,
    });
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
    onBrush(null);
    syncEditor();
    if (o.brush) result.view.addSignalListener("brush", (_name, value) => {
      brush = toBrush(value);
      onBrush(brush);
      syncEditor();
    });
  };
  const redraw = () => enqueue(draw);
  await redraw();
  return {
    redraw,
    setGalleries: (next) => {
      if (JSON.stringify(next) === JSON.stringify(galleries)) return;
      galleries = [...next];
      void enqueue(async () => {
        // A documented Vega event stream clears the selection's data and geometry.
        // Avoid depending on the compiler's private brush_x / brush_y signal names.
        window.dispatchEvent(new Event("catalogclear"));
        await result?.view.signal("galleries", galleries).runAsync();
        syncEditor();
      });
    },
    clearBrush: () => {
      void enqueue(async () => {
        window.dispatchEvent(new Event("catalogclear"));
        await result?.view.runAsync();
        syncEditor();
      });
    },
    setMatches: (names) => {
      if (JSON.stringify(names) === JSON.stringify(matched)) return;
      matched = names;
      void enqueue(async () => {
        await result?.view.signal("matched", matched).runAsync();
        syncEditor();
      });
    },
    destroy: () => {
      destroyed = true;
      result?.finalize();
    },
  };
}
