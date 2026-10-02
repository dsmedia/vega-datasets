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
import { embedOptions, labelActions, loadVega, runView } from "./embed";

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

interface LiveChartOptions extends ChartOptions {
  /** Fit the page's live chart into the fallback's responsive outer height. */
  frameHeight?: number;
}

/**
 * Embed the live chart in `host`, replacing its static drawing. `options` is read on
 * every draw, so it can follow the host's width; `onBrush` hears every brush change
 * (null when cleared or redrawn).
 */
export async function mountCatalogChart(
  host: HTMLElement,
  rows: ChartRow[],
  options: () => LiveChartOptions,
  onBrush: (b: Brush | null) => void,
  onError: () => void = () => {},
): Promise<MountedChart> {
  const v = await loadVega();
  let live: HTMLElement | undefined;
  let result: Awaited<ReturnType<typeof v.vegaEmbed>> | undefined;
  let queue: Promise<void> = Promise.resolve();
  let destroyed = false;
  let matched: string[] | null = options().matched ?? null;
  let galleries = options().galleries ?? [];
  let brush: Brush | null = null;
  const enqueue = (work: () => Promise<void>) => {
    const task = queue.then(() => { if (!destroyed) return work(); });
    // Report a failed operation to its caller without poisoning later updates.
    queue = task.catch(() => {});
    return task;
  };
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
    // Keep the static or previous live chart until its replacement has drawn successfully.
    const next = document.createElement("div");
    next.className = "chart-live pending";
    host.append(next);
    const o = options();
    let drawn: NonNullable<typeof result>;
    try {
      const spec = {
        ...catalogSpec(rows, { ...o, galleries, matched }),
        // Vega measures the axes and fits the plot, without assuming fixed text metrics.
        // Standalone exports keep their normal plot height and room for the legend.
        ...(o.frameHeight === undefined ? {} : { height: o.frameHeight, autosize: { type: "fit", contains: "padding" } }),
      };
      drawn = await v.vegaEmbed(next, spec as never, {
        ...embedOptions(v, "svg", { export: true, source: false, compiled: false, editor: true }),
        viewClass: ExportableView,
      });
    } catch (err) {
      next.remove();
      throw err;
    }
    if (destroyed) {
      drawn.finalize();
      next.remove();
      return;
    }
    result?.finalize();
    live?.remove();
    live = next;
    result = drawn;
    brush = null;
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
  let resizeTimer: ReturnType<typeof setTimeout>;
  const pageHeight = () => { const o = options(); return o.frameHeight ?? o.height; };
  let lastHeight = pageHeight();
  const resize = () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => {
      const height = pageHeight();
      if (height === lastHeight) return;
      lastHeight = height;
      void enqueue(async () => {
        if (result) await runView(result.view, () => { result!.view.height(height); });
        syncEditor();
      }).catch(onError);
    }, 100);
  };
  let scheduled = false;
  let resetBrush = false;
  const scheduleSignals = () => {
    if (scheduled || destroyed) return;
    scheduled = true;
    void enqueue(async () => {
      scheduled = false;
      const clear = resetBrush;
      resetBrush = false;
      // A filter action changes both signals. Apply the latest values together in one
      // evaluation, after any previous run, rather than rendering intermediate states.
      if (clear) window.dispatchEvent(new Event("catalogclear"));
      if (result) await runView(result.view, () => {
        result!.view.signal("galleries", galleries).signal("matched", matched);
      });
      syncEditor();
    }).catch(onError);
  };
  await redraw();
  window.addEventListener("resize", resize);
  return {
    redraw,
    setGalleries: (next) => {
      if (JSON.stringify(next) === JSON.stringify(galleries)) return;
      galleries = [...next];
      resetBrush = true;
      scheduleSignals();
    },
    clearBrush: () => {
      resetBrush = true;
      scheduleSignals();
    },
    setMatches: (names) => {
      if (JSON.stringify(names) === JSON.stringify(matched)) return;
      matched = names ? [...names] : null;
      scheduleSignals();
    },
    destroy: () => {
      destroyed = true;
      clearTimeout(resizeTimer);
      window.removeEventListener("resize", resize);
      result?.finalize();
      live?.remove();
    },
  };
}
