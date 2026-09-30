/**
 * Explore, live: the dataset's chart (a scatter plot of two measures, the starter time
 * series, or the starter map), drawn into the section the page already has, with the
 * line naming its Vega-Lite features and the Editor button following the pickers.
 * dataset.ts loads this module when the section comes near the screen.
 *
 * The chart runs the spec the Editor opens, public data URL included (embed.ts adapts
 * Vega to the page's CSP). How and when it draws follows the large-data policy
 * (lib/large-data.ts): long tables open on their density overview (bins from the page,
 * no download) until the reader asks for all points; mid-size tables draw by themselves
 * only on a desktop-class device (lib/device.ts); heavy maps open on a picture.
 */
import type { Dataset } from "../lib/catalog";
import { deviceSignals, isDesktopClass } from "../lib/device";
import { bothValuesNote, chartFeatures, defaultAxes, exploreModes, type Mode, modeChart, pickScale, scatterFields, scatterSpec } from "../lib/explore-model";
import { scaleFor } from "../lib/chart-rules";
import { allowed, BAND_POLICY, type DensityGrid, densityPageSpec, tableBand } from "../lib/large-data";
import { editorUrl, starterSpec } from "../lib/starter";
import { pointSource } from "../lib/vega-data";
import { $, h, readJson } from "./dom";
import { serial } from "../lib/serial";
import { ChartCodeError, embedOptions, labelActions, loadVega } from "./embed";
import { ENTRY, legendKeys, selectionStores } from "./legend-keys";
import { onThemeChange } from "./theme";

type Spec = Record<string, unknown>;

const PHONE = "(max-width: 640px)";

/** A data file that didn't load (Vega itself only logs it, and draws no rows). */
class LoadError extends Error {
  constructor(readonly file: string) {
    super(`Couldn't load ${file}.`);
  }
}

/** "an origin", "a species". */
function withArticle(word: string): string {
  return `${/^[aeiou]/i.test(word) ? "an" : "a"} ${word}`;
}

export function enhanceExplore(section: HTMLElement, d: Dataset): void {
  const modes = exploreModes(d);
  if (!modes.length) return;
  const phone = matchMedia(PHONE);
  const desktop = isDesktopClass(deviceSignals(window));
  const band = tableBand(d);
  const policy = band ? BAND_POLICY[band] : null;
  const canvas = policy?.renderer === "canvas";
  // Scroll to zoom traps page scrolling on a narrow screen, and redraws every point per wheel step.
  const zoom = () => !phone.matches && (!policy || allowed(policy.zoom, desktop));
  const fields = scatterFields(d);
  /** A picked field's scale and axis, as text to compare: the same for any two linear fields. */
  const scaleOf = (name: string): string => {
    const m = fields?.measures.find((x) => x.name === name);
    return m && pickScale(fields!, name) !== "linear" ? JSON.stringify(scaleFor(m)) : "linear";
  };
  const state: { mode: Mode; x: string; y: string } = { mode: modes[0]!, ...(fields ? defaultAxes(d, fields) : { x: "", y: "" }) };

  const binds = $(".binds", section);
  const host = $(".explore-chart", section);
  const note = $(".chart-caption .hint", section);
  const features = $(".chart-caption .features", section);
  const edit = $<HTMLAnchorElement>("[data-editor]", section);
  const drawButton = host.querySelector<HTMLButtonElement>("button.draw");
  const drawAll = section.querySelector<HTMLButtonElement>("[data-draw-all]");
  // The density overview, while it shows: bins written into the page when it was built.
  let density = section.hasAttribute("data-density") ? readJson<DensityGrid>("density-data") : null;
  // How many rows the scatter plot draws: counted for the default fields when the site was
  // built (so the caption keeps its length), then read from the view after each run.
  let plotted: number | null = note.dataset.plotted ? Number(note.dataset.plotted) : null;

  section.querySelectorAll<HTMLButtonElement>(".seg [data-mode]").forEach((b) => {
    b.addEventListener("click", () => {
      const m = b.dataset.mode as Mode;
      if (state.mode === m) return;
      state.mode = m;
      // The build reserved the first mode's height; another mode sizes the box itself.
      host.style.removeProperty("--chart-h");
      host.style.removeProperty("--chart-h-phone");
      section.querySelectorAll(".seg [data-mode]").forEach((x) => x.setAttribute("aria-pressed", String(x === b)));
      void render();
    });
  });

  const height = () => (phone.matches ? 300 : 380);
  // The overview's spec per height, built once: a thousand bins, each with its tooltip text.
  const overviews = new Map<number, Spec>();
  const overview = (g: DensityGrid, h: number): Spec => {
    let spec = overviews.get(h);
    if (!spec) overviews.set(h, (spec = densityPageSpec(d, g, h, phone.matches)));
    return spec;
  };
  /** The spec the page draws, exactly as the Editor opens it (the overview's Editor spec bins the public file). */
  const currentSpec = (): Spec => {
    if (density) return overview(density, height());
    if (state.mode === "scatter" && fields) return scatterSpec(d, fields, { x: state.x, y: state.y, zoom: zoom(), height: height(), phone: phone.matches });
    return modeChart(d, state.mode, phone.matches) ?? starterSpec(d)!;
  };

  const describe = () => {
    // The overview's caption, features and Editor link are in the page as built (they
    // don't change while it shows); working them out again would cost a thousand-bin spec.
    if (density) return;
    const spec = currentSpec();
    edit.href = editorUrl(spec);
    features.textContent = chartFeatures(spec).join(" · ");
    note.hidden = state.mode !== "scatter" || !fields;
    if (state.mode !== "scatter" || !fields) return;
    const color = fields.color ? ` ${phone.matches ? "Tap" : "Click"} the legend to isolate ${withArticle(fields.color.name.toLowerCase())}.` : "";
    const lead = zoom() ? `Pick two fields. Scroll to zoom, drag to pan.${color}` : `Pick two fields.${color}`;
    const count = bothValuesNote(d, plotted);
    note.textContent = count ? `${lead} ${count}` : lead;
  };

  let result: import("vega-embed").Result | undefined;
  let drawnSpec: Spec | null = null;
  let drawnMode: Mode = state.mode;
  let disposeKeys = () => {};
  /** The picker (0 x, 1 y) to focus once the next draw has put new ones in place. */
  let refocus: number | null = null;
  /** The legend entry (by its label) to focus once the next draw is in place. */
  let refocusEntry: string | null = null;
  /**
   * What the reader set on each mode's chart, kept across redraws: the selection stores
   * (legend isolation, zoom) by name. A redraw for a new screen size or theme puts them back;
   * a store the new chart lacks (no zoom on a phone) waits for a chart that has it.
   */
  const kept = new Map<Mode, Map<string, unknown[]>>();
  /** The fields a zoom is on: the drawn chart's, and the kept one's (put back only on the same fields). */
  const axes = () => JSON.stringify([state.x, state.y]);
  let zoomedAxes = axes();
  let keptZoomAxes = "";
  /** Keep the stores and the focus of the chart about to go. */
  const keep = () => {
    if (!result || !drawnSpec) return;
    const stores = kept.get(drawnMode) ?? new Map<string, unknown[]>();
    for (const name of selectionStores(drawnSpec)) {
      try {
        stores.set(name, [...(result.view.data(name) as unknown[])]);
        if (name === "zoom_store") keptZoomAxes = zoomedAxes;
      } catch {
        // Not in this view: keep what was kept.
      }
    }
    kept.set(drawnMode, stores);
    const active = document.activeElement;
    if (active && host.contains(active)) refocusEntry = active.closest(ENTRY)?.getAttribute("aria-label") ?? null;
    const pickers = [...binds.querySelectorAll("select")];
    if (active && pickers.includes(active as HTMLSelectElement)) refocus = pickers.indexOf(active as HTMLSelectElement);
  };
  /** Put the kept stores back into a new chart of the same mode (those it has). */
  const restore = async (view: import("vega").View, spec: Spec) => {
    const stores = kept.get(state.mode);
    if (!stores) return;
    let changed = false;
    for (const name of selectionStores(spec)) {
      const values = stores.get(name);
      // A zoom on other fields than the ones now picked would hide them.
      if (!values?.length || (name === "zoom_store" && keptZoomAxes !== axes())) continue;
      view.data(name, values);
      changed = true;
    }
    if (changed) await view.runAsync();
  };
  const draw = async () => {
    const v = await loadVega();
    // The box keeps its height while the old chart goes and the new one draws (nothing below
    // moves), then fits the new chart.
    host.style.minHeight = `${host.offsetHeight}px`;
    keep();
    disposeKeys();
    result?.finalize();
    drawnSpec = null;
    binds.replaceChildren();
    plotted = null;
    const spec = currentSpec();
    binds.hidden = state.mode !== "scatter" || density !== null;
    // The live chart takes the place of the build's picture and its button.
    host.querySelectorAll(".chart-preview, button.draw, .load-error").forEach((el) => el.remove());
    const failed: string[] = [];
    result = await v.vegaEmbed(host, spec as never, {
      ...embedOptions(v, canvas && !density ? "canvas" : "svg", { export: true, source: true, compiled: true, editor: false }, (uri) => failed.push(uri)),
      bind: binds,
    });
    host.style.minHeight = "";
    // How many times the chart has been drawn: once on open, unless asked (the browser check reads it).
    section.dataset.draws = String(Number(section.dataset.draws ?? 0) + 1);
    // Vega draws an empty chart when its file doesn't load: say so instead, and count nothing.
    if (failed.length) throw new LoadError(failed[0]!.split("/").pop()!);
    drawnSpec = spec;
    drawnMode = state.mode;
    zoomedAxes = axes();
    labelActions(host);
    // What the reader had set on this chart before the redraw: isolation, zoom.
    await restore(result.view, spec);
    // Legend entries isolate from the keyboard too (SVG charts; a canvas legend has no elements).
    disposeKeys = legendKeys(host, result.view, selectionStores(spec, "legend"));
    if (refocusEntry !== null) {
      const label = refocusEntry;
      [...host.querySelectorAll<SVGGElement>(ENTRY)].find((g) => g.getAttribute("aria-label") === label)?.focus();
    }
    refocusEntry = null;
    // A select narrowed to fit its row clips a long field name: its tooltip gives it in full.
    binds.querySelectorAll("select").forEach((select) => {
      const name = () => (select.title = select.value);
      name();
      select.addEventListener("change", name);
    });
    if (refocus !== null) binds.querySelectorAll("select")[refocus]?.focus();
    refocus = null;
    const view = result.view;
    const source = state.mode === "scatter" && !density ? pointSource(result.vgSpec as never) : null;
    const recount = () => {
      plotted = source ? (view.data(source) as unknown[]).length : null;
      describe();
    };
    recount();
    if (source) {
      const follow = (axis: "x" | "y") => (_name: string, value: unknown) => {
        const before = scaleOf(state[axis]);
        state[axis] = String(value);
        // A field on a log scale (or leaving one, or to another log field's domain and ticks)
        // needs a new chart: a scale's type, fitted domain and ticks can't follow a param.
        if (scaleOf(state[axis]) !== before) {
          // The redraw replaces the pickers: the one in use gets focus back.
          refocus = axis === "x" ? 0 : 1;
          void render();
          return;
        }
        // A zoom on the old fields would hide the new ones: clear it (the scale domains read this store).
        if (zoom()) void view.change("zoom_store", view.changeset().remove(() => true)).runAsync();
        zoomedAxes = axes();
        void view.runAsync().then(recount);
      };
      view.addSignalListener("xField", follow("x"));
      view.addSignalListener("yField", follow("y"));
    }
  };
  let requested = false;
  // A Retry after the chart code failed to load is under way.
  let retryingCode = false;
  // One draw at a time; requests during a draw (a window dragged across the breakpoint) join
  // the next one instead of queuing one each.
  const queued = serial(() =>
    draw().catch((err: unknown) => {
      // Chrome keeps a failed dynamic import in its module map, so importing again fails at
      // once even when the connection is back (other browsers fetch again). When a Retry of
      // the chart code fails while online, reload the page, as Vite advises: a new document
      // fetches every module afresh.
      if (err instanceof ChartCodeError && retryingCode && navigator.onLine) {
        location.reload();
        return;
      }
      retryingCode = false;
      disposeKeys();
      disposeKeys = () => {};
      result?.finalize();
      drawnSpec = null;
      result = undefined;
      binds.replaceChildren();
      plotted = null;
      describe();
      const retry = h("button", { class: "btn", type: "button", "data-retry": "" }, "Retry");
      retry.addEventListener("click", () => {
        retryingCode = err instanceof ChartCodeError;
        void render();
      }, { once: true });
      const message =
        err instanceof LoadError ? `Couldn't load ${err.file}.`
        : err instanceof ChartCodeError ? err.message
        : `The chart didn't load: ${err instanceof Error ? err.message : String(err)}`;
      host.replaceChildren(h("p", { class: "muted load-error" }, message, " ", retry));
    }),
  );
  const render = () => {
    requested = true;
    return queued();
  };

  // Redraw for a new screen size once a chart is asked for (a large file waits for its
  // button): a change during the first draw, while the file is still loading, queues a
  // second draw at the new size. Canvas charts also redraw for a new theme (SVG charts
  // restyle through the stylesheet, except for the overview's data colors).
  const redraw = () => {
    if (requested) void render();
  };
  // The density overview's ramp follows the theme (config.range.heatmap), so it redraws too.
  onThemeChange(() => {
    if (canvas || density) redraw();
  });
  phone.addEventListener("change", redraw);
  describe();
  drawAll?.addEventListener("click", () => {
    density = null;
    drawAll.remove();
    // The overview is of the scatter plot: its points, whatever mode was pressed.
    state.mode = "scatter";
    section.querySelectorAll(".seg [data-mode]").forEach((x) => x.setAttribute("aria-pressed", String((x as HTMLElement).dataset.mode === "scatter")));
    void render();
  }, { once: true });
  // A mid-size table's button (data-auto-draw="desktop") stands aside on a desktop-class device.
  if (!drawButton || (drawButton.dataset.autoDraw === "desktop" && desktop)) void render();
  else drawButton.addEventListener("click", () => void render(), { once: true });
}
