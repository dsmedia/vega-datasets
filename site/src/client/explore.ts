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
import { bothValuesNote, chartFeatures, defaultAxes, exploreModes, type Mode, scatterFields, scatterSpec, starterChart } from "../lib/explore-model";
import { allowed, BAND_POLICY, type DensityGrid, densityCaption, densityPageSpec, densitySpec, tableBand } from "../lib/large-data";
import { editorUrl, starterSpec } from "../lib/starter";
import { pointSource } from "../lib/vega-data";
import { $, h, readJson } from "./dom";
import { embedOptions, labelActions, loadVega } from "./embed";
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
  const state: { mode: Mode; x: string; y: string } = { mode: modes[0]!, ...(fields ? defaultAxes(fields) : { x: "", y: "" }) };

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
      section.querySelectorAll(".seg [data-mode]").forEach((x) => x.setAttribute("aria-pressed", String(x === b)));
      void render();
    });
  });

  const height = () => (phone.matches ? 300 : 380);
  /** The spec the page draws, exactly as the Editor opens it (the overview's Editor spec bins the public file). */
  const currentSpec = (): Spec => {
    if (density) return densityPageSpec(d, density, height());
    if (state.mode === "scatter" && fields) return scatterSpec(d, fields, { x: state.x, y: state.y, zoom: zoom(), height: height() });
    return starterChart(d) ?? starterSpec(d)!;
  };

  const describe = () => {
    if (density) {
      edit.href = editorUrl(densitySpec(d, density, 380));
      features.textContent = chartFeatures(densityPageSpec(d, density, 380)).join(" · ");
      note.hidden = false;
      note.textContent = densityCaption(density);
      return;
    }
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
  let queue: Promise<void> = Promise.resolve();
  const draw = async () => {
    const v = await loadVega();
    result?.finalize();
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
    // Vega draws an empty chart when its file doesn't load: say so instead, and count nothing.
    if (failed.length) throw new LoadError(failed[0]!.split("/").pop()!);
    labelActions(host);
    const view = result.view;
    const source = state.mode === "scatter" && !density ? pointSource(result.vgSpec as never) : null;
    const recount = () => {
      plotted = source ? (view.data(source) as unknown[]).length : null;
      describe();
    };
    recount();
    if (source) {
      const follow = (axis: "x" | "y") => (_name: string, value: unknown) => {
        state[axis] = String(value);
        // A zoom on the old fields would hide the new ones: clear it (the scale domains read this store).
        if (zoom()) void view.change("zoom_store", view.changeset().remove(() => true)).runAsync();
        void view.runAsync().then(recount);
      };
      view.addSignalListener("xField", follow("x"));
      view.addSignalListener("yField", follow("y"));
    }
  };
  let requested = false;
  const render = () => {
    requested = true;
    return (queue = queue.then(draw).catch((err: unknown) => {
      result?.finalize();
      result = undefined;
      binds.replaceChildren();
      plotted = null;
      describe();
      const retry = h("button", { class: "btn", type: "button", "data-retry": "" }, "Retry");
      retry.addEventListener("click", () => void render(), { once: true });
      const message = err instanceof LoadError ? `Couldn't load ${err.file}.` : `The chart didn't load: ${err instanceof Error ? err.message : String(err)}`;
      host.replaceChildren(h("p", { class: "muted load-error" }, message, " ", retry));
    }));
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
    void render();
  }, { once: true });
  // A mid-size table's button (data-auto-draw="desktop") stands aside on a desktop-class device.
  if (!drawButton || (drawButton.dataset.autoDraw === "desktop" && desktop)) void render();
  else drawButton.addEventListener("click", () => void render(), { once: true });
}
