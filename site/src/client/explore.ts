/**
 * Explore, live: the dataset's chart (a scatter plot of two measures, the starter time
 * series, or the starter map), drawn into the section the page already has, with the
 * line naming its Vega-Lite features and the Editor button following the pickers.
 * dataset.ts loads this module when the section comes near the screen.
 *
 * Long tables open on their density overview (bins from the page, no download) until
 * the reader asks to draw all points; heavy maps open on a picture until asked.
 */
import type { Dataset } from "../lib/catalog";
import {
  bothValuesNote,
  chartFeatures,
  defaultAxes,
  type DensityBin,
  densityFromBins,
  densitySpec,
  exploreModes,
  type Mode,
  parseTable,
  readsRows,
  scatterFields,
  scatterSpec,
  siteDataUrl,
  starterChart,
  withDataUrl,
  withValues,
} from "../lib/explore-model";
import { formatCount } from "../lib/format";
import { editorUrl, starterSpec } from "../lib/starter";
import { $, h, readJson } from "./dom";
import { embedOptions, labelActions, loadVega } from "./embed";
import { onThemeChange } from "./theme";

type Spec = Record<string, unknown>;

const PHONE = "(max-width: 640px)";
/** Above this many rows, draw on canvas: SVG slows down with tens of thousands of marks. */
const CANVAS_ROWS = 5000;

/** "an origin", "a species". */
function withArticle(word: string): string {
  return `${/^[aeiou]/i.test(word) ? "an" : "a"} ${word}`;
}

export function enhanceExplore(section: HTMLElement, d: Dataset): void {
  const modes = exploreModes(d);
  if (!modes.length) return;
  const phone = matchMedia(PHONE);
  const fields = scatterFields(d);
  const state: { mode: Mode; x: string; y: string } = { mode: modes[0]!, ...(fields ? defaultAxes(fields) : { x: "", y: "" }) };
  const canvas = (d.rows ?? 0) > CANVAS_ROWS;

  const binds = $(".binds", section);
  const host = $(".explore-chart", section);
  const note = $(".chart-caption .hint", section);
  const features = $(".chart-caption .features", section);
  const edit = $<HTMLAnchorElement>("[data-editor]", section);
  const drawButton = host.querySelector<HTMLButtonElement>("button.draw");
  const drawAll = section.querySelector<HTMLButtonElement>("[data-draw-all]");
  // The density overview, while it shows: bins written into the page when it was built.
  let density = section.hasAttribute("data-density") ? readJson<DensityBin[]>("density-data") : null;

  section.querySelectorAll<HTMLButtonElement>(".seg [data-mode]").forEach((b) => {
    b.addEventListener("click", () => {
      const m = b.dataset.mode as Mode;
      if (state.mode === m) return;
      state.mode = m;
      section.querySelectorAll(".seg [data-mode]").forEach((x) => x.setAttribute("aria-pressed", String(x === b)));
      void render();
    });
  });

  /** The spec as the page draws it (and, with the public data URL, as the Editor opens it). */
  const currentSpec = (): Spec => {
    if (state.mode === "scatter" && fields) {
      return scatterSpec(d, fields, { x: state.x, y: state.y, zoom: !phone.matches, height: phone.matches ? 300 : 380 });
    }
    return starterChart(d) ?? starterSpec(d)!;
  };

  // Tables are read once, here, when the chart is first drawn, and handed to Vega (see
  // parseTable); the caption's row count reuses them but never reads the file itself.
  let rowsPromise: Promise<Record<string, unknown>[]> | null = null;
  let loadedRows: Record<string, unknown>[] | null = null;
  const dataUrl = `${import.meta.env.BASE_URL}${siteDataUrl(d)}`;
  const rows = () => (rowsPromise ??= fetch(dataUrl).then((res) => {
    if (!res.ok) throw new Error(`Could not load ${d.file} (HTTP ${res.status})`);
    return res.text();
  }).then((text) => (loadedRows = parseTable(text, d.format))));

  const describe = (spec: Spec) => {
    if (density) {
      edit.href = editorUrl(densitySpec(d, state, 380));
      features.textContent = chartFeatures(densitySpec(d, state, 380)).join(" · ");
      note.hidden = false;
      note.textContent = `Rows per bin: all ${formatCount(d.rows)} rows, binned when the site was built. Draw all points to pick the fields.`;
      return;
    }
    edit.href = editorUrl(spec);
    features.textContent = chartFeatures(spec).join(" · ");
    note.hidden = state.mode !== "scatter" || !fields;
    if (state.mode !== "scatter" || !fields) return;
    const color = fields.color ? ` ${phone.matches ? "Tap" : "Click"} the legend to isolate ${withArticle(fields.color.name.toLowerCase())}.` : "";
    const lead = phone.matches ? `Pick two fields.${color}` : `Pick two fields. Scroll to zoom, drag to pan.${color}`;
    const count = bothValuesNote(d, loadedRows, state.x, state.y);
    note.textContent = count ? `${lead} ${count}` : lead;
  };

  let result: { view: import("vega").View; finalize(): void } | undefined;
  let queue: Promise<void> = Promise.resolve();
  const draw = async () => {
    const v = await loadVega();
    result?.finalize();
    binds.replaceChildren();
    const spec = currentSpec();
    const site = density
      ? densityFromBins(d, state, phone.matches ? 300 : 380, density)
      : readsRows(d) ? withValues(spec, await rows()) : withDataUrl(spec, dataUrl);
    binds.hidden = state.mode !== "scatter" || density !== null;
    // The live chart takes the place of the build's picture and its button.
    host.querySelectorAll(".chart-preview, button.draw").forEach((el) => el.remove());
    // The menu's Editor action would open the page's same-origin data path; the button opens the public one.
    result = await v.vegaEmbed(host, site as never, {
      ...embedOptions(v, canvas && !density ? "canvas" : "svg", { export: true, source: true, compiled: true, editor: false }),
      bind: binds,
    });
    labelActions(host);
    describe(spec);
    if (state.mode === "scatter" && !density) {
      const view = result.view;
      const follow = (axis: "x" | "y") => (_name: string, value: unknown) => {
        state[axis] = String(value);
        // A zoom on the old fields would hide the new ones: clear it (the scale domains read this store).
        if (!phone.matches) void view.change("zoom_store", view.changeset().remove(() => true)).runAsync();
        describe(currentSpec());
      };
      view.addSignalListener("xField", follow("x"));
      view.addSignalListener("yField", follow("y"));
    }
  };
  let requested = false;
  const render = () => {
    requested = true;
    return (queue = queue.then(draw).catch((err: unknown) => {
      host.replaceChildren(h("p", { class: "muted" }, `The chart didn't load: ${err instanceof Error ? err.message : String(err)}`));
    }));
  };

  // Redraw for a new screen size once a chart is asked for (a large file waits for its
  // button): a change during the first draw, while the file is still loading, queues a
  // second draw at the new size. Canvas charts also redraw for a new theme (SVG charts
  // restyle through the stylesheet).
  const redraw = () => {
    if (requested) void render();
  };
  if (canvas) onThemeChange(redraw);
  phone.addEventListener("change", redraw);
  describe(currentSpec());
  drawAll?.addEventListener("click", () => {
    density = null;
    drawAll.remove();
    void render();
  }, { once: true });
  if (drawButton) drawButton.addEventListener("click", () => void render(), { once: true });
  else void render();
}
