/**
 * Explore: a live chart of the dataset (a scatter plot of two measures, the starter
 * time series, or the starter map), a line naming the Vega-Lite features it uses,
 * and a button that opens exactly this chart in the Vega Editor.
 */
import type { Dataset } from "./catalog";
import { h } from "./dom";
import { formatBytes } from "./format";
import {
  bothValuesNote,
  chartFeatures,
  defaultAxes,
  exploreModes,
  MODE_LABEL,
  parseTable,
  readsRows,
  scatterFields,
  scatterSpec,
  siteDataUrl,
  starterChart,
  withDataUrl,
  withValues,
} from "./explore-model";
import { editorUrl, starterSpec } from "./starter";
import { onThemeChange } from "./theme";

type Spec = Record<string, unknown>;

const PHONE = "(max-width: 640px)";
/** Files above this size load only when asked (flights_200k_json is ~10 MB). */
const AUTO_LOAD_BYTES = 3e6;
/** Above this many rows, draw on canvas: SVG slows down with tens of thousands of marks. */
const CANVAS_ROWS = 5000;

let teardown: (() => void) | null = null;

export function stopExplore(): void {
  teardown?.();
  teardown = null;
}

/** "an origin", "a species". */
function withArticle(word: string): string {
  return `${/^[aeiou]/i.test(word) ? "an" : "a"} ${word}`;
}

export function exploreSection(d: Dataset): HTMLElement | null {
  stopExplore();
  const modes = exploreModes(d);
  if (!modes.length) return null;
  const phone = matchMedia(PHONE);
  const fields = scatterFields(d);
  const state = { mode: modes[0]!, ...(fields ? defaultAxes(fields) : { x: "", y: "" }) };

  const binds = h("div", { class: "binds" });
  const host = h("div", { class: "explore-chart" });
  const note = h("span", { class: "hint" });
  const features = h("span", { class: "features mono" });
  const edit = h("a", { class: "btn btn-primary", target: "_blank", rel: "noopener", href: "#" }, "Edit This Chart in the Vega Editor");
  const seg = modes.length > 1
    ? h("div", { class: "seg", role: "group", "aria-label": "Chart" }, modes.map((m) =>
        h("button", {
          type: "button",
          "aria-pressed": String(m === state.mode),
          onclick: (e: Event) => {
            if (state.mode === m) return;
            state.mode = m;
            for (const b of (e.currentTarget as HTMLElement).parentElement!.children) b.setAttribute("aria-pressed", String(b === e.currentTarget));
            void render();
          },
        }, MODE_LABEL[m])))
    : null;

  const section = h("section", { class: "ds-sec explore", id: "sec-explore", "aria-labelledby": "explore-h" },
    h("div", { class: "sec-head" }, h("h2", { id: "explore-h" }, "Explore"), seg),
    binds,
    host,
    h("div", { class: "chart-foot" }, h("div", { class: "chart-caption" }, note, features), edit),
  );

  /** The spec as the page draws it (and, with the public data URL, as the Editor opens it). */
  const currentSpec = (): Spec => {
    if (state.mode === "scatter" && fields) {
      return scatterSpec(d, fields, { x: state.x, y: state.y, zoom: !phone.matches, height: phone.matches ? 300 : 380 });
    }
    return starterChart(d) ?? starterSpec(d)!;
  };

  // Tables are read once, when the chart is first drawn, and handed to Vega (see parseTable);
  // the caption's count reuses them but never reads the file itself.
  let rowsPromise: Promise<Record<string, unknown>[]> | null = null;
  let loadedRows: Record<string, unknown>[] | null = null;
  const rows = () => (rowsPromise ??= fetch(siteDataUrl(d)).then((res) => {
    if (!res.ok) throw new Error(`Could not load ${d.file} (HTTP ${res.status})`);
    return res.text();
  }).then((text) => (loadedRows = parseTable(text, d.format))));
  const describe = (spec: Spec) => {
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
  let destroyed = false;
  const draw = async () => {
    if (destroyed) return;
    const [{ default: vegaEmbed }, { expressionInterpreter }, { themeConfig }] = await Promise.all([
      import("vega-embed"), import("vega-interpreter"), import("./vl"),
    ]);
    if (destroyed) return;
    result?.finalize();
    binds.replaceChildren();
    const spec = currentSpec();
    const site = readsRows(d) ? withValues(spec, await rows()) : withDataUrl(spec, siteDataUrl(d));
    if (destroyed) return;
    binds.hidden = state.mode !== "scatter";
    result = await vegaEmbed(host, site as never, {
      config: themeConfig(),
      renderer: (d.rows ?? 0) > CANVAS_ROWS ? "canvas" : "svg",
      ast: true,
      expr: expressionInterpreter,
      tooltip: { theme: "custom" },
      bind: binds,
      // The menu's Editor action would open the page's same-origin data path; the button opens the public one.
      actions: { export: true, source: true, compiled: true, editor: false },
    });
    if (destroyed) { result.finalize(); return; }
    describe(spec);
    if (state.mode === "scatter") {
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
  const render = () => (queue = queue.then(draw).catch((err: unknown) => {
    host.replaceChildren(h("p", { class: "muted" }, `The chart didn't load: ${err instanceof Error ? err.message : String(err)}`));
  }));

  // Redraw for a new theme or screen size only once drawn (a large file waits to be asked for).
  const redraw = () => { if (result) void render(); };
  const unsubscribeTheme = onThemeChange(redraw);
  const onScreen = redraw;
  phone.addEventListener("change", onScreen);
  teardown = () => {
    destroyed = true;
    unsubscribeTheme();
    phone.removeEventListener("change", onScreen);
    result?.finalize();
  };
  describe(currentSpec());
  if ((d.bytes ?? 0) > AUTO_LOAD_BYTES) {
    host.append(h("button", {
      class: "btn draw", type: "button",
      onclick: () => void render(),
    }, `Draw Chart (${formatBytes(d.bytes)})`));
  } else {
    // draw() awaits its imports before it measures the host, and by then the section is in the page.
    // (Not requestAnimationFrame: background tabs never run it.)
    void render();
  }
  return section;
}
