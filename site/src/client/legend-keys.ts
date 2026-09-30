/**
 * Legend isolation from the keyboard (Codex round 5, #6). A legend bound to a point
 * selection (`bind: "legend"`) isolates a series on a click; Vega draws its entries as SVG
 * groups no one can Tab to. Here each entry becomes a toggle button: focusable, Enter or
 * Space picks it as a click does (Shift adds it to the others, as Shift+click does), Escape
 * clears the pick, and `aria-pressed` says which are picked. The focus ring is the site's
 * `:focus-visible` outline.
 *
 * SVG charts only: a canvas chart (a large table's scatter plot) draws its legend as pixels,
 * with no element to focus; its legend still isolates with the mouse or a tap.
 */
import type { View } from "vega";

/** A legend entry: an item of the entries group (its symbol and label). */
export const ENTRY = "g.role-legend-entry g.role-scope > g";

/** The legend value an entry stands for (on its label's scenegraph item, which Vega keeps on the element). */
function entryValue(g: Element): unknown {
  const label = g.querySelector("g.role-legend-label text, g.role-legend-symbol path");
  return (label as unknown as { __data__?: { datum?: { value?: unknown } } } | null)?.__data__?.datum?.value;
}

/** The values picked in a point selection's store. */
function picked(view: View, store: string): unknown[] {
  try {
    return (view.data(store) as { values?: unknown[] }[]).map((t) => t.values?.[0]);
  } catch {
    return [];
  }
}

/** The selection stores of a Vega-Lite spec's params (`<name>_store`), those bound to `bind` or all. */
export function selectionStores(spec: unknown, bind?: string): string[] {
  const out: string[] = [];
  const walk = (x: unknown) => {
    if (Array.isArray(x)) x.forEach(walk);
    else if (x && typeof x === "object") {
      for (const [k, v] of Object.entries(x)) {
        if (k === "params" && Array.isArray(v)) {
          for (const p of v as { name?: string; select?: unknown; bind?: unknown }[]) {
            if (p.name && p.select && (bind === undefined || p.bind === bind)) out.push(`${p.name}_store`);
          }
        } else walk(v);
      }
    }
  };
  walk(spec);
  return [...new Set(out)];
}

/**
 * Make the legend entries under `host` keyboard toggles for the selection stores named in
 * `stores` (the chart's legend-bound params). Returns `dispose`, to call when the view goes.
 */
export function legendKeys(host: HTMLElement, view: View, stores: string[]): () => void {
  if (!stores.length) return () => {};
  const mark = () => {
    const values = stores.flatMap((s) => picked(view, s));
    host.querySelectorAll(`svg ${ENTRY}`).forEach((g) => {
      const label = g.querySelector("g.role-legend-label text")?.textContent ?? "";
      g.setAttribute("tabindex", "0");
      g.setAttribute("role", "button");
      g.setAttribute("aria-label", label);
      g.setAttribute("aria-pressed", String(values.some((v) => v === entryValue(g))));
    });
  };
  const onKey = (e: KeyboardEvent) => {
    const g = (e.target as Element | null)?.closest?.(ENTRY);
    if (!g || !host.contains(g)) return;
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      // Vega's handler reads the item from the element the click lands on: the entry's symbol.
      const target = g.querySelector("g.role-legend-symbol path") ?? g;
      const r = target.getBoundingClientRect();
      target.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true, view: window, clientX: r.x + r.width / 2, clientY: r.y + r.height / 2, shiftKey: e.shiftKey }));
    } else if (e.key === "Escape") {
      e.preventDefault();
      const run = stores.reduce((v, s) => v.change(s, v.changeset().remove(() => true)), view);
      void run.runAsync();
    }
  };
  host.addEventListener("keydown", onKey);
  // A pick changes the entries' state (and Vega may draw the legend afresh): mark them again.
  const again = () => requestAnimationFrame(mark);
  for (const s of stores) view.addDataListener(s, again);
  mark();
  return () => {
    host.removeEventListener("keydown", onKey);
    for (const s of stores) view.removeDataListener(s, again);
  };
}
