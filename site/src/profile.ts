/**
 * Field profiles for the fields table, from the precomputed summaries in
 * catalog.json: a sparkline histogram for numbers and dates, a one-line summary,
 * and the missing count.
 */
import type { Field } from "./catalog";
import { svg } from "./dom";
import { formatCount, formatDate, formatNumber, parseDate, plural, TYPE_LABEL } from "./format";
import { attachTip, hideTip, showTip } from "./tooltip";

function binEdges(lo: number, hi: number, n: number): number[] {
  return Array.from({ length: n + 1 }, (_, i) => lo + ((hi - lo) * i) / n);
}

/** Bars grow from one baseline with a 4px-rounded data end (clipped to the bar width). */
function barPath(x: number, y: number, w: number, hgt: number, r: number): string {
  const rr = Math.min(r, w / 2, hgt);
  return `M${x},${y + hgt}V${y + rr}Q${x},${y} ${x + rr},${y}H${x + w - rr}Q${x + w},${y} ${x + w},${y + rr}V${y + hgt}Z`;
}

/** Numbers the histograms on the page, for their bins' ids. */
let histograms = 0;

/**
 * A histogram that is one tab stop: focused, the arrow keys (and Home, End) step through
 * its bins, each highlighted, shown in the tooltip and read out via aria-activedescendant.
 * The pointer shows a bin's tooltip on hover.
 */
function histogram(
  bins: number[],
  label: (i: number) => string,
  width: number,
  height: number,
  name: string,
): SVGSVGElement {
  const max = Math.max(...bins, 1);
  const gap = 2;
  const bw = (width - gap * (bins.length - 1)) / bins.length;
  const id = `hist-${++histograms}`;
  const root = svg("svg", {
    viewBox: `0 0 ${width} ${height}`,
    width,
    height,
    class: "hist",
    tabindex: 0,
    role: "group",
    "aria-roledescription": "histogram",
    "aria-label": `${name} distribution, ${bins.length} bins`,
  });
  root.append(svg("line", { x1: 0, x2: width, y1: height - 0.5, y2: height - 0.5, class: "hist-base", "aria-hidden": "true" }));
  const cells = bins.map((count, i) => {
    const x = i * (bw + gap);
    const bh = count === 0 ? 0 : Math.max(1.5, ((height - 2) * count) / max);
    const lines = [label(i), plural(count, "row")];
    const g = svg("g", { class: "hist-bin", id: `${id}-${i}`, role: "img", "aria-label": lines.join(": ") });
    // Hit target is the full column, larger than the mark.
    g.append(svg("rect", { x, y: 0, width: bw + gap, height, class: "hit" }));
    if (bh > 0) g.append(svg("path", { d: barPath(x, height - 1 - bh, bw, bh, 2), class: "bar" }));
    attachTip(g, lines);
    root.append(g);
    return { g, lines };
  });

  let active = -1;
  const select = (i: number) => {
    cells[active]?.g.classList.remove("active");
    active = Math.max(0, Math.min(cells.length - 1, i));
    const cell = cells[active]!;
    cell.g.classList.add("active");
    root.setAttribute("aria-activedescendant", cell.g.id);
    showTip(cell.g, cell.lines);
  };
  root.addEventListener("focus", () => select(active < 0 ? 0 : active));
  root.addEventListener("blur", () => {
    cells[active]?.g.classList.remove("active");
    root.removeAttribute("aria-activedescendant");
    hideTip();
  });
  root.addEventListener("keydown", (e) => {
    const next = ({ ArrowRight: active + 1, ArrowLeft: active - 1, Home: 0, End: cells.length - 1 } as Record<string, number>)[e.key];
    if (next === undefined || e.altKey || e.ctrlKey || e.metaKey) return;
    // Handled here: the page doesn't step to another dataset.
    e.preventDefault();
    select(next);
  });
  return root;
}

/**
 * The range each histogram bin covers, as text; null for categories and empty fields.
 * A date field's bins read as dates (their edges fall at odd hours, which aren't in the data).
 */
export function binLabels(f: Field): string[] | null {
  const p = f.profile;
  if (p.kind === "quantitative") {
    const edges = binEdges(p.min, p.max, p.bins.length);
    return p.bins.map((_, i) => `${formatNumber(edges[i] ?? p.min)} – ${formatNumber(edges[i + 1] ?? p.max)}`);
  }
  if (p.kind === "temporal" && p.bins?.length) {
    const lo = parseDate(p.min).getTime();
    const hi = parseDate(p.max).getTime();
    const edges = binEdges(lo, hi, p.bins.length);
    const time = f.type === "date" ? false : undefined;
    const text = (t: number) => formatDate(new Date(t).toISOString(), time);
    return p.bins.map((_, i) => `${text(edges[i] ?? lo)} – ${text(edges[i + 1] ?? hi)}`);
  }
  return null;
}

/** A small histogram for the fields table, or null for categories and empty fields. */
export function sparkline(f: Field): SVGSVGElement | null {
  const p = f.profile;
  const labels = binLabels(f);
  if (!labels || (p.kind !== "quantitative" && p.kind !== "temporal") || !p.bins) return null;
  return spark(histogram(p.bins, (i) => labels[i]!, 120, 26, f.name));
}

function spark(chart: SVGSVGElement): SVGSVGElement {
  chart.classList.add("spark");
  return chart;
}

/** Dates on January 1 at midnight are years (a "Year" column stored as a date). */
function yearsOnly(iso: string): boolean {
  return /^\d{4}-01-01(T00:00:00(\.0+)?Z?)?$/.test(iso);
}

/** One line about a field's values: range and mean, date span, or the most common values. */
export function profileSummary(f: Field): string {
  const p = f.profile;
  switch (p.kind) {
    case "quantitative":
      return `${formatNumber(p.min)} – ${formatNumber(p.max)} · mean ${formatNumber(p.mean)}`;
    case "temporal":
      return yearsOnly(p.min) && yearsOnly(p.max)
        ? `${p.min.slice(0, 4)} – ${p.max.slice(0, 4)}`
        : `${formatDate(p.min)} – ${formatDate(p.max)}`;
    case "nominal":
      return p.top.slice(0, 3).map(([v, n]) => `${v} ${formatCount(n)}`).join(" · ");
    default:
      return "No values";
  }
}

/** Missing values as "8 · 2.0%", or "0". */
export function missingCount(f: Field, rows: number | null): { text: string; any: boolean } {
  const m = f.profile.missing ?? 0;
  if (!m || !rows) return { text: "0", any: false };
  return { text: `${formatCount(m)} · ${((100 * m) / rows).toFixed(1)}%`, any: true };
}

export function typeLabel(f: Field): string {
  return TYPE_LABEL[f.type] ?? f.type;
}
