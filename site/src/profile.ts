/**
 * Field profiles for the fields table, from the precomputed summaries in
 * catalog.json: a sparkline histogram for numbers and dates, a one-line summary,
 * and the missing count.
 */
import type { Field } from "./catalog";
import { svg } from "./dom";
import { formatCount, formatDate, formatNumber, TYPE_LABEL } from "./format";
import { attachTip } from "./tooltip";

function binEdges(lo: number, hi: number, n: number): number[] {
  return Array.from({ length: n + 1 }, (_, i) => lo + ((hi - lo) * i) / n);
}

/** Bars grow from one baseline with a 4px-rounded data end (clipped to the bar width). */
function barPath(x: number, y: number, w: number, hgt: number, r: number): string {
  const rr = Math.min(r, w / 2, hgt);
  return `M${x},${y + hgt}V${y + rr}Q${x},${y} ${x + rr},${y}H${x + w - rr}Q${x + w},${y} ${x + w},${y + rr}V${y + hgt}Z`;
}

function histogram(
  bins: number[],
  label: (i: number) => string,
  width: number,
  height: number,
): SVGSVGElement {
  const max = Math.max(...bins, 1);
  const gap = 2;
  const bw = (width - gap * (bins.length - 1)) / bins.length;
  const root = svg("svg", {
    viewBox: `0 0 ${width} ${height}`,
    width,
    height,
    class: "hist",
    role: "img",
  });
  root.append(svg("line", { x1: 0, x2: width, y1: height - 0.5, y2: height - 0.5, class: "hist-base" }));
  bins.forEach((count, i) => {
    const x = i * (bw + gap);
    const bh = count === 0 ? 0 : Math.max(1.5, ((height - 2) * count) / max);
    const g = svg("g", { class: "hist-bin", tabindex: 0 });
    // Hit target is the full column, larger than the mark.
    g.append(svg("rect", { x, y: 0, width: bw + gap, height, class: "hit" }));
    if (bh > 0) g.append(svg("path", { d: barPath(x, height - 1 - bh, bw, bh, 2), class: "bar" }));
    attachTip(g, [label(i), `${formatCount(count)} rows`]);
    root.append(g);
  });
  return root;
}

/** A small histogram for the fields table, or null for categories and empty fields. */
export function sparkline(f: Field): SVGSVGElement | null {
  const p = f.profile;
  if (p.kind === "quantitative") {
    const edges = binEdges(p.min, p.max, p.bins.length);
    return spark(histogram(p.bins, (i) => `${formatNumber(edges[i] ?? p.min)} – ${formatNumber(edges[i + 1] ?? p.max)}`, 120, 26), f);
  }
  if (p.kind === "temporal" && p.bins?.length) {
    const lo = new Date(p.min).getTime();
    const hi = new Date(p.max).getTime();
    const edges = binEdges(lo, hi, p.bins.length);
    const label = (i: number) => `${formatDate(new Date(edges[i] ?? lo).toISOString())} – ${formatDate(new Date(edges[i + 1] ?? hi).toISOString())}`;
    return spark(histogram(p.bins, label, 120, 26), f);
  }
  return null;
}

function spark(chart: SVGSVGElement, f: Field): SVGSVGElement {
  chart.classList.add("spark");
  chart.setAttribute("aria-label", `${f.name} distribution`);
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
