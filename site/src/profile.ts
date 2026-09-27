/**
 * Field profiles: a compact picture of each column, drawn as plain SVG from the
 * precomputed summaries in catalog.json (a histogram for numbers and dates, the
 * most common values for everything else).
 */
import type { Field, NominalProfile, QuantProfile, TemporalProfile } from "./catalog";
import { h, svg } from "./dom";
import { formatCount, formatDate, formatNumber, TYPE_LABEL } from "./format";
import { attachTip } from "./tooltip";

export interface ProfileOptions {
  width?: number;
  height?: number;
  /** Rows in the dataset, for "missing" percentages. */
  rows: number | null;
}

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

function quantitative(p: QuantProfile, o: Required<Omit<ProfileOptions, "rows">>): HTMLElement {
  const edges = binEdges(p.min, p.max, p.bins.length);
  const chart = histogram(
    p.bins,
    (i) => `${formatNumber(edges[i] ?? p.min)} – ${formatNumber(edges[i + 1] ?? p.max)}`,
    o.width,
    o.height,
  );
  return h("div", { class: "profile profile-q" },
    chart,
    h("div", { class: "axis-ends" }, h("span", null, formatNumber(p.min)), h("span", null, formatNumber(p.max))),
  );
}

function temporal(p: TemporalProfile, o: Required<Omit<ProfileOptions, "rows">>): HTMLElement {
  const lo = new Date(p.min).getTime();
  const hi = new Date(p.max).getTime();
  const bins = p.bins ?? [];
  const edges = binEdges(lo, hi, bins.length || 1);
  const chart = bins.length
    ? histogram(bins, (i) => `${formatDate(new Date(edges[i] ?? lo).toISOString())} – ${formatDate(new Date(edges[i + 1] ?? hi).toISOString())}`, o.width, o.height)
    : null;
  return h("div", { class: "profile profile-t" },
    chart,
    h("div", { class: "axis-ends" }, h("span", null, formatDate(p.min)), h("span", null, formatDate(p.max))),
  );
}

function nominal(p: NominalProfile, rows: number | null, width: number): HTMLElement {
  const total = rows ?? p.top.reduce((s, [, c]) => s + c, 0);
  const shown = p.top.slice(0, 4);
  const max = Math.max(...shown.map(([, c]) => c), 1);
  const list = h("ul", { class: "topvals" },
    shown.map(([value, count]) => {
      const pct = total ? (100 * count) / total : 0;
      const bar = h("span", { class: "tv-bar" });
      bar.style.width = `${Math.max(2, (100 * count) / max)}%`;
      const li = h("li", { tabindex: 0 },
        h("span", { class: "tv-label" }, value),
        h("span", { class: "tv-track" }, bar),
        h("span", { class: "tv-count" }, formatCount(count)),
      );
      attachTip(li, [value, `${formatCount(count)} rows · ${pct < 1 ? pct.toFixed(1) : Math.round(pct)}%`]);
      return li;
    }),
  );
  list.style.maxWidth = `${width + 80}px`;
  const more = p.distinct > shown.length ? h("div", { class: "tv-more" }, `${formatCount(p.distinct)} distinct values`) : null;
  return h("div", { class: "profile profile-n" }, list, more);
}

export function fieldProfile(f: Field, opts: ProfileOptions): HTMLElement {
  const o = { width: opts.width ?? 168, height: opts.height ?? 40 };
  const p = f.profile;
  switch (p.kind) {
    case "quantitative":
      return quantitative(p, o);
    case "temporal":
      return temporal(p, o);
    case "nominal":
      return nominal(p, opts.rows, o.width);
    default:
      return h("div", { class: "profile profile-empty" }, "No values");
  }
}

export function missingNote(f: Field, rows: number | null): string | null {
  const m = f.profile.missing;
  if (!m || !rows) return null;
  const pct = (100 * m) / rows;
  return `${formatCount(m)} missing (${pct < 1 ? pct.toFixed(1) : Math.round(pct)}%)`;
}

export function typeLabel(f: Field): string {
  return TYPE_LABEL[f.type] ?? f.type;
}
