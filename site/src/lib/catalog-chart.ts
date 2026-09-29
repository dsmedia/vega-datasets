/**
 * The home page's catalog chart: every dataset by file size and gallery use,
 * colored by format. A real Vega-Lite spec, so "Open in Vega Editor" shows how
 * it is made. On wide screens an interval brush filters the cards below; on
 * phones a tap opens the dataset (href channel), with no brush to fight scrolling.
 *
 * The spec is drawn twice: to static SVG when the site is built (so the chart is
 * in the HTML), then live in the browser (client/catalog-chart.ts).
 */
import type { Brush, ChartRow, FormatGroup } from "./home-model";
import { FORMAT_COLORS, FORMAT_GROUPS } from "./home-model";

type Spec = Record<string, unknown>;

export interface ChartOptions {
  /** Offer the interval brush (wide screens only). */
  brush: boolean;
  height: number;
  /** How many of the most used datasets get a name label. */
  labels: number;
  /** Put the legend above the plot (narrow screens) instead of at the right. */
  legendTop: boolean;
  /** Legend entries per row when it sits on top: 2 on the narrowest phones, so it clears the menu button. */
  legendColumns?: number;
  /** Font for the name labels (the page's mono stack). */
  monoFont: string;
}

/** File sizes on the axis: decimal units at powers of ten. */
const BYTES_LABEL = "datum.value >= 1e6 ? datum.value / 1e6 + ' MB' : datum.value >= 1e3 ? datum.value / 1e3 + ' KB' : datum.value + ' B'";

/** Labels right of their point, or left of it near the right edge. */
const LABEL_FLIP = 5e5;

export function catalogSpec(rows: ChartRow[], counts: Record<FormatGroup, number>, o: ChartOptions): Spec {
  const labeled = new Set([...rows].sort((a, b) => b.examples - a.examples || a.name.localeCompare(b.name)).slice(0, o.labels).map((r) => r.name));
  const values = rows.map((r) => ({
    ...r,
    label: labeled.has(r.name),
    description: `${r.name}: ${r.format}, ${r.size}, ${r.examples} gallery ${r.examples === 1 ? "example" : "examples"}`,
  }));
  // Points outside the search and chip filters fade (the `matched` param, set by the page);
  // on wide screens, so do points outside the brush.
  const dim = {
    opacity: {
      condition: [
        { test: "matched && indexof(matched, datum.name) < 0", value: 0.1 },
        ...(o.brush ? [{ param: "brush", empty: true, value: 1 }] : []),
      ],
      value: o.brush ? 0.18 : 1,
    },
  };
  const legendLabel = FORMAT_GROUPS.reduceRight(
    (rest, g) => `datum.label === '${g}' ? '${g} ${counts[g]}' : ${rest}`,
    "datum.label",
  );
  const x = {
    field: "bytes",
    type: "quantitative",
    scale: { type: "log", domain: [50, 2e7] },
    axis: { title: "File size (log scale)", values: [1e2, 1e3, 1e4, 1e5, 1e6, 1e7], labelExpr: BYTES_LABEL, grid: false },
  };
  const y = { field: "examples", type: "quantitative", title: "Gallery examples", axis: { tickMinStep: 1 } };
  const label = (test: string, align: "left" | "right") => ({
    transform: [{ filter: `datum.label && ${test}` }],
    // Names repeat the points' descriptions, so screen readers skip them.
    mark: { type: "text", align, dx: align === "left" ? 8 : -8, baseline: "middle", fontSize: 11, font: o.monoFont, aria: false },
    encoding: { x, y, text: { field: "name" }, href: { field: "href" }, ...dim },
  });
  return {
    $schema: "https://vega.github.io/schema/vega-lite/v6.json",
    description: "Every vega-datasets file by size and by the number of gallery examples that use it, colored by format. Drag to filter the list; click a point to open its dataset.",
    width: "container",
    height: o.height,
    autosize: { type: "fit-x", contains: "padding" },
    data: { values },
    // Names of the datasets the page's filters match; null when nothing is filtered.
    params: [{ name: "matched", value: null }],
    layer: [
      {
        ...(o.brush ? { params: [{ name: "brush", select: { type: "interval", encodings: ["x", "y"] } }] } : {}),
        mark: { type: "circle", size: 64, opacity: 1 },
        encoding: {
          x,
          y,
          color: {
            field: "format",
            type: "nominal",
            title: "Format",
            scale: { domain: [...FORMAT_GROUPS], range: FORMAT_GROUPS.map((g) => FORMAT_COLORS[g]) },
            legend: o.legendTop
              ? { orient: "top", direction: "horizontal", columns: o.legendColumns ?? 4, title: null, labelExpr: legendLabel, columnPadding: 10, symbolSize: 50, offset: 6 }
              : { labelExpr: legendLabel },
          },
          ...dim,
          href: { field: "href" },
          description: { field: "description" },
          tooltip: [
            { field: "name", title: "Dataset" },
            { field: "format", title: "Format" },
            { field: "size", title: "Size" },
            { field: "examples", title: "Gallery examples" },
          ],
        },
      },
      label(`datum.bytes <= ${LABEL_FLIP}`, "left"),
      label(`datum.bytes > ${LABEL_FLIP}`, "right"),
    ],
  };
}

/** The brush signal's value ({} when cleared) as a Brush, or null. */
export function toBrush(value: unknown): Brush | null {
  const v = value as { bytes?: number[]; examples?: number[] } | null;
  if (!v?.bytes || !v.examples || v.bytes.length !== 2 || v.examples.length !== 2) return null;
  return { bytes: [v.bytes[0]!, v.bytes[1]!], examples: [v.examples[0]!, v.examples[1]!] };
}
