/**
 * The home page's catalog chart: every dataset by file size and gallery use,
 * colored by format. A real Vega-Lite spec, so "Open in Vega Editor" shows how
 * it is made. On wide screens an interval brush filters the cards below; on
 * phones a tap opens the dataset (href channel), with no brush to fight scrolling.
 */
import type { Brush, ChartRow, FormatGroup } from "./home-model";
import { FORMAT_COLORS, FORMAT_GROUPS } from "./home-model";
import { onThemeChange } from "./theme";

type Spec = Record<string, unknown>;

export interface ChartOptions {
  /** Offer the interval brush (wide screens only). */
  brush: boolean;
  height: number;
  /** How many of the most used datasets get a name label. */
  labels: number;
  /** Put the legend above the plot (narrow screens) instead of at the right. */
  legendTop: boolean;
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
              ? { orient: "top", direction: "horizontal", title: null, labelExpr: legendLabel, columnPadding: 10, symbolSize: 50, offset: 6 }
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

export interface MountedChart {
  /** Draw again (clears the brush). */
  redraw(): Promise<void>;
  /**
   * Fade the points not in `names` (null: none). Sets a signal on the live view, so the
   * brush and the axes stay as they are; kept across redraws.
   */
  setMatches(names: string[] | null): void;
  destroy(): void;
}

/**
 * Embed the chart in `host`. `options` is read on every draw, so it can follow the
 * host's width; `onBrush` hears every brush change (null when cleared or redrawn).
 */
export async function mountCatalogChart(
  host: HTMLElement,
  rows: ChartRow[],
  counts: Record<FormatGroup, number>,
  options: () => ChartOptions,
  onBrush: (b: Brush | null) => void,
): Promise<MountedChart> {
  const [{ default: vegaEmbed }, { expressionInterpreter }, { themeConfig }] = await Promise.all([
    import("vega-embed"), import("vega-interpreter"), import("./vl"),
  ]);
  let result: Awaited<ReturnType<typeof vegaEmbed>> | undefined;
  let queue: Promise<void> = Promise.resolve();
  let destroyed = false;
  let matched: string[] | null = null;
  const draw = async () => {
    if (destroyed) return;
    result?.finalize();
    const o = options();
    result = await vegaEmbed(host, catalogSpec(rows, counts, o) as never, {
      config: themeConfig(),
      renderer: "svg",
      ast: true,
      expr: expressionInterpreter,
      tooltip: { theme: "custom" },
      actions: { export: true, source: false, compiled: false, editor: true },
    });
    if (destroyed) { result.finalize(); return; }
    if (matched) await result.view.signal("matched", matched).runAsync();
    onBrush(null);
    if (o.brush) result.view.addSignalListener("brush", (_name, value) => onBrush(toBrush(value)));
  };
  const redraw = () => (queue = queue.then(draw));
  const unsubscribe = onThemeChange(() => void redraw());
  await redraw();
  return {
    redraw,
    setMatches: (names) => {
      matched = names;
      void result?.view.signal("matched", names).runAsync();
    },
    destroy: () => {
      destroyed = true;
      unsubscribe();
      result?.finalize();
    },
  };
}
