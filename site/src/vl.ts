/**
 * Vega config built from the page's theme tokens, so charts match light and dark mode,
 * or from the system colors in a forced-colors mode (Windows High Contrast).
 * Marks keep Vega's defaults (tableau10, which the --chart-* tokens mirror); only the
 * chrome around them (axes, legends, text) follows the page. Also what every embed needs
 * from the page afterwards.
 */
import { type ChartInk, chartInk, token } from "./theme";

/**
 * Name vega-embed's actions menu for screen readers: its <summary> holds only an icon
 * (axe: summary-name). Call after each embed into `host`.
 */
export function labelActions(host: Element): void {
  host.querySelector(".vega-actions")?.closest("details")?.querySelector("summary")?.setAttribute("aria-label", "Chart actions");
}

type Config = Record<string, unknown>;

/**
 * The config for chart chrome `c` (see ChartInk), in `font`. It sets every color Vega draws
 * the chrome with, since neither renderer takes them from CSS: the canvas renderer paints
 * pixels, and forced-colors mode leaves SVG attributes alone.
 */
export function chartConfig(c: ChartInk, font: string): Config {
  const guide = {
    labelColor: c.muted,
    titleColor: c.ink,
    labelFontSize: 11,
    titleFontSize: 11,
    titleFontWeight: "bold",
  };
  return {
    background: null,
    font,
    view: { stroke: null },
    axis: { ...guide, domainColor: c.rule, tickColor: c.rule, gridColor: c.grid, gridWidth: 1 },
    // The base colors draw a legend's symbols when no color scale does (size, shape).
    legend: { ...guide, symbolBaseStrokeColor: c.rule },
    header: { labelColor: c.ink, titleColor: c.ink },
    title: { color: c.strong, subtitleColor: c.ink },
    text: { color: c.ink },
    selection: {
      interval: { mark: { fill: c.brush, fillOpacity: c.forced ? 0.25 : 0.08, stroke: c.forced ? c.brush : c.muted, strokeWidth: 1 } },
    },
  };
}

/** The config for the page's theme now, or for its forced colors. */
export function themeConfig(): Config {
  return chartConfig(chartInk(), token("--font-sans"));
}
