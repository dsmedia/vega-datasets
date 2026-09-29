/**
 * Vega config built from the page's theme tokens, so charts match light and dark mode.
 * Marks keep Vega's defaults (tableau10, which the --chart-* tokens mirror); only the
 * chrome around them (axes, legends, text) follows the page. Also what every embed needs
 * from the page afterwards.
 */
import { token } from "./theme";

/**
 * Name vega-embed's actions menu for screen readers: its <summary> holds only an icon
 * (axe: summary-name). Call after each embed into `host`.
 */
export function labelActions(host: Element): void {
  host.querySelector(".vega-actions")?.closest("details")?.querySelector("summary")?.setAttribute("aria-label", "Chart actions");
}

type Config = Record<string, unknown>;

export function themeConfig(): Config {
  const ink = token("--ink");
  const strong = token("--ink-strong");
  const muted = token("--ink-muted");
  const rule = token("--rule");
  const grid = token("--rule-faint");
  const font = token("--font-sans");
  const guide = {
    labelColor: muted,
    titleColor: ink,
    labelFontSize: 11,
    titleFontSize: 11,
    titleFontWeight: "bold",
  };
  return {
    background: null,
    font,
    view: { stroke: null },
    axis: { ...guide, domainColor: rule, tickColor: rule, gridColor: grid, gridWidth: 1 },
    legend: guide,
    header: { labelColor: ink, titleColor: ink },
    title: { color: strong },
    text: { color: ink },
    selection: { interval: { mark: { fill: ink, fillOpacity: 0.08, stroke: muted, strokeWidth: 1 } } },
  };
}
