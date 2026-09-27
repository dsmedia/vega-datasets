/** Vega config built from the page's theme tokens, so charts match light and dark mode. */
import { token } from "./theme";

type Config = Record<string, unknown>;

export function themeConfig(): Config {
  const ink = token("--ink");
  const ink2 = token("--ink-2");
  const muted = token("--muted");
  const grid = token("--grid");
  const axis = token("--axis");
  const font = token("--font-data") || token("--font-body");
  return {
    background: null,
    font,
    view: { stroke: null },
    axis: {
      domainColor: axis,
      gridColor: grid,
      gridWidth: 1,
      tickColor: axis,
      labelColor: muted,
      titleColor: ink2,
      labelFont: font,
      titleFont: font,
      titleFontWeight: 500,
      labelFontSize: 11,
      titleFontSize: 11,
    },
    legend: { labelColor: ink2, titleColor: ink2, labelFont: font, titleFont: font },
    title: { color: ink, font },
    text: { color: ink2, font },
    bar: { cornerRadiusEnd: 4 },
    line: { strokeWidth: 2, strokeCap: "round", strokeJoin: "round" },
  };
}
