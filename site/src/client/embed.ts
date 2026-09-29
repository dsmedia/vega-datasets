/**
 * Vega, loaded on demand (the first chart a reader touches), and the options every
 * chart embeds with: expressions run through vega-interpreter, since the page's CSP
 * forbids compiling code, and tooltips take the page's look.
 */
import type { EmbedOptions } from "vega-embed";
import { chartConfig } from "../lib/vega-theme";
import { chartInk, token } from "./theme";

export interface VegaModules {
  vegaEmbed: typeof import("vega-embed").default;
  expressionInterpreter: typeof import("vega-interpreter").expressionInterpreter;
}

let loading: Promise<VegaModules> | null = null;

export function loadVega(): Promise<VegaModules> {
  return (loading ??= Promise.all([import("vega-embed"), import("vega-interpreter")]).then(([embed, interp]) => ({
    vegaEmbed: embed.default,
    expressionInterpreter: interp.expressionInterpreter,
  })));
}

/**
 * Options for a chart drawn with `renderer`. SVG charts follow a theme switch through
 * site.css; canvas charts bake the colors in, so they are drawn again (see onThemeChange).
 */
export function embedOptions(v: VegaModules, renderer: "svg" | "canvas", actions: EmbedOptions["actions"]): EmbedOptions {
  return {
    config: chartConfig(chartInk(), token("--font-sans")) as EmbedOptions["config"],
    renderer,
    ast: true,
    expr: v.expressionInterpreter,
    tooltip: { theme: "custom" },
    actions,
  };
}

/** vega-embed's actions menu opens from a `<summary>` that holds only an icon: give it a name. */
export function labelActions(host: Element): void {
  host.querySelector(".vega-embed details > summary")?.setAttribute("aria-label", "Chart actions");
}
