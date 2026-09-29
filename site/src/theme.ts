/**
 * Resolve theme tokens and notify when the viewer's theme changes (data-theme toggle, OS
 * setting, or a forced-colors mode such as Windows High Contrast turning on, off or over).
 */

export function token(name: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

const FORCED = "(forced-colors: active)";

/** Whether a forced-colors mode (Windows High Contrast) has replaced the page's colors. */
export function forcedColors(): boolean {
  return matchMedia(FORCED).matches;
}

/** The forced palette's system colors, as concrete values. */
export interface SystemColors {
  canvasText: string;
  canvas: string;
  grayText: string;
  highlight: string;
}

/**
 * Read the system colors off a hidden probe. Charts need them as values: the mode recolors
 * the page's CSS, but not a canvas's pixels or the colors Vega writes into SVG attributes.
 */
export function systemColors(): SystemColors {
  const probe = document.createElement("span");
  const set = (property: string, value: string) => probe.style.setProperty(property, value);
  set("position", "absolute");
  set("visibility", "hidden");
  set("forced-color-adjust", "none");
  set("color", "CanvasText");
  set("background-color", "Canvas");
  set("border-top-color", "GrayText");
  set("border-bottom-color", "Highlight");
  document.body.append(probe);
  const style = getComputedStyle(probe);
  const colors = {
    canvasText: style.color,
    canvas: style.backgroundColor,
    grayText: style.borderTopColor,
    highlight: style.borderBottomColor,
  };
  probe.remove();
  return colors;
}

/**
 * The colors of a chart's chrome: text, axis and legend rules, grid lines, the ground
 * behind marks, and the brush. Data marks keep their own colors (tableau10 and the
 * --chart-* tokens), in forced colors too.
 */
export interface ChartInk {
  forced: boolean;
  ink: string;
  strong: string;
  muted: string;
  rule: string;
  grid: string;
  surface: string;
  brush: string;
}

/** Chart chrome from the theme tokens. */
export function tokenInk(): ChartInk {
  return {
    forced: false,
    ink: token("--ink"),
    strong: token("--ink-strong"),
    muted: token("--ink-muted"),
    rule: token("--rule"),
    grid: token("--rule-faint"),
    surface: token("--surface"),
    brush: token("--ink"),
  };
}

/**
 * Chart chrome in a forced-colors mode: text and rules in CanvasText (a muted gray could
 * fall below contrast on the forced ground), grid lines in GrayText, the brush in Highlight.
 */
export function forcedInk(c: SystemColors): ChartInk {
  return {
    forced: true,
    ink: c.canvasText,
    strong: c.canvasText,
    muted: c.canvasText,
    rule: c.canvasText,
    grid: c.grayText,
    surface: c.canvas,
    brush: c.highlight,
  };
}

/** The chart chrome for the page as it is now. */
export function chartInk(): ChartInk {
  return forcedColors() ? forcedInk(systemColors()) : tokenInk();
}

export function isDark(): boolean {
  const attr = document.documentElement.getAttribute("data-theme");
  if (attr === "dark") return true;
  if (attr === "light") return false;
  return matchMedia("(prefers-color-scheme: dark)").matches;
}

/** What the charts' colors depend on: the theme, or in forced colors the system palette. */
function themeState(): string {
  return forcedColors() ? `forced ${JSON.stringify(systemColors())}` : isDark() ? "dark" : "light";
}

/**
 * Call `fn` when the charts' colors change: the theme button, the OS light/dark setting, or a
 * forced-colors mode turning on or off or switching palettes (which also flips the scheme
 * the OS reports).
 */
export function onThemeChange(fn: () => void): () => void {
  let last = themeState();
  const check = () => {
    const now = themeState();
    if (now !== last) {
      last = now;
      fn();
    }
  };
  const observer = new MutationObserver(check);
  observer.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
  const media = [matchMedia("(prefers-color-scheme: dark)"), matchMedia(FORCED)];
  for (const m of media) m.addEventListener("change", check);
  return () => {
    observer.disconnect();
    for (const m of media) m.removeEventListener("change", check);
  };
}

const STORAGE_KEY = "vega-datasets-theme";

/**
 * Wire the light/dark button. The page starts light (like the other Vega sites);
 * site/static/theme-init.js restores a saved dark choice before first paint.
 * It is a toggle button: the label stays "Dark mode" and aria-pressed carries the state.
 */
export function initThemeToggle(button: HTMLButtonElement): void {
  const sync = () => button.setAttribute("aria-pressed", String(isDark()));
  sync();
  button.addEventListener("click", () => {
    const next = isDark() ? "light" : "dark";
    document.documentElement.setAttribute("data-theme", next);
    try {
      if (next === "dark") localStorage.setItem(STORAGE_KEY, "dark");
      else localStorage.removeItem(STORAGE_KEY);
    } catch {
      // Storage blocked: the choice lasts for this page view only.
    }
    sync();
  });
}

export function reducedMotion(): boolean {
  return matchMedia("(prefers-reduced-motion: reduce)").matches;
}
