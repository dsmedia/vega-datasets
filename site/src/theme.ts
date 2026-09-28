/** Resolve theme tokens and notify when the viewer's theme changes (data-theme toggle or OS setting). */

export function token(name: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

export function isDark(): boolean {
  const attr = document.documentElement.getAttribute("data-theme");
  if (attr === "dark") return true;
  if (attr === "light") return false;
  return matchMedia("(prefers-color-scheme: dark)").matches;
}

export function onThemeChange(fn: () => void): () => void {
  let last = isDark();
  const check = () => {
    const now = isDark();
    if (now !== last) {
      last = now;
      fn();
    }
  };
  const observer = new MutationObserver(check);
  observer.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
  const media = matchMedia("(prefers-color-scheme: dark)");
  media.addEventListener("change", check);
  return () => {
    observer.disconnect();
    media.removeEventListener("change", check);
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
