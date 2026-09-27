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

export function reducedMotion(): boolean {
  return matchMedia("(prefers-reduced-motion: reduce)").matches;
}
