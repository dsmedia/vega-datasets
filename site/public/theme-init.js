// Runs before the stylesheet: a saved dark choice paints without a flash of light,
// and the `js` class lets the stylesheet collapse long lists that the page's scripts
// can expand (without scripts, everything shows). The page defaults to light, like
// the other Vega sites; site/src/client/theme.ts owns the toggle.
document.documentElement.classList.add("js");
try {
  if (localStorage.getItem("vega-datasets-theme") === "dark") document.documentElement.setAttribute("data-theme", "dark");
} catch {
  // Storage blocked (private mode, disabled site data): stay light.
}
