// Runs before the stylesheet so a saved dark choice paints without a flash of light.
// The page defaults to light, like the other Vega sites; site/src/theme.ts owns the toggle.
try {
  if (localStorage.getItem("vega-datasets-theme") === "dark") document.documentElement.setAttribute("data-theme", "dark");
} catch {
  // Storage blocked (private mode, disabled site data): stay light.
}
