/** Vega Datasets: the home page and one page per dataset, routed by `#name`. */
import { type Catalog, type Dataset, loadCatalog } from "./catalog";
import { $, clear, h, hash, showError } from "./dom";
import { renderDataset, stopDataset } from "./dataset";
import { renderHome, stopHome } from "./home";
import { initThemeToggle } from "./theme";

const REPO = "https://github.com/vega/vega-datasets";
const HOME_TITLE = "Vega Datasets – The Data Behind the Examples";
let current = "";

function renderFooter(d: Dataset | undefined): void {
  const link = (href: string, text: string) => h("a", { href }, text);
  $("#footer-note").replaceChildren(...(d
    ? ["Spot an error? ", link(`${REPO}/blob/main/_data/datapackage_additions.toml`, "Edit this dataset's metadata"), " on GitHub."]
    : [
        "Documented in ", link(`${REPO}/blob/main/datapackage.json`, "datapackage.json"),
        " · Code BSD-3-Clause · ", link(`${REPO}/blob/main/README.md`, "Edit this page"),
      ]));
}

/** Show a dataset's page, or the home page for "". */
function show(c: Catalog, name: string, userAction: boolean): void {
  current = name;
  hash.set(name);
  const d = c.dataset(name);
  const page = $("#page");
  stopDataset();
  stopHome();
  clear(page);
  if (d) renderDataset(c, d, page);
  else renderHome(c, page);
  renderFooter(d);
  document.title = d ? `${d.name} · Vega Datasets` : HOME_TITLE;
  if (userAction) {
    window.scrollTo({ top: 0 });
    page.focus({ preventScroll: true });
  }
}

async function main(): Promise<void> {
  initThemeToggle($("#theme-toggle") as HTMLButtonElement);
  let c: Catalog;
  try {
    c = await loadCatalog();
  } catch (err) {
    showError($("#page"), err);
    return;
  }
  for (const link of document.querySelectorAll<HTMLAnchorElement>("a[data-home]")) {
    link.addEventListener("click", (e) => {
      e.preventDefault();
      show(c, "", true);
    });
  }
  const initial = hash.get();
  show(c, c.dataset(initial) ? initial : "", false);
  window.addEventListener("hashchange", () => {
    const name = hash.get();
    if (name !== current && (name === "" || c.dataset(name))) show(c, name, true);
  });
  document.addEventListener("keydown", (e) => {
    // Tabs and other widgets that use the arrow keys mark them handled.
    if (e.defaultPrevented || e.target instanceof HTMLInputElement || e.target instanceof HTMLSelectElement) return;
    if (e.metaKey || e.ctrlKey || e.altKey) return;
    // Between dataset pages only: on the home page the arrow keys scroll.
    const i = c.datasets.findIndex((d) => d.name === current);
    if (i < 0) return;
    const n = c.datasets.length;
    if (e.key === "ArrowRight" || e.key === "j") show(c, c.datasets[(i + 1) % n]!.name, true);
    if (e.key === "ArrowLeft" || e.key === "k") show(c, c.datasets[(i - 1 + n) % n]!.name, true);
  });
}

void main();
