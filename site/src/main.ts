/** Vega Datasets: the home page and one page per dataset, routed by `#name`. */
import { type Catalog, type Dataset, loadCatalog } from "./catalog";
import { $, clear, h, hash, showError } from "./dom";
import { renderDataset, stopDataset } from "./dataset";
import { renderHome, stopHome } from "./home";
import { datasetStep } from "./keys";
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
        " · Code BSD-3-Clause",
      ]));
}

/** Show a dataset's page, or the home page for "". */
function show(c: Catalog, name: string, userAction: boolean): void {
  const from = current;
  current = name;
  hash.set(name);
  const d = c.dataset(name);
  const page = $("#page");
  stopDataset();
  stopHome();
  clear(page);
  // Back to the list from a dataset: that dataset's card takes focus, in view.
  let cardFocused = false;
  if (d) renderDataset(c, d, page);
  else cardFocused = renderHome(c, page, from || undefined);
  renderFooter(d);
  document.title = d ? `${d.name} · Vega Datasets` : HOME_TITLE;
  if (userAction && !cardFocused) {
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
      // A modified or middle click opens the link as usual (in a new tab, say).
      if (e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
      e.preventDefault();
      // A new history entry, so Back returns to the dataset (show() only replaces the hash).
      if (current !== "") history.pushState(null, "", location.pathname + location.search);
      show(c, "", true);
    });
  }
  const initial = hash.get();
  show(c, c.dataset(initial) ? initial : "", false);
  window.addEventListener("hashchange", () => {
    const name = hash.get();
    if (name !== current && (name === "" || c.dataset(name))) show(c, name, true);
  });
  const page = $("#page");
  document.addEventListener("keydown", (e) => {
    // Focus on the page itself (after a step, #page holds it), not on a widget that uses the arrows.
    const onPage = e.target === document.body || e.target === page || e.target === document.documentElement;
    const step = datasetStep(e, onPage);
    // Between dataset pages only: on the home page the arrow keys scroll.
    const i = c.datasets.findIndex((d) => d.name === current);
    if (!step || i < 0) return;
    const n = c.datasets.length;
    show(c, c.datasets[(i + step + n) % n]!.name, true);
  });
}

void main();
