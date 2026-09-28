/** Vega Datasets: the home page and one page per dataset, routed by `#name`. */
import {
  type Catalog,
  type Dataset,
  type Example,
  GALLERIES,
  licenseFamily,
  loadCatalog,
} from "./catalog";
import {
  datasetActions,
  exampleLinks,
  galleryTag,
  licenseBlock,
  previewTable,
  sourcesBlock,
  thumbImg,
  urlRow,
} from "./components";
import { $, append, clear, h, hash, showError } from "./dom";
import { formatBytes, formatCount, FORMAT_LABEL, plural } from "./format";
import { renderHome, stopHome } from "./home";
import { renderMarkdown } from "./markdown";
import { motionSection, stopMotion } from "./motion";
import { fieldProfile, missingNote, typeLabel } from "./profile";
import { initThemeToggle } from "./theme";

const REPO = "https://github.com/vega/vega-datasets";
const HOME_TITLE = "Vega Datasets – The Data Behind the Examples";
const PER_GALLERY = 6;
let current = "";

function galleryShelf(c: Catalog, d: Dataset): HTMLElement {
  const all = c.examplesFor(d);
  if (!all.length) {
    return h("div", { class: "shelf-empty" },
      h("p", null, "Not yet seen in any gallery."),
      h("p", { class: "muted" }, "This dataset is published and documented but no Vega, Vega-Lite or Altair example uses it. A new example would be a welcome contribution."),
    );
  }
  return h("div", { class: "shelves" }, GALLERIES.map((g) => {
    const items = all.filter((e) => e.gallery === g);
    if (!items.length) return null;
    const list = h("ul", { class: "shelf" });
    const draw = (n: number) => {
      clear(list);
      list.append(...items.slice(0, n).map((e: Example) =>
        h("li", { class: "specimen" },
          h("a", { href: e.url, target: "_blank", rel: "noopener", class: "specimen-img" }, thumbImg(e)),
          h("p", { class: "specimen-name" }, e.name),
          exampleLinks(e),
        )));
    };
    draw(PER_GALLERY);
    const more = items.length > PER_GALLERY
      ? h("button", { class: "btn btn-quiet more", type: "button" }, `Show all ${items.length}`)
      : null;
    more?.addEventListener("click", () => {
      draw(items.length);
      more.remove();
    });
    return h("section", { class: `shelf-group g-${g}` },
      h("h3", { class: "shelf-head" }, galleryTag(g), h("span", { class: "shelf-count" }, plural(items.length, "example"))),
      list,
      more,
    );
  }));
}

function renderPlate(c: Catalog, d: Dataset, page: HTMLElement): void {
  const i = c.datasets.indexOf(d);
  const n = c.datasets.length;
  const prev = c.datasets[(i - 1 + n) % n]!;
  const next = c.datasets[(i + 1) % n]!;

  const [lede = "", ...rest] = (d.description || "No description recorded.").trim().split(/\n\s*\n/);
  const ledeEl = h("div", { class: "lede md" });
  renderMarkdown(ledeEl, lede);
  const restEl = h("div", { class: "md body-md" });
  if (rest.length) renderMarkdown(restEl, rest.join("\n\n"));

  const label = [
    `No. ${i + 1} of ${c.datasets.length}`,
    FORMAT_LABEL[d.format] ?? d.format,
    formatBytes(d.bytes),
    d.rows !== null ? `${formatCount(d.rows)} rows` : null,
    d.fields.length ? plural(d.fields.length, "field") : null,
  ].filter(Boolean).join("  ·  ");

  const preview = previewTable(d);
  const plate = h("div", { class: "wrap plate" });
  page.append(plate);
  append(plate, [
    h("header", { class: "plate-head" },
      h("p", { class: "plate-label" }, label),
      h("h1", { class: "plate-name" }, d.name),
      ledeEl,
      rest.length ? restEl : null,
      datasetActions(d),
    ),
    motionSection(d),
    d.image ? h("figure", { class: "plate-image" }, h("img", { src: d.image, alt: `The ${d.name} image` })) : null,
    d.fields.length
      ? h("section", { class: "plate-sec", "aria-labelledby": "fields" },
          h("h2", { id: "fields" }, "Fields"),
          h("p", { class: "sec-intro" }, "Each field with its type and the shape of its values across every row."),
          h("ul", { class: "field-list" }, d.fields.map((f) => {
            const miss = missingNote(f, d.rows);
            return h("li", { class: "field-card" },
              h("div", { class: "field-head" }, h("span", { class: "field-name" }, f.name), h("span", { class: "field-type" }, typeLabel(f))),
              f.description ? h("p", { class: "field-desc" }, f.description) : null,
              fieldProfile(f, { rows: d.rows, width: 240, height: 44 }),
              miss ? h("p", { class: "field-missing" }, miss) : null,
            );
          })),
        )
      : null,
    preview
      ? h("section", { class: "plate-sec", "aria-labelledby": "rows" }, h("h2", { id: "rows" }, "A few rows"), preview)
      : null,
    h("section", { class: "plate-sec", "aria-labelledby": "seen" },
      h("h2", { id: "seen" }, "Seen in the galleries"),
      galleryShelf(c, d),
    ),
    h("section", { class: "plate-sec provenance", "aria-labelledby": "prov" },
      h("h2", { id: "prov" }, "Provenance"),
      h("div", { class: "prov-grid" },
        h("div", null, h("h3", null, "Collected from"), sourcesBlock(d)),
        h("div", null, h("h3", null, `License · ${licenseFamily(d)}`), licenseBlock(d)),
        h("div", { class: "prov-file" }, h("h3", null, "File"), h("p", { class: "mono" }, `data/${d.file}`), urlRow(d)),
      ),
    ),
    h("nav", { class: "plate-nav", "aria-label": "Neighbouring datasets" },
      h("a", { href: `#${encodeURIComponent(prev.name)}` }, h("span", null, "Previous"), h("b", null, prev.name)),
      h("a", { href: `#${encodeURIComponent(next.name)}`, class: "next" }, h("span", null, "Next"), h("b", null, next.name)),
    ),
  ]);
}

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
  stopMotion();
  stopHome();
  clear(page);
  if (d) renderPlate(c, d, page);
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
    if (e.target instanceof HTMLInputElement || e.target instanceof HTMLSelectElement) return;
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
