/** The vega-datasets Field Guide: one dataset at a time, presented as a specimen plate. */
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
  usageBar,
} from "./components";
import { $, append, clear, h, hash, showError } from "./dom";
import { formatBytes, formatCount, FORMAT_LABEL, plural } from "./format";
import { renderMarkdown } from "./markdown";
import { motionSection, stopMotion } from "./motion";
import { fieldProfile, missingNote, typeLabel } from "./profile";
import { initThemeToggle } from "./theme";

const PER_GALLERY = 6;
let current = "";

function renderIndex(c: Catalog, q = ""): void {
  const host = $("#index-list");
  clear(host);
  const needle = q.trim().toLowerCase();
  const items = c.datasets.filter((d) => !needle || d.name.includes(needle) || d.description.toLowerCase().includes(needle));
  const max = Math.max(...c.datasets.map((d) => d.usedBy.length));
  const byLetter = new Map<string, Dataset[]>();
  for (const d of items) {
    const letter = d.name[0]?.toUpperCase() ?? "#";
    byLetter.set(letter, [...(byLetter.get(letter) ?? []), d]);
  }
  if (!items.length) {
    host.append(h("p", { class: "index-empty" }, "No dataset matches."));
    return;
  }
  for (const [letter, list] of byLetter) {
    host.append(h("section", { class: "index-group" },
      h("h2", { class: "index-letter" }, letter),
      h("ul", null, list.map((d) =>
        h("li", null, h("a", {
          href: `#${d.name}`,
          class: d.name === current ? "is-current" : "",
          "aria-current": d.name === current ? "page" : null,
          onclick: (e: Event) => {
            e.preventDefault();
            show(c, d.name, true);
          },
        }, h("span", { class: "index-name" }, d.name), usageBar(c.usage(d), max, 44)))))));
  }
}

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

function renderPlate(c: Catalog): void {
  const d = c.dataset(current);
  const plate = $("#plate");
  stopMotion();
  clear(plate);
  if (!d) {
    renderHome(c, plate);
    return;
  }
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
      h("a", { href: `#${prev.name}`, onclick: (e: Event) => { e.preventDefault(); show(c, prev.name, true); } }, h("span", null, "Previous"), h("b", null, prev.name)),
      h("a", { href: `#${next.name}`, class: "next", onclick: (e: Event) => { e.preventDefault(); show(c, next.name, true); } }, h("span", null, "Next"), h("b", null, next.name)),
    ),
  ]);
}

/** The home page: what vega-datasets is and how to use it, from the README. */
function renderHome(c: Catalog, plate: HTMLElement): void {
  const [lede = "", ...rest] = c.readme.trim().split(/\n\s*\n/);
  const ledeEl = h("div", { class: "lede md" });
  renderMarkdown(ledeEl, lede);
  const readme = h("div", { class: "md readme" });
  renderMarkdown(readme, rest.join("\n\n"), { sections: true });
  const label = [
    `Version ${c.package.version}`,
    plural(c.datasets.length, "dataset"),
    plural(c.examples.length, "gallery example"),
  ].join("  ·  ");
  append(plate, [
    h("header", { class: "plate-head" },
      h("p", { class: "plate-label" }, label),
      h("h1", { class: "plate-name" }, "Vega Datasets"),
      ledeEl,
      h("div", { class: "ds-actions" },
        h("button", {
          class: "btn btn-primary",
          type: "button",
          onclick: () => {
            const q = $("#index-q");
            q.scrollIntoView({ block: "nearest" });
            q.focus();
          },
        }, "Find a dataset"),
        h("a", { class: "btn", href: "https://www.npmjs.com/package/vega-datasets", target: "_blank", rel: "noopener" }, "npm package"),
        h("a", { class: "btn btn-quiet", href: "https://github.com/vega/vega-datasets", target: "_blank", rel: "noopener" }, "View on GitHub"),
      ),
    ),
    h("section", { class: "plate-sec", "aria-label": "About vega-datasets" }, readme),
  ]);
}

/** Show a dataset's plate, or the home page for "". */
function show(c: Catalog, name: string, userAction: boolean): void {
  current = name;
  hash.set(name);
  document.title = name ? `${name} · vega-datasets Field Guide` : "vega-datasets Field Guide";
  renderPlate(c);
  renderIndex(c, ($("#index-q") as HTMLInputElement).value);
  if (userAction) {
    window.scrollTo({ top: 0 });
    $("#plate").focus({ preventScroll: true });
  }
  // Scroll only the index list; scrollIntoView would also move the page.
  const list = $("#index-list");
  const item = list.querySelector<HTMLElement>(".is-current");
  if (item) list.scrollTop = item.offsetTop - list.offsetTop - list.clientHeight / 2 + item.offsetHeight / 2;
}

/** Which release and commit this page describes, so readers can tell what they're looking at. */
function renderBuildNote(c: Catalog): void {
  const { version, commit } = c.package;
  const repo = "https://github.com/vega/vega-datasets";
  $("#build-note").replaceChildren(
    `Built from `,
    h("a", { href: `${repo}/tree/${commit}` }, commit.slice(0, 7)),
    ` · latest release `,
    h("a", { href: `${repo}/releases/tag/v${version}` }, `v${version}`),
  );
}

async function main(): Promise<void> {
  initThemeToggle($("#theme-toggle") as HTMLButtonElement);
  let c: Catalog;
  try {
    c = await loadCatalog();
  } catch (err) {
    showError($("#plate"), err);
    return;
  }
  $("#guide-count").textContent = `${c.datasets.length} datasets · ${c.examples.length} gallery examples`;
  renderBuildNote(c);
  const q = $("#index-q") as HTMLInputElement;
  q.addEventListener("input", () => renderIndex(c, q.value));
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
    if (e.target instanceof HTMLInputElement || e.metaKey || e.ctrlKey || e.altKey) return;
    // From the home page (index -1), right goes to the first dataset and left to the last.
    const i = c.datasets.findIndex((d) => d.name === current);
    const n = c.datasets.length;
    if (e.key === "ArrowRight" || e.key === "j") show(c, c.datasets[(i + 1) % n]!.name, true);
    if (e.key === "ArrowLeft" || e.key === "k") show(c, c.datasets[(i - 1 + n) % n]!.name, true);
  });
}

void main();
