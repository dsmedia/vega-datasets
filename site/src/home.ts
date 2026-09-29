/**
 * The home page: what vega-datasets is, how to load it, and every dataset as a
 * card, filtered by search, chips and the catalog chart's brush.
 */
import { type Catalog, type Dataset, type Gallery, GALLERIES, GALLERY_LABEL } from "./catalog";
import { mountCatalogChart, type MountedChart } from "./catalog-chart";
import { snippetTabs, thumbImg, usageStack } from "./components";
import { append, clear, h } from "./dom";
import { formatBytes, formatCount, FORMAT_LABEL, plural } from "./format";
import {
  baseMatches,
  type Brush,
  chartRows,
  type Filters,
  FORMAT_GROUPS,
  type FormatGroup,
  homeCounts,
  isFiltered,
  listDatasets,
  NO_FILTERS,
  plainSummary,
  readmeSection,
  type Sort,
  SORT_LABEL,
  SORT_NOTE,
  showcase,
} from "./home-model";
import { renderMarkdown } from "./markdown";
import { token } from "./theme";

const REPO = "https://github.com/vega/vega-datasets";
const PHONE = "(max-width: 640px)";
const CARDS = { wide: 9, phone: 4 };

let teardown: (() => void) | null = null;

/**
 * What the reader set on the home page, kept while they visit a dataset so Back (or the
 * Datasets link) returns to the same list. Only for this page view; the brush is not
 * kept, since the chart is drawn afresh.
 */
const saved = {
  filters: { ...NO_FILTERS, formats: new Set<FormatGroup>(), galleries: new Set<Gallery>() },
  expanded: false,
};

/** Stop the home page's chart and listeners (before showing another page). */
export function stopHome(): void {
  teardown?.();
  teardown = null;
}

function cdnUrl(c: Catalog, file: string): string {
  const major = c.package.version.split(".")[0];
  return `https://cdn.jsdelivr.net/npm/vega-datasets@${major}/data/${file}`;
}

function card(c: Catalog, d: Dataset, max: number): HTMLElement {
  const n = d.usedBy.length;
  const geo = d.format === "topojson" || d.format === "geojson";
  return h("a", { class: "card", href: `#${encodeURIComponent(d.name)}` },
    h("div", { class: "card-head" },
      h("span", { class: "card-name" }, d.name),
      h("span", { class: "mono" }, FORMAT_LABEL[d.format] ?? d.format)),
    h("p", { class: "card-desc" }, plainSummary(d.description)),
    h("div", { class: "card-foot" },
      h("div", { class: "card-meta mono" },
        d.rows !== null ? h("span", null, plural(d.rows, "row")) : null,
        d.rows !== null && d.fields.length ? h("span", null, plural(d.fields.length, "field")) : null,
        geo ? h("span", null, "Geographic") : null,
        h("span", { class: "card-size" }, formatBytes(d.bytes))),
      h("span", { class: "card-usage" },
        usageStack(c.usage(d), max),
        h("span", { class: "mono" }, n ? formatCount(n) : "0", h("span", { class: "card-ex" }, n === 1 ? " example" : " examples")))),
  );
}

function galleryKey(): HTMLElement {
  return h("div", { class: "gallery-key" }, GALLERIES.map((g) =>
    h("span", { class: `gtag g-${g}` }, h("span", { class: "gdot", "aria-hidden": "true" }), GALLERY_LABEL[g])));
}

function chip(label: Node | string, count: number, onToggle: (on: boolean) => void): HTMLButtonElement {
  const b = h("button", { class: "chip", type: "button", "aria-pressed": "false" }, label, h("span", { class: "n" }, formatCount(count)));
  b.addEventListener("click", () => {
    const on = b.getAttribute("aria-pressed") !== "true";
    b.setAttribute("aria-pressed", String(on));
    onToggle(on);
  });
  return b;
}

function aboutItem(id: string, title: string, hint: string, body: HTMLElement | string, open = false): HTMLElement {
  return h("details", { class: "about-item", id, open },
    h("summary", null, h("span", { class: "about-title" }, title), h("span", { class: "hint" }, hint)),
    typeof body === "string" ? h("div", { class: "about-body" }, body) : body);
}

function readmeBody(c: Catalog, heading: string): HTMLElement | null {
  const md = readmeSection(c.readme, heading);
  if (md === null) return null;
  const el = h("div", { class: "about-body md" });
  renderMarkdown(el, md);
  return el;
}

/**
 * Render the home page. `returningFrom` names the dataset the reader just left: its card
 * is scrolled into view and focused. Returns whether a card took focus.
 */
export function renderHome(c: Catalog, root: HTMLElement, returningFrom?: string): boolean {
  stopHome();
  const counts = homeCounts(c);
  const phone = matchMedia(PHONE);
  const max = Math.max(...c.datasets.map((d) => d.usedBy.length), 1);

  // --- Title, showcase and introduction ---------------------------------------------------
  const strip = h("div", { class: "showcase", "aria-hidden": "true" },
    showcase(c, 8).map((e) => {
      const img = thumbImg(e);
      img.loading = "eager";
      return h("a", { href: e.url, target: "_blank", rel: "noopener", tabindex: -1 }, img);
    }));

  const browse = h("section", { class: "wrap browse", id: "browse", "aria-labelledby": "browse-h" });
  const about = h("section", { class: "wrap about", "aria-labelledby": "about-h" });
  const openVersioning = () => {
    const item = about.querySelector<HTMLDetailsElement>("#about-versioning");
    if (!item) return;
    item.open = true;
    item.scrollIntoView({ block: "start" });
    item.querySelector("summary")?.focus({ preventScroll: true });
  };

  const intro = h("section", { class: "wrap intro", "aria-label": "Introduction" },
    h("div", { class: "intro-text" },
      h("p", { class: "lead" },
        "The example data behind the Vega, Vega-Lite and Altair galleries. ",
        "Each dataset documents its fields, source and license, and links to every gallery example that uses it: ",
        `${formatCount(counts.datasets)} datasets and ${formatCount(counts.examplesWithData)} examples in all.`),
      h("p", { class: "release mono" },
        `Release ${c.package.version}  ·  Data Package v2`, h("span", { class: "wide-only" }, "  ·  BSD-3-Clause code")),
      h("div", { class: "lead-buttons" },
        h("button", {
          class: "btn btn-primary",
          type: "button",
          onclick: () => {
            browse.scrollIntoView({ block: "start" });
            browse.querySelector<HTMLInputElement>("input[type=search]")?.focus({ preventScroll: true });
          },
        }, "Browse Datasets"),
        h("a", { class: "btn", href: REPO }, "View on GitHub"))),
    h("div", { class: "quickstart" },
      snippetTabs("qs", "Quick start", [
        { name: "URL", code: cdnUrl(c, "cars.json") },
        {
          name: "JavaScript",
          code: "npm install vega-datasets\n\nimport data from 'vega-datasets';\nconst cars = await data['cars.json']();",
        },
        {
          name: "Vega-Lite",
          code: [
            "{",
            `  "data": {"url": "${cdnUrl(c, "cars.json")}"},`,
            `  "mark": "point",`,
            `  "encoding": {`,
            `    "x": {"field": "Horsepower", "type": "quantitative"},`,
            `    "y": {"field": "Miles_per_Gallon", "type": "quantitative"}`,
            "  }",
            "}",
          ].join("\n"),
        },
        { name: "Python", code: "from altair.datasets import data\n\ncars = data.cars()" },
      ]),
      // A major-version URL floats; say so, and give the exact release for a fixed version.
      h("p", { class: "note" },
        "The ", h("code", null, `@${c.package.version.split(".")[0]}`), ` URL tracks ${c.package.version.split(".")[0]}.x releases, so it gets fixes without breaking changes. To lock a version, use `,
        h("code", null, `@${c.package.version}`), ". ",
        h("button", { class: "link", type: "button", onclick: openVersioning }, "Versioning"))),
  );

  // --- Datasets: chart, filters, cards ----------------------------------------------------------
  const filters: Filters & { formats: Set<FormatGroup>; galleries: Set<Gallery> } = saved.filters;
  filters.brush = null;
  const status = h("span", { class: "status mono", "aria-live": "polite" });
  const chartHost = h("div", { class: "catalog-chart" });
  const chartNote = h("span", { class: "hint" });
  const chartFeatures = h("span", { class: "mono" });
  const search = h("input", {
    type: "search", id: "home-q", placeholder: "Search by name, field or description", autocomplete: "off",
  });
  const sort = h("select", { id: "home-sort" },
    (Object.keys(SORT_LABEL) as Sort[]).map((s) => h("option", { value: s }, SORT_LABEL[s])));
  const cards = h("div", { class: "cards" });
  const more = h("button", { class: "btn more", type: "button" });
  let chart: MountedChart | null = null;

  search.value = filters.query;
  sort.value = filters.sort;
  const formatChips = FORMAT_GROUPS.map((g) => chip(g, counts.formats[g], (on) => {
    if (on) filters.formats.add(g); else filters.formats.delete(g);
    refilter();
  }));
  const galleryChips = GALLERIES.map((g) => chip(
    h("span", { class: `gtag g-${g}` }, h("span", { class: "gdot", "aria-hidden": "true" }), GALLERY_LABEL[g]),
    counts.galleries[g],
    (on) => {
      if (on) filters.galleries.add(g); else filters.galleries.delete(g);
      refilter();
    },
  ));

  formatChips.forEach((b, i) => b.setAttribute("aria-pressed", String(filters.formats.has(FORMAT_GROUPS[i]!))));
  galleryChips.forEach((b, i) => b.setAttribute("aria-pressed", String(filters.galleries.has(GALLERIES[i]!))));

  const clearAll = () => {
    filters.query = "";
    search.value = "";
    filters.formats.clear();
    filters.galleries.clear();
    for (const b of [...formatChips, ...galleryChips]) b.setAttribute("aria-pressed", "false");
    if (filters.brush) void chart?.redraw();
    filters.brush = null;
    refilter();
  };

  /** A filter changed: show the first cards of the new list, then redraw. */
  function refilter(): void {
    saved.expanded = false;
    update();
  }

  function update(): void {
    const list = listDatasets(c, filters);
    const base = isFiltered({ ...filters, brush: null }) ? baseMatches(c, filters).map((d) => d.name) : null;
    chart?.setMatches(base);
    const limit = phone.matches ? CARDS.phone : CARDS.wide;
    const shown = saved.expanded ? list : list.slice(0, limit);
    clear(status);
    if (isFiltered(filters)) {
      status.append(
        `${formatCount(list.length)} of ${formatCount(counts.datasets)}`,
        filters.brush ? "  ·  filtered by chart" : "",
        "  ·  ",
        h("button", { class: "link", type: "button", onclick: clearAll }, "Clear"),
      );
    } else {
      status.append(SORT_NOTE[filters.sort]);
    }
    // Re-rendering replaces the cards: keep focus on the same dataset's card if it still shows.
    const focused = cards.contains(document.activeElement) ? document.activeElement?.getAttribute("href") : null;
    cards.replaceChildren(...shown.map((d) => card(c, d, max)));
    if (focused) cards.querySelector<HTMLElement>(`a.card[href="${CSS.escape(focused)}"]`)?.focus({ preventScroll: true });
    if (!list.length) cards.append(h("p", { class: "cards-empty" }, "No dataset matches."));
    more.hidden = shown.length === list.length;
    more.textContent = `Show All ${formatCount(list.length)} Datasets`;
  }

  search.addEventListener("input", () => {
    filters.query = search.value;
    refilter();
  });
  sort.addEventListener("change", () => {
    filters.sort = sort.value as Sort;
    update();
  });
  more.addEventListener("click", () => {
    saved.expanded = true;
    update();
  });

  const describeChart = () => {
    chartNote.textContent = phone.matches
      ? "Tap a point to open its dataset."
      : "Drag across the chart to filter the list. Click a point to open its dataset.";
    chartFeatures.textContent = phone.matches
      ? "Vega-Lite · href channel · log scale"
      : "Vega-Lite · interval selection · href channel · log scale";
  };
  describeChart();

  append(browse, [
    h("div", { class: "sec-head" }, h("h2", { id: "browse-h" }, "Datasets"), status),
    h("div", { class: "filters" },
      h("label", { class: "visually-hidden", for: "home-q" }, "Search datasets"),
      search,
      h("div", { class: "chip-row" },
        h("div", { class: "chip-group", role: "group", "aria-label": "Format" }, h("span", { class: "chip-label" }, "Format"), formatChips),
        h("div", { class: "chip-group", role: "group", "aria-label": "Used in" }, h("span", { class: "chip-label" }, "Used in"), galleryChips)),
      h("label", { class: "sort", for: "home-sort" }, "Sort", sort)),
    chartHost,
    h("div", { class: "chart-note" }, chartNote, chartFeatures),
    cards,
    h("div", { class: "browse-foot" }, galleryKey(), more),
  ]);

  // --- Use the Data --------------------------------------------------------------------------------
  const use = h("section", { class: "wrap use", "aria-labelledby": "use-h" },
    h("h2", { id: "use-h" }, "Use the Data"),
    h("div", { class: "use-grid" },
      h("div", null, h("h3", null, "Load by URL"),
        h("p", null, "Every file has a CDN URL. Publish with the versioned jsDelivr link."),
        h("a", { class: "use-link", href: `${REPO}#http-direct-access` }, "URL Patterns")),
      h("div", null, h("h3", null, "Import in JavaScript"),
        h("pre", { class: "snippet" }, h("code", null, "npm install vega-datasets")),
        h("a", { class: "use-link", href: `${REPO}#using-esm-import` }, "ESM Example")),
      h("div", null, h("h3", null, "Reference in a Spec"),
        h("pre", { class: "snippet" }, h("code", null, `"data": {"url": ".../cars.json"}`)),
        h("a", { class: "use-link", href: `${REPO}#in-vegavega-lite-specifications` }, "Vega-Lite Example")),
      h("div", null, h("h3", null, "Load in Python"),
        h("pre", { class: "snippet" }, h("code", null, "from altair.datasets import data")),
        h("a", { class: "use-link", href: `${REPO}#language-interfaces` }, "Julia and Observable"))),
  );

  // --- About and Contribute ---------------------------------------------------------------------------
  const readmeItems: [string, string, string, string][] = [
    ["about-metadata", "Metadata", "Data Package v2: schema, sources, licenses", "Dataset Information"],
    ["about-versioning", "Versioning", "Semantic versioning, applied to data", "Versioning"],
    ["about-use", "Intended Use", "Teaching and demos. Some flaws are deliberate.", "Data Usage Note"],
    ["about-gallery", "Gallery Index", `All ${formatCount(counts.examples)} examples, mapped to their data. It's a dataset too.`, "Example Galleries"],
  ];
  append(about, [
    h("div", { class: "about-list" },
      h("h2", { id: "about-h" }, "About the Collection"),
      aboutItem("about-licensing", "Licensing", "Each dataset keeps its source's license",
        "Code is BSD-3-Clause. Each dataset keeps its source's license, recorded in its metadata and shown on its page. Check the source's terms before reuse.",
        true),
      readmeItems.map(([id, title, hint, heading]) => {
        const body = readmeBody(c, heading);
        return body ? aboutItem(id, title, hint, body) : null;
      })),
    h("aside", { class: "contribute", "aria-labelledby": "contribute-h" },
      h("h3", { id: "contribute-h" }, "Contribute"),
      h("p", null, "Add a dataset, document one, or fix an error. Existing files rarely change: Vega, Vega-Lite and the Vega Editor test against them."),
      h("a", { href: `${REPO}/blob/main/CONTRIBUTING.md` }, "Contribution Guidelines")),
  ]);

  root.append(
    h("div", { class: "wrap" }, h("h1", { class: "home-title" }, h("b", null, "Vega Datasets"), " – The Data Behind the Examples")),
    strip, intro, browse, use, about,
  );
  update();
  const back = returningFrom ? cards.querySelector<HTMLElement>(`a.card[href="#${CSS.escape(encodeURIComponent(returningFrom))}"]`) : null;
  if (back) {
    back.scrollIntoView({ block: "center" });
    back.focus({ preventScroll: true });
  }

  // --- The chart, and following the screen size -------------------------------------------------------
  let live = true;
  const rows = chartRows(c, formatBytes);
  const onBrush = (b: Brush | null) => {
    // Every redraw (a theme toggle, say) reports "no brush"; only a real change refilters.
    if (JSON.stringify(b) === JSON.stringify(filters.brush)) return;
    filters.brush = b;
    refilter();
  };
  const options = () => ({
    brush: !phone.matches,
    height: phone.matches ? 214 : 240,
    labels: phone.matches ? 5 : 9,
    legendTop: phone.matches,
    legendColumns: chartHost.clientWidth < 340 ? 2 : 4,
    monoFont: token("--font-mono"),
  });
  mountCatalogChart(chartHost, rows, counts.formats, options, onBrush).then(
    (m) => {
      if (!live) { m.destroy(); return; }
      chart = m;
      update(); // Filters set while the chart loaded.
    },
    (err: unknown) => {
      chartHost.replaceChildren(h("p", { class: "muted" }, `The chart didn't load: ${err instanceof Error ? err.message : String(err)}`));
    },
  );
  const onScreen = () => {
    describeChart();
    update();
    void chart?.redraw();
  };
  phone.addEventListener("change", onScreen);
  teardown = () => {
    live = false;
    phone.removeEventListener("change", onScreen);
    chart?.destroy();
  };
  return back !== null;
}
