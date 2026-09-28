/**
 * A dataset's page: what it is, its fields, a live chart, a preview, the gallery
 * examples that use it and where it comes from, with a rail for loading it.
 */
import { type Catalog, type Dataset, type Example, type Gallery, GALLERIES, GALLERY_LABEL, licenseFamily } from "./catalog";
import { licenseBlock, snippetTabs, sourcesBlock, thumbImg, usageStack } from "./components";
import {
  fileName,
  formatDescription,
  interleave,
  isReleased,
  linkText,
  sizeDescription,
  useSnippets,
} from "./dataset-model";
import { h } from "./dom";
import { exploreSection, stopExplore } from "./explore";
import { formatBytes, formatCount, FORMAT_LABEL, plural } from "./format";
import { renderMarkdown } from "./markdown";
import { motionSection, stopMotion } from "./motion";
import { missingCount, profileSummary, sparkline, typeLabel } from "./profile";

const REPO = "https://github.com/vega/vega-datasets";
const METADATA = `${REPO}/blob/main/_data/datapackage_additions.toml`;
const EXAMPLES_SHOWN = 8;
const PREVIEW_ROWS = 5;

let teardown: (() => void) | null = null;

/** Stop the page's charts and observers (before showing another page). */
export function stopDataset(): void {
  stopMotion();
  stopExplore();
  teardown?.();
  teardown = null;
}

function gdot(g: Gallery): HTMLElement {
  return h("span", { class: `gdot g-${g}`, "aria-hidden": "true" });
}

/** "Datasets / cars" and the previous and next datasets. */
function topBar(c: Catalog, d: Dataset): HTMLElement {
  const i = c.datasets.indexOf(d);
  const n = c.datasets.length;
  const prev = c.datasets[(i - 1 + n) % n]!;
  const next = c.datasets[(i + 1) % n]!;
  const step = (to: Dataset, dir: "Previous" | "Next") => h("a", {
    class: "step",
    href: `#${encodeURIComponent(to.name)}`,
    "aria-label": `${dir} dataset: ${to.name}`,
  }, dir === "Previous" ? "‹" : null, h("span", { class: "wide-only" }, dir === "Previous" ? ` ${to.name}` : `${to.name} `), dir === "Next" ? "›" : null);
  return h("div", { class: "wrap ds-bar" },
    h("nav", { class: "crumbs", "aria-label": "Breadcrumb" },
      h("a", { href: "#", class: "wide-only" }, "Datasets"),
      h("span", { class: "wide-only", "aria-hidden": "true" }, "/"),
      h("span", { class: "wide-only", "aria-current": "page" }, d.name),
      h("a", { href: "#", class: "phone-only" }, "‹ All datasets")),
    h("nav", { class: "stepper", "aria-label": "Neighbouring datasets" },
      step(prev, "Previous"),
      h("span", { class: "mono" }, h("span", { class: "wide-only" }, `${i + 1} of ${n}`), h("span", { class: "phone-only" }, `${i + 1} / ${n}`)),
      step(next, "Next")),
  );
}

function badges(c: Catalog, d: Dataset): HTMLElement {
  const usage = c.usage(d);
  const examples = d.usedBy.length;
  const badge = (...children: (Node | string | null)[]) => h("span", { class: "badge" }, ...children);
  return h("div", { class: "badges" },
    badge(FORMAT_LABEL[d.format] ?? d.format),
    d.rows !== null ? badge(plural(d.rows, "row")) : null,
    d.fields.length ? badge(plural(d.fields.length, "field")) : null,
    badge(formatBytes(d.bytes)),
    examples
      ? badge(
          h("span", { class: "badge-dots wide-only" }, GALLERIES.filter((g) => usage[g]).map(gdot)),
          h("span", { class: "wide-only" }, plural(examples, "gallery example")),
          h("span", { class: "phone-only" }, plural(examples, "example")))
      : null,
    licenseFamily(d) === "Not specified" ? h("span", { class: "badge warn" }, "License not specified") : null,
  );
}

function sectionHead(id: string, title: string, ...right: (Node | null)[]): HTMLElement {
  return h("div", { class: "sec-head" }, h("h2", { id }, title), ...right);
}

function fieldsSection(d: Dataset): HTMLElement | null {
  if (!d.fields.length) return null;
  const described = d.fields.some((f) => f.description);
  const rows = d.fields.map((f) => {
    const spark = sparkline(f);
    const miss = missingCount(f, d.rows);
    return h("tr", null,
      h("td", { class: "f-name" }, h("code", null, f.name), f.description ? h("p", { class: "f-desc" }, f.description) : null),
      h("td", { class: "f-type" }, h("span", { class: "type" }, typeLabel(f))),
      h("td", { class: "f-dist" }, spark ?? (f.profile.kind === "nominal" ? `${formatCount(f.profile.distinct)} distinct` : "")),
      h("td", { class: `f-sum${f.profile.kind === "nominal" ? "" : " mono"}` }, profileSummary(f)),
      h("td", { class: `f-miss mono${miss.any ? " any" : ""}` }, h("span", { class: "phone-only" }, "Missing "), miss.text),
    );
  });
  return h("section", { class: "ds-sec", id: "sec-fields", "aria-labelledby": "fields-h" },
    sectionHead("fields-h", "Fields"),
    h("table", { class: "dict" },
      h("thead", null, h("tr", null,
        h("th", { scope: "col" }, "Field"), h("th", { scope: "col" }, "Type"), h("th", { scope: "col" }, "Distribution"),
        h("th", { scope: "col" }, "Summary"), h("th", { scope: "col", class: "f-miss" }, "Missing"))),
      h("tbody", null, rows)),
    h("p", { class: "sec-note" },
      d.rows !== null ? `Profiled across all ${formatCount(d.rows)} rows. ` : "",
      described ? null : "No field descriptions yet. ",
      described ? null : h("a", { href: METADATA }, "Add them")),
  );
}

function previewSection(d: Dataset): HTMLElement | null {
  if (d.image) {
    return h("section", { class: "ds-sec", id: "sec-preview", "aria-labelledby": "preview-h" },
      sectionHead("preview-h", "Preview"),
      h("figure", { class: "ds-image" }, h("img", { src: d.image, alt: `The ${d.name} image` })));
  }
  const p = d.preview;
  if (!p || !p.rows.length) return null;
  const shown = p.rows.slice(0, PREVIEW_ROWS);
  const total = d.rows ?? shown.length;
  return h("section", { class: "ds-sec", id: "sec-preview", "aria-labelledby": "preview-h" },
    sectionHead("preview-h", "Preview",
      h("span", { class: "mono sec-meta" },
        h("span", { class: "wide-only" }, `First ${shown.length} of ${formatCount(total)} rows`),
        h("span", { class: "phone-only" }, `${shown.length} of ${formatCount(total)} rows`))),
    h("div", { class: "table-scroll", tabindex: 0, role: "region", "aria-label": `First rows of ${d.name}` },
      h("table", { class: "preview" },
        h("thead", null, h("tr", null, p.columns.map((col) => h("th", { scope: "col" }, col)))),
        h("tbody", null, shown.map((r) => h("tr", null, r.map((cell) => h("td", null, cell))))))),
    p.columns.length > 4 ? h("p", { class: "sec-note phone-only" }, `Scroll sideways for all ${p.columns.length} fields.`) : null,
  );
}

function exampleCard(e: Example): HTMLElement {
  const second = e.gallery === "altair"
    ? h("a", { href: e.source, target: "_blank", rel: "noopener" }, "Python source")
    : e.editor
      ? h("a", { href: e.editor, target: "_blank", rel: "noopener" }, "Open in Vega Editor")
      : h("a", { href: e.source, target: "_blank", rel: "noopener" }, "Spec");
  return h("li", { class: "ex" },
    h("a", { class: "ex-thumb", href: e.url, target: "_blank", rel: "noopener", "aria-label": `${e.name} in the gallery`, tabindex: -1 }, thumbImg(e)),
    h("a", { class: "ex-name", href: e.url, target: "_blank", rel: "noopener" }, gdot(e.gallery), e.name),
    h("span", { class: "ex-links" }, second));
}

function examplesSection(c: Catalog, d: Dataset): HTMLElement {
  const all = interleave(c.examplesFor(d));
  const head = sectionHead("examples-h", "Examples");
  const section = h("section", { class: "ds-sec", id: "sec-examples", "aria-labelledby": "examples-h" }, head);
  if (!all.length) {
    section.append(h("div", { class: "shelf-empty" },
      h("p", null, "Not yet seen in any gallery."),
      h("p", { class: "muted" }, "This dataset is published and documented but no Vega, Vega-Lite or Altair example uses it. A new example would be a welcome contribution.")));
    return section;
  }
  const usage = c.usage(d);
  let filter: Gallery | null = null;
  let expanded = false;
  const grid = h("ul", { class: "ex-grid" });
  const more = h("button", { class: "btn", type: "button" });
  const draw = () => {
    const list = filter ? all.filter((e) => e.gallery === filter) : all;
    const shown = expanded ? list : list.slice(0, EXAMPLES_SHOWN);
    grid.replaceChildren(...shown.map(exampleCard));
    more.hidden = shown.length === list.length;
    more.textContent = `Show All ${formatCount(list.length)} Examples`;
  };
  const choices: [Gallery | null, string][] = [
    [null, `All ${formatCount(all.length)}`],
    ...GALLERIES.filter((g) => usage[g]).map((g) => [g, `${GALLERY_LABEL[g]} ${formatCount(usage[g])}`] as [Gallery, string]),
  ];
  const seg = h("div", { class: "seg", role: "group", "aria-label": "Filter by gallery" }, choices.map(([g, label]) =>
    h("button", {
      type: "button",
      "aria-pressed": String(g === filter),
      onclick: (e: Event) => {
        filter = g;
        expanded = false;
        for (const b of seg.children) b.setAttribute("aria-pressed", String(b === e.currentTarget));
        draw();
      },
    }, label)));
  more.addEventListener("click", () => {
    expanded = true;
    draw();
  });
  if (choices.length > 2) head.append(seg);
  draw();
  section.append(grid, h("div", { class: "sec-foot" },
    h("p", { class: "sec-note" }, "Vega and Vega-Lite examples open in the Editor with this dataset loaded. Altair examples link to their Python source."),
    more));
  return section;
}

/** "Not specified. <what the source says> (<link>). Check the source's terms before reuse." */
function licenseText(d: Dataset): HTMLElement {
  if (licenseFamily(d) !== "Not specified") return licenseBlock(d);
  const l = d.licenses[0];
  const title = l?.title?.replace(/\.\s*$/, "");
  const link = l?.path ? h("a", { href: l.path, target: "_blank", rel: "noopener" }, linkText(l.path)) : null;
  return h("p", null,
    "Not specified.",
    title ? ` ${title}` : "",
    link ? " (" : "", link, link ? ")" : "",
    title || link ? "." : "",
    " Check the source's terms before reuse.");
}

function provenanceSection(d: Dataset): HTMLElement {
  return h("section", { class: "ds-sec", id: "sec-provenance", "aria-labelledby": "provenance-h" },
    sectionHead("provenance-h", "Provenance"),
    h("dl", { class: "kv prov" },
      h("dt", null, "Source"), h("dd", null, sourcesBlock(d)),
      h("dt", null, "License"), h("dd", null, licenseText(d)),
      h("dt", null, "File"), h("dd", null,
        h("code", null, `data/${d.file}`),
        d.bytes !== null ? ` · ${formatCount(d.bytes)} bytes · ` : " · ",
        h("a", { href: `${REPO}/blob/main/data/${d.file}` }, "view on GitHub"))),
  );
}

function rail(c: Catalog, d: Dataset): HTMLElement {
  const usage = c.usage(d);
  const total = d.usedBy.length;
  const firstSource = d.sources[0];
  return h("aside", { class: "ds-rail", "aria-label": "Using this dataset" },
    h("div", { class: "rail-box rail-use" },
      h("h3", null, "Use This Dataset"),
      snippetTabs("use", "Snippet language", useSnippets(d)),
      h("a", { class: "btn", href: d.url, download: fileName(d) }, `Download ${fileName(d)}`)),
    h("div", { class: "rail-box wide-only" },
      h("h3", null, "At a Glance"),
      h("dl", { class: "kv" },
        h("dt", null, "Format"), h("dd", null, formatDescription(d)),
        h("dt", null, "Size"), h("dd", null, sizeDescription(d)),
        h("dt", null, "Source"), h("dd", null,
          firstSource ? (firstSource.path ? h("a", { href: firstSource.path, target: "_blank", rel: "noopener" }, firstSource.title) : firstSource.title) : "Not recorded",
          d.sources.length > 1 ? ` and ${d.sources.length - 1} more` : ""),
        h("dt", null, "License"), h("dd", null, licenseFamily(d)),
        h("dt", null, "Release"), h("dd", null, isReleased(d) ? `v${c.package.version}` : "Not yet released"))),
    h("div", { class: "rail-box wide-only" },
      h("h3", null, "Used in the Galleries"),
      total
        ? [
            h("div", { class: "rail-stack" }, usageStack(usage, total)),
            h("div", { class: "rail-usage" }, GALLERIES.map((g) => [gdot(g), h("span", null, GALLERY_LABEL[g]), h("span", { class: "mono" }, formatCount(usage[g]))]).flat()),
          ]
        : h("p", { class: "muted" }, "Not used in a gallery example yet.")),
  );
}

/** Section links under the title, marking the section in view. */
function sectionNav(sections: HTMLElement[]): { nav: HTMLElement; observe(): () => void } {
  const links = sections.map((s) => {
    const heading = s.querySelector("h2")!;
    const count = s.dataset.count;
    return h("button", {
      type: "button",
      class: "tab",
      "data-target": s.id,
      onclick: () => {
        s.scrollIntoView({ block: "start" });
        heading.setAttribute("tabindex", "-1");
        heading.focus({ preventScroll: true });
      },
    }, heading.textContent, count ? h("span", { class: "n" }, count) : null);
  });
  const nav = h("nav", { class: "tabs section-nav", "aria-label": "Sections" }, links);
  const observe = () => {
    const visible = new Map<string, boolean>();
    const io = new IntersectionObserver((entries) => {
      for (const e of entries) visible.set(e.target.id, e.isIntersecting);
      const current = sections.find((s) => visible.get(s.id)) ?? null;
      for (const b of links) {
        if (current && b.dataset.target === current.id) b.setAttribute("aria-current", "true");
        else b.removeAttribute("aria-current");
      }
    }, { rootMargin: "-30% 0px -60% 0px" });
    sections.forEach((s) => io.observe(s));
    return () => io.disconnect();
  };
  return { nav, observe };
}

export function renderDataset(c: Catalog, d: Dataset, page: HTMLElement): void {
  stopDataset();
  const [lede = "", ...rest] = (d.description || "No description recorded.").trim().split(/\n\s*\n/);
  const ledeEl = h("div", { class: "lede md" });
  renderMarkdown(ledeEl, lede);
  const restEl = rest.length ? h("div", { class: "md body-md" }) : null;
  if (restEl) renderMarkdown(restEl, rest.join("\n\n"));

  const fields = fieldsSection(d);
  if (fields) fields.dataset.count = String(d.fields.length);
  const explore = exploreSection(d);
  const motion = motionSection(d);
  if (motion) motion.id = "sec-motion";
  const preview = previewSection(d);
  const examples = examplesSection(c, d);
  if (d.usedBy.length) examples.dataset.count = String(d.usedBy.length);
  const provenance = provenanceSection(d);
  const sections = [fields, explore, motion, preview, examples, provenance].filter((s): s is HTMLElement => s !== null);
  const { nav, observe } = sectionNav(sections);

  page.append(
    topBar(c, d),
    h("header", { class: "wrap ds-head" },
      h("h1", { class: "ds-name" }, d.name),
      ledeEl,
      restEl,
      badges(c, d)),
    h("div", { class: "wrap ds-body" },
      h("div", { class: "ds-main" }, nav, ...sections),
      rail(c, d)),
  );
  teardown = observe();
}
