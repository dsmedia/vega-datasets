/** UI pieces of a dataset plate. */
import { type Dataset, type Example, type Gallery, GALLERIES, GALLERY_LABEL, githubSource } from "./catalog";
import { h } from "./dom";
import { formatCount, FORMAT_LABEL } from "./format";
import { starterEditorUrl } from "./starter";

/** Gallery identity: a colored dot always paired with its name (color is never the only cue). */
export function galleryTag(g: Gallery, count?: number): HTMLElement {
  return h("span", { class: `gtag g-${g}` },
    h("span", { class: "gdot", "aria-hidden": "true" }),
    GALLERY_LABEL[g],
    count === undefined ? null : h("span", { class: "gcount" }, formatCount(count)),
  );
}


/**
 * A flat stacked bar of example counts per gallery (Vega-Lite, Vega, Altair),
 * scaled to `max` so cards compare. The counts are also given as text.
 */
export function usageStack(counts: Record<Gallery, number>, max: number): HTMLElement {
  const bar = h("span", { class: "stack", "aria-hidden": "true" });
  for (const g of GALLERIES) {
    if (!counts[g]) continue;
    const seg = h("span", { class: `g-${g}` });
    seg.style.width = `${(100 * counts[g]) / Math.max(max, 1)}%`;
    bar.append(seg);
  }
  const detail = GALLERIES.filter((g) => counts[g]).map((g) => `${GALLERY_LABEL[g]} ${counts[g]}`).join(", ");
  return h("span", { class: "usage" }, bar, detail ? h("span", { class: "visually-hidden" }, ` (${detail})`) : null);
}

export function thumbImg(ex: Example): HTMLImageElement {
  const img = h("img", {
    src: ex.thumb ?? "",
    alt: `Thumbnail of the ${GALLERY_LABEL[ex.gallery]} example “${ex.name}”`,
    loading: "lazy",
    decoding: "async",
  });
  if (ex.thumbSize) {
    img.width = ex.thumbSize[0];
    img.height = ex.thumbSize[1];
  }
  return img;
}

export function exampleLinks(ex: Example): HTMLElement {
  return h("div", { class: "ex-links" },
    h("a", { href: ex.url, target: "_blank", rel: "noopener" }, "Gallery page"),
    ex.editor ? h("a", { href: ex.editor, target: "_blank", rel: "noopener" }, "Open in Editor") : null,
    h("a", { href: ex.source, target: "_blank", rel: "noopener" }, ex.gallery === "altair" ? "Python source" : "Spec"),
  );
}

/** Actions every dataset view offers. */
export function datasetActions(d: Dataset): HTMLElement {
  const starter = starterEditorUrl(d);
  return h("div", { class: "ds-actions" },
    starter
      ? h("a", { class: "btn btn-primary", href: starter, target: "_blank", rel: "noopener" }, "Try it in the Vega Editor")
      : null,
    h("a", { class: "btn", href: d.url, target: "_blank", rel: "noopener" }, `Open ${FORMAT_LABEL[d.format] ?? d.format} file`),
    h("a", { class: "btn btn-quiet", href: githubSource(d), target: "_blank", rel: "noopener" }, "View on GitHub"),
  );
}

/** A Copy button for `text()`; if the clipboard is blocked it selects `fallback` for Ctrl/⌘+C. */
export function copyButton(text: () => string, fallback: () => Element | null, cls = "btn btn-quiet copy"): HTMLButtonElement {
  const btn = h("button", { class: cls, type: "button" }, "Copy");
  const label = btn.textContent!;
  btn.addEventListener("click", () => {
    navigator.clipboard.writeText(text()).then(
      () => {
        btn.textContent = "Copied";
        btn.dataset.copied = "true";
        setTimeout(() => {
          btn.textContent = label;
          delete btn.dataset.copied;
        }, 1600);
      },
      () => {
        const sel = window.getSelection();
        const target = fallback();
        if (sel && target) {
          const range = document.createRange();
          range.selectNodeContents(target);
          sel.removeAllRanges();
          sel.addRange(range);
        }
        btn.textContent = "Press Ctrl/⌘+C";
      },
    );
  });
  return btn;
}

export interface Snippet {
  name: string;
  code: string;
}

/**
 * Code snippets behind tabs, each with a Copy button (ARIA tabs: arrow keys, Home
 * and End move between tabs). `id` prefixes the element ids.
 */
export function snippetTabs(id: string, label: string, snippets: Snippet[]): HTMLElement {
  const code = h("code");
  const panel = h("pre", { class: "snippet", role: "tabpanel", id: `${id}-panel`, tabindex: 0 }, code);
  panel.prepend(copyButton(() => code.textContent ?? "", () => code, "copy-btn"));
  const tabs = snippets.map((s, i) => h("button", {
    class: "tab",
    role: "tab",
    type: "button",
    id: `${id}-tab-${i}`,
    "aria-controls": `${id}-panel`,
  }, s.name));
  const select = (i: number, focus: boolean) => {
    tabs.forEach((t, j) => {
      t.setAttribute("aria-selected", String(i === j));
      t.tabIndex = i === j ? 0 : -1;
    });
    code.textContent = snippets[i]!.code;
    panel.setAttribute("aria-labelledby", `${id}-tab-${i}`);
    if (focus) tabs[i]!.focus();
  };
  tabs.forEach((t, i) => {
    t.addEventListener("click", () => select(i, false));
    t.addEventListener("keydown", (e) => {
      const n = tabs.length;
      const next = { ArrowRight: (i + 1) % n, ArrowLeft: (i - 1 + n) % n, Home: 0, End: n - 1 }[e.key];
      if (next === undefined) return;
      e.preventDefault();
      select(next, true);
    });
  });
  select(0, false);
  return h("div", { class: "snippets" }, h("div", { class: "tabs", role: "tablist", "aria-label": label }, tabs), panel);
}

export function urlRow(d: Dataset): HTMLElement {
  const code = h("code", null, d.url);
  return h("div", { class: "url-row" }, code, copyButton(() => d.url, () => code));
}


export function licenseBlock(d: Dataset): HTMLElement {
  if (!d.licenses.length || d.licenses.every((l) => l.name === "notspecified")) {
    return h("p", { class: "license-missing" }, "No license recorded. Check the original source before reusing this data.");
  }
  return h("ul", { class: "plain-list" }, d.licenses.map((l) =>
    h("li", null, l.path ? h("a", { href: l.path, target: "_blank", rel: "noopener" }, l.title ?? l.name) : l.title ?? l.name,
      l.title && l.title !== l.name ? h("span", { class: "muted" }, ` (${l.name})`) : null)));
}

export function sourcesBlock(d: Dataset): HTMLElement {
  if (!d.sources.length) return h("p", { class: "muted" }, "No source recorded.");
  return h("ul", { class: "plain-list" }, d.sources.map((s) =>
    h("li", null, s.path ? h("a", { href: s.path, target: "_blank", rel: "noopener" }, s.title) : s.title)));
}

export function previewTable(d: Dataset): HTMLElement | null {
  if (!d.preview || !d.preview.rows.length) return null;
  return h("div", { class: "table-scroll", tabindex: 0, role: "region", "aria-label": `First rows of ${d.name}` },
    h("table", { class: "preview" },
      h("thead", null, h("tr", null, d.preview.columns.map((c) => h("th", { scope: "col" }, c)))),
      h("tbody", null, d.preview.rows.map((r) => h("tr", null, r.map((c) => h("td", null, c))))),
    ),
  );
}


