/** UI pieces of a dataset plate. */
import { type Dataset, type Example, type Gallery, GALLERIES, GALLERY_LABEL, githubSource } from "./catalog";
import { h, svg } from "./dom";
import { formatCount, FORMAT_LABEL, plural } from "./format";
import { starterEditorUrl } from "./starter";
import { attachTip } from "./tooltip";

/** Gallery identity: a colored dot always paired with its name (color is never the only cue). */
export function galleryTag(g: Gallery, count?: number): HTMLElement {
  return h("span", { class: `gtag g-${g}` },
    h("span", { class: "gdot", "aria-hidden": "true" }),
    GALLERY_LABEL[g],
    count === undefined ? null : h("span", { class: "gcount" }, formatCount(count)),
  );
}


/**
 * A thin stacked bar of example counts per gallery, scaled to `max` so rows compare.
 * Segments are separated by a 2px surface gap; the total is labeled at the end.
 */
export function usageBar(counts: Record<Gallery, number>, max: number, width = 120): HTMLElement {
  const total = GALLERIES.reduce((s, g) => s + counts[g], 0);
  const height = 8;
  const root = svg("svg", { width, height, viewBox: `0 0 ${width} ${height}`, class: "usage", "aria-hidden": "true" });
  root.append(svg("rect", { x: 0, y: 0, width, height, rx: 4, class: "usage-track" }));
  let x = 0;
  const scale = max > 0 ? width / max : 0;
  const segs = GALLERIES.filter((g) => counts[g] > 0);
  segs.forEach((g, i) => {
    const w = Math.max(2, counts[g] * scale - (i < segs.length - 1 ? 2 : 0));
    const first = i === 0;
    const last = i === segs.length - 1;
    const r = Math.min(4, w / 2);
    // Round only the outer ends of the stack.
    const d = `M${x + (first ? r : 0)},0H${x + w - (last ? r : 0)}${last ? `Q${x + w},0 ${x + w},${r}V${height - r}Q${x + w},${height} ${x + w - r},${height}` : `V${height}`}H${x + (first ? r : 0)}${first ? `Q${x},${height} ${x},${height - r}V${r}Q${x},0 ${x + r},0` : `V0`}Z`;
    root.append(svg("path", { d, class: `usage-seg g-${g}` }));
    x += w + 2;
  });
  const wrap = h("span", { class: "usage-wrap", tabindex: total ? 0 : null },
    root,
    h("span", { class: "usage-total" }, total ? formatCount(total) : "–"),
  );
  if (total) {
    attachTip(wrap, [
      plural(total, "gallery example"),
      ...GALLERIES.filter((g) => counts[g]).map((g) => `${GALLERY_LABEL[g]}: ${counts[g]}`),
    ]);
  } else {
    wrap.setAttribute("aria-label", "Not used by any gallery example");
  }
  return wrap;
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

export function copyUrlButton(d: Dataset): HTMLButtonElement {
  const url = d.url;
  const btn = h("button", { class: "btn btn-quiet copy", type: "button" }, "Copy URL");
  btn.addEventListener("click", () => {
    navigator.clipboard.writeText(url).then(
      () => {
        btn.textContent = "Copied";
        setTimeout(() => (btn.textContent = "Copy URL"), 1600);
      },
      () => {
        const sel = window.getSelection();
        const code = btn.previousElementSibling;
        if (sel && code) {
          const range = document.createRange();
          range.selectNodeContents(code);
          sel.removeAllRanges();
          sel.addRange(range);
        }
        btn.textContent = "Press Ctrl/⌘+C";
      },
    );
  });
  return btn;
}

export function urlRow(d: Dataset): HTMLElement {
  return h("div", { class: "url-row" }, h("code", null, d.url), copyUrlButton(d));
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


