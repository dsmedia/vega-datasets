/**
 * Render Markdown written in vega-datasets' own files (dataset descriptions, README).
 * Raw HTML in the source is escaped rather than passed through. Links to other
 * sites open in a new tab; `#dataset` links stay on the page.
 */
import { Marked } from "marked";

/** Escape text for a double-quoted attribute: a quote in a link would otherwise end it. */
function attr(value: string): string {
  return value.replace(/[&<>"]/g, (c) => `&#${c.charCodeAt(0)};`);
}

/** `headingShift` demotes headings so they nest under the section that holds them. */
function renderer(headingShift: number): Marked {
  return new Marked({
    gfm: true,
    breaks: false,
    renderer: {
      html({ text }) {
        return text.replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);
      },
      link({ href, title, tokens }) {
        const text = this.parser.parseInline(tokens);
        const t = title ? ` title="${attr(title)}"` : "";
        if (href.startsWith("#")) return `<a href="${attr(href)}"${t}>${text}</a>`;
        const safe = /^(https?:|mailto:)/i.test(href) ? href : "#";
        return `<a href="${attr(safe)}"${t} target="_blank" rel="noopener">${text}</a>`;
      },
      heading({ tokens, depth }) {
        const level = Math.min(6, depth + headingShift);
        return `<h${level}>${this.parser.parseInline(tokens)}</h${level}>`;
      },
    },
  });
}

const nested = renderer(2);
const topLevel = renderer(0);

/** HTML for `source`. `sections: true` keeps `##` as h2, for a page made of the Markdown itself. */
export function markdownToHtml(source: string, opts: { sections?: boolean } = {}): string {
  return (opts.sections ? topLevel : nested).parse(source.trim(), { async: false });
}

/** Render into `el` (see markdownToHtml). */
export function renderMarkdown(el: HTMLElement, source: string, opts: { sections?: boolean } = {}): void {
  el.innerHTML = markdownToHtml(source, opts);
}
