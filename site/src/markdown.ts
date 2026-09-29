/**
 * Render Markdown written in vega-datasets' own files (dataset descriptions, README).
 * Raw HTML in the source is escaped rather than passed through. Links to other
 * sites open in a new tab; `#dataset` links stay on the page.
 */
import { Marked } from "marked";

/**
 * Write a link's destination or title (as marked hands it over: backslash escapes removed,
 * entity references not yet decoded) into a double-quoted attribute. A quote, `<` or `>` is
 * escaped, so it can't end the attribute; so is an `&` that doesn't start a reference.
 * A reference (`&amp;`, `&quot;`, `&#58;`) passes through for the browser to decode, once,
 * which is what CommonMark asks of a destination or title and needs no entity table here.
 */
function attr(value: string): string {
  return value.replace(/&(?![a-z][a-z\d]*;|#\d{1,7};|#x[\da-f]{1,6};)|[<>"]/gi, (c) => `&#${c.charCodeAt(0)};`);
}

/**
 * Whether a destination may keep its target: http(s) and mailto only. The check reads the
 * destination before references are decoded, so it passes only a scheme spelled out in plain
 * characters; decoding can't change those, and a scheme hidden behind references
 * (`&#106;avascript:`, `javascript&colon;`) never passes.
 */
function allowed(href: string): boolean {
  return /^(https?:|mailto:)/i.test(href);
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
        const safe = allowed(href) ? href : "#";
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
