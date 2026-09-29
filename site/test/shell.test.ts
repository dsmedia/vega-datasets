// The page shell (site/static/index.html) and stylesheet: light by default like
// the other Vega sites, a saved dark choice applied before the stylesheet loads,
// and every color, font and size drawn from the theme tokens.
import { readdirSync, readFileSync } from 'node:fs';
import { expect, test } from 'vitest';
import { pageShell } from '../scripts/page-shell.mjs';

const read = (path: string) => readFileSync(new URL(path, import.meta.url), 'utf8');
const html = read('../static/index.html');
const css = read('../static/site.css');

test('the page starts in light mode', () => {
  expect(html).toMatch(/<html [^>]*data-theme="light"/);
});

test('the saved theme is applied before the stylesheet loads', () => {
  const init = html.indexOf('assets/theme-init.js');
  expect(init).toBeGreaterThan(-1);
  expect(init).toBeLessThan(html.indexOf('assets/site.css'));
});

test('no link to the Jekyll-rendered datapackage.html (its tables do not render)', () => {
  expect(html).not.toContain('href="datapackage.html"');
});

test('no web fonts: system stacks only, and the CSP blocks font loads', () => {
  expect(html).not.toContain('fonts.css');
  expect(html).toContain("font-src 'none'");
  expect(css).not.toMatch(/@font-face|@import/);
});

test('the header marks Datasets as the current section and has the theme switch', () => {
  expect(html).toMatch(/<a [^>]*aria-current="page"[^>]*>Datasets<\/a>/);
  expect(html).toMatch(/<button [^>]*id="theme-toggle"[^>]*aria-pressed="false"/);
});

test('search and link previews: canonical URL, Open Graph and Twitter card, theme color', () => {
  const meta = (attr: string, key: string) => html.match(new RegExp(`<meta ${attr}="${key}" content="([^"]+)">`))?.[1];
  expect(html).toContain('<link rel="canonical" href="https://vega.github.io/vega-datasets/">');
  expect(meta('property', 'og:url')).toBe('https://vega.github.io/vega-datasets/');
  expect(meta('property', 'og:type')).toBe('website');
  expect(meta('property', 'og:site_name')).toBe('Vega Datasets');
  // Link previews say what the page says.
  expect(meta('property', 'og:title')).toBe(html.match(/<title>([^<]+)<\/title>/)![1]);
  expect(meta('property', 'og:description')).toBe(meta('name', 'description'));
  expect(meta('name', 'twitter:card')).toBe('summary');
  expect(meta('name', 'theme-color')).toBe(css.match(/--header: (#[0-9a-f]{6})/)![1]);
});

test("a fork's build (SITE_NOINDEX=1) keeps out of search results; the canonical build doesn't", () => {
  expect(pageShell(html)).toBe(html);
  expect(html).not.toContain('noindex');
  const fork = pageShell(html, { noindex: true });
  expect(fork).toContain('<meta name="robots" content="noindex">');
  expect(fork.indexOf('noindex')).toBeLessThan(fork.indexOf('</head>'));
});

test('the footer cannot shift: the empty page holds the fold until the script fills it', () => {
  // A footer painted under an empty <main> jumps down when the page renders (CLS 0.135).
  expect(css).toMatch(/@media \(scripting: enabled\) \{\s*#page:empty \{ min-height: 100vh; \}/);
  // Without scripting the note sits above the footer, not under it.
  expect(html.indexOf('<noscript>')).toBeGreaterThan(html.indexOf('<main id="page"'));
  expect(html.indexOf('<noscript>')).toBeLessThan(html.indexOf('<footer'));
});

test('the stylesheet parses: every block closes, none closes twice', () => {
  // A stray brace makes browsers drop the rule after it (a whole @media block, say).
  let depth = 0;
  const stray: number[] = [];
  // Blank out comments but keep their line breaks, so the reported line numbers are the file's.
  css.replace(/\/\*[\s\S]*?\*\//g, (c) => c.replace(/[^\n]/g, '')).split('\n').forEach((line, i) => {
    for (const ch of line) {
      if (ch === '{') depth++;
      if (ch === '}' && --depth < 0) {
        stray.push(i + 1);
        depth = 0;
      }
    }
  });
  expect(stray).toEqual([]);
  expect(depth).toBe(0);
});

/** Custom properties declared in a stretch of CSS. */
function declared(block: string): Set<string> {
  return new Set([...block.matchAll(/(--[\w-]+)\s*:/g)].map((m) => m[1]!));
}

/** The body of the first top-level rule with exactly this selector. */
function ruleBody(selector: string): string {
  const start = css.indexOf(`\n${selector} {`);
  expect(start, `${selector} rule`).toBeGreaterThan(-1);
  return css.slice(start, css.indexOf('\n}', start));
}

test('every token the stylesheet and scripts use is defined', () => {
  const defined = declared(css);
  const used = new Set([...css.matchAll(/var\((--[\w-]+)/g)].map((m) => m[1]!));
  const src = new URL('../src/', import.meta.url);
  for (const file of readdirSync(src)) {
    const text = readFileSync(new URL(file, src), 'utf8');
    for (const m of text.matchAll(/token\("(--[\w-]+)"\)/g)) used.add(m[1]!);
  }
  expect(used.size).toBeGreaterThan(20);
  expect([...used].filter((name) => !defined.has(name))).toEqual([]);
});

test('dark mode only overrides tokens that light mode defines', () => {
  const light = declared(ruleBody(':root'));
  const dark = declared(ruleBody(':root[data-theme="dark"]'));
  expect(dark.size).toBeGreaterThan(20);
  expect([...dark].filter((name) => !light.has(name))).toEqual([]);
});
