// The page shell (site/static/index.html) and stylesheet: light by default like
// the other Vega sites, a saved dark choice applied before the stylesheet loads,
// and every color, font and size drawn from the theme tokens.
import { readdirSync, readFileSync } from 'node:fs';
import { expect, test } from 'vitest';

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
