// The page shell (site/static/index.html): light by default like the other Vega
// sites, with a saved dark choice applied before the stylesheet loads.
import { readFileSync } from 'node:fs';
import { expect, test } from 'vitest';

const html = readFileSync(new URL('../static/index.html', import.meta.url), 'utf8');

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
