// The home page is README.md: its in-site links must land on real dataset pages.
import { expect, test } from 'vitest';
import { loadCatalog } from './catalog';

test('README links to datasets point at existing plates', () => {
  const catalog = loadCatalog();
  const targets = [...catalog.readme.matchAll(/\]\(#([^)]*)\)/g)].map((m) => m[1]!);
  expect(targets.length).toBeGreaterThan(0);
  expect(targets.filter((t) => t !== '' && !catalog.dataset(t))).toEqual([]);
});

test('README badges and title are left to GitHub', () => {
  const { readme } = loadCatalog();
  expect(readme).not.toMatch(/^# |\[!\[|\[!IMPORTANT\]/m);
});
