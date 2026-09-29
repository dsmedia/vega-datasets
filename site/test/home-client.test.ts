// @vitest-environment jsdom
// @vitest-environment-options {"url": "https://vega.github.io/vega-datasets/"}
// The home page's script (client/home.ts) run on the built page (site/dist/index.html, from
// `npm run site:build`): a list update keeps focus on the card.
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { afterAll, beforeAll, expect, test, vi } from 'vitest';
import { homeIndex } from '../src/lib/home-model';
import { loadCatalog, REPO } from './catalog';

const catalog = loadCatalog();
// jsdom can't navigate, and its location can't be spied on: the script gets this one instead.
const here = {
  hash: '',
  search: '',
  pathname: '/vega-datasets/',
  href: 'https://vega.github.io/vega-datasets/',
  replace: vi.fn(),
};
const order = () => [...document.querySelectorAll<HTMLElement>('.cards a.card')].map((a) => a.dataset.name);
const card = (name: string) => document.querySelector<HTMLAnchorElement>(`.cards a.card[data-name="${name}"]`)!;
const settle = () => new Promise((r) => setTimeout(r, 300));

beforeAll(async () => {
  const html = readFileSync(path.join(REPO, 'site', 'dist', 'index.html'), 'utf8');
  document.body.innerHTML = html.slice(html.indexOf('<body>') + 6, html.lastIndexOf('</body>'));
  vi.stubGlobal('location', here);
  vi.stubGlobal('matchMedia', (query: string) => ({ matches: false, media: query, addEventListener() {}, removeEventListener() {} }));
  vi.stubGlobal('fetch', async () => new Response(JSON.stringify(homeIndex(catalog))));
  await import('../src/client/home');
  await settle();
});

afterAll(() => vi.unstubAllGlobals());

test('a card that has to move keeps focus when the list is re-sorted', async () => {
  // Every card shown, so the focused one stays visible in either order.
  document.querySelector<HTMLButtonElement>('.browse-foot .more')!.click();
  await settle();
  const moved = card('airports');
  const before = order();
  moved.focus();
  expect(document.activeElement).toBe(moved);
  const removed: Node[] = [];
  const observer = new MutationObserver((records) => records.forEach((r) => removed.push(...r.removedNodes)));
  observer.observe(document.querySelector('.cards')!, { childList: true });
  const sort = document.querySelector<HTMLSelectElement>('#home-sort')!;
  sort.value = 'az';
  sort.dispatchEvent(new Event('change'));
  await settle();
  removed.push(...observer.takeRecords().flatMap((r) => [...r.removedNodes]));
  observer.disconnect();
  const after = order();
  expect(after).toEqual([...after].sort((x, y) => x!.localeCompare(y!)));
  // airports goes from further down (by use) to first (A to Z): it is detached and put back,
  // which blurs it, so its focus has to be restored.
  expect(before.indexOf('airports')).toBeGreaterThan(0);
  expect(after.indexOf('airports')).toBe(0);
  expect(removed).toContain(moved);
  expect(moved.hidden).toBe(false);
  expect(document.activeElement).toBe(moved);
});
