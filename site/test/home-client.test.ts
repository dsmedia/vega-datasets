// @vitest-environment jsdom
// @vitest-environment-options {"url": "https://vega.github.io/vega-datasets/"}
// The home page's script (client/home.ts) run on the built page (site/dist/index.html, from
// `npm run site:build`): a list update keeps focus on the card, and a legacy #name set after
// load opens the dataset while About anchors open their item.
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
const scrollIntoView = vi.fn();

beforeAll(async () => {
  const html = readFileSync(path.join(REPO, 'site', 'dist', 'index.html'), 'utf8');
  document.body.innerHTML = html.slice(html.indexOf('<body>') + 6, html.lastIndexOf('</body>'));
  vi.stubGlobal('location', here);
  vi.stubGlobal('matchMedia', (query: string) => ({ matches: false, media: query, addEventListener() {}, removeEventListener() {} }));
  vi.stubGlobal('fetch', async () => new Response(JSON.stringify(homeIndex(catalog))));
  // jsdom has no layout/scrolling; the browser check covers the actual position.
  document.querySelectorAll('.about-item').forEach((item) => {
    Object.defineProperty(item, 'scrollIntoView', { value: scrollIntoView });
  });
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

test('an About anchor set after load opens its item and stays on the page', () => {
  here.hash = '#about-versioning';
  window.dispatchEvent(new HashChangeEvent('hashchange'));
  expect(here.replace).not.toHaveBeenCalled();
  expect(document.querySelector<HTMLDetailsElement>('#about-versioning')!.open).toBe(true);
  expect(document.activeElement).toBe(document.querySelector('#about-versioning summary'));
  expect(scrollIntoView).toHaveBeenCalledWith({ block: 'start', behavior: 'instant' });
});

test('following the same About link again reopens the item before native navigation', () => {
  const item = document.querySelector<HTMLDetailsElement>('#about-versioning')!;
  item.open = false;
  const link = document.querySelector<HTMLAnchorElement>('[data-open-details]')!;
  const click = new MouseEvent('click', { bubbles: true, cancelable: true });
  link.dispatchEvent(click);
  expect(item.open).toBe(true);
  expect(item.classList.contains('about-reveal')).toBe(false);
  expect(document.activeElement).toBe(item.querySelector('summary'));
  expect(click.defaultPrevented).toBe(false);
});

test('modified About links leave this page alone', () => {
  const item = document.querySelector<HTMLDetailsElement>('#about-versioning')!;
  item.open = false;
  const link = document.querySelector<HTMLAnchorElement>('[data-open-details]')!;
  link.dispatchEvent(new MouseEvent('click', { bubbles: true, ctrlKey: true }));
  expect(item.open).toBe(false);
});

test('malformed and unrelated fragments leave the disclosures alone', () => {
  const open = [...document.querySelectorAll<HTMLDetailsElement>('.about-item')].map((item) => item.open);
  for (const hash of ['#%E0%A4%A', '#does-not-exist', '#browse']) {
    here.hash = hash;
    window.dispatchEvent(new HashChangeEvent('hashchange'));
  }
  expect([...document.querySelectorAll<HTMLDetailsElement>('.about-item')].map((item) => item.open)).toEqual(open);
});

test('a legacy #name set after load opens that dataset\'s page', () => {
  here.hash = '#cars';
  window.dispatchEvent(new HashChangeEvent('hashchange'));
  expect(here.replace).toHaveBeenCalledTimes(1);
  expect(here.replace).toHaveBeenCalledWith('https://vega.github.io/vega-datasets/datasets/cars/');
});
