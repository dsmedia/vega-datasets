// @vitest-environment jsdom
// @vitest-environment-options {"url": "https://vega.github.io/vega-datasets/#cars"}
// The dataset page as it renders into the document: what the rail's Download button links to.
import { afterEach, beforeAll, expect, test, vi } from 'vitest';
import { renderDataset, stopDataset } from '../src/dataset';
import { loadCatalog } from './catalog';

const catalog = loadCatalog();

beforeAll(() => {
  // jsdom has neither; the page only reads them.
  vi.stubGlobal('matchMedia', (query: string) => ({
    matches: false, media: query, addEventListener() {}, removeEventListener() {},
  }));
  vi.stubGlobal('IntersectionObserver', class { observe() {} unobserve() {} disconnect() {} });
});

afterEach(() => {
  stopDataset();
  document.body.replaceChildren();
});

/** Render `name`'s page and return its Download button. */
function downloadButton(name: string): HTMLAnchorElement {
  const page = document.createElement('main');
  document.body.append(page);
  renderDataset(catalog, catalog.dataset(name)!, page);
  const buttons = [...page.querySelectorAll<HTMLAnchorElement>('.ds-rail a')].filter((a) => a.textContent!.startsWith('Download'));
  expect(buttons, name).toHaveLength(1);
  return buttons[0]!;
}

// Browsers ignore `download` on another origin (the file's jsDelivr URL) and open the file
// instead, so the button must link to the site's own copy.
test("the Download button saves the site's own copy of the file", () => {
  for (const name of ['cars', 'flights_200k_json', 'us_10m', 'icon_7zip', 'seattle_weather']) {
    const d = catalog.dataset(name)!;
    const a = downloadButton(name);
    const url = new URL(a.href);
    expect(url.origin, name).toBe(location.origin);
    expect(url.pathname, name).toBe(`/vega-datasets/data/${d.file}`);
    expect(a.getAttribute('download'), name).toBe(d.file);
    expect(a.textContent, name).toBe(`Download ${d.file}`);
  }
});
