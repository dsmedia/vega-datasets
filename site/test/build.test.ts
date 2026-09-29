// The built site (site/dist, from `npm run site:build`): a page per dataset with its
// content, head and JSON-LD in the HTML; no inline scripts (the CSP allows none);
// every link within the site leading to a page or a file; and the sitemap.
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import { describe, expect, test } from 'vitest';
import { loadCatalog, REPO } from './catalog';

const catalog = loadCatalog();
const dist = path.join(REPO, 'site', 'dist');
const BASE = '/vega-datasets/';

function pages(dir: string): string[] {
  return readdirSync(dir).flatMap((f) => {
    const p = path.join(dir, f);
    return statSync(p).isDirectory() ? pages(p) : p.endsWith('.html') ? [p] : [];
  });
}
const html = (file: string) => readFileSync(file, 'utf8');
const all = pages(dist);

/** Does a same-site URL path lead to a built page or asset, or to a file in the repository (data/…)? */
function resolves(urlPath: string): boolean {
  const rel = decodeURIComponent(urlPath.slice(BASE.length));
  const candidates = [path.join(dist, rel), path.join(dist, rel, 'index.html'), path.join(REPO, rel)];
  return urlPath.startsWith(BASE) && candidates.some((c) => existsSync(c) && statSync(c).isFile());
}

test('a page per dataset, the home page and a 404 page', () => {
  expect(existsSync(path.join(dist, 'index.html'))).toBe(true);
  expect(existsSync(path.join(dist, '404.html'))).toBe(true);
  for (const d of catalog.datasets) expect(existsSync(path.join(dist, 'datasets', d.name, 'index.html')), d.name).toBe(true);
});

describe.each(all.map((f) => [path.relative(dist, f).replace(/\\/g, '/'), f] as const))('%s', (_name, file) => {
  const text = html(file);

  test('one h1, a canonical URL, a title and a description', () => {
    expect(text.match(/<h1[\s>]/g)).toHaveLength(1);
    expect(text).toMatch(/<link rel="canonical" href="https:\/\/vega\.github\.io\/vega-datasets\/[^"]*">/);
    expect(text).toMatch(/<title>[^<]{10,}<\/title>/);
    expect(text).toMatch(/<meta name="description" content="[^"]{20,}">/);
  });

  test('scripts are files (the CSP allows no inline code); data blocks parse', () => {
    for (const m of text.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/g)) {
      const [, attrs = '', body = ''] = m;
      const type = attrs.match(/type="([^"]+)"/)?.[1];
      if (type === 'application/ld+json' || type === 'application/json') expect(() => JSON.parse(body)).not.toThrow();
      else expect(attrs, 'external script').toMatch(/\bsrc="/);
    }
  });

  test('theme-init.js runs before the stylesheet', () => {
    const init = text.indexOf('theme-init.js');
    expect(init).toBeGreaterThan(-1);
    expect(init).toBeLessThan(text.indexOf('rel="stylesheet"'));
  });

  test('every link within the site leads somewhere', () => {
    const hrefs = [...text.matchAll(/(?:href|src|xlink:href)="([^"#?]*)[^"]*"/g)].map((m) => m[1]!).filter((h) => h !== '');
    const dir = path.relative(dist, path.dirname(file)).replace(/\\/g, '/');
    const here = `${BASE}${dir ? `${dir}/` : ''}`;
    const broken = hrefs
      .filter((h) => !/^(https?:|mailto:|data:)/.test(h))
      .map((h) => (h.startsWith('/') ? h : new URL(h, `https://example.org${here}`).pathname))
      .filter((p) => !resolves(p));
    expect([...new Set(broken)]).toEqual([]);
  });
});

test('dataset pages carry Dataset and BreadcrumbList JSON-LD', () => {
  for (const d of catalog.datasets) {
    const text = html(path.join(dist, 'datasets', d.name, 'index.html'));
    const types = [...text.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/g)].map((m) => JSON.parse(m[1]!)['@type']);
    expect(types, d.name).toEqual(['Dataset', 'BreadcrumbList']);
  }
});

test('the sitemap lists every page', () => {
  const xml = readFileSync(path.join(dist, 'sitemap.xml'), 'utf8');
  expect(xml.match(/<loc>/g)).toHaveLength(catalog.datasets.length + 1);
  expect(xml).toContain('<loc>https://vega.github.io/vega-datasets/datasets/cars/</loc>');
});

test('the home page has every dataset card, and the chart drawn', () => {
  const text = html(path.join(dist, 'index.html'));
  expect(text.match(/<a class="card"/g)).toHaveLength(catalog.datasets.length);
  expect(text.match(/class="chart-static /g)).toHaveLength(2);
  expect(text).toContain('<a tabindex="-1" xlink:href="datasets/cars/"');
});

test('long tables carry their density bins: every row binned or counted outside, in under 30 KB, with no data file to load', () => {
  const d = catalog.dataset('flights_200k_json')!;
  const text = html(path.join(dist, 'datasets', d.name, 'index.html'));
  const json = text.match(/<script type="application\/json" id="density-data">([\s\S]*?)<\/script>/)![1]!;
  const g = JSON.parse(json) as { rows: number; complete: number; outside: number; cells: [number, number, number][] };
  expect(g.cells.reduce((s, [, , n]) => s + n, 0) + g.outside + (g.rows - g.complete)).toBe(d.rows);
  expect(json.length).toBeLessThan(30_000);
  expect(text).toContain('data-draw-all>Draw All 200,000 Points (9.9 MB)</button>');
});

test('mid-size tables wait for a button, which stands aside on desktop-class devices only up to 20,000 rows', () => {
  const button = (name: string) => html(path.join(dist, 'datasets', name, 'index.html')).match(/<button class="btn draw"[^>]*>[^<]*<\/button>/)?.[0] ?? null;
  expect(button('flights_20k')).toBe('<button class="btn draw" type="button" data-auto-draw="desktop">Draw 20,000 Points (1.8 MB)</button>');
  expect(button('flights_5k')).toBeNull();
  expect(button('cars')).toBeNull();
});

test('heavy maps carry a picture of the map', () => {
  for (const name of ['airports', 'earthquakes', 'us_10m', 'windvectors', 'zipcodes']) {
    const text = html(path.join(dist, 'datasets', name, 'index.html'));
    expect(text, name).toContain(`src="/vega-datasets/previews/${name}.webp"`);
    expect(statSync(path.join(dist, 'previews', `${name}.webp`)).size, name).toBeGreaterThan(1000);
  }
});

test('copy: both Editor buttons read "Open This Chart in the Vega Editor", headings and buttons in Title Case', () => {
  const gapminder = html(path.join(dist, 'datasets', 'gapminder', 'index.html'));
  expect(gapminder.match(/>Open This Chart in the Vega Editor<\/a>/g)).toHaveLength(2);
  for (const file of all) {
    expect(html(file), file).not.toMatch(/Edit This Chart|No region<|Neighbouring|All datasets<|Python source<|view on GitHub<|Contribution guidelines</);
  }
});

test('Download saves the site\'s own copy of the file (browsers ignore `download` across origins)', () => {
  for (const d of catalog.datasets) {
    const text = html(path.join(dist, 'datasets', d.name, 'index.html'));
    const links = [...text.matchAll(/<a class="btn" href="([^"]+)" download="([^"]+)">(Download [^<]+)<\/a>/g)];
    expect(links, d.name).toHaveLength(1);
    const [, href, download, label] = links[0]!;
    // As the browser resolves it from the page: same origin, the repository's data/ path.
    const page = `https://vega.github.io${BASE}datasets/${d.name}/`;
    const url = new URL(href!, page);
    expect(url.origin, d.name).toBe(new URL(page).origin);
    expect(url.pathname, d.name).toBe(`${BASE}data/${d.file}`);
    expect(resolves(url.pathname), d.name).toBe(true);
    const file = d.file.split('/').pop();
    expect(download, d.name).toBe(file);
    expect(label, d.name).toBe(`Download ${file}`);
  }
});

test('each field histogram is one tab stop, its bins none', () => {
  const text = html(path.join(dist, 'datasets', 'seattle_weather', 'index.html'));
  const wraps = [...text.matchAll(/<span class="spark-wrap"([^>]*)>([\s\S]*?)<\/span>/g)];
  expect(wraps.length).toBeGreaterThan(3);
  for (const [, attrs, body] of wraps) {
    expect(attrs).toMatch(/tabindex="0"/);
    expect(body).not.toMatch(/tabindex/);
    expect(body).not.toMatch(/\b1 rows\b/);
  }
});
