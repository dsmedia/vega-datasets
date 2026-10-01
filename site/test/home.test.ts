// The home page's copy states numbers, and its cards and chart come from the
// catalog: check the numbers, the filters, the README sections it quotes, and
// that the catalog chart compiles and draws a point per dataset.
import { readFileSync } from 'node:fs';
import path from 'node:path';
import * as vega from 'vega';
import { compile, type TopLevelSpec } from 'vega-lite';
import { describe, expect, test } from 'vitest';
import { catalogSpec, toBrush } from '../src/lib/catalog-chart';
import { Catalog } from '../src/lib/catalog';
import { formatBytes } from '../src/lib/format';
import {
  baseMatches,
  chartRows,
  FORMAT_GROUPS,
  formatCounts,
  homeCounts,
  homeIndex,
  indexCatalog,
  listDatasets,
  NO_FILTERS,
  plainSummary,
  readmeSection,
  showcase,
} from '../src/lib/home-model';
import { loadCatalog, REPO } from './catalog';

const catalog = loadCatalog();
const counts = homeCounts(catalog);

describe('counts the copy states', () => {
  test('datasets, and examples with and without data', () => {
    expect(counts.datasets).toBe(catalog.datasets.length);
    expect(counts.examples).toBe(catalog.examples.length);
    expect(counts.examplesWithData).toBeLessThan(counts.examples);
    expect(counts.examplesWithData).toBe(new Set(catalog.datasets.flatMap((d) => d.usedBy)).size);
  });

  test('format groups cover every dataset', () => {
    expect(FORMAT_GROUPS.reduce((s, g) => s + counts.formats[g], 0)).toBe(counts.datasets);
  });

  test('each gallery counts the datasets it uses', () => {
    for (const g of ['vega', 'vega-lite', 'altair'] as const) {
      expect(counts.galleries[g]).toBe(catalog.datasets.filter((d) => catalog.usage(d)[g] > 0).length);
    }
  });
});

describe('the card list', () => {
  test('lists everything, most used first, ties A to Z', () => {
    const list = listDatasets(catalog, NO_FILTERS);
    expect(list).toHaveLength(counts.datasets);
    for (let i = 1; i < list.length; i++) {
      const [a, b] = [list[i - 1]!, list[i]!];
      expect(a.usedBy.length > b.usedBy.length || (a.usedBy.length === b.usedBy.length && a.name < b.name)).toBe(true);
    }
  });

  test('sorts A to Z and by size', () => {
    const az = listDatasets(catalog, { ...NO_FILTERS, sort: 'az' }).map((d) => d.name);
    expect(az).toEqual(catalog.datasets.map((d) => d.name));
    const sizes = listDatasets(catalog, { ...NO_FILTERS, sort: 'size' }).map((d) => d.bytes ?? 0);
    expect(sizes).toEqual([...sizes].sort((a, b) => b - a));
  });

  test('search matches names, field names and descriptions', () => {
    const q = (query: string) => listDatasets(catalog, { ...NO_FILTERS, query }).map((d) => d.name);
    expect(q('CARS')).toContain('cars');
    expect(q('Miles_per_Gallon')).toContain('cars');
    expect(q('penguin')).toContain('penguins');
    expect(q('no dataset has this')).toEqual([]);
  });

  test('chips in one group widen, groups narrow', () => {
    const topo = listDatasets(catalog, { ...NO_FILTERS, formats: new Set(['TopoJSON']) });
    expect(topo).toHaveLength(counts.formats.TopoJSON);
    const either = listDatasets(catalog, { ...NO_FILTERS, formats: new Set(['TopoJSON', 'CSV']) });
    expect(either).toHaveLength(counts.formats.TopoJSON + counts.formats.CSV);
    const vega = listDatasets(catalog, { ...NO_FILTERS, galleries: new Set(['vega']) });
    expect(vega).toHaveLength(counts.galleries.vega);
    const both = listDatasets(catalog, { ...NO_FILTERS, formats: new Set(['TopoJSON']), galleries: new Set(['vega']) });
    expect(both.every((d) => d.format === 'topojson' && catalog.usage(d).vega > 0)).toBe(true);
  });

  test('the chart brush keeps datasets inside it, in either drag direction', () => {
    const cars = catalog.dataset('cars')!;
    const around = { bytes: [cars.bytes! * 1.1, cars.bytes! * 0.9] as [number, number], examples: [60, 40] as [number, number] };
    const list = listDatasets(catalog, { ...NO_FILTERS, brush: around }).map((d) => d.name);
    expect(list).toContain('cars');
    expect(list.every((name) => catalog.dataset(name)!.usedBy.length >= 40)).toBe(true);
  });
});

test('the chart matches follow search and chips, never the brush', () => {
  const brush = { bytes: [1, 2] as [number, number], examples: [0, 0] as [number, number] };
  const topo = { ...NO_FILTERS, formats: new Set(['TopoJSON'] as const), brush };
  expect(listDatasets(catalog, topo)).toEqual([]);
  expect(baseMatches(catalog, topo)).toHaveLength(counts.formats.TopoJSON);
});

test('card summaries are the first paragraph as plain text', () => {
  expect(plainSummary('A [TopoJSON](https://x) map with `code` and **bold**.\n\nMore.')).toBe('A TopoJSON map with code and bold.');
  expect(plainSummary('Wrapped\nline.')).toBe('Wrapped line.');
});

test('the showcase mixes all three galleries without repeats', () => {
  const picks = showcase(catalog).map((p) => p.example);
  expect(picks).toHaveLength(10);
  expect(new Set(picks.map((e) => e.id)).size).toBe(10);
  expect(new Set(picks.map((e) => e.gallery)).size).toBe(3);
  expect(picks.every((e) => e.thumb && e.thumbSize && e.thumbSize.every((size) => size > 0))).toBe(true);
});

test('each showcase thumbnail stands for a different dataset that its example uses', () => {
  const picks = showcase(catalog);
  expect(new Set(picks.map((p) => p.dataset)).size).toBe(10);
  for (const { example, dataset } of picks) {
    const d = catalog.datasets.find((x) => x.name === dataset);
    expect(d, dataset).toBeTruthy();
    expect(catalog.examplesFor(d!).map((e) => e.id)).toContain(example.id);
  }
});

test('a missing featured thumbnail falls back to another example of the same dataset', () => {
  const first = showcase(catalog)[0]!;
  const degraded = new Catalog({ ...catalog, examples: catalog.examples.map((e) => e.id === first.example.id ? { ...e, thumb: null } : e) });
  const replacement = showcase(degraded).find((p) => p.dataset === first.dataset)!;
  expect(replacement.example.id).not.toBe(first.example.id);
  expect(replacement.example.thumb).toBeTruthy();
  expect(replacement.example.datasets).toContain(first.dataset);
});

test('unavailable datasets and examples never produce broken showcase tiles', () => {
  const first = showcase(catalog)[0]!;
  const missingDataset = new Catalog({ ...catalog, datasets: catalog.datasets.filter((d) => d.name !== first.dataset) });
  expect(showcase(missingDataset).some((p) => p.dataset === first.dataset)).toBe(false);
  const missingThumbnails = new Catalog({ ...catalog, examples: catalog.examples.map((e) => ({ ...e, thumb: null })) });
  expect(showcase(missingThumbnails)).toEqual([]);
});

test('About quotes README sections that exist', () => {
  for (const heading of ['Dataset Information', 'Versioning', 'Data Usage Note', 'Example Galleries']) {
    const body = readmeSection(catalog.readme, heading);
    expect(body, heading).toBeTruthy();
    expect(body).not.toMatch(/^## /m);
  }
  expect(readmeSection(catalog.readme, 'No such heading')).toBeNull();
});

test('links to README sections use anchors GitHub gives its headings', () => {
  // GitHub's slug: lowercase, punctuation other than - and _ dropped, spaces to hyphens.
  const slug = (heading: string) => heading.trim().toLowerCase().replace(/[^\w\- ]/g, '').replace(/ /g, '-');
  const readme = readFileSync(path.join(REPO, 'README.md'), 'utf8');
  const anchors = new Set([...readme.matchAll(/^#{1,6} (.+)$/gm)].map((m) => slug(m[1]!)));
  const source = readFileSync(path.join(REPO, 'site', 'src', 'pages', 'index.astro'), 'utf8');
  const used = [...source.matchAll(/`\$\{REPO\}#([^`]+)`/g)].map((m) => m[1]!);
  expect(used.length).toBeGreaterThanOrEqual(4);
  expect(used.filter((a) => !anchors.has(a))).toEqual([]);
});

describe('the catalog chart', () => {
  const rows = chartRows(catalog, formatBytes);
  const options = { brush: true, height: 240, labels: 9, legendTop: false, monoFont: 'monospace' };

  test('describes each point for screen readers without internals', () => {
    const values = (catalogSpec(rows, counts.formats, options).data as { values: { name: string; description: string }[] }).values;
    expect(values.find((v) => v.name === 'cars')!.description).toMatch(/^cars: JSON, \d+ KB, \d+ gallery examples$/);
  });

  test('has a row per dataset, linking to its page', () => {
    expect(rows).toHaveLength(counts.datasets);
    expect(rows.every((r) => r.href === `datasets/${encodeURIComponent(r.name)}/` && r.bytes > 0)).toBe(true);
  });

  test.each([
    ['wide, with brush', options],
    ['phone, tap only', { ...options, brush: false, height: 214, labels: 5, legendTop: true }],
    ['narrowest phone', { ...options, brush: false, height: 214, labels: 5, legendTop: true, legendColumns: 2 }],
  ])('%s: compiles without warnings and draws every point', async (_name, o) => {
    const warnings: string[] = [];
    const logger = {
      level: () => logger,
      error: (...m: unknown[]) => { throw new Error(m.join(' ')); },
      warn: (...m: unknown[]) => { warnings.push(m.join(' ')); return logger; },
      info: () => logger,
      debug: () => logger,
    };
    const spec = { ...catalogSpec(rows, counts.formats, o), width: 800 };
    const { spec: vgSpec } = compile(spec as TopLevelSpec, { logger: logger as never });
    expect(warnings).toEqual([]);
    const view = new vega.View(vega.parse(vgSpec), { renderer: 'none' });
    try {
      await view.runAsync();
      const svg = await view.toSVG();
      expect(svg.match(/<path [^>]*class="[^"]*"|<path\b/g)?.length ?? 0).toBeGreaterThan(rows.length);
      expect(svg).not.toMatch(/NaN|undefined/);
      for (const g of FORMAT_GROUPS) expect(svg).toContain(`${g} ${counts.formats[g]}`);
      expect(svg).toContain('10 MB');
      const labels = [...svg.matchAll(/<text[^>]*font-family="monospace"[^>]*>([^<]+)<\/text>/g)].map((m) => m[1]);
      expect(labels).toHaveLength(o.labels);
      expect(labels).toContain('cars');
    } finally {
      view.finalize();
    }
  });

  test('filters fade the points they exclude, and the axes stay put', async () => {
    const view = new vega.View(vega.parse(compile({ ...catalogSpec(rows, counts.formats, options), width: 800 } as TopLevelSpec).spec), { renderer: 'none' });
    try {
      await view.runAsync();
      const ticks = (svg: string) => [...svg.matchAll(/<text[^>]*>([\d.,]+(?: [KM]?B)?)<\/text>/g)].map((m) => m[1]).join('|');
      const faded = (svg: string) => (svg.match(/<path[^>]*opacity="0\.1"/g) ?? []).length;
      const before = await view.toSVG();
      expect(faded(before)).toBe(0);
      await view.signal('matched', ['cars', 'movies']).runAsync();
      const after = await view.toSVG();
      expect(faded(after)).toBe(rows.length - 2);
      expect(ticks(after)).toBe(ticks(before));
      await view.signal('matched', null).runAsync();
      expect(faded(await view.toSVG())).toBe(0);
    } finally {
      view.finalize();
    }
  });

  test('reads the brush signal, and treats a cleared brush as none', () => {
    expect(toBrush({ bytes: [1, 2], examples: [3, 4] })).toEqual({ bytes: [1, 2], examples: [3, 4] });
    expect(toBrush({})).toBeNull();
    expect(toBrush(null)).toBeNull();
  });
});

test('the home page index (home-index.json) lists, counts and charts like the full catalog', () => {
  const index = indexCatalog(JSON.parse(JSON.stringify(homeIndex(catalog))));
  const names = (c: typeof catalog, f: Parameters<typeof listDatasets>[1]) => listDatasets(c, f).map((d) => d.name);
  for (const f of [
    NO_FILTERS,
    { ...NO_FILTERS, query: 'weather' },
    { ...NO_FILTERS, query: 'Miles_per_Gallon' },
    { ...NO_FILTERS, formats: new Set(['CSV', 'TopoJSON'] as const), sort: 'size' as const },
    { ...NO_FILTERS, galleries: new Set(['altair'] as const), sort: 'az' as const },
  ]) {
    expect(names(index, f)).toEqual(names(catalog, f));
  }
  expect(formatCounts(index)).toEqual(counts.formats);
  expect(chartRows(index, formatBytes)).toEqual(chartRows(catalog, formatBytes));
  for (const d of catalog.datasets) expect(index.usage(index.dataset(d.name)!)).toEqual(catalog.usage(d));
});

test('legacy #name links name a dataset; About anchors and unknown names do not', async () => {
  const { legacyDataset } = await import('../src/lib/home-model');
  const names = new Set(['cars', 'us_10m', 'weather']);
  expect(legacyDataset('#cars', names)).toBe('cars');
  expect(legacyDataset('#us_10m', names)).toBe('us_10m');
  expect(legacyDataset('#about-versioning', names)).toBeNull();
  expect(legacyDataset('#browse', names)).toBeNull();
  expect(legacyDataset('', names)).toBeNull();
  expect(legacyDataset('#', names)).toBeNull();
  expect(legacyDataset('#%E0', names)).toBeNull();
  expect(legacyDataset('#c%61rs', names)).toBe('cars');
});
