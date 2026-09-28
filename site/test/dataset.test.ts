// The dataset page: how it tells you to load a file, what it says about each field,
// and which live chart Explore draws — every scatter plot must compile and draw
// points from the real file, with pickers and axis titles that follow each other.
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { csvParse } from 'd3-dsv';
import LZString from 'lz-string';
import * as vega from 'vega';
import { compile, type TopLevelSpec } from 'vega-lite';
import { describe, expect, test } from 'vitest';
import { interleave, isReleased, linkText, useSnippets } from '../src/dataset-model';
import {
  chartFeatures,
  defaultAxes,
  exploreModes,
  parseTable,
  readsRows,
  scatterFields,
  scatterSpec,
  siteDataUrl,
  starterChart,
  withDataUrl,
  withValues,
} from '../src/explore-model';
import { missingCount, profileSummary } from '../src/profile';
import { editorUrl } from '../src/starter';
import { loadCatalog, readDataUrl, REPO } from './catalog';

const catalog = loadCatalog();
const ds = (name: string) => catalog.dataset(name)!;
const snippetNames = (name: string) => useSnippets(ds(name)).map((s) => s.name);

describe('Use This Dataset snippets', () => {
  test('a released table gets URL, JS, Vega-Lite and Altair', () => {
    const s = Object.fromEntries(useSnippets(ds('cars')).map((x) => [x.name, x.code]));
    expect(Object.keys(s)).toEqual(['URL', 'JS', 'Vega-Lite', 'Altair']);
    expect(s.URL).toBe(ds('cars').url);
    expect(s.JS).toContain("const cars = await data['cars.json']();");
    expect(JSON.parse(`{${s['Vega-Lite']}}`)).toEqual({ data: { url: ds('cars').url } });
    expect(s.Altair).toBe('from altair.datasets import data\n\ncars = data.cars()');
  });

  test('files that are not tables give their URL to JS and Altair', () => {
    const s = Object.fromEntries(useSnippets(ds('gimp')).map((x) => [x.name, x.code]));
    expect(Object.keys(s)).toEqual(['URL', 'JS', 'Altair']);
    expect(s.JS).toContain("data['gimp.png'].url");
    expect(s.Altair).toContain('url = data.gimp.url');
  });

  test('TopoJSON names its object for Vega-Lite', () => {
    const vl = useSnippets(ds('us_10m')).find((x) => x.name === 'Vega-Lite')!;
    expect(JSON.parse(`{${vl.code}}`).data.format).toEqual({ type: 'topojson', feature: ds('us_10m').objects![0] });
  });

  test('files not yet on npm skip the npm and Altair loaders', () => {
    const unreleased = catalog.datasets.filter((d) => !isReleased(d));
    expect(unreleased.length).toBeGreaterThan(0);
    for (const d of unreleased) expect(snippetNames(d.name)).not.toContain('JS');
    for (const d of unreleased) expect(snippetNames(d.name)).not.toContain('Altair');
  });
});

describe('field summaries', () => {
  const cars = ds('cars');
  const field = (name: string) => cars.fields.find((f) => f.name === name)!;

  test('numbers: range and mean; dates stored as January 1: years', () => {
    expect(profileSummary(field('Cylinders'))).toBe('3 – 8 · mean 5.48');
    expect(profileSummary(field('Year'))).toBe('1970 – 1982');
  });

  test('categories: the three most common values', () => {
    expect(profileSummary(field('Origin'))).toBe('USA 254 · Japan 79 · Europe 73');
  });

  test('missing values as a count and a share of rows', () => {
    expect(missingCount(field('Miles_per_Gallon'), cars.rows)).toEqual({ text: '8 · 2.0%', any: true });
    expect(missingCount(field('Name'), cars.rows)).toEqual({ text: '0', any: false });
  });
});

describe('Explore', () => {
  test('offers a scatter plot and the time series for cars, the map for maps, nothing for images', () => {
    expect(exploreModes(ds('cars'))).toEqual(['scatter', 'time']);
    expect(exploreModes(ds('us_10m'))).toEqual(['starter']);
    expect(exploreModes(ds('windvectors'))).toEqual(['starter']);
    expect(exploreModes(ds('gimp'))).toEqual([]);
    expect(exploreModes(ds('flights_3m'))).toEqual([]);
  });

  test('names the features each chart uses', () => {
    const cars = ds('cars');
    const f = scatterFields(cars)!;
    expect(chartFeatures(scatterSpec(cars, f, { ...defaultAxes(f), zoom: true, height: 380 })))
      .toEqual(['Vega-Lite', 'input binding', 'scale binding', 'legend binding']);
    expect(chartFeatures(scatterSpec(cars, f, { ...defaultAxes(f), zoom: false, height: 300 })))
      .toEqual(['Vega-Lite', 'input binding', 'legend binding']);
    expect(chartFeatures(starterChart(ds('us_10m'))!)).toContain('albersUsa projection');
    expect(chartFeatures(starterChart(cars)!)).toContain('line mark');
  });

  test('the Editor link opens the chart as shown: the chosen fields and the public data URL', () => {
    const cars = ds('cars');
    const f = scatterFields(cars)!;
    const url = editorUrl(scatterSpec(cars, f, { x: 'Horsepower', y: 'Miles_per_Gallon', zoom: true, height: 380 }));
    const spec = JSON.parse(LZString.decompressFromEncodedURIComponent(url.split('#/url/vega-lite/')[1]!)!);
    expect(spec.data).toEqual({ url: cars.url });
    expect(spec.params.map((p: { name: string; value: string }) => `${p.name}=${p.value}`)).toEqual(['xField=Horsepower', 'yField=Miles_per_Gallon']);
  });

  test('the page loads data from its own origin; the Editor keeps the public URL', () => {
    const spec = starterChart(ds('cars'))!;
    expect((spec.data as { url: string }).url).toBe(ds('cars').url);
    expect((withDataUrl(spec, siteDataUrl(ds('cars'))).data as { url: string }).url).toBe('data/cars.json');
  });

  const scatters = catalog.datasets.filter((d) => exploreModes(d)[0] === 'scatter');
  test('scatter plots cover many datasets', () => expect(scatters.length).toBeGreaterThan(20));

  describe.each(scatters.map((d) => [d.name, d] as const))('%s scatter', (_name, d) => {
    test('compiles without warnings, draws points, and its titles follow the pickers', async () => {
      const f = scatterFields(d)!;
      const axes = defaultAxes(f);
      const warnings: string[] = [];
      const logger = {
        level: () => logger,
        error: (...m: unknown[]) => { throw new Error(m.join(' ')); },
        warn: (...m: unknown[]) => { warnings.push(m.join(' ')); return logger; },
        info: () => logger,
        debug: () => logger,
      };
      const spec = { ...scatterSpec(d, f, { ...axes, zoom: true, height: 380 }), width: 600 };
      const { spec: vg } = compile(spec as TopLevelSpec, { logger: logger as never });
      expect(warnings).toEqual([]);
      // Zoom clips the view's marks; the axis titles (text marks outside the plot) must opt out.
      const clipped = JSON.stringify(vg).match(/"type":"text"[^{}]*"clip":true/g);
      expect(clipped).toBeNull();
      const columns = new Set(d.fields.map((x) => x.name));
      for (const m of f.measures) expect(columns.has(m.name)).toBe(true);

      const loader = vega.loader();
      loader.load = async (uri: string) => readDataUrl(uri);
      const view = new vega.View(vega.parse(vg), { renderer: 'none', loader });
      try {
        await view.runAsync();
        const svg = await view.toSVG();
        expect(svg.match(/<path\b/g)?.length ?? 0).toBeGreaterThan(5);
        expect(svg).not.toMatch(/NaN|undefined/);
        expect(svg).toContain(`>${axes.x}<`);
        expect(svg).toContain(`>${axes.y}<`);
        const other = f.measures[2] ?? f.measures[0]!;
        await view.signal('xField', other.name).runAsync();
        expect(await view.toSVG()).toContain(`>${other.name}<`);
      } finally {
        view.finalize();
      }
    }, 60_000); // flights_200k_json draws 200,000 points.
  });
});

/** Run `fn` as under the page's CSP (script-src 'self', no 'unsafe-eval'): compiling code throws. */
function withoutEval<T>(fn: () => T): T {
  const original = globalThis.Function;
  globalThis.Function = new Proxy(original, {
    apply() { throw new EvalError('CSP: unsafe-eval'); },
    construct() { throw new EvalError('CSP: unsafe-eval'); },
  });
  try {
    return fn();
  } finally {
    globalThis.Function = original;
  }
}

describe('reading tables under the page CSP', () => {
  const tables = catalog.datasets.filter((d) => readsRows(d) && d.format !== 'json');
  const text = (d: (typeof tables)[number]) => readFileSync(path.join(REPO, 'data', d.file), 'utf8');

  test('the harness catches code compilation (d3 csvParse, which Vega uses, fails)', () => {
    expect(() => withoutEval(() => csvParse(text(ds('seattle_weather'))))).toThrow(EvalError);
  });

  test.each(tables.map((d) => [d.name, d] as const))('%s parses without compiling code', (_name, d) => {
    const rows = withoutEval(() => parseTable(text(d), d.format));
    expect(rows).toHaveLength(d.rows!);
    expect(Object.keys(rows[0]!)).toEqual(d.fields.map((f) => f.name));
  });
});

describe('every Explore chart draws from the rows the page reads', () => {
  const charts = catalog.datasets.flatMap((d) => exploreModes(d).map((mode) => [`${d.name} ${mode}`, d, mode] as const));

  test.each(charts)('%s', async (_name, d, mode) => {
    const f = scatterFields(d);
    const spec = mode === 'scatter' ? scatterSpec(d, f!, { ...defaultAxes(f!), zoom: true, height: 380 }) : starterChart(d)!;
    const site = readsRows(d)
      ? withValues(spec, parseTable(readFileSync(path.join(REPO, 'data', d.file), 'utf8'), d.format))
      : spec;
    const loader = vega.loader();
    loader.load = async (uri: string) => readDataUrl(uri);
    const view = new vega.View(vega.parse(compile({ ...site, width: 600 } as TopLevelSpec).spec), { renderer: 'none', loader });
    try {
      await view.runAsync();
      const svg = await view.toSVG();
      // At least 3, as in starters.test.ts: world_110m draws its land as a single shape.
      expect(svg.match(/<(path|line|rect)\b/g)?.length ?? 0).toBeGreaterThanOrEqual(3);
      expect(svg).not.toMatch(/NaN|undefined/);
    } finally {
      view.finalize();
    }
  }, 60_000);
});

test('examples take the galleries in turn', () => {
  const order = interleave(catalog.examplesFor(ds('cars'))).slice(0, 6).map((e) => e.gallery);
  expect(order).toEqual(['vega-lite', 'vega', 'altair', 'vega-lite', 'vega', 'altair']);
});

test('link text is the file name, or the host', () => {
  expect(linkText('http://lib.stat.cmu.edu/datasets/cars.desc')).toBe('cars.desc');
  expect(linkText('http://lib.stat.cmu.edu/datasets/')).toBe('lib.stat.cmu.edu');
});
