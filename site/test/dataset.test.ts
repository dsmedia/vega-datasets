// The dataset page: how it tells you to load a file, what it says about each field,
// and which live chart Explore draws — every scatter plot must compile and draw
// points from the real file, with pickers and axis titles that follow each other.
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { csvParse } from 'd3-dsv';
import LZString from 'lz-string';
import * as vega from 'vega';
import { compile, type TopLevelSpec } from 'vega-lite';
import { describe, expect, test } from 'vitest';
import { interleave, isReleased, linkText, useSnippets } from '../src/lib/dataset-model';
import {
  bothValuesNote,
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
} from '../src/lib/explore-model';
import { missingCount, profileSummary } from '../src/lib/profile';
import { editorUrl } from '../src/lib/starter';
import { loadCatalog, readDataUrl, REPO } from './catalog';

const catalog = loadCatalog();
const ds = (name: string) => catalog.dataset(name)!;


const snippetNames = (name: string) => useSnippets(ds(name)).map((s) => s.name);

/**
 * Vega-Lite 6.4.3 warns this for a fit autosize with a step-sized height even when the fit
 * is only "fit-x" (which `width: "container"` implies) and nothing is dropped: an upstream
 * bug (getTopLevelProperties). The starter bar charts with a category on y hit it. Any other
 * warning fails; the canary test below says when the allowance can go.
 */
const KNOWN_VL_WARNING = 'Dropping "fit-y" because spec has discrete height.';

/** A Vega-Lite logger that collects warnings (rather than printing them) and throws on errors. */
function collectingLogger() {
  const warnings: string[] = [];
  const logger = {
    level: () => logger,
    error: (...m: unknown[]) => { throw new Error(m.join(' ')); },
    warn: (...m: unknown[]) => { warnings.push(m.join(' ')); return logger; },
    info: () => logger,
    debug: () => logger,
  };
  return { logger: logger as never, warnings };
}

test('canary: Vega-Lite still warns "fit-y" for a container-width bar chart (when this fails, drop KNOWN_VL_WARNING)', () => {
  const { logger, warnings } = collectingLogger();
  compile({
    data: { values: [{ a: 'x', b: 1 }] },
    width: 'container',
    mark: 'bar',
    encoding: { y: { field: 'a', type: 'nominal' }, x: { field: 'b', type: 'quantitative' } },
  } as TopLevelSpec, { logger });
  expect(warnings).toEqual([KNOWN_VL_WARNING]);
});

describe('Use This Dataset snippets', () => {
  test('a released table gets URL, JavaScript, Vega-Lite and Python (Altair)', () => {
    const s = Object.fromEntries(useSnippets(ds('cars')).map((x) => [x.name, x.code]));
    expect(Object.keys(s)).toEqual(['URL', 'JavaScript', 'Vega-Lite', 'Python']);
    expect(s.URL).toBe(ds('cars').url);
    expect(s.JavaScript).toContain("const cars = await data['cars.json']();");
    expect(JSON.parse(`{${s['Vega-Lite']}}`)).toEqual({ data: { url: ds('cars').url } });
    expect(s.Python).toBe('from altair.datasets import data\n\ncars = data.cars()');
  });

  test('files that are not tables give their URL to JavaScript and Python', () => {
    const s = Object.fromEntries(useSnippets(ds('gimp')).map((x) => [x.name, x.code]));
    expect(Object.keys(s)).toEqual(['URL', 'JavaScript', 'Python']);
    expect(s.JavaScript).toContain("data['gimp.png'].url");
    expect(s.Python).toContain('url = data.gimp.url');
  });

  test('TopoJSON names its object for Vega-Lite', () => {
    const vl = useSnippets(ds('us_10m')).find((x) => x.name === 'Vega-Lite')!;
    expect(JSON.parse(`{${vl.code}}`).data.format).toEqual({ type: 'topojson', feature: ds('us_10m').objects![0] });
  });

  test('files not yet on npm skip the npm and Altair (Python) loaders', () => {
    const unreleased = catalog.datasets.filter((d) => !isReleased(d));
    expect(unreleased.length).toBeGreaterThan(0);
    for (const d of unreleased) expect(snippetNames(d.name)).not.toContain('JavaScript');
    for (const d of unreleased) expect(snippetNames(d.name)).not.toContain('Python');
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

  // build.test.ts checks that every built page's Download button links to this path.
  test('every file has a same-origin path, and the file is there', () => {
    const page = 'https://vega.github.io/vega-datasets/';
    for (const d of catalog.datasets) {
      expect(new URL(siteDataUrl(d), page).origin, d.name).toBe(new URL(page).origin);
      expect(existsSync(path.join(REPO, 'data', d.file)), d.name).toBe(true);
    }
  });

  test('the caption counts rows only once they are read to draw (it never loads a file itself)', () => {
    const cars = ds('cars');
    const rows = parseTable(readFileSync(path.join(REPO, 'data', cars.file), 'utf8'), cars.format);
    expect(bothValuesNote(cars, null, 'Horsepower', 'Miles_per_Gallon')).toBeNull();
    expect(bothValuesNote(cars, rows, 'Horsepower', 'Miles_per_Gallon')).toBe('Both fields have values in 392 of 406 rows.');
    expect(bothValuesNote(cars, rows, 'Displacement', 'Miles_per_Gallon')).toBe('Both fields have values in 398 of 406 rows.');
  });

  const scatters = catalog.datasets.filter((d) => exploreModes(d)[0] === 'scatter');
  test('scatter plots cover many datasets', () => expect(scatters.length).toBeGreaterThan(20));

  describe.each(scatters.map((d) => [d.name, d] as const))('%s scatter', (_name, d) => {
    test('compiles without warnings, draws points, and its titles follow the pickers', async () => {
      const f = scatterFields(d)!;
      const axes = defaultAxes(f);
      const { logger, warnings } = collectingLogger();
      const spec = { ...scatterSpec(d, f, { ...axes, zoom: true, height: 380 }), width: 600 };
      const { spec: vg } = compile(spec as TopLevelSpec, { logger });
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
    const { logger, warnings } = collectingLogger();
    const { spec: vg } = compile({ ...site, width: 600 } as TopLevelSpec, { logger });
    expect(warnings.filter((w) => w !== KNOWN_VL_WARNING)).toEqual([]);
    const view = new vega.View(vega.parse(vg), { renderer: 'none', loader });
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

/** The data of every item of the view's `type` marks (not legend or axis symbols). */
function markData(view: vega.View, type: string): Record<string, unknown>[] {
  type Node = { marktype?: string; role?: string; items?: (Node & { datum?: Record<string, unknown> })[] };
  const out: Record<string, unknown>[] = [];
  const visit = (mark: Node) => {
    if (mark.marktype === type && mark.role === 'mark') for (const i of mark.items ?? []) out.push(i.datum!);
    for (const item of mark.items ?? []) for (const child of item.items ?? []) visit(child);
  };
  visit((view.scenegraph() as unknown as { root: Node }).root);
  return out;
}

describe('every scatter plots each row at its own values', () => {
  const scatters = catalog.datasets.filter((d) => exploreModes(d)[0] === 'scatter');
  const num = (v: unknown) => (v === null || v === undefined || v === '' ? NaN : Number(v));
  const byXY = (a: number[], b: number[]) => a[0]! - b[0]! || a[1]! - b[1]!;

  test.each(scatters.map((d) => [d.name, d] as const))('%s', async (_name, d) => {
    const f = scatterFields(d)!;
    const text = readFileSync(path.join(REPO, 'data', d.file), 'utf8');
    const pristine = parseTable(text, d.format);
    // The default axes, and the same two fields swapped: a derived field must not overwrite a source field.
    const axes = defaultAxes(f);
    for (const { x, y } of [axes, { x: axes.y, y: axes.x }]) {
      const spec = scatterSpec(d, f, { x, y, zoom: false, height: 300 });
      const layer = (spec.layer as { encoding: { x: { field: string }; y: { field: string } } }[])[0]!;
      const view = new vega.View(vega.parse(compile({ ...withValues(spec, parseTable(text, d.format)), width: 600 } as TopLevelSpec).spec), { renderer: 'none' });
      try {
        await view.runAsync();
        const plotted = markData(view, 'symbol').map((r) => [r[layer.encoding.x.field] as number, r[layer.encoding.y.field] as number]).sort(byXY);
        const expected = pristine.map((r) => [num(r[x]), num(r[y])]).filter(([a, b]) => Number.isFinite(a) && Number.isFinite(b)).sort(byXY);
        expect(plotted.length, `${x} × ${y}`).toBe(expected.length);
        expect(plotted, `${x} × ${y}`).toEqual(expected);
      } finally {
        view.finalize();
      }
    }
  }, 120_000);
});

test('fields named x and y start on the x and y axes', () => {
  expect(defaultAxes(scatterFields(ds('platformer_terrain'))!)).toEqual({ x: 'x', y: 'y' });
  expect(defaultAxes(scatterFields(ds('anscombe'))!)).toEqual({ x: 'X', y: 'Y' });
  expect(defaultAxes(scatterFields(ds('cars'))!)).toEqual({ x: 'Displacement', y: 'Miles_per_Gallon' });
});

test('examples take the galleries in turn', () => {
  const order = interleave(catalog.examplesFor(ds('cars'))).slice(0, 6).map((e) => e.gallery);
  expect(order).toEqual(['vega-lite', 'vega', 'altair', 'vega-lite', 'vega', 'altair']);
});

test('link text is the file name, or the host', () => {
  expect(linkText('http://lib.stat.cmu.edu/datasets/cars.desc')).toBe('cars.desc');
  expect(linkText('http://lib.stat.cmu.edu/datasets/')).toBe('lib.stat.cmu.edu');
});
