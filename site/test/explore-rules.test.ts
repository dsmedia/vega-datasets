// The Explore rules G-1 to G-7 (DOSSIER §6.1), each on a fixture that isolates it, with its
// metadata-first branch where it has one. The real datasets' choices are pinned in
// starters.test.ts ("starter chart choices", "explore choices").
import * as vega from 'vega';
import { expressionInterpreter } from 'vega-interpreter';
import { compile, type TopLevelSpec } from 'vega-lite';
import { describe, expect, test } from 'vitest';
import type { Dataset, Field } from '../src/lib/catalog';
import { idName, informative, namedAxes, nearDuplicate, scaleFor, scaleType, summable } from '../src/lib/chart-rules';
import { chartFeatures, defaultAxes, exploreModes, pickScale, scatterFields, scatterSpec, starterChart } from '../src/lib/explore-model';
import { basemapUrl, starterSpec } from '../src/lib/starter';
import { draw } from './draw';
import { described, strip } from './fixtures';

type Spec = Record<string, unknown>;
type Enc = Record<string, Record<string, unknown>>;

const bins = (first: number, rest = 0) => [first, ...Array.from({ length: 23 }, () => rest)];
const quant = (name: string, min: number, max: number, extra: Partial<Field> = {}, profile: Record<string, unknown> = {}): Field => ({
  name, type: 'number', description: null, profile: { kind: 'quantitative', min, max, mean: (min + max) / 2, missing: 0, bins: bins(1, 1), ...profile }, ...extra,
});
const nominal = (name: string, top: [string, number][], extra: Partial<Field> = {}): Field => ({
  name, type: 'string', description: null, profile: { kind: 'nominal', distinct: top.length, top, missing: 0 }, ...extra,
});
const dates = (name: string, distinct: number, evenlySpaced = true): Field => ({
  name, type: 'date', description: null,
  profile: { kind: 'temporal', min: '2000-01-01T00:00:00Z', max: '2009-12-01T00:00:00Z', missing: 0, distinct, ...(evenlySpaced ? { evenlySpaced: true as const } : {}) },
});
/** A bare table of these fields and rows (no dataset-level metadata). */
const table = (fields: Field[], rows: number, extra: Partial<Dataset> = {}): Dataset => ({ ...strip(described()), fields, rows, ...extra });
const enc = (spec: Spec | null) => ((spec?.spec ?? (spec?.layer as Spec[] | undefined)?.at(-1) ?? spec) as { encoding: Enc }).encoding;

/** Compile without warnings (the interpreter-safe settings the page uses). */
function compiles(spec: Spec): void {
  const warnings: string[] = [];
  const logger = { level: () => logger, error: (...m: unknown[]) => { throw new Error(m.join(' ')); }, warn: (...m: unknown[]) => { warnings.push(m.join(' ')); return logger; }, info: () => logger, debug: () => logger };
  const { spec: vg } = compile(spec as TopLevelSpec, { logger: logger as never });
  expect(warnings).toEqual([]);
  new vega.View(vega.parse(vg, undefined, { ast: true }), { renderer: 'none', expr: expressionInterpreter } as vega.ViewOptions).finalize();
}

describe('G-1: Over Time first when the time is the table’s axis', () => {
  const a = quant('high', 20, 40);
  const b = quant('open', 20, 40);
  const series = table([dates('date', 120), a, b], 120, { timeKeys: { date: [] }, correlated: [['high', 'open', 0.96]] });

  test('a sampled series, one row per time: the line, then the scatter', () => {
    expect(exploreModes(series)).toEqual(['time', 'scatter']);
  });

  test('event times (not evenly spaced, repeating) keep the scatter first', () => {
    const events = table([dates('date', 90, false), a, b], 120);
    expect(exploreModes(events)).toEqual(['scatter', 'time']);
  });

  test('a category outside the key keeps the scatter first, unless the pair shares the trend', () => {
    const weather = nominal('weather', [['rain', 50], ['sun', 40], ['fog', 30]]);
    const independent = table([dates('date', 120), a, b, weather], 120, { timeKeys: { date: [] } });
    expect(exploreModes(independent)).toEqual(['scatter', 'time']);
    expect(exploreModes({ ...independent, correlated: [['high', 'open', 0.95]] })).toEqual(['time', 'scatter']);
  });

  test('a panel of many entities stays on the scatter, unless its measure counts things', () => {
    const country = nominal('country', Array.from({ length: 6 }, (_, i) => [`c${i}`, 10] as [string, number]), {});
    (country.profile as { distinct: number }).distinct = 62;
    const panel = (m: Field) => table([dates('date', 10), country, m, quant('other', 1, 9)], 620, { timeKeys: { date: ['country'] } });
    expect(exploreModes(panel(quant('fertility', 1, 9)))[0]).toBe('scatter');
    expect(exploreModes(panel(quant('count', 1, 900)))[0]).toBe('time');
  });

  test('a line sums counts across groups and averages the rest', () => {
    const sex = nominal('sex', [['f', 30], ['m', 30]]);
    const age = quant('age', 0, 90, { type: 'integer' }, { distinct: 10 });
    const people = quant('people', 5, 900, { type: 'integer' });
    const d = table([{ ...dates('year', 3) }, age, sex, people], 60, { timeKeys: { year: ['age', 'sex'] } });
    expect(enc(starterSpec(d)).y).toMatchObject({ field: 'people', aggregate: 'sum' });
    // Metadata first: a description that makes it a rate ("per") is averaged, whatever the name.
    const rate = { ...people, description: 'Number of people per household.' };
    expect(enc(starterSpec({ ...d, fields: [d.fields[0]!, age, sex, rate] })).y).toMatchObject({ aggregate: 'mean' });
    expect(summable(quant('births', 0, 9, { description: 'Number of births.' }))).toBe(true);
  });
});

describe('G-2: log and symlog axes for heavy tails', () => {
  test('positive values over three decades: log; a pile-up near zero with zeros: symlog; neither: linear', () => {
    expect(scaleType(quant('penicillin', 0.001, 870))).toBe('log');
    expect(scaleType(quant('cost', 0, 7e6, {}, { minPositive: 35, bins: bins(99, 0) }))).toBe('symlog');
    expect(scaleType(quant('rain', 0, 118, {}, { minPositive: 0.3, bins: bins(84, 1) }))).toBe('linear');
    expect(scaleType(quant('distance', 30, 4983, {}, { bins: bins(13, 4) }))).toBe('linear');
    expect(scaleType(quant('income', 599, 132877, {}, { bins: bins(36, 3) }))).toBe('log');
  });

  test('metadata first: a documented minimum of zero rules out log (the axis must reach it)', () => {
    const documented = quant('penicillin', 0.001, 870, { constraints: { minimum: 0 } });
    expect(scaleType(documented)).toBe('symlog');
  });

  test('symlog ticks sit at powers of ten, each labeled on its own, and draw with the interpreter', async () => {
    const cost = quant('cost', 0, 7e6, {}, { minPositive: 35, bins: bins(99, 0) });
    const s = scaleFor(cost);
    expect(s.scale).toEqual({ type: 'symlog', constant: 35 });
    expect(s.axis.values).toContain(1e6);
    const view = await draw({ mark: 'point', encoding: { x: { field: 'cost', type: 'quantitative', scale: s.scale, axis: s.axis } } }, [{ cost: 0 }, { cost: 50 }, { cost: 7e6 }]);
    try {
      const svg = await view.toSVG();
      expect(svg).toContain('>1M<');
      expect(svg).not.toMatch(/>0\.\dM</);
    } finally {
      view.finalize();
    }
  });

  test('the scatter plot draws each picked measure on its scale, and says when a pick changes it', () => {
    const d = table([quant('penicillin', 0.001, 870), quant('mass', 10, 20)], 16);
    const f = scatterFields(d)!;
    const axes = defaultAxes(d, f);
    // The log axis goes on x when the other is linear.
    expect(axes).toEqual({ x: 'penicillin', y: 'mass' });
    const spec = scatterSpec(d, f, { ...axes, zoom: true, height: 300 });
    expect(JSON.stringify(spec)).toContain('"type":"log"');
    expect(JSON.stringify(spec)).not.toContain('"zero":false,"type":"log"');
    compiles(spec);
    expect(pickScale(f, 'penicillin')).toBe('log');
    expect(pickScale(f, 'mass')).toBe('linear');
  });
});

describe('G-3: never open on a near-duplicate pair', () => {
  const fields = [quant('open', 1, 9), quant('close', 1, 9), quant('volume', 1, 9)];

  test('the first pair that isn’t one line', () => {
    const d = table(fields, 50, { correlated: [['open', 'close', 0.99]] });
    expect(nearDuplicate(d, 'close', 'open')).toBe(true);
    expect(defaultAxes(d, scatterFields(d)!)).toEqual({ x: 'volume', y: 'open' });
  });

  test('no scatter plot when every pair is a near-duplicate', () => {
    const d = table(fields.slice(0, 2), 50, { correlated: [['open', 'close', -0.98]] });
    expect(scatterFields(d)).toBeNull();
  });
});

describe('G-4: series stay apart', () => {
  test('`source` is an identifier only beside a `target`', () => {
    const source = nominal('source', [['coal', 17], ['wind', 17], ['nuclear', 17]]);
    expect(idName(source, [source])).toBe(false);
    expect(idName(source, [source, nominal('target', [['a', 1]])])).toBe(true);
  });

  test('a line over years colors each source instead of zig-zagging through them', async () => {
    const year = quant('year', 2001, 2017, { type: 'integer' }, { distinct: 17, evenlySpaced: true });
    const source = nominal('source', [['coal', 17], ['wind', 17], ['nuclear', 17]]);
    const d = table([year, source, quant('net', 1000, 40000, { type: 'integer' })], 51, { timeKeys: { year: ['source'] } });
    const spec = starterSpec(d)!;
    expect(enc(spec).color).toMatchObject({ field: 'source' });
    expect(enc(spec).y).not.toHaveProperty('aggregate');
    const rows = [2001, 2002, 2003].flatMap((y) => ['coal', 'wind', 'nuclear'].map((s, i) => ({ year: y, source: s, net: 1000 * (i + 1) })));
    const view = await draw(spec, rows);
    try {
      expect((await view.toSVG()).match(/<path/g)!.length).toBeGreaterThanOrEqual(3);
    } finally {
      view.finalize();
    }
  });

  test('two year fields that index the rows: a line per vintage, never an average across them', () => {
    const budget = quant('budgetYear', 1980, 2010, { type: 'integer' }, { distinct: 31, evenlySpaced: true });
    const forecast = quant('forecastYear', 1980, 2020, { type: 'integer' }, { distinct: 41, evenlySpaced: true });
    const d = table([budget, forecast, quant('value', -1.8, 0.4)], 230, { timeKeys: { budgetYear: ['forecastYear'] } });
    const e = enc(starterSpec(d));
    expect(e.x).toMatchObject({ field: 'forecastYear' });
    expect(e.detail).toMatchObject({ field: 'budgetYear' });
    expect(e.y).not.toHaveProperty('aggregate');
  });

  test('a series value that totals the others is left out', () => {
    const entity = nominal('Entity', [['All natural disasters', 10], ['Drought', 10], ['Flood', 10]]);
    const d = table([quant('Year', 1900, 1909, { type: 'integer' }, { distinct: 10, evenlySpaced: true }), entity, quant('Deaths', 1, 3.7e6, { type: 'integer' })], 30, { timeKeys: { Year: ['Entity'] } });
    expect(JSON.stringify(starterSpec(d)!.transform)).toContain('All natural disasters');
  });
});

describe('G-5: color only by informative categories', () => {
  test('not a category that is nearly all one value, nor a helper field', () => {
    expect(informative(nominal('country', [['USA', 95], ['Guam', 5]]), 100)).toBe(false);
    expect(informative(nominal('side', [['left', 50], ['right', 50]]), 100)).toBe(false);
    expect(informative(nominal('origin', [['USA', 60], ['Japan', 40]]), 100)).toBe(true);
  });

  test('metadata first: a documented, ordered category is informative, even a helper’s name or a dominant value', () => {
    const ordered = { categories: ['low', 'high'], categoriesOrdered: true };
    expect(informative(nominal('side', [['low', 95], ['high', 5]], ordered), 100)).toBe(true);
    // Documented but unordered: the helper's name still wins.
    expect(informative(nominal('side', [['left', 50], ['right', 50]], { categories: ['left', 'right'] }), 100)).toBe(false);
  });
});

describe('G-6: maps', () => {
  const lat = quant('latitude', 13, 71);
  const lon = quant('longitude', -176, 145);
  const us = table([lat, lon], 3000, { points: { latitude: 'latitude', longitude: 'longitude', box: { longitude: [-163, -68], latitude: [20, 66] }, us: 0.99 } });

  test('US points (95% or more): Albers USA over the US outline, without the points elsewhere', () => {
    const spec = starterSpec(us)!;
    expect(spec.projection).toEqual({ type: 'albersUsa' });
    const [base, points] = spec.layer as Spec[];
    expect(base!.data).toEqual({ url: basemapUrl(us), format: { type: 'topojson', feature: 'countries' } });
    expect(basemapUrl(us)).toMatch(/\/data\/world-110m\.json$/);
    expect(JSON.stringify(base!.transform)).toContain('840');
    expect(JSON.stringify(points!.transform)).toContain('-66.5');
    compiles(spec);
  });

  test('elsewhere: a world projection fitted to the middle 98% of the points, over every country', () => {
    const eu = { ...us, points: { ...us.points!, box: { longitude: [-9.9, 9.9] as [number, number], latitude: [45, 60] as [number, number] }, us: 0 } };
    const spec = starterSpec(eu)!;
    expect((spec.projection as Spec).type).toBe('equalEarth');
    expect(JSON.stringify((spec.projection as Spec).fit)).toContain('-9.9');
    expect((spec.layer as Spec[])[0]!.transform).toBeUndefined();
    compiles(spec);
  });

  test('points close together get no basemap (its coarse coast would mislead)', () => {
    const city = { ...us, points: { ...us.points!, box: { longitude: [-118.5, -117.9] as [number, number], latitude: [33.8, 34.3] as [number, number] }, us: 1 } };
    expect(starterSpec(city)!.layer).toBeUndefined();
  });

  test('a direction turns each point into a wedge, colored by the other measure', () => {
    const d = table([lat, lon, quant('dir', 0, 360, { type: 'integer' }), quant('speed', 0, 12)], 4800);
    const e = enc(starterSpec(d));
    expect(e.angle).toMatchObject({ field: 'dir' });
    expect(e.color).toMatchObject({ field: 'speed', type: 'quantitative' });
  });

  test('metadata first: columns titled Latitude and Longitude make a map', () => {
    const d = table([quant('cx', -0.5, 0.3, { title: 'Longitude' }), quant('cy', 51.3, 51.7, { title: 'Latitude' })], 33);
    expect(starterSpec(d)!.projection).toBeDefined();
    expect(exploreModes(d)).toEqual(['starter']);
  });

  test('LineStrings are strokes, colored by their id, never filled shapes', () => {
    const tube: Dataset = { ...table([], 0), kind: 'json', format: 'topojson', rows: null, objects: ['line'], objectFeatures: { line: 394 }, objectGeometryTypes: { line: ['LineString'] } };
    const spec = starterSpec(tube)!;
    expect(spec.mark).toMatchObject({ type: 'geoshape', filled: false });
    expect(enc(spec).color).toMatchObject({ field: 'id' });
    const shapes = { ...tube, objectGeometryTypes: { line: ['Polygon'] } };
    expect(starterSpec(shapes)!.mark).not.toHaveProperty('filled');
  });
});

describe('G-7: obvious axes and small multiples', () => {
  test('fields named for their axes go on them, whatever their order', () => {
    const d = table([quant('Y', 3, 13), quant('X', 4, 19)], 44);
    expect(defaultAxes(d, scatterFields(d)!)).toEqual({ x: 'X', y: 'Y' });
    expect(namedAxes([quant('cy', 51, 52), quant('cx', -1, 1)])).toMatchObject({ x: { name: 'cx' }, y: { name: 'cy' } });
    expect(namedAxes([quant('max', 1, 2), quant('may', 1, 2)])).toBeNull();
  });

  const series = nominal('Series', [['I', 11], ['II', 11], ['III', 11], ['IV', 11]]);
  const quartet = table([series, quant('X', 4, 19), quant('Y', 3, 13)], 44);

  test('a small table of equal groups: small multiples first, two columns, smaller on a phone', async () => {
    expect(exploreModes(quartet)).toEqual(['panels', 'scatter']);
    const wide = starterChart(quartet)!;
    expect(wide.facet).toMatchObject({ field: 'Series' });
    expect(wide.columns).toBe(2);
    expect((starterChart(quartet, true)!.spec as Spec).width).toBeLessThan((wide.spec as Spec).width as number);
    expect(chartFeatures(wide)).toContain('facet');
    const rows = ['I', 'II', 'III', 'IV'].flatMap((s) => [4, 8, 12].map((x) => ({ Series: s, X: x, Y: x / 2 })));
    const view = await draw(wide, rows);
    try {
      expect((await view.toSVG()).match(/<path/g)!.length).toBeGreaterThanOrEqual(12);
    } finally {
      view.finalize();
    }
  });

  test('unequal groups (a real category) stay one colored scatter plot', () => {
    const unequal = table([nominal('Series', [['I', 30], ['II', 14]]), quant('X', 4, 19), quant('Y', 3, 13)], 44);
    expect(exploreModes(unequal)[0]).toBe('scatter');
  });
});
