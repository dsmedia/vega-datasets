// The Explore rules G-1 to G-7 (DOSSIER §6.1), each on a fixture that isolates it, with its
// metadata-first branch where it has one. The real datasets' choices are pinned in
// starters.test.ts ("starter chart choices", "explore choices").
import * as vega from 'vega';
import { expressionInterpreter } from 'vega-interpreter';
import { compile, type TopLevelSpec } from 'vega-lite';
import { describe, expect, test } from 'vitest';
import type { Dataset, Field } from '../src/lib/catalog';
import { idName, informative, logTicks, nameTokens, namedAxes, nearDuplicate, scaleFor, scaleType, summable } from '../src/lib/chart-rules';
import { chartConfig, tokenInk } from '../src/lib/vega-theme';
import { chartFeatures, defaultAxes, discreteHeight, exploreModes, mapNote, pickScale, scatterFields, scatterSpec, starterChart } from '../src/lib/explore-model';
import * as largeData from '../src/lib/large-data';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { basemapUrl, starterSpec } from '../src/lib/starter';
import { draw, rowsWith } from './draw';
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

  test('a series value that totals the others is its own line: no row is removed (round 4)', () => {
    const entity = nominal('Entity', [['All natural disasters', 10], ['Drought', 10], ['Flood', 10]]);
    const d = table([quant('Year', 1900, 1909, { type: 'integer' }, { distinct: 10, evenlySpaced: true }), entity, quant('Deaths', 1, 3.7e6, { type: 'integer' })], 30, { timeKeys: { Year: ['Entity'] }, totalValues: { Entity: { Deaths: ['All natural disasters'] } } });
    const spec = starterSpec(d)!;
    expect(spec.transform).toBeUndefined();
    expect(enc(spec).color).toMatchObject({ field: 'Entity' });
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

describe('readability (orchestrator review)', () => {
  test('log axes: 1-2-5 steps over up to three decades, powers of ten beyond, a domain fitted to the data', async () => {
    expect(logTicks(5.97, 800)).toEqual([5, 10, 20, 50, 100, 200, 500, 1000]);
    const wide = logTicks(0.001, 870);
    expect(wide.every((v) => Number.isInteger(Math.log10(v)) || v === 0.001)).toBe(true);
    expect(wide[0]).toBe(0.001);
    expect(wide.at(-1)).toBe(1000);
    const svg = async (min: number, max: number) => {
      const s = scaleFor(quant('v', min, max));
      const view = await draw({ mark: 'point', encoding: { x: { field: 'v', type: 'quantitative', scale: s.scale, axis: s.axis } } }, [{ v: min }, { v: max }]);
      try {
        return await view.toSVG();
      } finally {
        view.finalize();
      }
    };
    // Below one, plain decimals, never a milli prefix ("1m").
    const small = await svg(0.001, 870);
    expect(small).toContain('>0.001<');
    expect(small).not.toMatch(/>\d+m</);
    const s = scaleFor(quant('price', 5.97, 800, {}, { bins: bins(20, 1) }));
    expect(s.scale).toEqual({ type: 'log', domainMin: 5, domainMax: 1000, nice: false });
    expect(s.axis.values).toEqual([5, 10, 20, 50, 100, 200, 500, 1000]);
  });

  test('a time axis ties its tick count to the plot width and drops overlapping labels', async () => {
    const d = table([dates('date', 120), quant('co2', 313, 420)], 120, { timeKeys: { date: [] } });
    const x = enc(starterChart(d)).x;
    expect(x.axis).toEqual({ tickCount: { expr: 'ceil(width / 90)' }, labelOverlap: 'greedy', format: '%Y' });
    for (const width of [880, 358]) {
      const rows = Array.from({ length: 120 }, (_, i) => ({ date: `${1960 + Math.floor(i / 2)}-0${1 + (i % 2) * 6}-01`, co2: 313 + i }));
      const view = await draw({ ...starterSpec(d)!, width }, rows);
      try {
        const labels = (await view.toSVG()).match(/role-axis-label[\s\S]*?<\/g>/)![0].match(/<text/g)!.length;
        // No crowding: at least 70 px of axis per label.
        expect(labels).toBeLessThanOrEqual(Math.floor(width / 70));
      } finally {
        view.finalize();
      }
    }
  });

  test('a measure colored over the basemap starts its ramp at mid luminance', () => {
    const d = table([quant('latitude', 45, 60), quant('longitude', -10, 10), quant('dir', 0, 360, { type: 'integer' }), quant('speed', 0, 12)], 4800);
    const range = (enc(starterSpec(d)).color!.scale as { range: string[] }).range;
    expect(range).toHaveLength(2);
    for (const c of range) {
      const [r, g, b] = [1, 3, 5].map((i) => parseInt(c.slice(i, i + 2), 16) / 255);
      const lum = 0.2126 * r! + 0.7152 * g! + 0.0722 * b!;
      expect(lum).toBeGreaterThan(0.25);
      expect(lum).toBeLessThan(0.6);
    }
  });

  test('small multiples label their panels at 12 px in the ink color', () => {
    const header = chartConfig(tokenInk(() => '#123456'), 'sans-serif').header as Record<string, unknown>;
    expect(header).toMatchObject({ labelFontSize: 12, titleFontSize: 12, labelColor: '#123456' });
  });
});

// Codex round 1: each finding reproduced as a failing test first.
describe('Codex round 1', () => {
  const regions = (n: number) => Array.from({ length: n }, (_, i) => [`r${String(i).padStart(2, '0')}`, 3] as [string, number]);
  const year = quant('year', 2000, 2002, { type: 'integer' }, { distinct: 3, evenlySpaced: true });

  test('#2 a sum is not bounded by the range documented for its rows', () => {
    const count = quant('count', 80, 80, { constraints: { minimum: 0, maximum: 100 } });
    const d = table([year, nominal('region', regions(13)), count, quant('other', 1, 9)], 39, { timeKeys: { year: ['region'] }, totalValues: {} });
    const y = enc(starterSpec(d)).y;
    expect(y).toMatchObject({ aggregate: 'sum' });
    expect(y.scale ?? {}).not.toHaveProperty('domainMax');
  });

  test('#3 a rate named for what it counts is not summed', () => {
    expect(nameTokens('deaths_per_100k')).toEqual(['deaths', 'per', '100', 'k']);
    expect(nameTokens('avgDeaths')).toEqual(['avg', 'Deaths']);
    expect(summable(quant('deaths_per_100k', 0, 100))).toBe(false);
    expect(summable(quant('pct_total', 0, 1))).toBe(false);
    expect(summable(quant('deaths', 0, 100))).toBe(true);
  });

  test('#4 times merged into one bucket are averaged, never summed', () => {
    const days: Field = { name: 'date', type: 'date', description: null, profile: { kind: 'temporal', min: '2019-01-01T00:00:00Z', max: '2021-12-31T00:00:00Z', missing: 0, distinct: 1096, evenlySpaced: true } };
    const d = table([days, quant('population', 100, 100)], 1096, { timeKeys: { date: [] } });
    expect(enc(starterSpec(d)).y).toMatchObject({ aggregate: 'mean' });
  });

  test('#5 a total among groups it can’t color: averaged, never summed with its parts (round 4: nothing removed)', async () => {
    const region = { ...nominal('region', regions(6)), profile: { kind: 'nominal' as const, distinct: 13, top: regions(6), missing: 0 } };
    const d = table([year, region, quant('count', 10, 120)], 39, { timeKeys: { year: ['region'] }, totalValues: { region: { count: ['Total'] } } });
    const spec = starterSpec(d)!;
    expect(enc(spec).y).toMatchObject({ aggregate: 'mean' });
    expect(spec.transform).toBeUndefined();
    const rows = [2000, 2001, 2002].flatMap((y) => [...regions(12).map(([r]) => ({ year: y, region: r, count: 10 })), { year: y, region: 'Total', count: 120 }]);
    const view = await draw(spec, rows);
    try {
      // The mean of twelve tens and one 120 (every row counted once).
      expect(new Set(rowsWith(view, 'mean_count').map((r) => Math.round((r.mean_count as number) * 1000) / 1000))).toEqual(new Set([Math.round((240 / 13) * 1000) / 1000]));
    } finally {
      view.finalize();
    }
  });

  test('#6 the top twenty leave out documented missing values before summing', async () => {
    const many = { ...nominal('origin', regions(6)), profile: { kind: 'nominal' as const, distinct: 61, top: regions(6), missing: 0 } };
    // The profile leaves the markers out, as the builder does.
    const count = quant('count', 1, 10, { missingValues: ['-99'] });
    const d = table([many, nominal('destination', regions(6)), count], 122, { totalValues: {} });
    (d.fields[1]!.profile as { distinct: number }).distinct = 61;
    const spec = starterSpec(d)!;
    const rows = Array.from({ length: 61 }, (_, i) => [{ origin: `o${i}`, count: 10 }, { origin: `o${i}`, count: -99 }]).flat();
    const view = await draw(spec, rows);
    try {
      const totals = rowsWith(view, 'total').map((r) => r.total);
      expect(totals.length).toBeGreaterThanOrEqual(20);
      expect(new Set(totals)).toEqual(new Set([10]));
    } finally {
      view.finalize();
    }
  });

  test('#8 a table past the points bands opens on the scatter overview, not Over Time', () => {
    const d = table([dates('date', 200000), quant('a', 1, 9), quant('b', 1, 9)], 200000, { timeKeys: { date: [] }, correlated: [['a', 'b', 0.95]] });
    expect(exploreModes(d)[0]).toBe('scatter');
  });

  test('#10 documented bounds set a log axis’s domain', () => {
    const f = quant('v', 0.001, 870, { constraints: { minimum: 0.0001, maximum: 10000 } });
    expect(scaleFor(f).scale).toMatchObject({ type: 'log', domainMin: 0.0001, domainMax: 10000 });
  });

  test('#11 a map that leaves out points outside the 50 states says how many', () => {
    const d = table([quant('latitude', 13, 71), quant('longitude', -176, 145)], 3376, {
      points: { latitude: 'latitude', longitude: 'longitude', box: { longitude: [-164, -69], latitude: [20, 66] }, us: 0.97, outsideUs: 28 },
    });
    expect(mapNote(d)).toBe('The map leaves out 28 of 3,376 rows, outside the 50 states: the Albers USA projection has no place for them.');
    expect(mapNote({ ...d, points: { ...d.points!, outsideUs: 0 } })).toBeNull();
  });

  test('#14 small multiples follow the documented order and labels', () => {
    const levels = ['low', 'medium', 'high'];
    const severity = nominal('severity', [['high', 4], ['low', 4], ['medium', 4]], {
      categories: levels.map((value) => ({ value, label: `${value[0]!.toUpperCase()}${value.slice(1)} severity` })), categoriesOrdered: true,
    });
    const facet = starterSpec(table([severity, quant('X', 1, 9), quant('Y', 1, 9)], 12))!.facet as Record<string, unknown>;
    expect(facet.sort).toEqual(levels);
    expect(JSON.stringify(facet.header)).toContain('Low severity');
  });
});

// Codex round 2: each test on Codex's input, shown failing on cd70a5b before the fix.
describe('Codex round 2', () => {
  const regions = (n: number, prefix = 'r') => Array.from({ length: n }, (_, i) => [`${prefix}${String(i).padStart(2, '0')}`, 3] as [string, number]);
  const year = quant('year', 2000, 2002, { type: 'integer' }, { distinct: 3, evenlySpaced: true });
  const many = (name: string, distinct: number, top: [string, number][]) => ({ ...nominal(name, top), profile: { kind: 'nominal' as const, distinct, top, missing: 0 } });

  test('#1 rate names with digits or a percent sign are never summed', () => {
    expect(nameTokens('deaths_per100k')).toEqual(['deaths', 'per', '100', 'k']);
    expect(summable(quant('deaths_per100k', 0, 100))).toBe(false);
    expect(summable(quant('deaths_%', 0, 100))).toBe(false);
    expect(summable(quant('deathsPer1000', 0, 100))).toBe(false);
    const d = table([year, many('country', 13, regions(6)), quant('deaths_per100k', 100, 100)], 39, { timeKeys: { year: ['country'] }, totalValues: {} });
    expect(enc(starterSpec(d)).y).toMatchObject({ aggregate: 'mean' });
  });

  test('#2 a bucket where an entity repeats is averaged, not summed', () => {
    const days: Field = { name: 'date', type: 'date', description: null, profile: { kind: 'temporal', min: '2019-01-02T00:00:00Z', max: '2021-01-03T00:00:00Z', missing: 0, distinct: 3 } };
    const region = many('region', 400, regions(6));
    const rows = 1200;
    // The builder records the buckets in which (bucket, key) still names one row: none here (Jan 2019 holds two dates).
    const d = table([days, region, quant('population', 100, 100)], rows, { timeKeys: { date: ['region'] }, timeKeyBuckets: { date: [] }, totalValues: {} });
    // 1,200 rows keep the time unit on: the monthly bucket merges the 2nd and 3rd of January.
    expect(enc(starterSpec(d)).y).toMatchObject({ aggregate: 'mean' });
    // Where every bucket holds one row per region, the sum stands.
    const once = { ...d, timeKeyBuckets: { date: ['yearmonthdate', 'yearmonth', 'year'] } };
    expect(enc(starterSpec(once)).y).toMatchObject({ aggregate: 'sum' });
  });

  test('#3 a total among more than sixty groups is never summed with its parts', async () => {
    const region = many('region', 61, regions(6, 'Region'));
    const d = table([year, region, quant('count', 10, 600)], 183, { timeKeys: { year: ['region'] }, totalValues: { region: { count: ['Total'] } } });
    const rows = [2000, 2001, 2002].flatMap((y) => [...Array.from({ length: 60 }, (_, i) => ({ year: y, region: `Region${String(i).padStart(2, '0')}`, count: 10 })), { year: y, region: 'Total', count: 600 }]);
    const view = await draw(starterSpec(d)!, rows);
    try {
      expect(rowsWith(view, 'sum_count')).toEqual([]);
      expect(new Set(rowsWith(view, 'mean_count').map((r) => Math.round(r.mean_count as number)))).toEqual(new Set([Math.round(1200 / 61)]));
    } finally {
      view.finalize();
    }
  });

  test('#4 a column named field (or groupby) still gets its missing-value filter', async () => {
    const origin = many('origin', 61, regions(6));
    const field = quant('field', 1, 10, { title: 'Count', missingValues: ['-99'] });
    const d = table([origin, many('destination', 61, regions(6)), field], 122, { totalValues: {} });
    const rows = Array.from({ length: 61 }, (_, i) => [{ origin: `o${i}`, field: 10 }, { origin: `o${i}`, field: -99 }]).flat();
    // The name alone must make it summable here: title Count.
    const view = await draw(starterSpec(d)!, rows);
    try {
      expect(new Set(rowsWith(view, 'total').map((r) => r.total))).toEqual(new Set([10]));
    } finally {
      view.finalize();
    }
  });

  test('#6 documented log bounds are the domain, exactly', () => {
    const f = quant('v', 0.01, 800, { constraints: { minimum: 0.002, maximum: 900 } });
    const s = scaleFor(f);
    expect(s.scale).toMatchObject({ type: 'log', domainMin: 0.002, domainMax: 900 });
    for (const v of s.axis.values as number[]) expect(v >= 0.002 && v <= 900).toBe(true);
  });

  test('#7 points either side of 180° draw side by side', async () => {
    const d = table([quant('latitude', -17, -17), quant('longitude', -179, 179)], 100, {
      points: { latitude: 'latitude', longitude: 'longitude', box: { longitude: [179, 181], latitude: [-17, -17] }, us: 0, outsideUs: 100 },
    });
    const spec = starterChart(d)!;
    // A third group 7° north of one: drawn on the right geography, the 2° across 180 is
    // shorter than those 7°; drawn across the whole world, the two sides are the width apart.
    const rows = [
      ...Array.from({ length: 50 }, () => ({ latitude: -17, longitude: 179 })),
      ...Array.from({ length: 50 }, () => ({ latitude: -17, longitude: -179 })),
      { latitude: -10, longitude: 179 },
    ];
    const view = await draw({ ...spec, width: 880 }, rows);
    try {
      const at = (lon: number, lat: number) => rowsWith(view, 'x').find((r) => r.longitude === lon && r.latitude === lat)! as { x: number; y: number };
      const across = Math.abs(at(179, -17).x - at(-179, -17).x);
      const north = Math.abs(at(179, -17).y - at(179, -10).y);
      expect(across).toBeLessThan(north);
    } finally {
      view.finalize();
    }
  });

  test('#8 facet headers show the documented labels (rendered)', async () => {
    const levels = ['low', 'medium', 'high'];
    const severity = nominal('severity', [['high', 4], ['low', 4], ['medium', 4]], {
      categories: levels.map((value) => ({ value, label: `${value[0]!.toUpperCase()}${value.slice(1)} severity` })), categoriesOrdered: true,
    });
    const spec = starterSpec(table([severity, quant('X', 1, 9), quant('Y', 1, 9)], 12))!;
    const rows = levels.flatMap((s) => [1, 2, 3, 4].map((x) => ({ severity: s, X: x, Y: x })));
    const view = await draw(spec, rows);
    try {
      const svg = await view.toSVG();
      for (const label of ['Low severity', 'Medium severity', 'High severity']) expect(svg).toContain(`>${label}<`);
    } finally {
      view.finalize();
    }
  });

  test('#10 a remainder category ("All other causes") is a series, not a total', () => {
    const cause = nominal('cause', [['All other causes', 3], ['Cancer', 3], ['Heart disease', 3]]);
    const d = table([year, cause, quant('deaths', 1, 900)], 9, { timeKeys: { year: ['cause'] } });
    expect(starterSpec(d)!.transform).toBeUndefined();
  });

  test('#11 a fitted world map says how many points it leaves outside the frame', () => {
    const d = table([quant('latitude', 0, 60), quant('longitude', 0, 120)], 100, {
      points: { latitude: 'latitude', longitude: 'longitude', box: { longitude: [0, 20], latitude: [0, 20] }, us: 0, outsideUs: 100, outsideBox: 1 },
    });
    expect((starterSpec(d)!.projection as Record<string, unknown>).fit).toBeDefined();
    expect(mapNote(d)).toBe('The map frames the middle 98% of the points; 1 of 100 rows lies outside the frame.');
  });
});

// Codex round 3: each test on Codex's input, shown failing on 42276a9 before the fix.
describe('Codex round 3', () => {
  const regions = (n: number, prefix = 'r') => Array.from({ length: n }, (_, i) => [`${prefix}${String(i).padStart(2, '0')}`, 3] as [string, number]);
  const year = quant('year', 2000, 2002, { type: 'integer' }, { distinct: 3, evenlySpaced: true });
  const many = (name: string, distinct: number, top: [string, number][]) => ({ ...nominal(name, top), profile: { kind: 'nominal' as const, distinct, top, missing: 0 } });

  test('#1 a rate in the title (or description) is never summed', () => {
    expect(summable(quant('deaths', 0, 100, { title: 'Deaths per100k' }))).toBe(false);
    expect(summable(quant('deaths', 0, 100, { description: 'Deaths per100k people.' }))).toBe(false);
    const d = table([year, many('country', 13, regions(6)), quant('deaths', 100, 100, { title: 'Deaths per100k' })], 39, { timeKeys: { year: ['country'] }, totalValues: {} });
    expect(enc(starterSpec(d)).y).toMatchObject({ aggregate: 'mean' });
  });

  test('#2 the chart buckets date-only values in UTC, as the builder checks them (New York and Tokyo)', () => {
    // ISO dates: the builder marks them utc (tested in test_build_site_catalog.py).
    const days: Field = { name: 'date', type: 'date', description: null, profile: { kind: 'temporal', min: '2019-01-31T00:00:00Z', max: '2021-02-01T00:00:00Z', missing: 0, distinct: 3, utc: true } };
    const d = table([days, many('region', 400, regions(6)), quant('population', 100, 100)], 1200, {
      timeKeys: { date: ['region'] }, timeKeyBuckets: { date: ['yearmonthdate', 'yearmonth'] }, totalValues: {},
    });
    const spec = starterSpec(d)!;
    const rows = ['2019-01-31', '2019-02-01', '2021-02-01'].flatMap((date) => Array.from({ length: 400 }, (_, i) => ({ date, region: `r${i}`, population: 100 })));
    const script = fileURLToPath(new URL('./tz-sums.mjs', import.meta.url));
    for (const tz of ['America/New_York', 'Asia/Tokyo', 'UTC']) {
      const out = execFileSync(process.execPath, [script], { input: JSON.stringify({ spec, rows }), env: { ...process.env, TZ: tz } }).toString();
      // Each month holds one date: 400 regions of 100.
      expect(JSON.parse(out)).toEqual([40000, 40000, 40000]);
    }
  });

  test('#3 the height counts only the categories the missing-value filter leaves', () => {
    const cat = many('cat', 60, regions(6));
    const m = quant('m', 1, 10, { missingValues: ['-99'] });
    const d = table([cat, m], 120, { presentCategories: { cat: { m: 1 } } });
    expect(discreteHeight(d, starterSpec(d)!)).toBe(20);
  });

  test('#4 a bracketed source field still draws the top twenty', async () => {
    const origin = many('origin', 61, regions(6));
    const count = quant('count[0]', 1, 10);
    const d = table([origin, many('destination', 61, regions(6)), count], 122, { totalValues: {} });
    const rows = Array.from({ length: 61 }, (_, i) => [{ origin: `o${i}`, 'count[0]': 10 }, { origin: `o${i}`, 'count[0]': 5 }]).flat();
    const view = await draw(starterSpec(d)!, rows);
    try {
      expect(new Set(rowsWith(view, 'total').map((r) => r.total))).toEqual(new Set([15]));
    } finally {
      view.finalize();
    }
  });

  test('#5 a big table on a phone never loads without a button, whatever its modes', () => {
    const gate = (largeData as unknown as { loadGate?: (rows: number, bytes: number, map: boolean) => { button: boolean; autoDraw: string } }).loadGate;
    expect(gate).toBeTypeOf('function');
    for (const rows of [100, 6000, 30000, 50001, 200000]) {
      const g = gate!(rows, 0, false);
      const band = largeData.rowBand(rows);
      // Anything past the SVG band waits for a button on a phone (canvas: on desktop it draws itself).
      expect(g.button).toBe(band !== 'svg');
      expect(g.autoDraw).toBe(band === 'svg' ? 'always' : band === 'canvas' ? 'desktop' : 'never');
    }
    expect(gate!(200000, 0, true).button).toBe(false);
  });
});

describe('field names with dots, brackets and backslashes draw in every chart kind (Codex round 3, #4)', () => {
  const odd = { cat: 'g.roup', m: 'm[0]', m2: 'w\\v', t: 'd.ate' };
  const cats = ['a', 'b', 'c', 'd'];
  const cat = nominal(odd.cat, cats.map((c) => [c, 6] as [string, number]));
  const kinds: [string, Dataset, Record<string, unknown>[]][] = [
    ['bars', table([cat, quant(odd.m, 1, 9)], 24), cats.flatMap((c) => [1, 2].map((v) => ({ [odd.cat]: c, [odd.m]: v })))],
    ['histogram', table([quant(odd.m, 1, 9)], 24), [1, 2, 3, 5, 8].map((v) => ({ [odd.m]: v }))],
    ['scatter', table([quant(odd.m, 1, 9), quant(odd.m2, 1, 9), cat], 24), cats.flatMap((c, i) => [{ [odd.m]: i + 1, [odd.m2]: 9 - i, [odd.cat]: c }])],
    ['time series', table([dates(odd.t, 3), quant(odd.m, 1, 9)], 3, { timeKeys: { [odd.t]: [] } }), ['2000-01-01', '2000-02-01', '2000-03-01'].map((t, i) => ({ [odd.t]: t, [odd.m]: i + 1 }))],
    ['top twenty', table([{ ...nominal(odd.cat, cats.map((c) => [c, 3] as [string, number])), profile: { kind: 'nominal', distinct: 61, top: cats.map((c) => [c, 3] as [string, number]), missing: 0 } }, quant('count.all', 1, 9)], 122, { totalValues: {} }), Array.from({ length: 61 }, (_, i) => ({ [odd.cat]: `c${i}`, 'count.all': i + 1 }))],
  ];
  test.each(kinds)('%s', async (_kind, d, rows) => {
    const view = await draw(starterSpec(d)!, rows);
    try {
      const svg = await view.toSVG();
      expect((svg.match(/<(path|rect|circle|line)\b/g) ?? []).length).toBeGreaterThanOrEqual(3);
      expect(svg).not.toMatch(/NaN|undefined/);
    } finally {
      view.finalize();
    }
  });
  test('the Explore scatter plot', async () => {
    const d = table([quant(odd.m, 1, 9), quant(odd.m2, 1, 9), cat], 24);
    const f = scatterFields(d)!;
    const rows = cats.map((c, i) => ({ [odd.m]: i + 1, [odd.m2]: 9 - i, [odd.cat]: c }));
    const view = await draw(scatterSpec(d, f, { ...defaultAxes(d, f), zoom: true, height: 300 }), rows);
    try {
      expect(rowsWith(view, odd.m).length).toBeGreaterThanOrEqual(4);
      expect(await view.toSVG()).not.toMatch(/NaN/);
    } finally {
      view.finalize();
    }
  });
});

describe('Codex round 4', () => {
  test('#1 a percent sign in brackets or before "of" marks a rate', () => {
    for (const title of ['Deaths (%)', 'Deaths [%]', '% of deaths']) expect(summable(quant('deaths', 0, 100, { title }))).toBe(false);
    expect(nameTokens('Deaths (%)')).toEqual(['Deaths', '(', '%', ')']);
  });
});
