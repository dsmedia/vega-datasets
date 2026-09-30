// Chart standards S9 to S17 (site/CHART-STANDARDS.md), on every real dataset's Explore charts:
// each property from the specs and, where it concerns what is drawn, from the rendered view
// of the real file. S8 (lines) is with S1, S3 and S5 in explore-invariants.test.ts; S10's
// rendered label boxes are browser/labels.mjs.
import * as vega from 'vega';
import { expressionInterpreter } from 'vega-interpreter';
import { compile, type TopLevelSpec } from 'vega-lite';
import { describe, expect, test } from 'vitest';
import type { Dataset, Field } from '../src/lib/catalog';
import { mostlyZero } from '../src/lib/chart-rules';
import { exploreModes, mapNote, modeChart } from '../src/lib/explore-model';
import { themeConfig } from '../src/lib/vega-theme';
import { loadCatalog, readDataUrl } from './catalog';

type Spec = Record<string, unknown>;
type Enc = Record<string, unknown>;

const catalog = loadCatalog();
const datasets = catalog.datasets;
/** Every dataset's Explore charts but the scatter plot (whose fields are the reader's), wide. */
const charts: { d: Dataset; mode: string; spec: Spec }[] = datasets.flatMap((d) =>
  exploreModes(d)
    .filter((m) => m !== 'scatter')
    .flatMap((mode) => {
      const spec = modeChart(d, mode, false);
      return spec ? [{ d, mode, spec }] : [];
    }),
);
const units = (spec: Spec): Spec[] => [spec.spec as Spec | undefined, ...((spec.layer as Spec[] | undefined) ?? []), spec].filter((u): u is Spec => !!u?.encoding);
const unitOf = (spec: Spec): Spec => units(spec)[0] ?? spec;
const encodingOf = (spec: Spec): Record<string, Enc> => (unitOf(spec).encoding ?? {}) as Record<string, Enc>;
const fieldOf = (d: Dataset, e: Enc | undefined): Field | undefined => {
  const name = typeof e?.field === 'string' ? e.field.replace(/\\(.)/g, '$1') : undefined;
  return name ? d.fields.find((f) => f.name === name) : undefined;
};
const markOf = (spec: Spec): string => {
  const m = unitOf(spec).mark as string | { type?: string } | undefined;
  return (typeof m === 'string' ? m : m?.type) ?? '';
};
const isTime = (spec: Spec) => ['time', 'total'].includes(String((spec.usermeta as { chart?: string } | undefined)?.chart));

/** The data of the marks a view draws (its scenegraph items), of the given mark types. */
function drawnData(view: vega.View, types: string[]): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = [];
  type Node = { marktype?: string; items?: Node[]; datum?: Record<string, unknown> };
  const walk = (node: Node, type?: string) => {
    for (const item of node.items ?? []) {
      const t = item.marktype ?? type;
      if (item.datum && t && types.includes(t)) out.push(item.datum);
      walk(item, t);
    }
  };
  walk((view.scenegraph() as unknown as { root: Node }).root);
  return out;
}

/** The chart drawn from its real file (no renderer), for its scales and data. */
async function render(spec: Spec): Promise<vega.View> {
  const quiet = { level: () => quiet, error: () => quiet, warn: () => quiet, info: () => quiet, debug: () => quiet };
  const { spec: vg } = compile(spec as TopLevelSpec, { logger: quiet as never, config: themeConfig(() => '#808080') as never });
  const loader = vega.loader();
  loader.load = async (uri: string) => readDataUrl(uri);
  const view = new vega.View(vega.parse(vg, undefined, { ast: true }), { renderer: 'none', loader, expr: expressionInterpreter });
  await view.runAsync();
  return view;
}

describe('chart standards, pass 2 (site/CHART-STANDARDS.md)', () => {
  test('coverage: the real charts include time charts, heatmaps and point maps', () => {
    expect(charts.filter((c) => isTime(c.spec)).length).toBeGreaterThan(15);
    expect(charts.filter((c) => markOf(c.spec) === 'rect' && isTime(c.spec)).length).toBeGreaterThanOrEqual(2);
    expect(charts.filter((c) => (c.spec.projection as Spec | undefined) && units(c.spec).some((u) => (u.encoding as Enc).latitude)).length).toBeGreaterThanOrEqual(5);
  });

  test('S9: every point map has a basemap, and says how many rows its frame leaves out', () => {
    const maps = charts.filter((c) => units(c.spec).some((u) => (u.encoding as Enc).latitude));
    const problems = maps.flatMap(({ d, spec }) => {
      const layers = (spec.layer as Spec[] | undefined) ?? [];
      const basemap = layers.some((l) => (l.mark as Spec | undefined)?.type === 'geoshape' && !(l.encoding as Enc | undefined)?.latitude);
      const out: string[] = basemap ? [] : [`${d.name}: points with no basemap`];
      const fit = (spec.projection as Spec | undefined)?.fit;
      if (fit && (d.points?.outsideBox ?? 0) > 0 && !mapNote(d)) out.push(`${d.name}: the frame leaves out rows, unsaid`);
      return out;
    });
    expect(problems).toEqual([]);
    // Coordinates named as a centroid's (cx, cy) are a map, over the most detailed basemap that holds them.
    const centroids = charts.find((c) => c.d.name === 'london_centroids')!;
    expect(centroids.spec.projection).toBeTruthy();
    expect(JSON.stringify(centroids.spec)).toContain('londonBoroughs.json');
  });

  test('S9: a fitted map frames every point but the outliers, none on its edge', () => {
    // An outlier: beyond the middle 90% of the points by more than that middle's span, along either axis.
    const quantile = (xs: number[], q: number) => {
      const v = [...xs].sort((a, b) => a - b);
      const i = (v.length - 1) * q;
      return v[Math.floor(i)]! + (v[Math.ceil(i)]! - v[Math.floor(i)]!) * (i - Math.floor(i));
    };
    const problems: string[] = [];
    let checked = 0;
    for (const { d, spec } of charts) {
      const fit = (spec.projection as { fit?: { geometry: { coordinates: [number, number][] } } } | undefined)?.fit;
      if (!fit || !d.points || d.points.box.longitude[1] > 180) continue;
      const rows = vega.read(readDataUrl(d.url), { type: d.format as 'csv' | 'json' }) as Record<string, unknown>[];
      const pts = rows.map((r) => [Number(r[d.points!.longitude]), Number(r[d.points!.latitude])] as const).filter(([x, y]) => Number.isFinite(x) && Number.isFinite(y));
      const far = (i: 0 | 1) => {
        const v = pts.map((p) => p[i]);
        const [lo, hi] = [quantile(v, 0.05), quantile(v, 0.95)];
        // Or a quarter of the whole range, when the middle is one place.
        const span = Math.max(hi - lo, (Math.max(...v) - Math.min(...v)) / 4);
        return (x: number) => x < lo - span || x > hi + span;
      };
      const [farX, farY] = [far(0), far(1)];
      const xs = fit.geometry.coordinates.map((c) => c[0]);
      const ys = fit.geometry.coordinates.map((c) => c[1]);
      const [w, e, south, n] = [Math.min(...xs), Math.max(...xs), Math.min(...ys), Math.max(...ys)];
      checked++;
      const cropped = pts.filter(([x, y]) => !farX(x) && !farY(y) && !(x > w && x < e && y > south && y < n));
      if (cropped.length) problems.push(`${d.name}: ${cropped.length} points that are no outliers lie on or outside the frame`);
    }
    expect(problems).toEqual([]);
    expect(checked).toBeGreaterThanOrEqual(3);
    // London's 33 boroughs, all shown.
    expect(datasets.find((d) => d.name === 'london_centroids')!.points?.outsideBox).toBe(0);
  });

  test('S11: a time axis ends within a tick of the data', async () => {
    const problems: string[] = [];
    let checked = 0;
    for (const { d, mode, spec } of charts.filter((c) => isTime(c.spec) && ['line', 'point'].includes(markOf(c.spec)))) {
      const x = encodingOf(spec).x;
      if (!x || x.timeUnit) continue;
      const view = await render({ ...spec, width: 640 });
      try {
        const scale = view.scale('x') as unknown as { domain(): (number | Date)[]; ticks(n: number): (number | Date)[] };
        const [lo, hi] = scale.domain().map(Number) as [number, number];
        const ticks = scale.ticks(Math.ceil(640 / 90)).map(Number);
        const step = ticks.length > 1 ? (ticks.at(-1)! - ticks[0]!) / (ticks.length - 1) : hi - lo;
        const t = fieldOf(d, x)!;
        const p = t.profile as { min: number | string; max: number | string };
        const [min, max] = [p.min, p.max].map((v) => (typeof v === 'string' ? Date.parse(v) : v)) as [number, number];
        checked++;
        // Within half a tick: the data's ends are where the axis ends, give or take a label.
        if (hi - max > step * 0.5 || min - lo > step * 0.5) problems.push(`${d.name} ${mode}: the axis runs ${lo}–${hi}, the data ${min}–${max} (a tick is ${step})`);
      } finally {
        view.finalize();
      }
    }
    expect(problems).toEqual([]);
    expect(checked).toBeGreaterThan(8);
  }, 120_000);

  test('S11: no date lies past the catalog build year (a time axis past the data would follow it)', () => {
    const year = new Date().getUTCFullYear();
    const future = datasets.flatMap((d) => d.fields.filter((f) => f.profile.kind === 'temporal' && new Date((f.profile as { max: string }).max).getUTCFullYear() > year + 1).map((f) => `${d.name}.${f.name}`));
    // Known data error, reported (the file's two-digit years were expanded into the wrong
    // century: Duel in the Sun, 1946, is dated Dec 31 2046). Its metadata is not ours to
    // edit (_data/datapackage_additions.toml); the fix belongs upstream. Any other is new.
    expect(future).toEqual(['movies.Release Date']);
  });

  test('S12: a band axis of years is labeled at round steps (rendered)', async () => {
    const problems: string[] = [];
    let checked = 0;
    for (const phone of [false, true]) {
      for (const { d, mode } of charts) {
        const spec = modeChart(d, mode as never, phone)!;
        const x = encodingOf(spec).x;
        const f = fieldOf(d, x);
        if (!x || x.type !== 'ordinal' || !f || f.profile.kind !== 'quantitative' || !/year/i.test(f.name)) continue;
        const view = await render({ ...spec, width: phone ? 288 : 640 });
        try {
          const svg = await view.toSVG();
          // The x axis's labels as drawn (the y axis is the series' names).
          const labels = [...svg.matchAll(/role-axis-label[\s\S]*?<\/g>/g)].flatMap((m) => [...m[0].matchAll(/>(\d{4})</g)].map((l) => Number(l[1])));
          const steps = new Set(labels.slice(1).map((v, i) => v - labels[i]!));
          const step = [...steps][0] ?? 0;
          checked++;
          const round = steps.size === 1 && [1, 2, 5, 10, 20, 25, 50, 100, 200, 500].includes(step) && labels.every((v) => v % step === 0);
          if (!round || labels.length > (phone ? 5 : 12) || labels.length < 2) problems.push(`${d.name} ${mode}${phone ? ' (phone)' : ''}: year labels ${labels.join(', ')}`);
        } finally {
          view.finalize();
        }
      }
    }
    expect(problems).toEqual([]);
    expect(checked).toBeGreaterThanOrEqual(2);
  }, 60_000);

  test('S10: a log or symlog color legend labels its decades', () => {
    const problems = [false, true].flatMap((phone) =>
      datasets.flatMap((d) =>
        exploreModes(d)
          .filter((m) => m !== 'scatter')
          .flatMap((mode) => {
            const spec = modeChart(d, mode, phone);
            const c = spec ? encodingOf(spec).color : undefined;
            const type = (c?.scale as { type?: string } | undefined)?.type;
            if (!c || c.type !== 'quantitative' || !type || type === 'linear') return [];
            const values = (c.legend as { values?: number[] } | undefined)?.values ?? [];
            return values.length >= 3 ? [] : [`${d.name} ${mode}${phone ? ' (phone)' : ''}: a ${type} color legend labels ${values.length} values`];
          }),
      ),
    );
    expect(problems).toEqual([]);
  });

  test('S10: a band axis across keeps its labels upright (Vega-Lite would turn them on their side)', () => {
    // Canvas charts (football's 2,600 rows) can't be measured in the browser test: this holds them.
    const problems = charts.flatMap(({ d, mode, spec }) => {
      const x = encodingOf(spec).x;
      if (!x || !['ordinal', 'nominal'].includes(String(x.type))) return [];
      return (x.axis as { labelAngle?: number } | undefined)?.labelAngle === 0 ? [] : [`${d.name} ${mode}: labels on their side`];
    });
    expect(problems).toEqual([]);
  });

  test('S10: an integer histogram steps and labels by whole numbers', () => {
    const problems = charts.flatMap(({ d, spec }) => {
      const x = encodingOf(spec).x;
      const f = fieldOf(d, x);
      if (!x?.bin || f?.type !== 'integer') return [];
      const ok = (x.bin as { minstep?: number }).minstep === 1 && (x.axis as { format?: string } | undefined)?.format === 'd';
      return ok ? [] : [`${d.name}: bins of an integer without whole-number steps and labels`];
    });
    expect(problems).toEqual([]);
  });

  test('S13: a heatmap shows change over time (within its rows, not only between them)', async () => {
    const problems: string[] = [];
    let checked = 0;
    for (const { d, mode, spec } of charts.filter((c) => markOf(c.spec) === 'rect' && isTime(c.spec))) {
      const view = await render(spec);
      try {
        // The drawn cells: their row (y), and color value (on the color scale's own terms: log for log).
        const enc = encodingOf(spec);
        const yField = String(enc.y!.field).replace(/\\(.)/g, '$1');
        const c = enc.color!;
        const colorField = c.aggregate ? `${c.aggregate}_${String(c.field).replace(/\\(.)/g, '$1')}` : String(c.field);
        const cells = drawnData(view, ['rect']).filter((r) => yField in r && colorField in r && Number.isFinite(Number(r[colorField])));
        const type = (c.scale as { type?: string } | undefined)?.type;
        const value = (v: number) => (type === 'log' ? Math.log10(v) : type === 'symlog' ? Math.sign(v) * Math.log10(1 + Math.abs(v)) : v);
        const values = cells.map((r) => value(Number(r[colorField])));
        const mean = (xs: number[]) => xs.reduce((a, b) => a + b, 0) / xs.length;
        const all = mean(values);
        const total = values.reduce((a, v) => a + (v - all) ** 2, 0);
        const rows = new Map<string, number[]>();
        cells.forEach((r, i) => rows.set(String(r[yField]), [...(rows.get(String(r[yField])) ?? []), values[i]!]));
        const within = [...rows.values()].reduce((a, vs) => a + vs.reduce((b, v) => b + (v - mean(vs)) ** 2, 0), 0);
        checked++;
        if (total > 0 && within / total < 0.2) problems.push(`${d.name} ${mode}: ${Math.round((100 * within) / total)}% of the color's variation is within rows`);
      } finally {
        view.finalize();
      }
    }
    expect(problems).toEqual([]);
    expect(checked).toBeGreaterThanOrEqual(2);
  }, 120_000);

  test('S14: categorical colors on a map stay within the palette (ten)', () => {
    const problems = datasets.flatMap((d) => {
      const spec = modeChart(d, 'starter', false);
      if (!spec?.projection) return [];
      const text = JSON.stringify(spec);
      const colored = units(spec).some((u) => ((u.encoding as Enc).color as Enc | undefined)?.type === 'nominal');
      const ids = Object.values(d.objectIds ?? {})[0] ?? 0;
      return /tableau20|category20/.test(text) || (colored && ids > 10) ? [`${d.name}: more categories than ten colors`] : [];
    });
    expect(problems).toEqual([]);
  });

  test('S15: a time chart of series has three or more times per series', () => {
    const problems = charts.flatMap(({ d, mode, spec }) => {
      if (!isTime(spec)) return [];
      const enc = encodingOf(spec);
      const t = fieldOf(d, enc.x);
      const series = markOf(spec) === 'rect' ? fieldOf(d, enc.y) : enc.color?.type === 'nominal' ? fieldOf(d, enc.color) : undefined;
      const m = fieldOf(d, markOf(spec) === 'rect' ? enc.color : enc.y);
      if (!t || !series || !m) return [];
      const per = d.lineShapes?.[t.name]?.[m.name]?.[series.name]?.perSeries;
      return per !== undefined && per < 3 ? [`${d.name} ${mode}: ${per} times per ${series.name}`] : [];
    });
    expect(problems).toEqual([]);
  });

  test('S16: a time chart never averages a measure that is mostly zero', () => {
    const problems = charts.flatMap(({ d, mode, spec }) => {
      if (!isTime(spec)) return [];
      const y = encodingOf(spec).y;
      const m = fieldOf(d, y);
      return m && y?.aggregate === 'mean' && mostlyZero(m) ? [`${d.name} ${mode}: the mean of ${m.name}, zero in most rows`] : [];
    });
    expect(problems).toEqual([]);
    expect(datasets.some((d) => d.fields.some(mostlyZero))).toBe(true);
  });

  test('S17: measures of one kind over time are drawn together', () => {
    // One kind: descriptions that open with the same two words, non-negative integers, one row per time.
    const kindOf = (f: Field) => ((f.description ?? '').toLowerCase().match(/[a-z]+/g) ?? []).slice(0, 2).join(' ');
    const problems = charts.flatMap(({ d, mode, spec }) => {
      if (!isTime(spec) || markOf(spec) === 'rect') return [];
      const y = fieldOf(d, encodingOf(spec).y);
      if (!y || !kindOf(y) || d.timeKeys?.[fieldOf(d, encodingOf(spec).x)?.name ?? ''] === undefined) return [];
      const kin = d.fields.filter((f) => f !== y && f.type === 'integer' && kindOf(f) === kindOf(y) && f.profile.kind === 'quantitative' && f.profile.min >= 0);
      return kin.length ? [`${d.name} ${mode}: ${y.name} alone, not with ${kin.map((f) => f.name).join(', ')}`] : [];
    });
    expect(problems).toEqual([]);
    expect(JSON.stringify(charts.find((c) => c.d.name === 'crimea' && isTime(c.spec))?.spec ?? {})).toContain('"fold"');
  });

  test('S4: a log axis draws grid lines only where it has labels (the decades)', async () => {
    type Item = { role?: string; items?: Item[]; datum?: { value?: number }; text?: string; opacity?: number; strokeOpacity?: number };
    const problems: string[] = [];
    let checked = 0;
    for (const { d, mode, spec } of charts) {
      if ((encodingOf(spec).y?.scale as { type?: string } | undefined)?.type !== 'log') continue;
      const view = await render({ ...spec, width: 640 });
      try {
        const grid: number[] = [];
        const labeled: number[] = [];
        const walk = (node: Item, role?: string) => {
          for (const item of node.items ?? []) {
            const r = item.role ?? role;
            const v = item.datum?.value;
            if (typeof v === 'number' && r === 'axis-grid' && (item.opacity ?? 1) > 0 && (item.strokeOpacity ?? 1) > 0) grid.push(v);
            if (typeof v === 'number' && r === 'axis-label' && item.text && (item.opacity ?? 1) > 0) labeled.push(v);
            walk(item, r);
          }
        };
        walk((view.scenegraph() as unknown as { root: Item }).root);
        checked++;
        const unlabeled = grid.filter((g) => !labeled.some((l) => Math.abs(l - g) <= 1e-9 * Math.abs(g)));
        if (unlabeled.length) problems.push(`${d.name} ${mode}: grid lines at ${unlabeled.slice(0, 4).join(', ')} with no label`);
      } finally {
        view.finalize();
      }
    }
    expect(problems).toEqual([]);
    expect(checked).toBeGreaterThanOrEqual(2);
  }, 60_000);

  test('S4: a log axis over part of the rows fits whole decades around them', async () => {
    const problems: string[] = [];
    let checked = 0;
    for (const { d, mode, spec } of charts.filter((c) => c.mode === 'total')) {
      const y = encodingOf(spec).y!;
      if ((y.scale as { type?: string } | undefined)?.type !== 'log') continue;
      const view = await render({ ...spec, width: 640 });
      try {
        const [lo, hi] = (view.scale('y') as unknown as { domain(): number[] }).domain();
        const field = String(y.field).replace(/\\(.)/g, '$1');
        const values = drawnData(view, ['line', 'symbol']).map((r) => Number(r[field])).filter((v) => Number.isFinite(v) && v > 0);
        const [min, max] = [Math.min(...values), Math.max(...values)];
        checked++;
        if (lo! < 10 ** Math.floor(Math.log10(min)) / 1.0001 || hi! > 10 ** Math.ceil(Math.log10(max)) * 1.0001) problems.push(`${d.name} ${mode}: log axis ${lo}–${hi} for values ${min}–${max}`);
      } finally {
        view.finalize();
      }
    }
    expect(problems).toEqual([]);
    expect(checked).toBeGreaterThanOrEqual(1);
  }, 60_000);
});

