// Invariants of the Explore rules on a few hundred random tables (seeded, so reproducible),
// generated and profiled by the real catalog builder (property/tables.py):
//   (a) no data loss: the only filters a chart applies are its fields' documented missing
//       values and unplottable cells, and a chart that doesn't aggregate draws every other row;
//   (b) no double counting: a summed value is the sum of distinct rows of its bucket (no
//       entity twice in a bucket, no total summed with its parts);
//   (c) time-zone independence: bucketed values are the same in New York, Tokyo and UTC;
//   (d) the phone gate: a table over the row or byte limit never loads on a phone without a button;
//   (e) distinct colors: a color encoding never has more values than its scheme has colors;
// and the chart standards (site/CHART-STANDARDS.md), on the drawn scenegraph:
//   S1 at most six colored lines on one set of axes (four on a phone);
//   S2 a detected total never shares axes with its parts (its own "Total" mode);
//   S3 no line crosses a gap in its times; S5 no line so jagged that the lines dominate.
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import * as vega from 'vega';
import { compile, type TopLevelSpec } from 'vega-lite';
import { describe, expect, test } from 'vitest';
import { type Dataset, effectiveMissing, type Field } from '../src/lib/catalog';
import { defaultAxes, exploreModes, modeChart, scatterFields, scatterSpec, starterChart } from '../src/lib/explore-model';
import { JAGGED, scaleType, SERIES_LIMIT } from '../src/lib/chart-rules';
// S8: a line marks its values when it has at most this many.
const FEW_POINTS = 60;
import { totalOf } from '../src/lib/starter';
import { loadCatalog, readDataUrl } from './catalog';
import * as largeData from '../src/lib/large-data';
import { fieldRef, markerForms } from '../src/lib/starter';

type Spec = Record<string, unknown>;
type Row = Record<string, string>;
interface Case { dataset: Dataset; csv: string }

const SEED = 7;
const COUNT = 240;
const COVERAGE = { filtered: 40, unaggregated: 40, rows: 800, colored: 80, nearLimit: 5 };
const here = (p: string) => fileURLToPath(new URL(p, import.meta.url));

const cases: Case[] = JSON.parse(
  execFileSync('uv', ['run', '--group', 'site', 'python', here('./property/tables.py'), '--seed', String(SEED), '--count', String(COUNT)], {
    maxBuffer: 1 << 28,
  }).toString(),
);

function parseCsv(text: string): Row[] {
  const [head, ...lines] = text.trimEnd().split('\n');
  const columns = head!.split(',');
  // The generator writes no quoted commas (its names avoid them).
  return lines.map((l) => Object.fromEntries(l.split(',').map((v, i) => [columns[i]!, v])));
}

/** The spec's unit specs (a layer's, a facet's, or itself) and every filter in it. */
function filters(spec: Spec): string[] {
  const units = [spec, ...((spec.layer as Spec[] | undefined) ?? []), ...(spec.spec ? [spec.spec as Spec] : [])];
  return units.flatMap((u) => ((u.transform as Spec[] | undefined) ?? []).filter((t) => 'filter' in t).map((t) => (typeof t.filter === 'string' ? t.filter : JSON.stringify(t.filter))));
}

/** Is a filter one a chart may apply: documented missing values, or cells it can't plot? */
function allowed(d: Dataset, filter: string): boolean {
  if (/^isValid\(/.test(filter)) return true;
  // The parts without their detected total, which the chart's "Total" mode shows (S2).
  const total = totalOf(d);
  if (total && filter === `indexof(${JSON.stringify(total.totals.map((v) => `v:${v}`))}, "v:" + datum[${JSON.stringify(total.series.name)}]) < 0`) return true;
  // The top twenty: a chart of the largest groups, by its own rank (its title says so).
  if (/^datum\["[^"]*"\] <= \d+$/.test(filter)) return true;
  // A missing-value filter lists only markers the metadata declares, per field.
  const lists = [...filter.matchAll(/indexof\((\[[^\]]*\]), "v:" \+ datum\[("(?:[^"\\]|\\.)*")\]\)/g)];
  if (!lists.length) return false;
  return lists.every(([, list, name]) => {
    const f = d.fields.find((x) => x.name === JSON.parse(name!));
    const markers = f ? markerForms(f, effectiveMissing(d, f) ?? []) : [];
    return (JSON.parse(list!) as string[]).every((v) => markers.includes(v.slice(2)));
  });
}

interface VgMark { from?: { data?: string; facet?: { data: string } }; marks?: VgMark[] }

/** The dataset a spec's first mark draws from (a faceted line's, the data it splits). */
function markSource(marks: VgMark[] = []): string | null {
  for (const m of marks) {
    if (m.from?.facet) return m.from.facet.data;
    if (m.from?.data) return m.from.data;
    const inner = markSource(m.marks);
    if (inner) return inner;
  }
  return null;
}

/** Draw a spec from its CSV text, here (in this process's time zone); with the rows its first mark draws. */
async function run(spec: Spec, csv: string): Promise<{ view: vega.View; drawn: number; source: string | null }> {
  const quiet = { level: () => quiet, error: () => quiet, warn: () => quiet, info: () => quiet, debug: () => quiet };
  const { spec: vg } = compile(spec as TopLevelSpec, { logger: quiet as never });
  const loader = vega.loader();
  loader.load = async () => csv;
  const view = new vega.View(vega.parse(vg), { renderer: 'none', loader });
  await view.runAsync();
  const source = markSource((vg as { marks?: VgMark[] }).marks);
  return { view, drawn: source ? (view.data(source) as unknown[]).length : 0, source };
}

/**
 * The values a line chart drew, keyed "series|time" (the time as its bucket's start in
 * milliseconds, or the number): read from the dataset its first mark draws from.
 */
function renderedValues(view: vega.View, spec: Spec, d: Dataset): Map<string, number> {
  const enc = encodingOf(spec);
  const names = Object.keys(view.getState({ data: vega.truthy, signals: vega.falsy }).data ?? {});
  const unit = enc.x?.timeUnit as string | undefined;
  const t = fieldOf(d, enc.x);
  const color = fieldOf(d, enc.color);
  const out = new Map<string, number>();
  for (const name of names) {
    for (const r of view.data(name) as Datum[]) {
      const valueKey = Object.keys(r).find((k) => /^(sum|mean)_/.test(k));
      if (!valueKey || !t) continue;
      const timeKey = unit ? Object.keys(r).find((k) => k.startsWith(`${unit}_`)) : t.name;
      const tv = timeKey ? r[timeKey] : undefined;
      const ms = tv instanceof Date ? tv.getTime() : Number(tv);
      if (!Number.isFinite(ms) || typeof r[valueKey] !== 'number') continue;
      out.set(`${color ? String(r[color.name] ?? '') : ''}|${ms}`, r[valueKey] as number);
    }
  }
  return out;
}

const encodingOf = (spec: Spec) => ((spec.spec ?? (spec.layer as Spec[] | undefined)?.at(-1) ?? spec) as { encoding?: Record<string, Spec> }).encoding ?? {};
const fieldOf = (d: Dataset, e: Spec | undefined): Field | undefined => (e?.field ? d.fields.find((f) => fieldRef(f.name) === e.field) : undefined);
const isMissing = (d: Dataset, f: Field, v: string | undefined) => v === undefined || markerForms(f, effectiveMissing(d, f) ?? []).includes(v);

const charts = cases.map((c) => ({ ...c, spec: starterChart(c.dataset), rows: parseCsv(c.csv) })).filter((c) => c.spec !== null) as (Case & { spec: Spec; rows: Row[] })[];

describe(`Explore invariants on ${COUNT} random tables (seed ${SEED})`, () => {
  test('there are charts of every kind to check', () => {
    const kinds = new Set(charts.map((c) => JSON.stringify((encodingOf(c.spec).y as Spec | undefined)?.aggregate ?? 'none')));
    expect(kinds.size).toBeGreaterThanOrEqual(2);
    expect(charts.length).toBeGreaterThan(COUNT / 2);
  });

  test('(a) no data loss: the only filters are documented missing values and unplottable cells', () => {
    const bad = charts.flatMap((c) => filters(c.spec).filter((f) => !allowed(c.dataset, f)).map((f) => `${c.dataset.name}: ${f}`));
    expect(bad).toEqual([]);
    // Coverage: charts with filters (documented markers, unplottable cells) came up to be judged.
    expect(charts.filter((c) => filters(c.spec).length > 0).length).toBeGreaterThan(COVERAGE.filtered);
  });

  test('(a) no data loss: a chart that doesn’t aggregate draws every plottable row', async () => {
    const problems: string[] = [];
    let checked = 0;
    let rowsChecked = 0;
    for (const c of charts) {
      const enc = encodingOf(c.spec);
      if (Object.values(enc).some((e) => e.aggregate || e.timeUnit || e.bin) || c.spec.facet || c.spec.layer) continue;
      const encoded = Object.values(enc).map((e) => fieldOf(c.dataset, e)).filter((f): f is Field => !!f);
      // Rows of a detected total are drawn in the Total mode instead (S2).
      const total = totalOf(c.dataset);
      const plottable = c.rows.filter((r) => !(total && total.totals.includes(r[total.series.name]!)) && encoded.every((f) => {
        const v = r[f.name];
        if (isMissing(c.dataset, f, v)) return false;
        const e = Object.values(enc).find((x) => x.field === fieldRef(f.name))!;
        if (e.type === 'quantitative') return v!.trim() !== '' && Number.isFinite(Number(v));
        if (e.type === 'temporal') return v!.trim() !== '' && !Number.isNaN(Date.parse(v!));
        return true;
      })).length;
      const { view, drawn } = await run(c.spec, c.csv);
      view.finalize();
      checked++;
      rowsChecked += plottable;
      // Lines keep rows with an empty value (they break the path there), so a line may hold more.
      if (drawn < plottable) problems.push(`${c.dataset.name}: drew ${drawn} of ${plottable} plottable rows`);
    }
    expect(problems).toEqual([]);
    // Coverage: unaggregated charts came up, with their rows.
    expect(checked).toBeGreaterThan(COVERAGE.unaggregated);
    expect(rowsChecked).toBeGreaterThan(COVERAGE.rows);
  }, 120_000);

  test('(b) no double counting: every rendered sum equals the sum of its bucket’s distinct rows', async () => {
    const problems: string[] = [];
    let summed = 0;
    let compared = 0;
    for (const c of charts) {
      const enc = encodingOf(c.spec);
      if (enc.y?.aggregate !== 'sum') continue;
      summed++;
      const rows = vega.read(c.csv, { type: 'csv' }) as Datum[];
      // Independently: the rows grouped as the chart draws them (series, bucket), summed.
      const expected = drawnSeries(c.dataset, c.spec, rows);
      const { view } = await run(c.spec, c.csv);
      const rendered = renderedValues(view, c.spec, c.dataset);
      view.finalize();
      for (const [series, points] of expected?.series ?? []) {
        const name = series.replace(/#\d+$/, '');
        for (const [t, v] of points) {
          compared++;
          const got = rendered.get(`${name}|${t}`);
          if (got === undefined || Math.abs(got - v) > 1e-6 * Math.max(1, Math.abs(v))) problems.push(`${c.dataset.name}: ${name} at ${t} drew ${got}, the rows sum to ${v}`);
        }
      }
      // And no entity twice in a bucket: the rows summed are distinct groups, not repeats.
      const m = fieldOf(c.dataset, enc.y)!;
      const x = fieldOf(c.dataset, enc.x);
      const color = fieldOf(c.dataset, enc.color);
      // Every field but the measures (number fields): categories and codes alike name an entity.
      const others = c.dataset.fields.filter((f) => f !== m && f !== x && f !== color && f.type !== 'number');
      const seen = new Set<string>();
      for (const r of rows) {
        const k = JSON.stringify([x ? String(r[x.name]) : '', color ? String(r[color.name]) : '', ...others.map((f) => String(r[f.name]))]);
        if (seen.has(k) && others.length) problems.push(`${c.dataset.name}: the same entity twice (${k})`);
        seen.add(k);
      }
    }
    expect(problems.slice(0, 20)).toEqual([]);
    // Coverage: summed charts came up, and their buckets were compared.
    expect(summed).toBeGreaterThanOrEqual(10);
    expect(compared).toBeGreaterThanOrEqual(150);
  }, 300_000);

  test('(c) time-zone independence: each bucket’s value is the same in New York, Tokyo and UTC', () => {
    const timed = charts.filter((c) => Object.values(encodingOf(c.spec)).some((e) => e.timeUnit));
    // Coverage: bucketed charts, month-end dates among them (a zone moves them across a month).
    expect(timed.length).toBeGreaterThan(10);
    const input = JSON.stringify(timed.map((c) => ({ spec: c.spec, csv: c.csv })));
    const inZone = (tz: string) => JSON.parse(execFileSync(process.execPath, [here('./property/render.mjs')], { input, env: { ...process.env, TZ: tz }, maxBuffer: 1 << 28 }).toString()) as Record<string, number>[];
    const [ny, tokyo, utc] = ['America/New_York', 'Asia/Tokyo', 'UTC'].map(inZone);
    const buckets = utc!.reduce((n, m) => n + Object.keys(m ?? {}).length, 0);
    expect(buckets).toBeGreaterThan(500);
    const differ = timed.flatMap((c, i) => (JSON.stringify(ny![i]) !== JSON.stringify(utc![i]) || JSON.stringify(tokyo![i]) !== JSON.stringify(utc![i]) ? [c.dataset.name] : []));
    expect(differ).toEqual([]);
  }, 180_000);

  test('(d) the phone gate: over the row or byte limit, a table waits for a button', () => {
    // Read loosely, so the test also runs (and fails) on code before the byte limit joined the gate.
    const lib = largeData as unknown as { loadGate: (rows: number, bytes: number, map: boolean) => { button: boolean; autoDraw: string }; AUTO_LOAD_BYTES?: number };
    const gate = lib.loadGate;
    const LIMIT = lib.AUTO_LOAD_BYTES ?? 3e6;
    const rng = (() => { let s = SEED; return () => ((s = (s * 1103515245 + 12345) % 2 ** 31) / 2 ** 31); })();
    const cases = { button: 0, drawn: 0, map: 0, bytes: 0 };
    for (let i = 0; i < 500; i++) {
      const rows = Math.floor(rng() ** 3 * 300_000);
      const bytes = Math.floor(rng() ** 2 * 12e6);
      const map = rng() < 0.2;
      const g = gate(rows, bytes, map);
      const heavy = largeData.rowBand(rows) !== 'svg' || bytes > LIMIT;
      expect(g.button, `${rows} rows, ${bytes} bytes, map ${map}`).toBe(!map && heavy);
      // A phone never draws a gated table by itself; only the canvas band of a small file draws itself on a desktop.
      if (g.button) expect(g.autoDraw === 'never' || (g.autoDraw === 'desktop' && largeData.rowBand(rows) === 'canvas' && bytes <= LIMIT)).toBe(true);
      cases[g.button ? 'button' : 'drawn']++;
      if (map && heavy) cases.map++;
      if (bytes > LIMIT && largeData.rowBand(rows) === 'svg') cases.bytes++;
    }
    // Coverage: both sides of the gate, maps (never gated) and files heavy by bytes alone.
    expect(Math.min(cases.button, cases.drawn)).toBeGreaterThan(50);
    expect(Math.min(cases.map, cases.bytes)).toBeGreaterThan(10);
  });
});

/** How many colors a nominal color encoding's scale has: its range, else its scheme (Vega's default, tableau10). */
function capacity(color: Spec): number {
  const scale = (color.scale ?? {}) as { range?: unknown[]; scheme?: string };
  if (Array.isArray(scale.range)) return scale.range.length;
  return scale.scheme === 'tableau20' || scale.scheme === 'category20' ? 20 : 10;
}

/** A spec's nominal color encoding, if any (with a condition, the field part). */
function nominalColor(spec: Spec): Spec | null {
  const color = encodingOf(spec).color;
  return color && color.field && (color.type === 'nominal' || color.type === 'ordinal') ? color : null;
}

describe('(e) distinct colors: never more color values than the scheme has colors', () => {
  test('in every generated chart', () => {
    let colored = 0;
    let full = 0;
    const problems = charts.flatMap((c) => {
      const color = nominalColor(c.spec);
      const f = fieldOf(c.dataset, color ?? undefined);
      if (!color || !f) return [];
      const values = new Set(c.rows.map((r) => r[f.name]));
      colored++;
      if (values.size >= capacity(color) - 3) full++;
      return values.size > capacity(color) ? [`${c.dataset.name}: ${values.size} values of ${f.name}, ${capacity(color)} colors`] : [];
    });
    expect(problems).toEqual([]);
    // Coverage: colored charts came up, some near the palette's limit.
    expect(colored).toBeGreaterThan(COVERAGE.colored);
    expect(full).toBeGreaterThan(COVERAGE.nearLimit);
  });

  test('in every real dataset’s starter and Explore charts', () => {
    let colored = 0;
    const problems = loadCatalog().datasets.flatMap((d) => {
      const sf = scatterFields(d);
      const specs = [starterChart(d), ...(sf && exploreModes(d).includes('scatter') ? [scatterSpec(d, sf, { ...defaultAxes(d, sf), zoom: true, height: 380 })] : [])];
      return specs.flatMap((spec) => {
        const color = spec ? nominalColor(spec) : null;
        const f = fieldOf(d, color ?? undefined);
        if (!color || !f || f.profile.kind === 'empty') return [];
        const values = f.profile.kind === 'nominal' ? f.profile.distinct + (f.profile.missing ? 1 : 0) : (f.profile as { distinct?: number }).distinct ?? 0;
        colored++;
        return values > capacity(color) ? [`${d.name}: ${values} values of ${f.name}, ${capacity(color)} colors`] : [];
      });
    });
    expect(problems).toEqual([]);
    expect(colored).toBeGreaterThan(10);
  });
});

type Datum = Record<string, unknown>;

/**
 * A time chart's series as drawn, computed here from the rows (independently of the
 * builder): per series (the color field's value, or one), the times in order (bucketed by
 * the spec's time unit, in UTC or local time as the spec says) and the measure there
 * (averaged, or summed, as the spec aggregates), with missing values left out.
 */
function drawnSeries(d: Dataset, spec: Spec, rows: Datum[]): { series: Map<string, [number, number][]>; whole: Map<string, [number, number][]>; times: number[]; log: boolean } | null {
  const enc = encodingOf(spec);
  const x = enc.x;
  const y = enc.y;
  const t = fieldOf(d, x);
  const m = fieldOf(d, y);
  if (!x || !y || !t || !m || y.type !== 'quantitative' || !['temporal', 'quantitative'].includes(String(x.type))) return null;
  const color = enc.color?.type === 'nominal' ? fieldOf(d, enc.color) : undefined;
  const unit = x.timeUnit as string | undefined;
  const utc = unit?.startsWith('utc');
  const base = unit?.replace(/^utc/, '');
  const markersOf = (f: Field) => markerForms(f, effectiveMissing(d, f) ?? []);
  const time = (v: unknown): number | null => {
    if (v === null || v === undefined || v === '' || markersOf(t).includes(String(v))) return null;
    if (x.type === 'quantitative') {
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    }
    const ms = v instanceof Date ? v.getTime() : Date.parse(String(v));
    if (Number.isNaN(ms)) return null;
    if (!base) return ms;
    const dt = new Date(ms);
    const [Y, M, D] = utc ? [dt.getUTCFullYear(), dt.getUTCMonth(), dt.getUTCDate()] : [dt.getFullYear(), dt.getMonth(), dt.getDate()];
    const parts: [number, number, number] = base === 'year' ? [Y, 0, 1] : base === 'yearmonth' ? [Y, M, 1] : [Y, M, D];
    return utc ? Date.UTC(...parts) : new Date(...parts).getTime();
  };
  const cells = new Map<string, number[]>();
  // The parts' chart leaves out a detected total (its own mode, S2).
  const total = totalOf(d);
  const drawnHere = (r: Datum) => !(total && (spec.usermeta as { chart?: string } | undefined)?.chart !== 'total' && total.totals.includes(String(r[total.series.name] ?? '')));
  for (const r of rows) {
    if (!drawnHere(r)) continue;
    const tv = time(r[t.name]);
    const raw = r[m.name];
    // A documented marker's row is filtered out; an empty or unreadable value is a null the line breaks at.
    if (tv === null || markersOf(m).includes(String(raw))) continue;
    const v = raw === null || raw === undefined || String(raw).trim() === '' ? NaN : Number(raw);
    const k = JSON.stringify([color ? String(r[color.name] ?? '') : '', tv]);
    cells.set(k, [...(cells.get(k) ?? []), v]);
  }
  const series = new Map<string, [number, number][]>();
  for (const [k, vs] of cells) {
    const [c, tv] = JSON.parse(k) as [string, number];
    const valid = vs.filter((v) => Number.isFinite(v));
    const sum = valid.reduce((a, b) => a + b, 0);
    series.set(c, [...(series.get(c) ?? []), [tv, valid.length ? (y.aggregate === 'sum' ? sum : sum / valid.length) : NaN]]);
  }
  // Each series whole (its nulls left out), and split where a null (every value empty) breaks the line.
  const whole = new Map([...series].map(([c, s]) => [c, s.filter((p) => Number.isFinite(p[1])).sort((a, b) => a[0] - b[0])]));
  for (const [c, s] of [...series]) {
    s.sort((a, b) => a[0] - b[0]);
    const runs: [number, number][][] = [[]];
    for (const p of s) (Number.isFinite(p[1]) ? runs.at(-1)!.push(p) : runs.push([]));
    series.delete(c);
    runs.filter((r) => r.length).forEach((r, i) => series.set(`${c}#${i}`, r));
  }
  const times = [...new Set([...series.values()].flat().map(([tv]) => tv))].sort((a, b) => a - b);
  return { series, whole, times, log: (y.scale as { type?: string } | undefined)?.type === 'log' };
}

const median = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  return s.length ? s[Math.floor((s.length - 1) / 2)]! : 0;
};
const quantileLow = (xs: number[], q: number) => {
  const s = [...xs].sort((a, b) => a - b);
  return s.length ? s[Math.floor(q * (s.length - 1))]! : 0;
};

/** How often each standard's case came up (a coverage guard: a property that never runs proves nothing). */
const seen = { lines: 0, colored: 0, gapped: 0, segmented: 0, pointsForJagged: 0, fewPoints: 0 };

/** The standards' problems in one time chart, from its rows. */
function standardsProblems(name: string, d: Dataset, spec: Spec, rows: Datum[], phone: boolean): string[] {
  const unit = (spec.spec ?? (spec.layer as Spec[] | undefined)?.at(-1) ?? spec) as Spec;
  const mark = (typeof unit.mark === 'string' ? unit.mark : (unit.mark as { type?: string } | undefined)?.type) ?? '';
  if (mark !== 'line' && mark !== 'point') return [];
  const drawn = drawnSeries(d, spec, rows);
  if (!drawn) return [];
  const value = (v: number) => (drawn.log ? Math.log10(v) : v);
  const all = [...drawn.whole.values()].flat().map(([, v]) => value(v));
  const range = Math.max(...all) - Math.min(...all);
  const steps = [...drawn.whole.values()].filter((s) => s.length > 1).map((s) => median(s.slice(1).map(([, v], i) => Math.abs(value(v) - value(s[i]![1])))));
  // (Fewer than three times make no trend to judge; the builder doesn't judge them either.)
  const jag = range > 0 && steps.length && drawn.times.length >= 3 ? median(steps) / range : 0;
  if (mark === 'point') {
    if (jag > JAGGED) seen.pointsForJagged++;
    return [];
  }
  seen.lines++;
  const problems: string[] = [];
  // S8: straight segments (a curve invents peaks and troughs), and the values marked when a line has few.
  const markSpec = (typeof unit.mark === 'object' ? unit.mark : {}) as { interpolate?: string; point?: unknown };
  if (markSpec.interpolate && markSpec.interpolate !== 'linear') problems.push(`${name}: ${markSpec.interpolate} interpolation (S8)`);
  const longest = Math.max(0, ...[...drawn.whole.values()].map((s) => s.length));
  if (longest <= FEW_POINTS) {
    seen.fewPoints++;
    if (!markSpec.point) problems.push(`${name}: ${longest} values on a line, unmarked (S8)`);
  }
  // S1: at most six colored lines (four on a phone).
  const limit = phone ? SERIES_LIMIT.phone : SERIES_LIMIT.wide;
  if (encodingOf(spec).color?.type === 'nominal') {
    seen.colored++;
    const colors = new Set([...drawn.series.keys()].map((k) => k.replace(/#\d+$/, ''))).size;
    if (colors > limit) problems.push(`${name}: ${colors} colored lines (S1, at most ${limit})`);
  }
  // S3: a series' neighbouring times farther apart than 1.5 times the 90th-percentile gap of
  // the whole time axis is a gap: the line must break there (the segment transform), or not be a line.
  const axisGaps = drawn.times.slice(1).map((tv, i) => tv - drawn.times[i]!);
  const breakAt = 1.5 * quantileLow(axisGaps, 0.9);
  const segmented = JSON.stringify(spec.transform ?? []).includes('"op":"lag"');
  const gapped = [...drawn.series.values()].some((s) => s.slice(1).some(([tv], i) => tv - s[i]![0] > breakAt * 1.0001));
  if (gapped) seen.gapped++;
  if (segmented) seen.segmented++;
  if (gapped && !segmented) problems.push(`${name}: a line crosses a gap (S3)`);
  // S5: the median step within series over the range (log values on a log axis).
  if (jag > JAGGED + 0.005) problems.push(`${name}: lines too jagged (S5: ${jag.toFixed(2)})`);
  return problems;
}

describe('chart standards (site/CHART-STANDARDS.md)', () => {
  test('S1, S3, S5 on every generated chart, wide and on a phone', () => {
    const problems = charts.flatMap((c) => {
      const rows = vega.read(c.csv, { type: 'csv' }) as Datum[];
      return [false, true].flatMap((phone) => {
        const spec = starterChart(c.dataset, phone);
        return spec ? standardsProblems(`${c.dataset.name}${phone ? ' (phone)' : ''}`, c.dataset, spec, rows, phone) : [];
      });
    });
    expect(problems).toEqual([]);
    // Coverage: the cases each standard protects came up.
    expect(seen.lines).toBeGreaterThan(50);
    expect(seen.colored).toBeGreaterThan(20);
    expect(seen.gapped).toBeGreaterThan(5);
    expect(seen.pointsForJagged).toBeGreaterThan(5);
    expect(seen.fewPoints).toBeGreaterThan(20);
  }, 60_000); // Renders every chart twice (wide, phone): about 2 s alone.

  test('S1, S3, S5 on every real dataset’s Explore charts, wide and on a phone', () => {
    const problems = loadCatalog().datasets.flatMap((d) => {
      if ((d.bytes ?? 0) > 3e6 || d.kind !== 'table' || !['csv', 'tsv', 'json'].includes(d.format)) return [];
      const rows = vega.read(readDataUrl(d.url), { type: d.format }) as Datum[];
      return exploreModes(d)
        .filter((m) => m !== 'scatter')
        .flatMap((mode) =>
          [false, true].flatMap((phone) => {
            const spec = modeChart(d, mode, phone);
            return spec ? standardsProblems(`${d.name} ${mode}${phone ? ' (phone)' : ''}`, d, spec, rows, phone) : [];
          }),
        );
    });
    expect(problems).toEqual([]);
  }, 60_000); // Renders every chart twice (wide, phone): about 2 s alone.

  test('S2: a detected total never shares axes with its parts; it has its own mode', () => {
    const datasets = [...charts.map((c) => c.dataset), ...loadCatalog().datasets];
    const problems = datasets.flatMap((d) => {
      const total = totalOf(d);
      if (!total) return [];
      const spec = starterChart(d)!;
      const excluded = filters(spec).some((f) => total.totals.every((v) => f.includes(JSON.stringify(`v:${v}`).slice(1, -1))) && f.endsWith('< 0'));
      return [...(excluded ? [] : [`${d.name}: its chart draws the total with the parts`]), ...(exploreModes(d).includes('total') ? [] : [`${d.name}: no Total mode`])];
    });
    expect(problems).toEqual([]);
    expect(datasets.some((d) => totalOf(d)?.series.name === 'Entity')).toBe(true);
  });

  test('S4: log only on positive values, symlog only where zero is in range, a heavy tail (or its mean) on its scale', () => {
    const scales = { log: 0, symlog: 0 };
    // Each mode's chart, and for the scatter plot the fields its pickers open on (its x and y read params).
    const specsOf = (d: Dataset): [string, Spec, Record<string, string>][] => {
      const sf = scatterFields(d);
      return exploreModes(d).flatMap((mode): [string, Spec, Record<string, string>][] => {
        const axes = mode === 'scatter' && sf ? defaultAxes(d, sf) : null;
        const spec = axes ? scatterSpec(d, sf!, { ...axes, zoom: false, height: 300 }) : modeChart(d, mode, false);
        return spec ? [[`${d.name} ${mode}`, spec, axes ?? {}]] : [];
      });
    };
    const problems = [...charts.map((c) => c.dataset), ...loadCatalog().datasets].flatMap((d) =>
      specsOf(d).flatMap(([name, spec, picked]) =>
        // Every layer's encoding (the scatter plot's points are its first layer, under its titles).
        [spec.spec as Spec | undefined, ...((spec.layer as Spec[] | undefined) ?? []), spec].flatMap((u) => Object.entries((u?.encoding ?? {}) as Record<string, Spec>)).flatMap(([channel, e]) => {
          const f = picked[channel] ? d.fields.find((x) => x.name === picked[channel]) : fieldOf(d, e);
          if (!f || f.profile.kind !== 'quantitative' || e.type !== 'quantitative') return [];
          const type = (e.scale as { type?: string } | undefined)?.type ?? 'linear';
          if (type === 'log') scales.log++;
          if (type === 'symlog') scales.symlog++;
          const out: string[] = [];
          if (type === 'log' && !(f.profile.min > 0)) out.push(`${name}: log ${channel} on ${f.name}, whose values reach ${f.profile.min}`);
          const documentedMin = (f.constraints as { minimum?: number } | undefined)?.minimum;
          if (type === 'symlog' && f.profile.min > 0 && !(documentedMin !== undefined && documentedMin <= 0)) out.push(`${name}: symlog ${channel} on ${f.name}, all positive`);
          // A mean stays within the measure's values: it keeps the scale too (a sum or count doesn't).
          if (['x', 'y'].includes(channel) && (!e.aggregate || e.aggregate === 'mean') && !e.bin && scaleType(f) !== 'linear' && type !== scaleType(f)) out.push(`${name}: ${f.name} (heavy-tailed, ${scaleType(f)}) on a ${type} ${channel}${e.aggregate ? ` (${e.aggregate})` : ''}`);
          return out;
        }),
      ),
    );
    expect(problems).toEqual([]);
    // Coverage: log and symlog axes came up.
    expect(scales.log).toBeGreaterThan(5);
    expect(scales.symlog).toBeGreaterThan(2);
  });
});
