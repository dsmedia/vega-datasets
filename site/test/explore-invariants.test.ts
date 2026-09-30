// Invariants of the Explore rules on a few hundred random tables (seeded, so reproducible),
// generated and profiled by the real catalog builder (property/tables.py):
//   (a) no data loss: the only filters a chart applies are its fields' documented missing
//       values and unplottable cells, and a chart that doesn't aggregate draws every other row;
//   (b) no double counting: a summed value is the sum of distinct rows of its bucket (no
//       entity twice in a bucket, no total summed with its parts);
//   (c) time-zone independence: bucketed values are the same in New York, Tokyo and UTC;
//   (d) the phone gate: a table over the row or byte limit never loads on a phone without a button;
//   (e) distinct colors: a color encoding never has more values than its scheme has colors.
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import * as vega from 'vega';
import { compile, type TopLevelSpec } from 'vega-lite';
import { describe, expect, test } from 'vitest';
import { type Dataset, effectiveMissing, type Field } from '../src/lib/catalog';
import { defaultAxes, exploreModes, scatterFields, scatterSpec, starterChart } from '../src/lib/explore-model';
import { loadCatalog } from './catalog';
import * as largeData from '../src/lib/large-data';
import { fieldRef, markerForms } from '../src/lib/starter';

type Spec = Record<string, unknown>;
type Row = Record<string, string>;
interface Case { dataset: Dataset; csv: string }

const SEED = 7;
const COUNT = 240;
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
async function run(spec: Spec, csv: string): Promise<{ view: vega.View; drawn: number }> {
  const quiet = { level: () => quiet, error: () => quiet, warn: () => quiet, info: () => quiet, debug: () => quiet };
  const { spec: vg } = compile(spec as TopLevelSpec, { logger: quiet as never });
  const loader = vega.loader();
  loader.load = async () => csv;
  const view = new vega.View(vega.parse(vg), { renderer: 'none', loader });
  await view.runAsync();
  const source = markSource((vg as { marks?: VgMark[] }).marks);
  return { view, drawn: source ? (view.data(source) as unknown[]).length : 0 };
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
  });

  test('(a) no data loss: a chart that doesn’t aggregate draws every plottable row', async () => {
    const problems: string[] = [];
    for (const c of charts) {
      const enc = encodingOf(c.spec);
      if (Object.values(enc).some((e) => e.aggregate || e.timeUnit || e.bin) || c.spec.facet || c.spec.layer) continue;
      const encoded = Object.values(enc).map((e) => fieldOf(c.dataset, e)).filter((f): f is Field => !!f);
      const plottable = c.rows.filter((r) => encoded.every((f) => {
        const v = r[f.name];
        if (isMissing(c.dataset, f, v)) return false;
        const e = Object.values(enc).find((x) => x.field === fieldRef(f.name))!;
        if (e.type === 'quantitative') return v!.trim() !== '' && Number.isFinite(Number(v));
        if (e.type === 'temporal') return v!.trim() !== '' && !Number.isNaN(Date.parse(v!));
        return true;
      })).length;
      const { view, drawn } = await run(c.spec, c.csv);
      view.finalize();
      // Lines keep rows with an empty value (they break the path there), so a line may hold more.
      if (drawn < plottable) problems.push(`${c.dataset.name}: drew ${drawn} of ${plottable} plottable rows`);
    }
    expect(problems).toEqual([]);
  }, 120_000);

  test('(b) no double counting: a sum never adds a total to its parts, nor an entity twice in a bucket', () => {
    const problems: string[] = [];
    for (const c of charts) {
      const enc = encodingOf(c.spec);
      const y = enc.y;
      if (y?.aggregate !== 'sum') continue;
      const m = fieldOf(c.dataset, y)!;
      const x = fieldOf(c.dataset, enc.x);
      const color = fieldOf(c.dataset, enc.color);
      const unit = enc.x?.timeUnit as string | undefined;
      const bucket = (v: string) => {
        if (!unit) return v;
        const t = new Date(v);
        const utc = unit.startsWith('utc');
        const [Y, M, D] = utc ? [t.getUTCFullYear(), t.getUTCMonth(), t.getUTCDate()] : [t.getFullYear(), t.getMonth(), t.getDate()];
        const base = unit.replace(/^utc/, '');
        return base === 'year' ? `${Y}` : base === 'yearmonth' ? `${Y}-${M}` : `${Y}-${M}-${D}`;
      };
      const summedOver = c.dataset.fields.filter((f) => f !== m && f !== x && f !== color && !(f.profile.kind === 'quantitative' && f.type === 'number'));
      const groups = new Map<string, Row[]>();
      for (const r of c.rows) {
        if (isMissing(c.dataset, m, r[m.name]) || r[m.name]!.trim() === '' || (x && (r[x.name] ?? '') === '')) continue;
        const k = JSON.stringify([x ? bucket(r[x.name]!) : '', color ? r[color.name] : '']);
        groups.set(k, [...(groups.get(k) ?? []), r]);
      }
      // No entity twice in a bucket.
      for (const [k, rs] of groups) {
        const entities = rs.map((r) => JSON.stringify(summedOver.map((f) => r[f.name])));
        if (new Set(entities).size < entities.length) problems.push(`${c.dataset.name}: an entity twice in bucket ${k}`);
      }
      // No total summed with its parts: a value equal to the others' sum in 90% of 3+ groups.
      for (const g of summedOver) {
        const byValue = new Map<string, { groups: number; close: number }>();
        for (const rs of groups.values()) {
          const rows = rs.filter((r) => (r[g.name] ?? '') !== '');
          if (rows.length < 3) continue;
          const sum = rows.reduce((a, r) => a + Number(r[m.name]), 0);
          for (const r of rows) {
            const v = Number(r[m.name]);
            const s = byValue.get(r[g.name]!) ?? { groups: 0, close: 0 };
            s.groups++;
            if (Math.abs(v - (sum - v)) <= 0.01 * Math.abs(sum - v) + 1e-9) s.close++;
            byValue.set(r[g.name]!, s);
          }
        }
        for (const [v, s] of byValue) if (s.groups >= 3 && s.close >= 0.9 * s.groups) problems.push(`${c.dataset.name}: sums ${g.name} = ${v}, a total of the others`);
      }
    }
    expect(problems).toEqual([]);
  });

  test('(c) time-zone independence: bucketed values are the same in New York, Tokyo and UTC', () => {
    const timed = charts.filter((c) => Object.values(encodingOf(c.spec)).some((e) => e.timeUnit));
    expect(timed.length).toBeGreaterThan(5);
    const input = JSON.stringify(timed.map((c) => ({ spec: c.spec, csv: c.csv })));
    const inZone = (tz: string) => JSON.parse(execFileSync(process.execPath, [here('./property/render.mjs')], { input, env: { ...process.env, TZ: tz }, maxBuffer: 1 << 28 }).toString()) as unknown[];
    const [ny, tokyo, utc] = ['America/New_York', 'Asia/Tokyo', 'UTC'].map(inZone);
    const differ = timed.flatMap((c, i) => (JSON.stringify(ny![i]) !== JSON.stringify(utc![i]) || JSON.stringify(tokyo![i]) !== JSON.stringify(utc![i]) ? [c.dataset.name] : []));
    expect(differ).toEqual([]);
  }, 180_000);

  test('(d) the phone gate: over the row or byte limit, a table waits for a button', () => {
    // Read loosely, so the test also runs (and fails) on code before the byte limit joined the gate.
    const lib = largeData as unknown as { loadGate: (rows: number, bytes: number, map: boolean) => { button: boolean; autoDraw: string }; AUTO_LOAD_BYTES?: number };
    const gate = lib.loadGate;
    const LIMIT = lib.AUTO_LOAD_BYTES ?? 3e6;
    const rng = (() => { let s = SEED; return () => ((s = (s * 1103515245 + 12345) % 2 ** 31) / 2 ** 31); })();
    for (let i = 0; i < 500; i++) {
      const rows = Math.floor(rng() ** 3 * 300_000);
      const bytes = Math.floor(rng() ** 2 * 12e6);
      const map = rng() < 0.2;
      const g = gate(rows, bytes, map);
      const heavy = largeData.rowBand(rows) !== 'svg' || bytes > LIMIT;
      expect(g.button, `${rows} rows, ${bytes} bytes, map ${map}`).toBe(!map && heavy);
      // A phone never draws a gated table by itself; only the canvas band of a small file draws itself on a desktop.
      if (g.button) expect(g.autoDraw === 'never' || (g.autoDraw === 'desktop' && largeData.rowBand(rows) === 'canvas' && bytes <= LIMIT)).toBe(true);
    }
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
    const problems = charts.flatMap((c) => {
      const color = nominalColor(c.spec);
      const f = fieldOf(c.dataset, color ?? undefined);
      if (!color || !f) return [];
      const values = new Set(c.rows.map((r) => r[f.name]));
      return values.size > capacity(color) ? [`${c.dataset.name}: ${values.size} values of ${f.name}, ${capacity(color)} colors`] : [];
    });
    expect(problems).toEqual([]);
  });

  test('in every real dataset’s starter and Explore charts', () => {
    const problems = loadCatalog().datasets.flatMap((d) => {
      const sf = scatterFields(d);
      const specs = [starterChart(d), ...(sf && exploreModes(d).includes('scatter') ? [scatterSpec(d, sf, { ...defaultAxes(d, sf), zoom: true, height: 380 })] : [])];
      return specs.flatMap((spec) => {
        const color = spec ? nominalColor(spec) : null;
        const f = fieldOf(d, color ?? undefined);
        if (!color || !f || f.profile.kind === 'empty') return [];
        const values = f.profile.kind === 'nominal' ? f.profile.distinct + (f.profile.missing ? 1 : 0) : (f.profile as { distinct?: number }).distinct ?? 0;
        return values > capacity(color) ? [`${d.name}: ${values} values of ${f.name}, ${capacity(color)} colors`] : [];
      });
    });
    expect(problems).toEqual([]);
  });
});
