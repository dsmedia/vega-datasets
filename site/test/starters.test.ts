// Every "Try it in the Vega Editor" starter chart must compile and draw real marks
// against the actual data file, and the Editor link must carry exactly that spec.
import LZString from 'lz-string';
import * as vega from 'vega';
import { compile, type TopLevelSpec } from 'vega-lite';
import { describe, expect, test } from 'vitest';
import type { Dataset } from '../src/lib/catalog';
import { defaultAxes, exploreModes, scatterFields } from '../src/lib/explore-model';
import { starterEditorUrl, starterSpec } from '../src/lib/starter';
import { loadCatalog, readDataUrl } from './catalog';

const catalog = loadCatalog();
const withStarter = catalog.datasets.filter((d) => starterSpec(d) !== null);

type Unit = { mark: { type: string; filled?: boolean }; encoding?: Record<string, Record<string, unknown>>; transform?: unknown[] };
type Starter = Unit & { layer?: Unit[]; spec?: Unit; facet?: { field: string }; projection?: { type: string; fit?: unknown } };

/** The part of a starter spec that draws the data: a map's last layer, small multiples' inner spec, or the spec itself. */
function unitOf(spec: Starter): Unit {
  return spec.spec ?? spec.layer?.at(-1) ?? spec;
}

/** One line per dataset: the chart the rules picked, for reviewing rule changes in the snapshot. */
function describeStarter(d: Dataset): string {
  const spec = starterSpec(d) as Starter | null;
  if (!spec) return `${d.name}: none`;
  const unit = unitOf(spec);
  const enc = unit.encoding ?? {};
  const channel = (k: string) => {
    const e = enc[k] as Record<string, string & { type?: string }> | undefined;
    if (!e) return null;
    const field = e.field ?? '';
    const inner = e.timeUnit ? `${e.timeUnit}(${field})` : field;
    const scale = (e.scale as { type?: string } | undefined)?.type;
    const value = e.aggregate ? `${e.aggregate}(${inner})` : inner;
    return `${k}=${scale ? `${scale}(${value})` : value}`;
  };
  const channels = ['x', 'x2', 'y', 'latitude', 'longitude', 'color', 'detail', 'angle'].map(channel).filter(Boolean);
  const extras = [
    spec.facet ? `facet=${spec.facet.field}` : null,
    spec.layer ? `over basemap` : null,
    spec.projection ? `${spec.projection.type}${spec.projection.fit ? ' fitted' : ''}` : null,
    unit.mark.filled === false ? 'unfilled' : null,
    unit.transform?.length || (spec as Unit).transform?.length ? 'transformed' : null,
  ].filter(Boolean);
  return `${d.name}: ${unit.mark.type} ${[...channels, ...extras].join(' ')}`.trimEnd();
}

/** One line per dataset: Explore's modes in order and the scatter plot's opening fields. */
function describeExplore(d: Dataset): string {
  const modes = exploreModes(d);
  const sf = scatterFields(d);
  const axes = sf ? defaultAxes(d, sf) : null;
  const scatter = axes ? ` x=${axes.x} y=${axes.y}${sf!.color ? ` color=${sf!.color.name}` : ''}` : '';
  return `${d.name}: ${modes.join(', ') || 'none'}${scatter}`;
}

test('starter chart choices', () => {
  expect(catalog.datasets.map(describeStarter).join('\n')).toMatchSnapshot();
});

test('explore choices', () => {
  expect(catalog.datasets.map(describeExplore).join('\n')).toMatchSnapshot();
});

/** Field names a spec encodes, with Vega-Lite's `\\.` / `\\[` escapes removed. */
function encodedFields(spec: unknown): string[] {
  const enc = (unitOf(spec as Starter).encoding ?? {}) as Record<string, { field?: string }>;
  return Object.values(enc).flatMap((e) => (e.field ? [e.field.replace(/\\(.)/g, '$1')] : []));
}

describe.each(withStarter.map((d) => [d.name, d] as const))('%s', (_name, d) => {
  test('encodes only columns the file has', () => {
    // Also what its transforms compute (a sum per group), and a geographic feature's own id.
    const spec = starterSpec(d) as Starter;
    const computed = [...(spec.transform ?? []), ...(unitOf(spec).transform ?? [])].flatMap((t) => JSON.stringify(t).match(/"as":"[^"]+"/g) ?? []).map((m) => m.slice(6, -1));
    const columns = new Set([...d.fields.map((f) => f.name), ...computed, ...(d.kind === 'json' && /json/.test(d.format) ? ['id'] : [])]);
    expect(encodedFields(starterSpec(d)).filter((f) => !columns.has(f))).toEqual([]);
  });

  test('compiles without warnings and draws marks from the real file', async () => {
    const warnings: string[] = [];
    const logger = {
      level: () => logger,
      error: (...m: unknown[]) => { throw new Error(m.join(' ')); },
      warn: (...m: unknown[]) => { warnings.push(m.join(' ')); return logger; },
      info: () => logger,
      debug: () => logger,
    };
    const { spec } = compile(starterSpec(d) as TopLevelSpec, { logger: logger as never });
    expect(warnings).toEqual([]);

    const loader = vega.loader();
    loader.load = async (uri: string) => readDataUrl(uri);
    const view = new vega.View(vega.parse(spec), { renderer: 'none', loader });
    try {
      await view.runAsync();
      const svg = await view.toSVG();
      const marks = svg.match(/<(path|line|rect|circle)\b/g) ?? [];
      expect(marks.length).toBeGreaterThanOrEqual(3);
      expect(svg).not.toMatch(/NaN|undefined/);
    } finally {
      view.finalize();
    }
  }, 60_000); // flights_200k_json draws 200,000 points; the default 5 s is too tight on a busy CPU.

  test('the Editor link decodes to the same spec', () => {
    const url = starterEditorUrl(d);
    const encoded = url?.split('#/url/vega-lite/')[1];
    expect(encoded).toBeTruthy();
    expect(JSON.parse(LZString.decompressFromEncodedURIComponent(encoded!)!)).toEqual(starterSpec(d));
  });
});
