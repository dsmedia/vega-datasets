// On a phone (the Explore column is 358 px), a colored chart's legend sits above the plot,
// so the plot keeps most of the width: for every dataset's Explore chart in every mode, the
// legends take no width from the plot (it is as wide as with no legend at all), and the plot
// is at least 70% of the chart unless its own axis labels already take more (barley's site
// names beside a dot plot). Drawn from the real files, as the build measures the reserve.
import * as vega from 'vega';
import { compile, type TopLevelSpec } from 'vega-lite';
import { describe, expect, test } from 'vitest';
import type { Dataset } from '../src/lib/catalog';
import { defaultAxes, exploreModes, modeChart, scatterFields, scatterSpec } from '../src/lib/explore-model';
import { themeConfig } from '../src/lib/vega-theme';
import { loadCatalog, readDataUrl } from './catalog';

type Spec = Record<string, unknown>;
const PHONE = 358;
const SMALL = 288;

/** The phone specs of a dataset's Explore modes that have a legend. */
function phoneSpecs(d: Dataset): [string, Spec][] {
  const sf = scatterFields(d);
  return exploreModes(d).flatMap((mode): [string, Spec][] => {
    // The scatter plot's legend, when it has one, isolates a category.
    const spec = mode === 'scatter' && sf ? (sf.color ? scatterSpec(d, sf, { ...defaultAxes(d, sf), zoom: false, height: 300, phone: true }) : null) : modeChart(d, mode, true);
    if (!spec || !JSON.stringify(spec).includes('"color":{') || (spec as { projection?: unknown }).projection) return [];
    return [[mode, spec.facet ? spec : { ...spec, width: PHONE }]];
  });
}

/** The spec with no legend on any channel. */
function withoutLegends(spec: Spec): Spec {
  const unit = (u: Spec): Spec => {
    const enc = u.encoding as Record<string, Spec> | undefined;
    if (!enc) return u;
    return { ...u, encoding: Object.fromEntries(Object.entries(enc).map(([k, e]) => [k, ['color', 'strokeDash', 'shape', 'size'].includes(k) && e?.field ? { ...e, legend: null } : e])) };
  };
  if (spec.layer) return { ...spec, layer: (spec.layer as Spec[]).map(unit) };
  if (spec.spec) return { ...spec, spec: unit(spec.spec as Spec) };
  return unit(spec);
}

/** The chart's width and its plot's, drawn from the real file. */
async function widths(spec: Spec): Promise<{ chart: number; plot: number; right: number }> {
  const quiet = { level: () => quiet, error: () => quiet, warn: () => quiet, info: () => quiet, debug: () => quiet };
  const { spec: vg } = compile(spec as TopLevelSpec, { logger: quiet as never, config: themeConfig(() => '#808080') as never });
  const loader = vega.loader();
  loader.load = async (uri: string) => readDataUrl(uri);
  const view = new vega.View(vega.parse(vg), { renderer: 'none', loader });
  try {
    await view.runAsync();
    const svg = await view.toSVG();
    const chart = Number(svg.match(/<svg [^>]*width="([\d.]+)"/)![1]);
    // How far right anything is drawn (a legend's last column): the scenegraph's bounds from the origin.
    const root = (view.scenegraph() as unknown as { root: { bounds: { x2: number } } }).root;
    const right = view.origin()[0] + root.bounds.x2;
    // A facet's plot is its panels together; otherwise the view's own width.
    return { chart, plot: spec.facet ? chart : view.width(), right };
  } finally {
    view.finalize();
  }
}

describe('phone legends leave the plot most of the width', () => {
  const cases = loadCatalog().datasets.flatMap((d) => phoneSpecs(d).map(([mode, spec]) => [`${d.name} ${mode}`, spec] as const));
  test('covers the colored charts', () => expect(cases.map(([n]) => n)).toEqual(expect.arrayContaining(['disasters time', 'stocks starter', 'cars scatter', 'seattle_weather scatter', 'gapminder scatter'])));
  test.each(cases)('%s', async (_name, spec) => {
    const { chart, plot, right } = await widths(spec);
    const bare = await widths(withoutLegends(spec));
    expect(plot / bare.plot).toBeGreaterThanOrEqual(0.95);
    expect(plot / chart).toBeGreaterThanOrEqual(Math.min(0.7, (bare.plot / bare.chart) * 0.95));
    // The legend's columns fit the chart: sized from the labels it shows (documented labels
    // where the metadata has them, not the codes; Codex round 5, #2), nothing is drawn past its edge.
    expect(Math.round(right)).toBeLessThanOrEqual(Math.max(chart, bare.right) + 1);
  }, 60_000);
  // The narrowest phones (320 px, a 288 px column): the legend still fits.
  test.each(cases.filter(([, spec]) => !spec.facet))('%s at 288 px', async (_name, spec) => {
    const narrow = { ...spec, width: SMALL };
    const { chart, right } = await widths(narrow);
    const bare = await widths(withoutLegends(narrow));
    expect(Math.round(right)).toBeLessThanOrEqual(Math.max(chart, bare.right) + 1);
  }, 60_000);
});
