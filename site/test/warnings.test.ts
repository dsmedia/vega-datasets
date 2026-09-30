// Every Vega-Lite spec the site generates for a dataset page compiles without a warning:
// the page's Explore charts (each mode, at a wide and a phone size), the starter chart the
// Editor opens, and a long table's density overview. The build compiles them the same way
// (prerender/compile.ts), and fails on a warning too.
import * as vega from 'vega';
import { compile, type TopLevelSpec } from 'vega-lite';
import { describe, expect, test } from 'vitest';
import type { Dataset } from '../src/lib/catalog';
import { defaultAxes, discreteHeight, exploreModes, hasDensity, scatterFields, scatterSpec, starterChart } from '../src/lib/explore-model';
import { densityPageSpec, densitySpec } from '../src/lib/large-data';
import { starterSpec } from '../src/lib/starter';
import { densityOf } from '../src/prerender/density';
import { themeConfig } from '../src/lib/vega-theme';
import { loadCatalog, readDataUrl } from './catalog';

type Spec = Record<string, unknown>;

/** The warnings Vega-Lite logs compiling `spec` with the site's theme (its colors don't change what warns). */
function warningsOf(spec: Spec): string[] {
  const warnings: string[] = [];
  const logger = {
    level: () => logger,
    error: (...m: unknown[]) => { throw new Error(m.join(' ')); },
    warn: (...m: unknown[]) => { warnings.push(m.join(' ')); return logger; },
    info: () => logger,
    debug: () => logger,
  };
  compile(spec as TopLevelSpec, { logger: logger as never, config: themeConfig(() => '#808080') as never });
  return warnings;
}

/** The specs a dataset's page draws or links to, labeled. */
function pageSpecs(d: Dataset): [string, Spec][] {
  const out: [string, Spec][] = [];
  const starter = starterSpec(d);
  if (starter) out.push(['starter', starter]);
  const sf = scatterFields(d);
  for (const mode of exploreModes(d)) {
    for (const phone of [false, true]) {
      const size = phone ? 'phone' : 'wide';
      if (mode === 'scatter' && sf) out.push([`scatter ${size}`, scatterSpec(d, sf, { ...defaultAxes(d, sf), zoom: !phone, height: phone ? 300 : 380 })]);
      else {
        const chart = starterChart(d, phone);
        // The build measures a chart at the column's width.
        if (chart) out.push([`${mode} ${size}`, chart.facet ? chart : { ...chart, width: phone ? 358 : 880 }], [`${mode} ${size} (live)`, chart]);
      }
    }
  }
  if (hasDensity(d) && sf) {
    const grid = densityOf(d, defaultAxes(d, sf));
    out.push(['density', densitySpec(d, grid, 380)], ['density page', densityPageSpec(d, grid, 380)]);
  }
  return out;
}

describe('no Vega-Lite warnings in the specs the site generates', () => {
  const catalog = loadCatalog();
  test.each(catalog.datasets.map((d) => [d.name, d] as const))('%s', (_name, d) => {
    const found = pageSpecs(d).flatMap(([label, spec]) => warningsOf(spec).map((w) => `${label}: ${w}`));
    expect(found).toEqual([]);
  });
});

/** A spec's drawn height (the SVG's), from the real file, with warnings ignored. */
async function drawnHeight(spec: Spec): Promise<number> {
  const quiet = { level: () => quiet, error: () => quiet, warn: () => quiet, info: () => quiet, debug: () => quiet };
  const { spec: vg } = compile(spec as TopLevelSpec, { logger: quiet as never, config: themeConfig(() => '#808080') as never });
  const loader = vega.loader();
  loader.load = async (uri: string) => readDataUrl(uri);
  const view = new vega.View(vega.parse(vg), { renderer: 'none', loader });
  try {
    await view.runAsync();
    return Number((await view.toSVG()).match(/<svg [^>]*height="([\d.]+)"/)![1]);
  } finally {
    view.finalize();
  }
}

describe('a discrete y axis given its height as a number draws exactly as its step did', () => {
  const catalog = loadCatalog();
  const discrete = catalog.datasets.filter((d) => { const c = starterChart(d); return c !== null && discreteHeight(d, starterSpec(d)!) !== null; });
  test('covers the bar, dot and heatmap charts', () => expect(discrete.map((d) => d.name)).toEqual(expect.arrayContaining(['barley', 'lookup_people', 'flights_airport', 'football', 'obesity'])));
  test.each(discrete.map((d) => [d.name, d] as const))('%s', async (_name, d) => {
    const chart = starterChart(d)!;
    const { height: _h, ...stepped } = chart;
    for (const width of [880, 358]) expect(await drawnHeight({ ...chart, width })).toBe(await drawnHeight({ ...stepped, width }));
  }, 60_000);
});
