// @vitest-environment jsdom
// Chart colors in forced-colors mode (Windows High Contrast): the mode recolors the page's CSS
// but not a canvas's pixels or Vega's SVG attributes, so the config itself must carry the
// system colors, to every piece of chart chrome, while data marks keep their colors.
import * as vega from 'vega';
import { compile, type TopLevelSpec } from 'vega-lite';
import { afterEach, describe, expect, test, vi } from 'vitest';
import { defaultAxes, scatterFields, scatterSpec } from '../src/lib/explore-model';
import { onThemeChange } from '../src/client/theme';
import { type ChartInk, chartConfig, forcedInk, type SystemColors } from '../src/lib/vega-theme';
import { loadCatalog, readDataUrl } from './catalog';

// jsdom has no 2D canvas: it returns null from getContext, but also logs "Not implemented" when
// vega-scenegraph probes it for text metrics. Return null quietly, before vega loads.
vi.hoisted(() => {
  HTMLCanvasElement.prototype.getContext = (() => null) as typeof HTMLCanvasElement.prototype.getContext;
});

// A dark forced palette, as Chrome's emulation and Windows' "Night sky" give it.
const SYSTEM: SystemColors = {
  canvasText: 'rgb(255, 255, 255)',
  canvas: 'rgb(0, 0, 0)',
  grayText: 'rgb(166, 166, 166)',
  highlight: 'rgb(26, 235, 255)',
};
// The light theme's tokens (site.css), for comparison.
const LIGHT: ChartInk = {
  forced: false,
  ink: '#373a3c',
  strong: '#000000',
  muted: '#666666',
  rule: '#cccccc',
  grid: '#eeeeee',
  surface: '#ffffff',
  brush: '#373a3c',
};

describe('the forced-colors config', () => {
  const config = chartConfig(forcedInk(SYSTEM), 'sans-serif') as Record<string, Record<string, unknown>>;

  test('text and rules take CanvasText, grid lines GrayText, the brush Highlight', () => {
    expect(config.axis).toMatchObject({
      labelColor: SYSTEM.canvasText, titleColor: SYSTEM.canvasText,
      domainColor: SYSTEM.canvasText, tickColor: SYSTEM.canvasText, gridColor: SYSTEM.grayText,
    });
    expect(config.legend).toMatchObject({ labelColor: SYSTEM.canvasText, titleColor: SYSTEM.canvasText, symbolBaseStrokeColor: SYSTEM.canvasText });
    expect(config.title).toMatchObject({ color: SYSTEM.canvasText, subtitleColor: SYSTEM.canvasText });
    expect(config.header).toMatchObject({ labelColor: SYSTEM.canvasText, titleColor: SYSTEM.canvasText });
    expect(config.text).toMatchObject({ color: SYSTEM.canvasText });
    expect(config.selection).toMatchObject({ interval: { mark: { fill: SYSTEM.highlight, stroke: SYSTEM.highlight } } });
  });

  test('the chart has no background or view stroke of its own: the forced ground shows through', () => {
    expect(config.background).toBeNull();
    expect(config.view).toEqual({ stroke: null });
  });
});

/** Every scenegraph item of `view` with mark role `role` (and mark type `type`, if given). */
function items(view: vega.View, role: string, type?: string): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = [];
  const walk = (node: { marktype?: string; role?: string; items?: unknown[] }) => {
    for (const item of (node.items ?? []) as { items?: unknown[] }[]) {
      if (node.role === role && (!type || node.marktype === type)) out.push(item as Record<string, unknown>);
      walk(item as never);
    }
  };
  walk((view.scenegraph() as unknown as { root: never }).root);
  return out;
}

// What either renderer draws comes from the scenegraph, so this holds for canvas and SVG alike.
test("the cars scatter plot draws its chrome in the system colors and its points in the data's", async () => {
  const catalog = loadCatalog();
  const d = catalog.dataset('cars')!;
  const f = scatterFields(d)!;
  const spec = scatterSpec(d, f, { ...defaultAxes(f), zoom: true, height: 300 });
  const loader = vega.loader();
  loader.load = async (uri: string) => readDataUrl(uri);
  const draw = async (ink: ChartInk) => {
    const view = new vega.View(vega.parse(compile({ ...spec, width: 600 } as TopLevelSpec, { config: chartConfig(ink, 'sans-serif') }).spec), { renderer: 'none', loader });
    await view.runAsync();
    const scene = (role: string, key: 'fill' | 'stroke') => items(view, role).map((i) => i[key]);
    const out = {
      axisText: [...scene('axis-label', 'fill'), ...scene('axis-title', 'fill')],
      axisRules: [...scene('axis-domain', 'stroke'), ...scene('axis-tick', 'stroke')],
      grid: scene('axis-grid', 'stroke'),
      legendText: [...scene('legend-label', 'fill'), ...scene('legend-title', 'fill')],
      legendSymbols: scene('legend-symbol', 'stroke'),
      points: items(view, 'mark', 'symbol').map((i) => [i.fill, i.stroke]),
    };
    view.finalize();
    return out;
  };
  const forced = await draw(forcedInk(SYSTEM));
  const light = await draw(LIGHT);
  expect(forced.axisText.length).toBeGreaterThan(10);
  expect(new Set(forced.axisText)).toEqual(new Set([SYSTEM.canvasText]));
  expect(new Set(forced.axisRules)).toEqual(new Set([SYSTEM.canvasText]));
  expect(new Set(forced.grid)).toEqual(new Set([SYSTEM.grayText]));
  expect(forced.legendText.length).toBeGreaterThan(1);
  expect(new Set(forced.legendText)).toEqual(new Set([SYSTEM.canvasText]));
  // The data keep their colors: the legend's keys and the points are as in the light theme.
  expect(forced.legendSymbols).toEqual(light.legendSymbols);
  expect(new Set(forced.legendSymbols).size).toBe(3);
  expect(forced.points.length).toBeGreaterThan(300);
  expect(forced.points).toEqual(light.points);
});

describe('charts redraw when forced colors turn on or off', () => {
  afterEach(() => vi.unstubAllGlobals());

  test('a change of the forced-colors media query calls the listener; the same state does not', () => {
    let forced = false;
    const listeners: Record<string, (() => void)[]> = {};
    vi.stubGlobal('matchMedia', (query: string) => ({
      get matches() { return query === '(forced-colors: active)' ? forced : false; },
      media: query,
      addEventListener: (_: string, fn: () => void) => (listeners[query] ??= []).push(fn),
      removeEventListener: (_: string, fn: () => void) => { listeners[query] = (listeners[query] ?? []).filter((f) => f !== fn); },
    }));
    const fn = vi.fn();
    const stop = onThemeChange(fn);
    const fire = () => listeners['(forced-colors: active)']?.forEach((f) => f());
    fire();
    expect(fn).not.toHaveBeenCalled();
    forced = true;
    fire();
    expect(fn).toHaveBeenCalledTimes(1);
    forced = false;
    fire();
    expect(fn).toHaveBeenCalledTimes(2);
    stop();
    forced = true;
    fire();
    expect(fn).toHaveBeenCalledTimes(2);
  });
});
