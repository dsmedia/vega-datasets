/** Draw a Vega-Lite spec the way the page does (the expression interpreter, no eval), for tests. */
import * as vega from 'vega';
import { expressionInterpreter } from 'vega-interpreter';
import { compile, type TopLevelSpec } from 'vega-lite';
import { expect } from 'vitest';

type Spec = Record<string, unknown>;

/** Compile to Vega (no warnings) and run it on `rows` in place of the spec's file, with the page's CSP-safe settings. */
export async function draw(spec: Spec, rows: object[], signals: Record<string, unknown> = {}): Promise<vega.View> {
  const warnings: string[] = [];
  const logger = {
    level: () => logger,
    error: (...m: unknown[]) => { throw new Error(m.join(' ')); },
    warn: (...m: unknown[]) => { warnings.push(m.join(' ')); return logger; },
    info: () => logger,
    debug: () => logger,
  };
  // The rows replace the file; the spec's own data format (its parse) still applies.
  const { url: _, ...data } = (spec.data ?? {}) as Record<string, unknown>;
  const { spec: vg } = compile({ ...spec, data: { ...data, values: rows } } as TopLevelSpec, { logger: logger as never });
  expect(warnings).toEqual([]);
  const view = new vega.View(vega.parse(vg, undefined, { ast: true }), { renderer: 'none', expr: expressionInterpreter } as vega.ViewOptions);
  for (const [k, v] of Object.entries(signals)) view.signal(k, v);
  await view.runAsync();
  return view;
}

/** Every row of every dataset in the view that has `key` (e.g. an aggregate's output field). */
export function rowsWith(view: vega.View, key: string): Record<string, unknown>[] {
  const names = (view.getState({ data: vega.truthy, signals: vega.falsy }).data ?? {}) as Record<string, unknown>;
  return Object.keys(names).flatMap((n) => (view.data(n) as Record<string, unknown>[]).filter((r) => key in r));
}

/** Every rendered legend label (non-empty) and whether it lies within its legend's gradient, along the gradient. */
export function legendLabelsOutside(view: vega.View): string[] {
  type Item = { role?: string; marktype?: string; items?: Item[]; x?: number; y?: number; bounds?: { x1: number; x2: number; y1: number; y2: number }; text?: string; opacity?: number };
  const out: string[] = [];
  const legends: { gradient?: { x1: number; x2: number; y1: number; y2: number }; labels: { text: string; x: number; y: number }[] }[] = [];
  // A mark (role, marktype) holds its items (no role, no marktype): both pass down.
  const walk = (node: Item, ox: number, oy: number, role: string | undefined, type: string | undefined, legend: (typeof legends)[number] | undefined) => {
    for (const item of node.items ?? []) {
      const r = item.role ?? role;
      const t = item.marktype ?? type;
      let current = legend;
      if (item.role === 'legend' && item.marktype === 'group') legends.push((current = { labels: [] }));
      // A group item (no marktype of its own under a group mark) moves its children.
      const isGroupItem = !item.marktype && t === 'group';
      const [x, y] = isGroupItem ? [ox + (item.x ?? 0), oy + (item.y ?? 0)] : [ox, oy];
      if (current && r === 'legend-gradient' && !item.marktype && t === 'rect' && item.bounds) current.gradient = { x1: ox + item.bounds.x1, x2: ox + item.bounds.x2, y1: oy + item.bounds.y1, y2: oy + item.bounds.y2 };
      if (current && r === 'legend-label' && !item.marktype && t === 'text' && item.text && (item.opacity ?? 1) > 0 && item.bounds) {
        current.labels.push({ text: String(item.text), x: ox + (item.bounds.x1 + item.bounds.x2) / 2, y: oy + (item.bounds.y1 + item.bounds.y2) / 2 });
      }
      walk(item, x, y, r, t, current);
    }
  };
  walk((view.scenegraph() as unknown as { root: Item }).root, 0, 0, undefined, undefined, undefined);
  for (const l of legends) {
    if (!l.gradient) continue;
    const vertical = l.gradient.y2 - l.gradient.y1 > l.gradient.x2 - l.gradient.x1;
    for (const label of l.labels) {
      const [at, lo, hi] = vertical ? [label.y, l.gradient.y1, l.gradient.y2] : [label.x, l.gradient.x1, l.gradient.x2];
      if (at < lo - 2 || at > hi + 2) out.push(`"${label.text}" at ${Math.round(at)}, gradient ${Math.round(lo)} to ${Math.round(hi)}`);
    }
  }
  return out;
}

