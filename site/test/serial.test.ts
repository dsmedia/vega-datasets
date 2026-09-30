import { describe, expect, test } from 'vitest';
import { serial } from '../src/lib/serial';

describe('serial (Explore redraws)', () => {
  test('a burst of requests during a run runs the task once more, not once each', async () => {
    let runs = 0;
    let release: () => void = () => {};
    const run = serial(async () => {
      runs++;
      if (runs === 1) await new Promise<void>((r) => (release = r));
    });
    const first = run();
    await Promise.resolve();
    // Three requests (a window dragged back and forth across the breakpoint) while the first draws.
    const burst = [run(), run(), run()];
    expect(new Set(burst).size).toBe(1);
    release();
    await Promise.all([first, ...burst]);
    expect(runs).toBe(2);
  });

  test('a request once the waiting run has started queues another run', async () => {
    const seen: number[] = [];
    let state = 0;
    const run = serial(async () => {
      seen.push(state);
      await new Promise((r) => setTimeout(r, 5));
    });
    const a = run();
    await Promise.resolve();
    state = 1;
    const b = run();
    await new Promise((r) => setTimeout(r, 1));
    // b has started (its state read); a change after that needs a run of its own.
    state = 2;
    const c = run();
    await Promise.all([a, b, c]);
    expect(seen.at(-1)).toBe(2);
  });

  test('runs never overlap', async () => {
    let active = 0;
    let most = 0;
    const run = serial(async () => {
      most = Math.max(most, ++active);
      await new Promise((r) => setTimeout(r, 2));
      active--;
    });
    await Promise.all(Array.from({ length: 5 }, (_, i) => new Promise((r) => setTimeout(r, i)).then(run)));
    expect(most).toBe(1);
  });
});
