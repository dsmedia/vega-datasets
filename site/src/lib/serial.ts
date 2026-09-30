/**
 * One task at a time, and requests that arrive while one waits to start join it: a burst of
 * requests during a run (a window dragged across the phone breakpoint while the chart draws)
 * runs the task once more after it, not once per request. The task sees the state as it is
 * when it starts, so the one run covers every request in the burst. `task` never rejects
 * (it handles its own errors).
 */
export function serial(task: () => Promise<void>): () => Promise<void> {
  let running: Promise<void> = Promise.resolve();
  let waiting: Promise<void> | null = null;
  return () => {
    if (waiting) return waiting;
    const next = running.then(() => {
      waiting = null;
      return task();
    });
    waiting = next;
    running = next;
    return next;
  };
}
