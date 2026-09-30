/**
 * Compile a Vega-Lite spec when the site is built, failing on any warning: a warning means
 * the chart isn't drawn the way its spec says (a dropped fit, an ignored property), and the
 * build is where to find out.
 */
import { compile, type TopLevelSpec } from "vega-lite";

type Spec = Record<string, unknown>;

export function compileStrict(spec: Spec, config: Record<string, unknown>, what: string): ReturnType<typeof compile> {
  const warnings: string[] = [];
  const logger = {
    level: () => logger,
    error: (...m: unknown[]) => {
      throw new Error(`${what}: ${m.join(" ")}`);
    },
    warn: (...m: unknown[]) => {
      warnings.push(m.join(" "));
      return logger;
    },
    info: () => logger,
    debug: () => logger,
  };
  const out = compile(spec as unknown as TopLevelSpec, { config: config as never, logger: logger as never });
  if (warnings.length) throw new Error(`Vega-Lite warned compiling ${what}: ${warnings.join("; ")}`);
  return out;
}
