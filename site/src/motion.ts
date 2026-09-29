/**
 * "In Motion": smooth, eased animation between a dataset's keyframes, built on the
 * easing functions and `interpolateLinear` that Vega 6.4 added to the expression
 * language. Vega-Lite's animation support for these is still in review
 * (vega/vega-lite#9916, #9914), so this is a hand-written Vega spec.
 *
 * Only gapminder has a motion chart today. Vega loads on demand, so plates
 * without one stay light.
 */
import LZString from "lz-string";
import type { Result } from "vega-embed";
import { h } from "./dom";
import { onThemeChange, reducedMotion, token } from "./theme";

type Spec = Record<string, unknown>;

export interface Country {
  country: string;
  region: string;
  fert: number[];
  life: number[];
  pop: number[];
}

const REGION: Record<number, string> = {
  0: "South Asia",
  1: "Europe & Central Asia",
  2: "Sub-Saharan Africa",
  3: "America",
  4: "East Asia & Pacific",
  5: "Middle East & North Africa",
};
export const LABELED = ["China", "India", "United States", "Japan", "Nigeria", "Brazil"];
export const FIRST_YEAR = 1955;
export const STEP_YEARS = 5;
export const SEGMENT_MS = 1100;
const HOLD_MS = 1600;

export interface GapminderRow {
  year: number;
  country: string;
  cluster: number;
  pop: number;
  life_expect: number;
  fertility: number;
}

/** One record per country with its keyframes in year order: the shape `interpolateLinear` reads. */
export function toCountries(rows: GapminderRow[]): Country[] {
  const by = new Map<string, Country>();
  for (const r of [...rows].sort((a, b) => a.year - b.year)) {
    const c = by.get(r.country) ?? { country: r.country, region: REGION[r.cluster] ?? "Other", fert: [], life: [], pop: [] };
    c.fert.push(r.fertility);
    c.life.push(r.life_expect);
    c.pop.push(r.pop);
    by.set(r.country, c);
  }
  return [...by.values()];
}

async function loadCountries(): Promise<Country[]> {
  const res = await fetch("data/gapminder.json");
  if (!res.ok) throw new Error(`Could not load gapminder.json (HTTP ${res.status})`);
  return toCountries((await res.json()) as GapminderRow[]);
}

export interface Colors {
  neutral: string;
  accent: string;
  focus: string;
  surface: string;
  watermark: string;
  trail: string;
  label: string;
}

function colors(): Colors {
  return {
    neutral: token("--motion-neutral"),
    accent: token("--chart-1"),
    focus: token("--ink-strong"),
    surface: token("--surface"),
    watermark: token("--motion-watermark"),
    trail: token("--ink"),
    label: token("--ink"),
  };
}

export function gapminderSpec(values: Country[], width: number, height: number, col: Colors, init: { clock: number; playing: boolean; follow: string; hl: string | null }): Spec {
  const n = values[0]?.fert.length ?? 11;
  const labelSet = JSON.stringify(LABELED);
  return {
    $schema: "https://vega.github.io/schema/vega/v6.json",
    description: "Gapminder: life expectancy against fertility, 1955–2005, with eased transitions between five-year snapshots (Vega 6.4 easing functions and interpolateLinear).",
    width,
    height,
    padding: 4,
    autosize: { type: "fit", contains: "padding" },
    signals: [
      { name: "n", value: n },
      { name: "seg", value: SEGMENT_MS },
      { name: "hold", value: HOLD_MS },
      { name: "playing", value: init.playing },
      // Animation clock in ms, advanced on each timer tick while playing; it loops after a short hold on the last frame.
      {
        name: "clock",
        value: init.clock,
        on: [{
          events: { type: "timer", throttle: 16 },
          update: "playing ? (clock + now() - last_tick > (n - 1) * seg + hold ? 0 : clock + now() - last_tick) : clock",
        }],
      },
      { name: "last_tick", init: "now()", on: [{ events: [{ signal: "clock" }, { signal: "playing" }], update: "now()" }] },
      { name: "t", update: "clamp(clock / seg, 0, n - 1)" },
      { name: "k", update: "min(floor(t), n - 2)" },
      // Ease within each five-year segment, then map to [0, 1] across all keyframes.
      { name: "frac", update: "(k + easeCubicInOut(t - k)) / (n - 1)" },
      { name: "year", update: `${FIRST_YEAR} + ${STEP_YEARS} * round(t)` },
      { name: "hl", value: init.hl },
      { name: "follow", value: init.follow, on: [{ events: "@bubble:click", update: "datum.country" }] },
    ],
    data: [
      {
        name: "countries",
        values,
        transform: [
          { type: "formula", as: "x", expr: "interpolateLinear(datum.fert, frac)" },
          { type: "formula", as: "y", expr: "interpolateLinear(datum.life, frac)" },
          { type: "formula", as: "p", expr: "interpolateLinear(datum.pop, frac)" },
          { type: "collect", sort: { field: "p", order: "descending" } },
        ],
      },
      {
        name: "trail",
        source: "countries",
        transform: [
          { type: "filter", expr: "datum.country === follow" },
          { type: "flatten", fields: ["fert", "life"], as: ["tx", "ty"], index: "i" },
          { type: "formula", as: "ty_year", expr: `${FIRST_YEAR} + ${STEP_YEARS} * datum.i` },
        ],
      },
      {
        name: "labeled",
        source: "countries",
        transform: [{ type: "filter", expr: `indexof(${labelSet}, datum.country) >= 0 || datum.country === follow` }],
      },
    ],
    scales: [
      { name: "x", type: "linear", domain: [0, 9], range: "width", nice: false, zero: true },
      { name: "y", type: "linear", domain: [25, 85], range: "height", nice: false, zero: false },
      { name: "size", type: "sqrt", domain: [0, 1.4e9], range: [0, Math.round(Math.min(3600, width * 4))] },
    ],
    axes: [
      { orient: "bottom", scale: "x", title: "Babies per woman", grid: true, tickCount: 9, domain: false, ticks: false },
      { orient: "left", scale: "y", title: "Life expectancy (years)", grid: true, tickCount: 6, domain: false, ticks: false },
    ],
    marks: [
      {
        type: "text",
        interactive: false,
        encode: {
          update: {
            x: { signal: "width - 6" },
            y: { signal: "height - 8" },
            align: { value: "right" },
            baseline: { value: "bottom" },
            text: { signal: "year" },
            fontSize: { signal: "clamp(width / 5, 56, 150)" },
            fontWeight: { value: 600 },
            fill: { value: col.watermark },
          },
        },
      },
      {
        type: "line",
        from: { data: "trail" },
        interactive: false,
        encode: {
          update: {
            x: { scale: "x", field: "tx" },
            y: { scale: "y", field: "ty" },
            stroke: { value: col.trail },
            strokeWidth: { value: 1.5 },
            strokeOpacity: { value: 0.55 },
            strokeCap: { value: "round" },
          },
        },
      },
      {
        type: "symbol",
        from: { data: "trail" },
        interactive: false,
        encode: {
          update: {
            x: { scale: "x", field: "tx" },
            y: { scale: "y", field: "ty" },
            size: { value: 14 },
            fill: { value: col.trail },
            fillOpacity: { value: 0.55 },
          },
        },
      },
      {
        type: "symbol",
        name: "bubble",
        from: { data: "countries" },
        encode: {
          update: {
            x: { scale: "x", field: "x" },
            y: { scale: "y", field: "y" },
            size: { scale: "size", field: "p" },
            fill: [
              { test: "datum.country === follow", value: col.focus },
              { test: "hl && datum.region === hl", value: col.accent },
              { value: col.neutral },
            ],
            fillOpacity: [{ test: "datum.country === follow", value: 0.95 }, { value: 0.78 }],
            stroke: { value: col.surface },
            strokeWidth: { value: 1.5 },
            cursor: { value: "pointer" },
            tooltip: {
              signal: "{title: datum.country, 'Region': datum.region, 'Year': year, 'Life expectancy': format(datum.y, '.1f'), 'Babies per woman': format(datum.x, '.2f'), 'Population': format(datum.p, ',.0f')}",
            },
          },
        },
      },
      {
        type: "text",
        from: { data: "labeled" },
        interactive: false,
        encode: {
          update: {
            x: { signal: "scale('x', datum.x) + sqrt(scale('size', datum.p)) / 2 + 4" },
            y: { scale: "y", field: "y" },
            baseline: { value: "middle" },
            text: { field: "country" },
            fontSize: { value: 11.5 },
            fontWeight: [{ test: "datum.country === follow", value: 600 }, { value: 400 }],
            fill: { value: col.label },
          },
        },
      },
    ],
  };
}

/** Open a Vega spec in the Vega Editor (the spec travels in the URL, compressed the way the Editor expects). */
export function vegaEditorUrl(spec: Spec): string {
  return `https://vega.github.io/editor/#/url/vega/${LZString.compressToEncodedURIComponent(JSON.stringify(spec, null, 2))}`;
}

let active: { destroy: () => void } | null = null;
/** Bumped whenever the plate changes, so a chart still loading knows it is no longer wanted. */
let generation = 0;

/** Stop and remove the current motion chart (called when the plate changes). */
export function stopMotion(): void {
  generation++;
  active?.destroy();
  active = null;
}

/**
 * Fill the "In Motion" section with its controls, chart and notes. dataset.ts makes the
 * section (with its heading) and loads this module only on the gapminder page.
 */
export function fillMotion(section: HTMLElement): void {
  const chartHost = h("div", { class: "motion-chart", role: "figure", "aria-label": "Animated bubble chart of life expectancy against fertility for 62 countries, 1955 to 2005. Bubble size is population." });
  const play = h("button", { class: "btn motion-play", type: "button", "aria-pressed": "false" }, "Play");
  const slider = h("input", { id: "motion-year", type: "range", min: 0, max: 10, step: 0.01, value: 0, "aria-label": "Year" }) as HTMLInputElement;
  const yearOut = h("output", { for: "motion-year", class: "motion-year" }, String(FIRST_YEAR));
  const regions = h("div", { class: "motion-regions", role: "group", "aria-label": "Highlight a region" });
  const followNote = h("p", { class: "motion-note" });
  const editor = h("a", { class: "btn btn-quiet", target: "_blank", rel: "noopener", href: "#" }, "Open This Chart in the Vega Editor");
  const status = h("p", { class: "motion-status muted" }, "Loading…");

  section.append(
    h("p", { class: "sec-intro" },
      "Life expectancy against babies per woman for 62 countries, 1955 to 2005. Gapminder publishes a snapshot every five years; the bubbles glide between them using the easing functions new in Vega 6.4. ",
      "Vega-Lite's own support for eased, interpolated animation is in review."),
    h("div", { class: "motion-controls" }, play, h("label", { class: "motion-scrub", for: "motion-year" }, slider, yearOut)),
    regions,
    chartHost,
    followNote,
    h("div", { class: "motion-foot" }, editor, h("span", { class: "features mono" }, "Vega 6.4 · timer events · easeCubicInOut · interpolateLinear"), status),
  );

  const gen = generation;
  void (async () => {
    let values: Country[];
    let vegaEmbed: typeof import("vega-embed").default;
    let interp: typeof import("vega-interpreter").expressionInterpreter;
    let themeConfig: typeof import("./vl").themeConfig;
    try {
      [values, { default: vegaEmbed }, { expressionInterpreter: interp }, { themeConfig }] = await Promise.all([
        loadCountries(), import("vega-embed"), import("vega-interpreter"), import("./vl"),
      ]);
    } catch (err) {
      status.textContent = `The chart didn't load: ${err instanceof Error ? err.message : String(err)}`;
      return;
    }
    // The reader may have moved to another plate while Vega and the data loaded.
    if (gen !== generation) return;
    status.textContent = "";
    const state = { clock: 0, playing: false, follow: "China", hl: null as string | null, userPaused: reducedMotion() };
    let result: Result | undefined;
    let destroyed = false;

    const setPlaying = (p: boolean) => {
      state.playing = p;
      play.textContent = p ? "Pause" : "Play";
      play.setAttribute("aria-pressed", String(p));
      result?.view.signal("playing", p).runAsync();
    };
    const describeFollow = () => {
      followNote.replaceChildren(h("b", null, state.follow), " is highlighted with its full path. Select any bubble to follow that country instead.");
    };
    const updateEditor = () => {
      const spec = gapminderSpec(values, 640, 400, colors(), { clock: 0, playing: true, follow: state.follow, hl: state.hl });
      editor.setAttribute("href", vegaEditorUrl(spec));
    };

    // Size to the content box: clientWidth includes padding, and sizing to it would grow the chart on every resize.
    const contentWidth = () => {
      const cs = getComputedStyle(chartHost);
      return Math.floor(chartHost.clientWidth - parseFloat(cs.paddingLeft) - parseFloat(cs.paddingRight));
    };

    // Renders run one at a time, so a resize and a theme change can't both embed a view.
    let queue: Promise<void> = Promise.resolve();
    const render = () => (queue = queue.then(draw).catch((err: unknown) => {
      status.textContent = `The chart didn't render: ${err instanceof Error ? err.message : String(err)}`;
    }));
    const draw = async () => {
      if (destroyed) return;
      if (result) {
        state.clock = result.view.signal("clock") as number;
        result.finalize();
      }
      const width = Math.max(280, contentWidth());
      const height = Math.round(Math.min(520, Math.max(300, width * 0.6)));
      result = await vegaEmbed(chartHost, gapminderSpec(values, width, height, colors(), state) as never, {
        config: themeConfig(),
        actions: false,
        renderer: "svg",
        ast: true,
        expr: interp,
        tooltip: { theme: "custom" },
      });
      if (destroyed) { result.finalize(); return; }
      result.view.addSignalListener("t", (_n, t: number) => {
        slider.value = String(t);
        yearOut.textContent = String(FIRST_YEAR + STEP_YEARS * Math.round(t));
      });
      result.view.addSignalListener("follow", (_n, f: string) => {
        state.follow = f;
        describeFollow();
        updateEditor();
      });
    };

    // Region chips: emphasis instead of six colors (a scatter plot can't keep six hues apart for every reader).
    const chip = (label: string, value: string | null) => {
      const b = h("button", { type: "button", class: "chip", "aria-pressed": String(state.hl === value) }, label);
      b.addEventListener("click", () => {
        state.hl = value;
        regions.querySelectorAll("button").forEach((x) => x.setAttribute("aria-pressed", String(x === b)));
        result?.view.signal("hl", value).runAsync();
        updateEditor();
      });
      return b;
    };
    regions.append(chip("None", null), ...Object.values(REGION).map((r) => chip(r, r)));

    play.addEventListener("click", () => {
      state.userPaused = state.playing;
      setPlaying(!state.playing);
    });
    slider.addEventListener("input", () => {
      state.userPaused = true;
      setPlaying(false);
      const t = Number(slider.value);
      result?.view.signal("clock", t * SEGMENT_MS).runAsync();
      yearOut.textContent = String(FIRST_YEAR + STEP_YEARS * Math.round(t));
    });

    // Play only while the chart is on screen, never automatically for reduced-motion viewers.
    const io = new IntersectionObserver(([entry]) => {
      if (!entry) return;
      if (entry.isIntersecting && entry.intersectionRatio >= 0.4 && !state.userPaused) setPlaying(true);
      else if (!entry.isIntersecting && state.playing) setPlaying(false);
    }, { threshold: [0, 0.4] });

    let resizeTimer = 0;
    let lastWidth = contentWidth();
    const ro = new ResizeObserver(() => {
      const w = contentWidth();
      if (Math.abs(w - lastWidth) < 8) return;
      lastWidth = w;
      clearTimeout(resizeTimer);
      resizeTimer = window.setTimeout(() => void render(), 150);
    });

    let unsubscribeTheme = () => {};
    active = {
      destroy: () => {
        destroyed = true;
        clearTimeout(resizeTimer);
        io.disconnect();
        ro.disconnect();
        unsubscribeTheme();
        result?.finalize();
      },
    };
    describeFollow();
    updateEditor();
    await render();
    if (destroyed) return;
    io.observe(chartHost);
    ro.observe(chartHost);
    unsubscribeTheme = onThemeChange(() => void render());
  })();
}
