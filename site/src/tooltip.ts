/** One floating tooltip shared by the hand-drawn SVG profiles. */
import { h } from "./dom";

let tip: HTMLDivElement | null = null;

function el(): HTMLDivElement {
  if (!tip) {
    tip = h("div", { class: "tip", role: "tooltip", hidden: true });
    document.body.append(tip);
  }
  return tip;
}

/** Show `lines` above `target`: at the pointer when `at` is given, else centered over the target. */
export function showTip(target: Element, lines: string[], at?: { x: number; y: number }): void {
  const t = el();
  t.replaceChildren(...lines.map((line, i) => h(i === 0 ? "strong" : "span", null, line)));
  t.hidden = false;
  const rect = target.getBoundingClientRect();
  const x = at ? at.x : rect.left + rect.width / 2;
  const y = at ? at.y : rect.top;
  const w = t.offsetWidth;
  const left = Math.min(Math.max(8, x - w / 2), window.innerWidth - w - 8);
  t.style.left = `${left}px`;
  t.style.top = `${Math.max(8, y - t.offsetHeight - 12)}px`;
}

export function hideTip(): void {
  if (tip) tip.hidden = true;
}

/** Show the tooltip while the pointer is over `target` (keyboard access is the caller's). */
export function attachTip(target: Element, lines: string[]): void {
  const follow = (e: Event) => showTip(target, lines, { x: (e as PointerEvent).clientX, y: (e as PointerEvent).clientY });
  target.addEventListener("pointerenter", follow);
  target.addEventListener("pointermove", follow);
  target.addEventListener("pointerleave", hideTip);
}
