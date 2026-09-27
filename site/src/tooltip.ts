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

export function showTip(event: PointerEvent | FocusEvent, lines: string[]): void {
  const t = el();
  t.replaceChildren(...lines.map((line, i) => h(i === 0 ? "strong" : "span", null, line)));
  t.hidden = false;
  const rect = (event.target as Element).getBoundingClientRect();
  const x = "clientX" in event ? event.clientX : rect.left + rect.width / 2;
  const y = "clientY" in event ? event.clientY : rect.top;
  const w = t.offsetWidth;
  const left = Math.min(Math.max(8, x - w / 2), window.innerWidth - w - 8);
  t.style.left = `${left}px`;
  t.style.top = `${Math.max(8, y - t.offsetHeight - 12)}px`;
}

export function hideTip(): void {
  if (tip) tip.hidden = true;
}

/** Wire hover + keyboard focus on an element to the shared tooltip. */
export function attachTip(target: Element, lines: string[]): void {
  target.addEventListener("pointerenter", (e) => showTip(e as PointerEvent, lines));
  target.addEventListener("pointermove", (e) => showTip(e as PointerEvent, lines));
  target.addEventListener("pointerleave", hideTip);
  target.addEventListener("focus", (e) => showTip(e as FocusEvent, lines));
  target.addEventListener("blur", hideTip);
}
