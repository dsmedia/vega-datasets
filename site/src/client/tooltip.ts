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

/** Show `lines` (the first in bold) centred above the point (x, y), kept on screen. */
export function showTipAt(x: number, y: number, lines: string[]): void {
  const t = el();
  t.replaceChildren(...lines.map((line, i) => h(i === 0 ? "strong" : "span", null, line)));
  t.hidden = false;
  const w = t.offsetWidth;
  t.style.left = `${Math.min(Math.max(8, x - w / 2), window.innerWidth - w - 8)}px`;
  t.style.top = `${Math.max(8, y - t.offsetHeight - 12)}px`;
}

export function hideTip(): void {
  if (tip) tip.hidden = true;
}
