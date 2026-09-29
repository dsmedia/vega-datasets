/** Minimal DOM helpers for the enhancement scripts. Text always goes through textContent. */

type Child = Node | string | number | null | undefined | false;
type Attrs = Record<string, string | number | boolean | EventListener | null | undefined>;

export function h<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  attrs: Attrs | null = null,
  ...children: (Child | Child[])[]
): HTMLElementTagNameMap[K] {
  const el = document.createElement(tag);
  if (attrs) {
    for (const [key, value] of Object.entries(attrs)) {
      if (value === null || value === undefined || value === false) continue;
      if (key.startsWith("on") && typeof value === "function") {
        el.addEventListener(key.slice(2).toLowerCase(), value);
      } else if (key === "class") {
        el.className = String(value);
      } else if (value === true) {
        el.setAttribute(key, "");
      } else {
        el.setAttribute(key, String(value));
      }
    }
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    el.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return el;
}

/** The element matching `selector`, or an error naming it (the markup and the scripts are built together). */
export function $<E extends Element = HTMLElement>(selector: string, root: ParentNode = document): E {
  const el = root.querySelector<E>(selector);
  if (!el) throw new Error(`Missing element ${selector}`);
  return el;
}

/** Parse a `<script type="application/json">` data block written into the page. */
export function readJson<T>(id: string): T {
  return JSON.parse($(`#${id}`).textContent ?? "null") as T;
}

/** Run `fn` when the page has nothing else to do (after `timeout` ms at most). */
export function whenIdle(fn: () => void, timeout = 2000): void {
  if ("requestIdleCallback" in window) requestIdleCallback(() => fn(), { timeout });
  else setTimeout(fn, 200);
}
