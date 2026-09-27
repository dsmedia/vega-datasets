/** Minimal DOM helpers. Text always goes through textContent; only trusted markdown uses innerHTML. */

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
  append(el, children);
  return el;
}

export function append(el: Element, children: (Child | Child[])[]): void {
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    el.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
}

const SVG_NS = "http://www.w3.org/2000/svg";
export function svg<K extends keyof SVGElementTagNameMap>(
  tag: K,
  attrs: Record<string, string | number> = {},
  ...children: (SVGElement | null)[]
): SVGElementTagNameMap[K] {
  const el = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, String(v));
  for (const c of children) if (c) el.append(c);
  return el;
}

export function $(selector: string, root: ParentNode = document): HTMLElement {
  const el = root.querySelector<HTMLElement>(selector);
  if (!el) throw new Error(`Missing element ${selector}`);
  return el;
}

export function clear(el: Element): void {
  el.replaceChildren();
}


/** Read and write a bare `#token` deep link (the only hash form the viewer passes through). */
export const hash = {
  get(): string {
    const raw = location.hash.replace(/^#/, "");
    try {
      return decodeURIComponent(raw);
    } catch {
      return raw; // A malformed escape (e.g. "#%E0") is just an unknown name.
    }
  },
  set(token: string): void {
    const next = token ? `#${token}` : " ";
    if (location.hash !== next) history.replaceState(null, "", next === " " ? location.pathname : next);
  },
};

export function showError(root: HTMLElement, err: unknown): void {
  root.replaceChildren(
    h("div", { class: "load-error", role: "alert" },
      h("strong", null, "The catalog didn't load."),
      h("p", null, err instanceof Error ? err.message : String(err)),
      h("p", null, "Reload the page to try again.")),
  );
}
