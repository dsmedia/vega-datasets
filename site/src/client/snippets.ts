/**
 * Code snippets behind tabs (components/SnippetTabs.astro): switching tabs (ARIA tabs:
 * arrow keys, Home and End) and a Copy button on each panel; if the clipboard is
 * blocked, Copy selects the code for Ctrl/⌘+C.
 */
import { h } from "./dom";

function copyButton(code: HTMLElement): HTMLButtonElement {
  const btn = h("button", { class: "copy-btn", type: "button" }, "Copy");
  btn.addEventListener("click", () => {
    navigator.clipboard.writeText(code.textContent ?? "").then(
      () => {
        btn.textContent = "Copied";
        btn.dataset.copied = "true";
        setTimeout(() => {
          btn.textContent = "Copy";
          delete btn.dataset.copied;
        }, 1600);
      },
      () => {
        const sel = window.getSelection();
        if (sel) {
          const range = document.createRange();
          range.selectNodeContents(code);
          sel.removeAllRanges();
          sel.addRange(range);
        }
        btn.textContent = "Press Ctrl/⌘+C";
      },
    );
  });
  return btn;
}

export function enhanceSnippets(root: HTMLElement): void {
  const tabs = [...root.querySelectorAll<HTMLButtonElement>('[role="tab"]')];
  const panels = tabs.map((t) => document.getElementById(t.getAttribute("aria-controls") ?? ""));
  for (const panel of panels) {
    const code = panel?.querySelector("code");
    if (panel && code) panel.prepend(copyButton(code));
  }
  const select = (i: number, focus: boolean) => {
    tabs.forEach((t, j) => {
      t.setAttribute("aria-selected", String(i === j));
      t.tabIndex = i === j ? 0 : -1;
      panels[j]?.toggleAttribute("hidden", i !== j);
    });
    if (focus) tabs[i]?.focus();
  };
  tabs.forEach((t, i) => {
    t.addEventListener("click", () => select(i, false));
    t.addEventListener("keydown", (e) => {
      const n = tabs.length;
      const next = ({ ArrowRight: (i + 1) % n, ArrowLeft: (i - 1 + n) % n, Home: 0, End: n - 1 } as Record<string, number>)[e.key];
      if (next === undefined) return;
      e.preventDefault();
      select(next, true);
    });
  });
}
