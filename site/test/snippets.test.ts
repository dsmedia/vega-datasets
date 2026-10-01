// @vitest-environment jsdom
// The snippet tabs remember the reader's tool (client/snippets.ts): a pick on one page
// opens the same tab on the next, and a blocked storage only loses the memory.
import { beforeEach, expect, test, vi } from 'vitest';
import { enhanceSnippets, TOOL_KEY } from '../src/client/snippets';

let boxes = 0;
function box(names: string[]): HTMLElement {
  const b = boxes++;
  const root = document.createElement('div');
  root.dataset.snippets = '';
  root.innerHTML = `<div role="tablist">${names.map((n, i) => `<button role="tab"${n === 'URL' ? '' : ` data-tool="${n}"`} aria-controls="b${b}p${i}" aria-selected="${i === 0}">${n}</button>`).join('')}</div>`
    + names.map((n, i) => `<pre role="tabpanel" id="b${b}p${i}"${i === 0 ? '' : ' hidden'}><code>${n} code</code></pre>`).join('');
  document.body.append(root);
  return root;
}
const selected = (root: HTMLElement) => root.querySelector('[aria-selected="true"]')!.textContent;

beforeEach(() => {
  document.body.replaceChildren();
  localStorage.clear();
  vi.restoreAllMocks();
});

test('a picked tool opens first on the next box', () => {
  const first = box(['Vega-Lite', 'Vega', 'Altair', 'JavaScript']);
  enhanceSnippets(first);
  first.querySelectorAll<HTMLButtonElement>('[role=tab]')[2]!.click();
  expect(localStorage.getItem(TOOL_KEY)).toBe('Altair');
  const next = box(['Vega-Lite', 'Vega', 'Altair', 'JavaScript']);
  enhanceSnippets(next);
  expect(selected(next)).toBe('Altair');
  expect(next.querySelectorAll<HTMLElement>('[role=tabpanel]')[2]!.hidden).toBe(false);
});

test('a remembered tool the box lacks leaves its first tab', () => {
  localStorage.setItem(TOOL_KEY, 'Vega');
  const imageBox = box(['Altair', 'JavaScript']);
  enhanceSnippets(imageBox);
  expect(selected(imageBox)).toBe('Altair');
});

test('blocked storage still switches tabs', () => {
  vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => { throw new Error('blocked'); });
  vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new Error('blocked'); });
  const root = box(['Vega-Lite', 'Vega']);
  enhanceSnippets(root);
  root.querySelectorAll<HTMLButtonElement>('[role=tab]')[1]!.click();
  expect(selected(root)).toBe('Vega');
});

test('copying the URL tab does not replace the remembered tool, and keyboard navigation includes it', () => {
  localStorage.setItem(TOOL_KEY, 'Altair');
  const root = box(['URL', 'Vega-Lite', 'Vega', 'Altair', 'JavaScript']);
  enhanceSnippets(root);
  expect(selected(root)).toBe('Altair');
  const url = root.querySelector<HTMLButtonElement>('[role=tab]')!;
  url.click();
  expect(selected(root)).toBe('URL');
  expect(localStorage.getItem(TOOL_KEY)).toBe('Altair');
  url.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowRight', bubbles: true }));
  expect(selected(root)).toBe('Vega-Lite');
  expect(document.activeElement?.textContent).toBe('Vega-Lite');
  expect(root.querySelectorAll('[role=tabpanel] .copy-btn')).toHaveLength(5);
});
