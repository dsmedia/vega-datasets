// Markdown from the repository's own files goes into the page as HTML (innerHTML):
// raw HTML is escaped, links can't break out of their attributes, and headings nest.
import { describe, expect, test } from 'vitest';
import { markdownToHtml as render } from '../src/markdown';

const markdownToHtml = (source: string, opts?: { sections?: boolean }) => render(source, opts).trim();

/** The attribute names of every <a> in `html`. */
function linkAttributes(html: string): string[][] {
  return [...html.matchAll(/<a\s([^>]*)>/g)].map((m) => [...m[1]!.matchAll(/([\w-]+)="[^"]*"/g)].map((a) => a[1]!));
}

describe('links', () => {
  test('a quote in a link cannot add attributes (event handlers)', () => {
    for (const source of [
      '[a](#x"onmouseover="alert(1))',
      '[b](https://e.com/"onfocus="alert(1)"autofocus=")',
      '[c](<https://e.com/a b"x>)',
      '[d](https://e.com "t\\" onmouseover=\\"alert(1)")',
    ]) {
      const html = markdownToHtml(source);
      for (const names of linkAttributes(html)) {
        expect(names.every((n) => ['href', 'title', 'target', 'rel'].includes(n)), `${source} → ${html}`).toBe(true);
      }
      expect(html).not.toMatch(/\son\w+="/);
    }
    expect(markdownToHtml('[a](#x"onmouseover="alert(1))')).toContain('href="#x&#34;onmouseover=&#34;alert(1)"');
  });

  test('#dataset links stay on the page; other sites open in a new tab', () => {
    expect(markdownToHtml('[cars](#cars)')).toBe('<p><a href="#cars">cars</a></p>');
    expect(markdownToHtml('[Vega](https://vega.github.io/vega/)')).toBe(
      '<p><a href="https://vega.github.io/vega/" target="_blank" rel="noopener">Vega</a></p>',
    );
  });

  test('only http(s) and mailto links keep their target', () => {
    expect(markdownToHtml('[x](javascript:alert(1))')).toContain('href="#"');
    expect(markdownToHtml('[x](data:text/html,hi)')).toContain('href="#"');
    expect(markdownToHtml('[x](mailto:a@b.org)')).toContain('href="mailto:a@b.org"');
  });

  test('ampersands in URLs are written as entities', () => {
    expect(markdownToHtml('[q](https://e.com/?a=1&b=2)')).toContain('href="https://e.com/?a=1&#38;b=2"');
  });
});

test('raw HTML is shown as text, not parsed', () => {
  const html = markdownToHtml('Before <script>alert(1)</script> after <img src=x onerror=alert(1)>');
  expect(html).not.toContain('<script');
  expect(html).not.toContain('<img');
  expect(html).toContain('&#60;script&#62;');
});

test('headings nest under the section that holds them, unless the Markdown is the page', () => {
  expect(markdownToHtml('## Sources')).toBe('<h4>Sources</h4>');
  expect(markdownToHtml('## Sources', { sections: true })).toBe('<h2>Sources</h2>');
  expect(markdownToHtml('#### Deep')).toBe('<h6>Deep</h6>');
});
