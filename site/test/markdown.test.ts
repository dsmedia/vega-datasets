// @vitest-environment jsdom
// Markdown from the repository's own files goes into the page as HTML (innerHTML):
// raw HTML is escaped, links can't break out of their attributes, and headings nest.
import { describe, expect, test } from 'vitest';
import { markdownToHtml as render } from '../src/markdown';

const markdownToHtml = (source: string, opts?: { sections?: boolean }) => render(source, opts).trim();

/** The one link `source` renders, as the browser parses it: its attribute values, decoded. */
function link(source: string): { href: string | null; title: string | null } {
  const host = document.createElement('div');
  host.innerHTML = markdownToHtml(source);
  const links = host.querySelectorAll('a');
  expect(links, source).toHaveLength(1);
  return { href: links[0]!.getAttribute('href'), title: links[0]!.getAttribute('title') };
}

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
      '[e](https://e.com/&quot;onmouseover=&quot;alert(1) "t&quot; onfocus=&quot;x")',
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

  // CommonMark decodes entity and numeric references in a destination or title exactly once.
  test('entity references in a link are decoded once, as the browser reads the attribute', () => {
    expect(link('[q](https://e.com/?a=1&amp;b=2)').href).toBe('https://e.com/?a=1&b=2');
    expect(link('[q](https://e.com/?a=1&b=2)').href).toBe('https://e.com/?a=1&b=2');
    expect(link('[q](https://e.com/?a=1&amp;amp;b=2)').href).toBe('https://e.com/?a=1&amp;b=2');
    expect(link('[q](https://e.com/?x=1&copy=2)').href).toBe('https://e.com/?x=1&copy=2');
    expect(link('[q](https://e.com/caf&#233;)').href).toBe('https://e.com/café');
    expect(link('[q](https://e.com "A &quot;quoted&quot; title")').title).toBe('A "quoted" title');
    expect(link('[q](#cars "Tom &amp; Jerry &lt;3")').title).toBe('Tom & Jerry <3');
  });

  test('a scheme hidden behind references is still blocked', () => {
    for (const source of [
      '[x](javascript&#58;alert(1))',
      '[x](&#106;avascript:alert(1))',
      '[x](&#x6A;avascript&colon;alert(1))',
      '[x](jav&Tab;ascript:alert(1))',
      '[x](data&colon;text/html,hi)',
    ]) {
      expect(link(source).href, source).toBe('#');
    }
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
