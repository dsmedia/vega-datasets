// The page shell (site/static/index.html) as the build writes it to site/dist.

/**
 * `noindex` adds `<meta name="robots" content="noindex">`: set for deploys other than
 * vega/vega-datasets' (a fork's Pages site, say), so search engines list only the
 * canonical site the shell's `<link rel="canonical">` names.
 */
export function pageShell(html, { noindex = false } = {}) {
  if (!noindex) return html;
  return html.replace('<link rel="canonical"', '<meta name="robots" content="noindex">\n<link rel="canonical"');
}
