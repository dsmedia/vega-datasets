/** On every page: the theme switch, snippet tabs and the fields table's histograms. */
import { $ } from "./dom";
import { enhanceSnippets } from "./snippets";
import { enhanceSparkline } from "./sparkline";
import { initThemeToggle } from "./theme";

initThemeToggle($<HTMLButtonElement>("#theme-toggle"));
document.querySelectorAll<HTMLElement>("[data-snippets]").forEach(enhanceSnippets);
document.querySelectorAll<HTMLElement>(".spark-wrap").forEach(enhanceSparkline);
