/** On every page: snippet tabs and the fields table's histograms. */
import { enhanceSnippets } from "./snippets";
import { enhanceSparkline } from "./sparkline";

document.querySelectorAll<HTMLElement>("[data-snippets]").forEach(enhanceSnippets);
document.querySelectorAll<HTMLElement>(".spark-wrap").forEach(enhanceSparkline);
