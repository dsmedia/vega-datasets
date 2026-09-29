/**
 * What the fields table says about a field's schema metadata, as plain text (unit-tested):
 * its documented range, values and rules, its format, and the table's keys. Each line
 * needs its property, so an undescribed field shows exactly what it did before.
 */
import { categoryValues, type Dataset, documentedRange, type Field, missingMarkers, primaryKey } from "./catalog";
import { formatNumber } from "./format";

/** At most this many allowed values are listed; the rest are counted. */
const ENUM_SHOWN = 12;

const bound = (v: number | string) => (typeof v === "number" ? formatNumber(v) : v);

/** A missing-value marker as a reader sees it: the empty one has no text to show. */
const marker = (v: string) => (v === "" ? "empty" : v);

function rangeNote(f: Field): string | null {
  const { minimum, maximum } = f.constraints ?? {};
  if (minimum === undefined && maximum === undefined) return null;
  const text =
    minimum !== undefined && maximum !== undefined ? `Documented range ${bound(minimum)} – ${bound(maximum)}`
    : minimum !== undefined ? `Documented minimum ${bound(minimum)}`
    : `Documented maximum ${bound(maximum!)}`;
  return documentedRange(f)?.fits === false ? `${text} (some values fall outside)` : text;
}

function valuesNote(f: Field): string | null {
  const values = categoryValues(f);
  if (values) {
    const list = values.map((c) => (c.label !== undefined && c.label !== String(c.value) ? `${c.value} (${c.label})` : String(c.value))).join(", ");
    return f.categoriesOrdered ? `Values, in order: ${list}` : `Values: ${list}`;
  }
  const allowed = f.constraints?.enum;
  if (!allowed?.length) return null;
  const more = allowed.length - ENUM_SHOWN;
  return `Allowed values: ${allowed.slice(0, ENUM_SHOWN).map(String).join(", ")}${more > 0 ? ` and ${more} more` : ""}`;
}

/** Quiet notes under a field's description: documented range, values, rules and missing-value markers. */
export function fieldNotes(f: Field): string[] {
  const c = f.constraints ?? {};
  const missing = missingMarkers(f.missingValues);
  return [
    rangeNote(f),
    valuesNote(f),
    c.required ? "Required" : null,
    c.unique ? "Unique" : null,
    missing.length ? `Counted as missing: ${missing.map(marker).join(", ")}` : null,
  ].filter((n): n is string => n !== null);
}

/** The field's format, shown with its type when it says something ("default" and "any" don't). */
export function formatNote(f: Field): string | null {
  return f.format && f.format !== "default" && f.format !== "any" ? f.format : null;
}

/** Whether the field is (part of) the table's primary key. */
export function isKey(d: Dataset, f: Field): boolean {
  return primaryKey(d).includes(f.name);
}

/**
 * The schema's own missing-value markers, for the note under the table ("NA counts as
 * missing."); null when the schema has none, or only Table Schema's default (empty cells).
 */
export function missingNote(d: Dataset): string | null {
  const markers = missingMarkers(d.missingValues);
  if (!markers.length || (markers.length === 1 && markers[0] === "")) return null;
  const named = markers.map((m) => (m === "" ? "empty cells" : `“${m}”`));
  const text = named.length === 1 ? named[0]! : `${named.slice(0, -1).join(", ")} and ${named.at(-1)}`;
  return `${text.charAt(0).toUpperCase()}${text.slice(1)} ${named.length === 1 ? "counts" : "count"} as missing.`;
}
