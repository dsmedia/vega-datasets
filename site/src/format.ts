const nf = new Intl.NumberFormat("en-US");

export function formatBytes(bytes: number | null): string {
  if (bytes === null) return "–";
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB"];
  let v = bytes / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v < 10 ? v.toFixed(1) : Math.round(v)} ${units[i]}`;
}

export function formatCount(n: number | null): string {
  return n === null ? "–" : nf.format(n);
}


export function formatNumber(n: number): string {
  if (Number.isInteger(n)) return nf.format(n);
  const abs = Math.abs(n);
  const digits = abs >= 100 ? 1 : abs >= 1 ? 2 : 3;
  return n.toLocaleString("en-US", { maximumFractionDigits: digits });
}

export function formatDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const hasTime = !/T00:00:00/.test(iso);
  return d.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    ...(hasTime ? { hour: "2-digit", minute: "2-digit" } : {}),
    timeZone: "UTC",
  });
}

export function plural(n: number, one: string, many = `${one}s`): string {
  return `${nf.format(n)} ${n === 1 ? one : many}`;
}


export const FORMAT_LABEL: Record<string, string> = {
  csv: "CSV",
  tsv: "TSV",
  json: "JSON",
  topojson: "TopoJSON",
  geojson: "GeoJSON",
  parquet: "Parquet",
  arrow: "Arrow",
  png: "PNG",
};

export const TYPE_LABEL: Record<string, string> = {
  integer: "integer",
  number: "number",
  string: "string",
  date: "date",
  datetime: "datetime",
  boolean: "boolean",
  array: "list",
};
