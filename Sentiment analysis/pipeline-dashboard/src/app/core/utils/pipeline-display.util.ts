import { formatDate, formatNumber } from '@angular/common';

const LOCALE = 'es';

/** Parsea YYYY-MM-DD como fecha local (evita desfases UTC). */
function parseIsoDateLocal(iso: string): Date | null {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso?.trim() ?? '');
  if (!m) return null;
  return new Date(+m[1], +m[2] - 1, +m[3]);
}

/** Fecha de pipeline en formato español dd/MM/yyyy. */
export function formatPipelineDate(iso: string): string {
  const d = parseIsoDateLocal(iso);
  if (!d) return iso;
  return formatDate(d, 'dd/MM/yyyy', LOCALE);
}

/** Rango de ejecución, p. ej. «01/01/2025 – 31/03/2025». */
export function formatPipelineRangeLabel(start: string, end: string): string {
  if (!start && !end) return '';
  if (!start) return formatPipelineDate(end);
  if (!end) return formatPipelineDate(start);
  return `${formatPipelineDate(start)} – ${formatPipelineDate(end)}`;
}

/** Subtítulo del selector: días hábiles y capital inicial. */
export function formatPipelineMetaSubtitle(
  reportCount: number,
  initialCapital: number,
): string {
  const days = formatNumber(reportCount, LOCALE, '1.0-0');
  const capital = formatNumber(initialCapital, LOCALE, '1.0-0');
  return `${days} días hábiles · Capital inicial ${capital} €`;
}
