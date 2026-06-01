/** Ejecución bootstrap independiente (capital inicial propio, p. ej. 10.000 €). */
export interface PipelineRun {
  id: string;
  label: string;
  startDate: string;
  endDate: string;
  reportCount: number;
  initialCapital: number;
  firstReportDate?: string;
  lastReportDate?: string;
}
