/** Ejecución bootstrap (corrida independiente en reports). */
export interface PipelineRun {
  id:              string;
  label:           string;
  startDate:       string;
  endDate:         string;
  reportCount:     number;
  initialCapital:  number;
  firstReportDate?: string;
  lastReportDate?:  string;
  type?:              'independent';
  sourcePipelines?:   string[];
}
