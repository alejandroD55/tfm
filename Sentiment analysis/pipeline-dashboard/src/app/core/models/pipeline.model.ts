export interface ChartDataPoint {
  name: string;
  value: number;
}

export interface ChartSeries {
  name: string;
  series: ChartDataPoint[];
}
