import { Component, OnInit, AfterViewInit, ViewChild, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatTableModule, MatTableDataSource } from '@angular/material/table';
import { MatSortModule, MatSort } from '@angular/material/sort';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatExpansionModule } from '@angular/material/expansion';
import { NgxChartsModule } from '@swimlane/ngx-charts';
import Highcharts from 'highcharts/highstock';
import { HighchartsChartModule } from 'highcharts-angular';
import { forkJoin, of, switchMap, catchError } from 'rxjs';
import { ReportService } from '../../core/services/report.service';
import { DailyReport, TickerView, ReportDateEntry } from '../../core/models/report.model';
import { ChartDataPoint, ChartSeries } from '../../core/models/pipeline.model';

type BacktestHistoryMetric = 'ai_return' | 'buy_hold' | 'alpha' | 'sharpe' | 'drawdown' | 'final_equity';

interface BacktestHistoryPoint {
  date: string;
  ticker: string;
  ai_return: number;
  buy_hold: number;
  alpha: number;
  sharpe: number;
  drawdown: number;
  final_equity: number;
}

@Component({
  selector: 'app-backtesting',
  standalone: true,
  imports: [
    CommonModule, FormsModule, MatTableModule, MatSortModule,
    MatButtonModule, MatIconModule, MatProgressSpinnerModule,
    MatTooltipModule, MatExpansionModule, NgxChartsModule, HighchartsChartModule,
  ],
  templateUrl: './backtesting.component.html',
  styleUrl: './backtesting.component.scss',
})
export class BacktestingComponent implements OnInit, AfterViewInit {
  private reportSvc = inject(ReportService);

  @ViewChild(MatSort) sort!: MatSort;

  Highcharts: typeof Highcharts = Highcharts;

  loading = true;
  availableDates: ReportDateEntry[] = [];
  selectedDate = '';
  tickerViews: TickerView[] = [];
  summary: any = { total_tickers: 0, avg_cumulative_return: 0, avg_sharpe_ratio: 0, avg_max_drawdown: 0, total_closed_trades: 0 };

  returnChart: ChartSeries[] = [];
  sharpeChart: ChartDataPoint[] = [];
  drawdownChart: ChartDataPoint[] = [];
  alphaChart: ChartDataPoint[] = [];

  historyLoading = false;
  historyError = '';
  historyRows: BacktestHistoryPoint[] = [];
  historyTicker = '__portfolio__';
  historyMetric: BacktestHistoryMetric = 'ai_return';
  historyChartOptions: Highcharts.Options = {};
  historyChartUpdate = false;
  historyMetricOptions: { value: BacktestHistoryMetric; label: string; unit: '%' | 'number' | '$'; description: string }[] = [
    { value: 'ai_return', label: 'Rentabilidad IA', unit: '%', description: 'Retorno acumulado de la estrategia Long/Cash' },
    { value: 'buy_hold', label: 'Mercado (B&H)', unit: '%', description: 'Retorno de comprar y mantener el ETF' },
    { value: 'alpha', label: 'Mejora vs Mercado', unit: '%', description: 'Diferencia entre IA y Buy & Hold' },
    { value: 'sharpe', label: 'Ratio Sharpe', unit: 'number', description: 'Retorno ajustado por riesgo anualizado' },
    { value: 'drawdown', label: 'Caída Máx.', unit: '%', description: 'Peor caída desde máximos de la curva' },
    { value: 'final_equity', label: 'Capital Final', unit: '$', description: 'Valor final partiendo de $10,000' },
  ];

  /** Altura del host para barras horizontales (Sharpe / Drawdown): escala con nº de ETFs. */
  sharpeDrawdownChartHeight = 320;

  tableSource = new MatTableDataSource<TickerView>();
  tableCols = ['ticker', 'signal', 'return', 'bh', 'alpha', 'sharpe', 'drawdown', 'equity', 'trades', 'winrate', 'pf'];

  compareScheme: any  = { domain: ['#2563EB', '#94A3B8'] }; // Azul corporativo (IA) vs Gris neutro (B&H)
  drawdownScheme: any = { domain: ['#EF4444'] }; // Rojo
  
  // Coloreado dinámico para Alpha (Verde si gana al mercado, Rojo si pierde)
  customColorsAlpha = (name: string) => {
    const item = this.alphaChart.find(d => d.name === name);
    return (item && item.value >= 0) ? '#22C55E' : '#EF4444'; 
  };

  // Coloreado dinámico para Sharpe (Verde/Azul si es positivo, Rojo si es negativo)
  customColorsSharpe = (name: string) => {
    const item = this.sharpeChart.find(d => d.name === name);
    return (item && item.value >= 0) ? '#06B6D4' : '#EF4444'; 
  };

  get winnersCount() { return this.tickerViews.filter(t => t.cumulative_return > 0).length; }

  /** Filas para @for en plantilla (evita optional chaining en el template). */
  get tableRows(): TickerView[] {
    return this.tableSource.data ?? [];
  }

  get historyTickerOptions(): string[] {
    return [...new Set(this.historyRows.map(r => r.ticker))].sort();
  }

  get selectedHistoryMetric() {
    return this.historyMetricOptions.find(m => m.value === this.historyMetric) ?? this.historyMetricOptions[0];
  }

  ngOnInit() {
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        this.availableDates = dates;
        this.loadBacktestingHistory(dates);
        if (!dates.length) { this.loading = false; return []; }
        this.selectedDate = dates[0].date;
        return this.reportSvc.loadReport(this.selectedDate);
      })
    ).subscribe({
      next: (r: any) => { if (r) this.processReport(r); this.loading = false; },
      error: () => { this.loading = false; },
    });
  }

  // Activa la ordenación de las columnas de la tabla
  ngAfterViewInit() {
    this.tableSource.sort = this.sort;
  }

  onDateChange(date: string) {
    this.loading = true;
    this.reportSvc.loadReport(date).subscribe({
      next: r => { this.processReport(r); this.loading = false; },
      error: () => { this.loading = false; },
    });
  }

  private processReport(report: DailyReport) {
    this.summary = report.summary;
    this.tickerViews = this.reportSvc.buildTickerViews(report);
    this.tableSource.data = this.tickerViews;
    
    // Reconecta la ordenación
    if (this.sort) {
      this.tableSource.sort = this.sort;
    }

    // MAPEO FORZADO PARA EL GRÁFICO AGRUPADO: Eje X = ETF, Eje Y = IA vs Mercado
    this.returnChart = this.tickerViews.map(t => ({
      name: t.ticker,
      series: [
        { name: 'Estrategia IA', value: t.cumulative_return * 100 },
        { name: 'Mercado (Buy & Hold)', value: t.buy_hold_return * 100 }
      ]
    }));

    this.sharpeChart   = this.reportSvc.sharpeChart(this.tickerViews);
    this.drawdownChart = this.reportSvc.drawdownChart(this.tickerViews);
    this.alphaChart    = this.reportSvc.alphaChart(this.tickerViews);
    this.refreshHorizontalChartHeight();
  }

  private loadBacktestingHistory(dates: ReportDateEntry[]) {
    if (!dates.length) return;
    this.historyLoading = true;
    this.historyError = '';

    const sortedDates = [...dates].sort((a, b) => a.date.localeCompare(b.date));
    forkJoin(
      sortedDates.map(d =>
        this.reportSvc.loadReport(d.date).pipe(catchError(() => of(null)))
      )
    ).subscribe(reports => {
      const rows: BacktestHistoryPoint[] = [];

      reports.forEach((report, idx) => {
        if (!report) return;
        const date = sortedDates[idx].date;
        for (const view of this.reportSvc.buildTickerViews(report)) {
          rows.push({
            date,
            ticker: view.ticker,
            ai_return: view.cumulative_return * 100,
            buy_hold: view.buy_hold_return * 100,
            alpha: view.alpha_vs_benchmark * 100,
            sharpe: view.sharpe_ratio,
            drawdown: view.max_drawdown * 100,
            final_equity: view.final_equity,
          });
        }
      });

      this.historyRows = rows;
      this.historyLoading = false;
      this.refreshHistoryChart();
    }, () => {
      this.historyLoading = false;
      this.historyError = 'No se pudo cargar la evolución histórica del backtesting.';
    });
  }

  onHistoryControlChange() {
    this.refreshHistoryChart();
  }

  private refreshHistoryChart() {
    if (!this.historyRows.length) {
      this.historyChartOptions = {};
      this.historyChartUpdate = true;
      return;
    }

    const metric = this.selectedHistoryMetric;
    const toTs = (date: string) => new Date(`${date}T00:00:00Z`).getTime();
    const valueOf = (row: BacktestHistoryPoint) => row[this.historyMetric];
    const byDate = new Map<string, BacktestHistoryPoint[]>();
    for (const row of this.historyRows) {
      if (!byDate.has(row.date)) byDate.set(row.date, []);
      byDate.get(row.date)!.push(row);
    }

    const data = this.historyTicker === '__portfolio__'
      ? [...byDate.entries()]
          .sort(([a], [b]) => a.localeCompare(b))
          .map(([date, rows]) => [toTs(date), rows.reduce((sum, row) => sum + valueOf(row), 0) / rows.length])
      : this.historyRows
          .filter(row => row.ticker === this.historyTicker)
          .sort((a, b) => a.date.localeCompare(b.date))
          .map(row => [toTs(row.date), valueOf(row)]);

    const color = this.historyMetric === 'alpha'
      ? '#16a34a'
      : this.historyMetric === 'drawdown'
      ? '#ef4444'
      : this.historyMetric === 'buy_hold'
      ? '#94a3b8'
      : this.historyMetric === 'final_equity'
      ? '#7c3aed'
      : '#2563eb';

    this.historyChartOptions = {
      chart: {
        height: 420,
        backgroundColor: 'transparent',
        zooming: { type: 'x' },
      },
      title: { text: undefined },
      credits: { enabled: false },
      rangeSelector: {
        selected: 3,
        inputEnabled: true,
        buttons: [
          { type: 'month', count: 1, text: '1M' },
          { type: 'month', count: 3, text: '3M' },
          { type: 'month', count: 6, text: '6M' },
          { type: 'all', text: 'Todo' },
        ],
      },
      navigator: { enabled: true },
      scrollbar: { enabled: true },
      legend: { enabled: false },
      xAxis: {
        type: 'datetime',
        plotLines: this.selectedDate ? [{
          value: toTs(this.selectedDate),
          color: '#0f766e',
          width: 2,
          dashStyle: 'Dash',
          label: { text: `Fecha seleccionada: ${this.selectedDate}`, rotation: 0, y: 14, style: { color: '#0f766e', fontWeight: '600' } },
        }] : [],
      },
      yAxis: {
        title: { text: metric.label },
        labels: {
          formatter: function () {
            const n = Number(this.value);
            if (metric.unit === '$') return `$${Highcharts.numberFormat(n, 0)}`;
            if (metric.unit === '%') return `${Highcharts.numberFormat(n, 1)}%`;
            return Highcharts.numberFormat(n, 2);
          },
        },
        plotLines: metric.unit !== '$' ? [{ value: 0, color: '#94a3b8', width: 1 }] : [],
      },
      tooltip: {
        valueDecimals: metric.unit === '$' ? 0 : 2,
        valuePrefix: metric.unit === '$' ? '$' : undefined,
        valueSuffix: metric.unit === '%' ? '%' : undefined,
      },
      plotOptions: {
        series: {
          dataGrouping: { enabled: false },
          marker: { enabled: false },
        } as any,
      },
      series: [{
        type: 'line',
        name: `${metric.label} · ${this.historyTicker === '__portfolio__' ? 'Media cartera' : this.historyTicker}`,
        data,
        color,
        lineWidth: 2.4,
      }] as any,
    };
    this.historyChartUpdate = true;
  }

  /** ngx-charts reparte la altura entre categorías; sin esto las barras quedan demasiado finas. */
  private refreshHorizontalChartHeight(): void {
    const n = Math.max(
      this.sharpeChart.length,
      this.drawdownChart.length,
      this.tickerViews.length,
      1
    );
    const perBar = 44;
    const chrome = 120;
    this.sharpeDrawdownChartHeight = Math.min(720, Math.max(260, n * perBar + chrome));
  }

  qualityLabel(s: number) {
    if (s >= 2) return 'Excelente';
    if (s >= 1) return 'Bueno';
    if (s >= 0) return 'Aceptable';
    return 'Deficiente';
  }
}
