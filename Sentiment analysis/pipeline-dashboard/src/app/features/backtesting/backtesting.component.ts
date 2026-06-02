import { Component, OnInit, OnDestroy, AfterViewInit, ViewChild, inject } from '@angular/core';
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
import { forkJoin, of, catchError, Subject, takeUntil } from 'rxjs';
import { ReportService } from '../../core/services/report.service';
import { PipelineContextService } from '../../core/services/pipeline-context.service';
import { DailyReport, TickerView, ReportDateEntry } from '../../core/models/report.model';
import { ChartDataPoint, ChartSeries } from '../../core/models/pipeline.model';

type BacktestHistoryMetric = 'ai_return' | 'buy_hold' | 'alpha' | 'sharpe' | 'drawdown' | 'final_equity' | 'avg_exposure';

interface BacktestHistoryPoint {
  date: string;
  ticker: string;
  ai_return: number;
  buy_hold: number;
  alpha: number;
  sharpe: number;
  drawdown: number;
  final_equity: number;
  avg_exposure: number;   // % de capital invertido en ese corte temporal
}

interface SignalCyclePoint {
  date: string;
  ticker: string;
  signal: 'BUY' | 'SELL' | 'HOLD';
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
export class BacktestingComponent implements OnInit, OnDestroy, AfterViewInit {
  private reportSvc = inject(ReportService);
  private pipelineCtx = inject(PipelineContextService);
  private destroy$ = new Subject<void>();

  @ViewChild(MatSort) sort!: MatSort;

  Highcharts: typeof Highcharts = Highcharts;

  loading = true;
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
  signalCycles: SignalCyclePoint[] = [];
  historyChartOptionsByMetric: Record<BacktestHistoryMetric, Highcharts.Options> = {
    ai_return: {}, buy_hold: {}, alpha: {}, sharpe: {}, drawdown: {}, final_equity: {}, avg_exposure: {},
  };
  historyChartUpdates: Record<BacktestHistoryMetric, boolean> = {
    ai_return: false, buy_hold: false, alpha: false, sharpe: false,
    drawdown: false, final_equity: false, avg_exposure: false,
  };
  cyclesChartOptions: Highcharts.Options = {};
  cyclesChartUpdate = false;
  exposureChartOptions: Highcharts.Options = {};
  exposureChartUpdate = false;
  historyMetricOptions: { value: BacktestHistoryMetric; label: string; unit: '%' | 'number' | '$'; description: string }[] = [
    { value: 'ai_return', label: 'Rentabilidad IA', unit: '%', description: 'Retorno acumulado de la estrategia Long/Cash' },
    { value: 'buy_hold', label: 'Mercado (B&H)', unit: '%', description: 'Retorno de comprar y mantener el ETF' },
    { value: 'alpha', label: 'Mejora vs Mercado', unit: '%', description: 'Diferencia entre IA y Buy & Hold' },
    { value: 'sharpe', label: 'Ratio Sharpe', unit: 'number', description: 'Retorno ajustado por riesgo anualizado' },
    { value: 'drawdown', label: 'Caída Máx.', unit: '%', description: 'Peor caída desde máximos de la curva' },
    { value: 'final_equity', label: 'Capital Final', unit: '$', description: 'Valor final partiendo de $10,000' },
    { value: 'avg_exposure', label: 'Capital Desplegado', unit: '%', description: 'Fracción media del capital invertido · arranca en 0% y escala según las señales' },
  ];

  /** Altura del host para barras horizontales (Sharpe / Drawdown): escala con nº de ETFs. */
  sharpeDrawdownChartHeight = 320;

  tableSource = new MatTableDataSource<TickerView>();
  // Exposición continua como primario | métricas binarias como referencia secundaria
  tableCols = ['ticker', 'exp_return', 'exp_sharpe', 'exp_drawdown', 'exp_equity', 'avg_exp', 'bh', 'alpha', 'winrate'];

  compareScheme: any  = { domain: ['#2563EB', '#94A3B8'] }; // Azul corporativo (IA) vs Gris neutro (B&H)
  tripleScheme: any   = { domain: ['#1d4ed8', '#7c3aed', '#64748b'] }; // Exposición | Binario | B&H
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

  get cycleTickers(): string[] {
    return [...new Set(this.signalCycles.map(r => r.ticker))].sort();
  }

  get pipelineLabel(): string {
    return this.pipelineCtx.selectedPipeline()?.label ?? '';
  }

  ngOnInit() {
    this.pipelineCtx.pipelineChanged$.pipe(takeUntil(this.destroy$)).subscribe(() => {
      this.reportSvc.clearCache();
      this.loadPipelineData();
    });
    this.loadPipelineData();
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private loadPipelineData() {
    this.loading = true;
    this.reportSvc.listAvailableDates().subscribe({
      next: (dates) => {
        this.loadBacktestingHistory(dates);
        if (!dates.length) {
          this.loading = false;
          return;
        }
        // Snapshot = último día del pipeline activo (métricas con 10k desde inicio de corrida).
        this.selectedDate = this.pipelineCtx.pipelineEndDate() ?? dates[0].date;
        this.reportSvc.loadReport(this.selectedDate).subscribe({
          next: (report) => {
            this.processReport(report);
            this.loading = false;
          },
          error: () => {
            this.loading = false;
          },
        });
      },
      error: () => {
        this.loading = false;
      },
    });
  }

  // Activa la ordenación de las columnas de la tabla
  ngAfterViewInit() {
    this.tableSource.sort = this.sort;
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
    // 3 series: Exposición continua (primario) | Binario Long/Cash | Buy & Hold
    this.returnChart = [
      {
        name: 'Exposición Gradual (IA)',
        series: this.tickerViews.map(t => ({ name: t.ticker, value: +(t.exp_cumulative_return * 100).toFixed(2) }))
      },
      {
        name: 'Estrategia Binaria (BUY/SELL)',
        series: this.tickerViews.map(t => ({ name: t.ticker, value: +(t.cumulative_return * 100).toFixed(2) }))
      },
      {
        name: 'Mercado (Buy & Hold)',
        series: this.tickerViews.map(t => ({ name: t.ticker, value: +(t.buy_hold_return * 100).toFixed(2) }))
      },
    ];

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
      const cycles: SignalCyclePoint[] = [];

      reports.forEach((report, idx) => {
        if (!report) return;
        const date = sortedDates[idx].date;
        const views = this.reportSvc.buildTickerViews(report);
        for (const view of views) {
          rows.push({
            date,
            ticker: view.ticker,
            // Exposición continua (primario — la IA solo usa datos pasados)
            ai_return:    view.exp_cumulative_return * 100,
            buy_hold:     view.buy_hold_return * 100,
            alpha:        (view.exp_cumulative_return - view.buy_hold_return) * 100,
            sharpe:       view.exp_sharpe_ratio,
            drawdown:     view.exp_max_drawdown * 100,
            final_equity: view.exp_final_equity,
            avg_exposure: view.avg_exposure,   // % capital desplegado hasta esa fecha
          });
        }
        for (const explain of report.top_signal_explanations ?? []) {
          cycles.push({
            date,
            ticker: explain.ticker,
            signal: explain.signal,
          });
        }
      });

      this.historyRows = rows;
      this.signalCycles = cycles;
      this.historyLoading = false;
      this.refreshAllHistoryCharts();
      this.refreshCyclesChart();
      this.refreshExposureChart();
    }, () => {
      this.historyLoading = false;
      this.historyError = 'No se pudo cargar la evolución histórica del backtesting.';
    });
  }

  private refreshAllHistoryCharts() {
    if (!this.historyRows.length) {
      for (const metric of this.historyMetricOptions) {
        this.historyChartOptionsByMetric[metric.value] = {};
        this.historyChartUpdates[metric.value] = true;
      }
      return;
    }

    const byDate = new Map<string, BacktestHistoryPoint[]>();
    for (const row of this.historyRows) {
      if (!byDate.has(row.date)) byDate.set(row.date, []);
      byDate.get(row.date)!.push(row);
    }
    const toTs = (date: string) => new Date(`${date}T00:00:00Z`).getTime();
    for (const metric of this.historyMetricOptions) {
      const valueOf = (row: BacktestHistoryPoint) => row[metric.value];
      const data = [...byDate.entries()]
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([date, rows]) => [toTs(date), rows.reduce((sum, row) => sum + valueOf(row), 0) / rows.length]);

      const color = metric.value === 'alpha'
        ? '#15803d'
        : metric.value === 'drawdown'
        ? '#b91c1c'
        : metric.value === 'buy_hold'
        ? '#64748b'
        : metric.value === 'final_equity'
        ? '#5b21b6'
        : '#1d4ed8';

      this.historyChartOptionsByMetric[metric.value] = {
        chart: {
          height: 300,
          backgroundColor: 'transparent',
          zooming: { type: 'x' },
        },
        title: { text: undefined },
        credits: { enabled: false },
        rangeSelector: { enabled: false },
        navigator: { enabled: false },
        scrollbar: { enabled: false },
        legend: { enabled: false },
        xAxis: { type: 'datetime' },
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
          name: `${metric.label} (media cartera)`,
          data,
          color,
          lineWidth: 2.2,
        }] as any,
      };
      this.historyChartUpdates[metric.value] = true;
    }
  }

  /** Gráfico de progresión de capital desplegado (% exposición) por ticker a lo largo del backtesting */
  private refreshExposureChart() {
    if (!this.historyRows.length) {
      this.exposureChartOptions = {};
      this.exposureChartUpdate = true;
      return;
    }

    const toTs = (date: string) => new Date(`${date}T00:00:00Z`).getTime();
    const tickers = [...new Set(this.historyRows.map(r => r.ticker))].sort();

    const colorMap: Record<string, string> = {
      SPY: '#1d4ed8', QQQ: '#7c3aed', GLD: '#d97706',
      IWM: '#16a34a', XLE: '#b91c1c', NVDA: '#0891b2',
    };
    const defaultColors = ['#1d4ed8','#7c3aed','#d97706','#16a34a','#b91c1c','#0891b2'];

    const series = tickers.map((ticker, idx) => {
      const rows = this.historyRows
        .filter(r => r.ticker === ticker)
        .sort((a, b) => a.date.localeCompare(b.date));
      return {
        type: 'line' as const,
        name: ticker,
        data: rows.map(r => [toTs(r.date), +(r.avg_exposure).toFixed(1)]),
        color: colorMap[ticker] ?? defaultColors[idx % defaultColors.length],
        lineWidth: 2.5,
        marker: { enabled: false },
      };
    });

    this.exposureChartOptions = {
      chart: {
        height: 280,
        backgroundColor: 'transparent',
        zooming: { type: 'x' },
      },
      title: { text: undefined },
      credits: { enabled: false },
      rangeSelector: { enabled: false },
      navigator: { enabled: false },
      scrollbar: { enabled: false },
      legend: { enabled: true, align: 'right', verticalAlign: 'top' },
      xAxis: { type: 'datetime' },
      yAxis: {
        title: { text: '% Capital desplegado' },
        min: 0, max: 100,
        plotLines: [
          { value: 50, color: '#94a3b8', width: 1, dashStyle: 'Dash',
            label: { text: 'Floor 50% (régimen NEUTRAL)', style: { color: '#94a3b8', fontSize: '10px' } } },
          { value: 70, color: '#22c55e', width: 1, dashStyle: 'Dot',
            label: { text: '70% — Alta convicción', style: { color: '#16a34a', fontSize: '10px' } } },
        ],
        labels: { formatter: function() { return `${this.value}%`; } },
      },
      tooltip: { valueSuffix: '%', valueDecimals: 1, shared: true },
      plotOptions: {
        series: { dataGrouping: { enabled: false } } as any,
      },
      series: series as any,
    };
    this.exposureChartUpdate = true;
  }

  private refreshCyclesChart() {
    if (!this.signalCycles.length) {
      this.cyclesChartOptions = {};
      this.cyclesChartUpdate = true;
      return;
    }

    const toTs = (date: string) => new Date(`${date}T00:00:00Z`).getTime();
    const tickers = this.cycleTickers;
    const yIndex = new Map(tickers.map((t, i) => [t, i]));
    const buyData = this.signalCycles
      .filter(point => point.signal === 'BUY')
      .map(point => ({ x: toTs(point.date), y: yIndex.get(point.ticker) ?? 0, name: point.ticker }));
    const sellData = this.signalCycles
      .filter(point => point.signal === 'SELL')
      .map(point => ({ x: toTs(point.date), y: yIndex.get(point.ticker) ?? 0, name: point.ticker }));

    this.cyclesChartOptions = {
      chart: {
        type: 'scatter',
        height: Math.max(340, tickers.length * 64),
        backgroundColor: 'transparent',
        zooming: { type: 'x' },
      },
      title: { text: undefined },
      credits: { enabled: false },
      xAxis: { type: 'datetime' },
      yAxis: {
        title: { text: 'Ticker' },
        categories: tickers,
        min: -0.5,
        max: Math.max(tickers.length - 0.5, 0),
        tickInterval: 1,
      },
      legend: { enabled: true },
      tooltip: {
        pointFormatter: function () {
          const d = Highcharts.dateFormat('%Y-%m-%d', Number(this.x));
          return `<span><b>${this.series.name}</b> · ${this.name}<br/>${d}</span>`;
        },
      },
      plotOptions: {
        series: {
          marker: { radius: 6, symbol: 'circle' },
        },
      },
      series: [
        { type: 'scatter', name: 'BUY', data: buyData as any, color: '#15803d' },
        { type: 'scatter', name: 'SELL', data: sellData as any, color: '#b91c1c' },
      ] as any,
    };
    this.cyclesChartUpdate = true;
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

  // ── Helpers exposición (mirrors dashboard) ───────────────────────────────
  expRecClass(rec: string): string { return (rec ?? '').toLowerCase().replace(/_/g, '-'); }

  expRecLabel(rec: string): string {
    const m: Record<string, string> = {
      INCREASE_STRONG: '↑↑ Aumentar fuerte', INCREASE_MILD: '↑ Aumentar',
      MAINTAIN: '→ Mantener', REDUCE_MILD: '↓ Reducir', REDUCE_STRONG: '↓↓ Reducir fuerte',
    };
    return m[rec] ?? rec;
  }

  expRecIcon(rec: string): string {
    const m: Record<string, string> = {
      INCREASE_STRONG: 'arrow_upward', INCREASE_MILD: 'trending_up',
      MAINTAIN: 'remove', REDUCE_MILD: 'trending_down', REDUCE_STRONG: 'arrow_downward',
    };
    return m[rec] ?? 'remove';
  }

  qualityLabel(s: number) {
    if (s >= 2) return 'Excelente';
    if (s >= 1) return 'Bueno';
    if (s >= 0) return 'Aceptable';
    return 'Deficiente';
  }
}
