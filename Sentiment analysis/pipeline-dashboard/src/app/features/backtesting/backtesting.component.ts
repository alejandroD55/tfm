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
import { ApiService, TickerPerformanceResponse } from '../../core/services/api.service';
import { ReportService } from '../../core/services/report.service';
import { PipelineContextService } from '../../core/services/pipeline-context.service';
import { DailyReport, TickerView, ReportDateEntry, ExposureRecommendation } from '../../core/models/report.model';
import { ChartDataPoint, ChartSeries } from '../../core/models/pipeline.model';
import {
  demoStockChrome,
  initDemoHighcharts,
  mergeStockOptions,
} from '../../core/charts/highcharts-stock-demo';

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
  exposure_recommendation: ExposureRecommendation;
}

/** Series del scatter de recomendaciones — colores alineados con la vista de Señales */
const EXPOSURE_CYCLE_SERIES: { key: ExposureRecommendation; label: string; color: string }[] = [
  { key: 'INCREASE_STRONG', label: '↑↑ Aumentar fuerte', color: '#15803d' },
  { key: 'INCREASE_MILD',   label: '↑ Aumentar',         color: '#22c55e' },
  { key: 'MAINTAIN',        label: '→ Mantener',         color: '#94a3b8' },
  { key: 'REDUCE_MILD',     label: '↓ Reducir',          color: '#a78bfa' },
  { key: 'REDUCE_STRONG',   label: '↓↓ Reducir fuerte',  color: '#7c3aed' },
];

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
  private apiSvc = inject(ApiService);
  private destroy$ = new Subject<void>();

  @ViewChild(MatSort) sort!: MatSort;

  Highcharts: typeof Highcharts = initDemoHighcharts();

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
  /** Gráfico principal: IA vs B&H en una sola vista */
  historyCompareChartOptions: Highcharts.Options = {};
  historyCompareChartUpdate = false;
  cyclesChartOptions: Highcharts.Options = {};
  cyclesChartUpdate = false;
  exposureChartOptions: Highcharts.Options = {};
  exposureChartUpdate = false;
  historyMetricOptions: { value: BacktestHistoryMetric; label: string; unit: '%' | 'number' | '$'; description: string }[] = [
    { value: 'ai_return', label: 'Rentabilidad IA', unit: '%', description: 'Retorno acumulado con exposición modulada desde 0%' },
    { value: 'buy_hold', label: 'Mercado (B&H)', unit: '%', description: 'Retorno de comprar y mantener el ETF al 100%' },
    { value: 'alpha', label: 'Mejora vs Mercado', unit: '%', description: 'Diferencia entre estrategia de exposición y Buy & Hold' },
    { value: 'sharpe', label: 'Ratio Sharpe', unit: 'number', description: 'Retorno ajustado por riesgo anualizado · > 1 es bueno' },
    { value: 'drawdown', label: 'Caída Máx.', unit: '%', description: 'Peor caída desde máximos · ideal < 15%' },
    { value: 'final_equity', label: 'Capital Final', unit: '$', description: 'Valor final partiendo de 10.000 € con exposición modulada' },
    { value: 'avg_exposure', label: 'Capital Desplegado', unit: '%', description: 'Fracción media del capital invertido · arranca en 0%' },
  ];

  /** Altura del host para barras horizontales (Sharpe / Drawdown): escala con nº de ETFs. */
  sharpeDrawdownChartHeight = 320;

  tableSource = new MatTableDataSource<TickerView>();
  // Exposición continua como primario | métricas binarias como referencia secundaria
  tableCols = ['ticker', 'exp_return', 'exp_sharpe', 'exp_drawdown', 'exp_equity', 'avg_exp', 'bh', 'alpha', 'winrate'];

  compareScheme: any  = { domain: ['#2563EB', '#94A3B8'] }; // Exposición (IA) vs B&H
  drawdownScheme: any = { domain: ['#EF4444'] };
  
  /** KPI agregado: media de capital desplegado en cartera */
  avgPortfolioExposure = 0;
  avgPortfolioCash = 0;
  selectedCapitalTicker = '';
  /** Histórico global: vacío = media cartera; ticker = serie individual */
  historyTickerFilter = '';
  /** Paneles expandibles en Detalle Financiero por Activo */
  expandedAssets = new Set<string>();
  assetPerformanceCache = new Map<string, TickerPerformanceResponse>();
  assetPerformanceOptions: Record<string, Highcharts.Options> = {};
  assetPerformanceUpdates: Record<string, boolean> = {};
  assetPerformanceLoading = new Set<string>();
  assetMiniChartOptions: Record<string, Highcharts.Options> = {};
  assetMiniChartUpdates: Record<string, boolean> = {};

  /** Métricas secundarias (sin duplicar IA / B&H — van en el gráfico combinado). */
  get historySecondaryMetrics() {
    return this.historyMetricOptions.filter(
      m => m.value !== 'ai_return' && m.value !== 'buy_hold',
    );
  }

  /** Tooltips informativos por gráfico */
  chartTooltips: Record<string, string> = {
    returnComparison: 'Compara el retorno acumulado de la estrategia de exposición continua (modula posición día a día desde 0%) frente a comprar y mantener el 100% desde el día 1. Ideal: barras azules por encima de las grises.',
    historyCompare: 'Evolución temporal de la rentabilidad acumulada: estrategia IA (exposición modulada) frente al mercado Buy & Hold. Usa los botones superiores para acotar fechas, el navigator inferior para desplazarte y el menú ⋮ para exportar imagen o datos.',
    alpha: 'Exceso de retorno de la estrategia de exposición vs Buy & Hold. Positivo = la IA añade valor. Objetivo: > 0% de forma consistente.',
    sharpe: 'Retorno ajustado por riesgo (anualizado). Sharpe > 1 = bueno · > 2 = excelente · < 0 = peor que el activo libre de riesgo.',
    drawdown: 'Mayor caída desde un máximo histórico. Ideal: lo más cercano a 0%. > 15% indica protección insuficiente del capital.',
    exposureProgress: 'Evolución del % de capital invertido por activo. Arranca en 0% el primer día y escala según recomendaciones bayesianas.',
    capitalDeployed: 'Desglose por activo: capital invertido (exposición media) y efectivo en reserva (cash no invertido).',
  };
  
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

  get winnersCount() { return this.tickerViews.filter(t => t.exp_cumulative_return > 0).length; }

  /** Filas para @for en plantilla (evita optional chaining en el template). */
  get tableRows(): TickerView[] {
    return this.tableSource.data ?? [];
  }

  get cycleTickers(): string[] {
    return [...new Set(this.signalCycles.map(r => r.ticker))].sort();
  }

  get pipelineLabel(): string {
    return this.pipelineCtx.rangeLabel();
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
    this.assetPerformanceCache.clear();
    this.assetPerformanceOptions = {};
    this.assetPerformanceUpdates = {};
    this.assetMiniChartOptions = {};
    this.assetMiniChartUpdates = {};
    this.expandedAssets.clear();
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
    this.tickerViews = this.reportSvc.buildTickerViews(report)
      .sort((a, b) => a.ticker.localeCompare(b.ticker));
    this.tableSource.data = this.tickerViews;
    this.avgPortfolioExposure = this.reportSvc.avgPortfolioExposure(this.tickerViews);
    this.avgPortfolioCash = 100 - this.avgPortfolioExposure;
    if (!this.selectedCapitalTicker && this.tickerViews.length) {
      this.selectedCapitalTicker = this.tickerViews[0].ticker;
    }
    
    if (this.sort) {
      this.tableSource.sort = this.sort;
    }

    this.returnChart = this.reportSvc.returnComparisonChart(this.tickerViews);
    this.sharpeChart   = this.reportSvc.sharpeChart(this.tickerViews);
    this.drawdownChart = this.reportSvc.drawdownChart(this.tickerViews);
    this.alphaChart    = this.reportSvc.alphaChart(this.tickerViews);
    this.refreshHorizontalChartHeight();
  }

  private loadBacktestingHistory(dates: ReportDateEntry[]) {
    if (!dates.length) return;
    this.historyLoading = true;
    this.historyError = '';

    const sortedDates = [...dates]
      .filter(d => this.inPipelineRange(d.date))
      .sort((a, b) => a.date.localeCompare(b.date));
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
        const explanations = this.reportSvc.topSignalExplanations(report);
        for (const explain of explanations) {
          const rec = explain.exposure_recommendation;
          if (!rec) continue;
          cycles.push({
            date,
            ticker: explain.ticker,
            exposure_recommendation: rec,
          });
        }
      });

      this.historyRows = rows;
      this.signalCycles = cycles;
      // Recalcular drawdown/sharpe desde la serie de equity encadenada del rango
      this.historyRows = this.rechainPeriodMetrics(rows);
      this.historyLoading = false;
      this.refreshAllHistoryCharts();
      this.refreshExposureChart();
      this.refreshCyclesChart();
      for (const ticker of this.expandedAssets) {
        this.refreshAssetMiniCharts(ticker);
        const cached = this.assetPerformanceCache.get(ticker);
        if (cached) {
          this.assetPerformanceOptions[ticker] = this.buildAssetPerformanceOptions(cached, 300);
          this.assetPerformanceUpdates[ticker] = true;
        }
      }
    }, () => {
      this.historyLoading = false;
      this.historyError = 'No se pudo cargar la evolución histórica del backtesting.';
    });
  }

  /**
   * Recalcula max_drawdown y sharpe desde la serie de equity encadenada.
   * SOLO estos dos: son correctamente derivables de la equity curve.
   *
   * avg_exposure NO se toca aquí — su valor correcto viene de los datos
   * reales de position_state (via exposure_backtesting_diagnostics).
   * La gráfica de Capital Desplegado mostrará la media por periodo,
   * que es el dato real de cuánto capital estuvo invertido cada trimestre.
   */
  private rechainPeriodMetrics(rows: BacktestHistoryPoint[]): BacktestHistoryPoint[] {
    const byTicker = new Map<string, BacktestHistoryPoint[]>();
    for (const row of rows) {
      if (!byTicker.has(row.ticker)) byTicker.set(row.ticker, []);
      byTicker.get(row.ticker)!.push(row);
    }

    const result: BacktestHistoryPoint[] = [...rows];

    for (const [ticker, series] of byTicker.entries()) {
      const sorted = [...series].sort((a, b) => a.date.localeCompare(b.date));
      const equities = sorted.map(r => r.final_equity);
      const n = equities.length;
      if (n < 2) continue;

      const dailyReturns: number[] = [];
      for (let i = 1; i < n; i++) {
        const prev = equities[i - 1];
        dailyReturns.push(prev > 0 ? (equities[i] - prev) / prev : 0);
      }

      for (let i = 0; i < n; i++) {
        const idx = result.findIndex(r => r.date === sorted[i].date && r.ticker === ticker);
        if (idx < 0) continue;

        // ── Max drawdown desde el pico histórico acumulado ───────────────
        const peak = Math.max(...equities.slice(0, i + 1));
        const dd   = peak > 0 ? ((equities[i] - peak) / peak) * 100 : 0;
        result[idx] = { ...result[idx], drawdown: dd };

        // ── Sharpe acumulado (todos los retornos desde día 0) ────────────
        if (i >= 10) {  // mínimo 10 días para un Sharpe significativo
          const rets     = dailyReturns.slice(0, i);
          const mean     = rets.reduce((s, r) => s + r, 0) / rets.length;
          const variance = rets.reduce((s, r) => s + (r - mean) ** 2, 0) / rets.length;
          const std      = Math.sqrt(variance);
          const rf       = 0.04 / 252;
          const sharpe   = std > 1e-8 ? ((mean - rf) / std) * Math.sqrt(252) : 0;
          result[idx]    = { ...result[idx], sharpe: Math.round(sharpe * 1000) / 1000 };
        }

        // avg_exposure: NO se modifica — se usa el valor real de los datos
      }
    }
    return result;
  }

  private refreshAllHistoryCharts() {
    this.refreshHistoryCompareChart();
    const scopedRows = this.pipelineScopedHistoryRows();
    if (!scopedRows.length) {
      for (const metric of this.historySecondaryMetrics) {
        this.historyChartOptionsByMetric[metric.value] = {};
        this.historyChartUpdates[metric.value] = true;
      }
      return;
    }

    for (const metric of this.historySecondaryMetrics) {
      const data = this.buildHistorySeriesData(metric.value);
      const color = metric.value === 'alpha'
        ? '#15803d'
        : metric.value === 'drawdown'
        ? '#b91c1c'
        : metric.value === 'final_equity'
        ? '#5b21b6'
        : metric.value === 'avg_exposure'
        ? '#0891b2'
        : '#06b6d4';

      this.historyChartOptionsByMetric[metric.value] = mergeStockOptions(
        demoStockChrome(340),
        {
          title: { text: undefined },
          legend: { enabled: false },
          xAxis: this.pipelineXAxis(),
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
            shared: true,
            valueDecimals: metric.unit === '$' ? 0 : 2,
            valuePrefix: metric.unit === '$' ? '$' : undefined,
            valueSuffix: metric.unit === '%' ? '%' : undefined,
          },
          plotOptions: {
            series: {
              dataGrouping: { enabled: false },
              marker: { enabled: false, radius: 3, states: { hover: { enabled: true, radius: 5 } } },
            } as Highcharts.PlotSeriesOptions,
          },
          series: [{
            type: 'line',
            name: data.seriesName,
            data: data.points,
            color,
            lineWidth: 2.4,
          }] as Highcharts.SeriesOptionsType[],
        },
      );
      this.historyChartUpdates[metric.value] = true;
    }
  }

  /** Gráfico combinado Rentabilidad IA vs Mercado (B&H). */
  private refreshHistoryCompareChart() {
    const scopedRows = this.pipelineScopedHistoryRows();
    if (!scopedRows.length) {
      this.historyCompareChartOptions = {};
      this.historyCompareChartUpdate = true;
      return;
    }

    const ai = this.buildHistorySeriesData('ai_return');
    const bh = this.buildHistorySeriesData('buy_hold');
    const label = this.historyTickerFilter
      ? `Activo: ${this.historyTickerFilter}`
      : 'Media cartera (todos los ETFs)';

    this.historyCompareChartOptions = mergeStockOptions(
      demoStockChrome(440),
      {
        title: { text: undefined },
        subtitle: {
          text: label,
          style: { color: '#64748b', fontSize: '12px', fontWeight: '600' },
        },
        legend: {
          enabled: true,
          align: 'center',
          verticalAlign: 'bottom',
          itemStyle: { fontWeight: '600', color: '#334155' },
        },
        xAxis: this.pipelineXAxis(),
        yAxis: {
          title: { text: 'Rentabilidad acumulada (%)' },
          plotLines: [{ value: 0, color: '#94a3b8', width: 1, zIndex: 2 }],
          labels: {
            formatter: function () {
              return `${Highcharts.numberFormat(Number(this.value), 1)}%`;
            },
          },
        },
        tooltip: {
          shared: true,
          valueDecimals: 2,
          valueSuffix: '%',
          headerFormat: '<span style="font-size:11px">{point.key:%d/%m/%Y}</span><br/>',
        },
        plotOptions: {
          series: {
            dataGrouping: { enabled: false },
            marker: { enabled: false, radius: 3, states: { hover: { enabled: true, radius: 5 } } },
          } as Highcharts.PlotSeriesOptions,
        },
        series: [
          {
            type: 'line',
            name: 'Estrategia IA (exposición)',
            data: ai.points,
            color: '#2563eb',
            lineWidth: 3,
            zIndex: 2,
          },
          {
            type: 'line',
            name: 'Mercado Buy & Hold',
            data: bh.points,
            color: '#64748b',
            lineWidth: 2.2,
            dashStyle: 'ShortDash',
            zIndex: 1,
          },
        ] as Highcharts.SeriesOptionsType[],
      },
    );
    this.historyCompareChartUpdate = true;
  }

  private buildHistorySeriesData(
    metric: BacktestHistoryMetric,
  ): { points: [number, number][]; seriesName: string } {
    const scopedRows = this.pipelineScopedHistoryRows();
    const valueOf = (row: BacktestHistoryPoint) => row[metric];
    const metricMeta = this.historyMetricOptions.find(m => m.value === metric)!;
    const tickerFilter = this.historyTickerFilter;

    if (tickerFilter) {
      const rows = scopedRows
        .filter(r => r.ticker === tickerFilter)
        .sort((a, b) => a.date.localeCompare(b.date));
      return {
        points: rows.map(r => [this.toChartTs(r.date), valueOf(r)]),
        seriesName: `${metricMeta.label} · ${tickerFilter}`,
      };
    }

    const byDate = new Map<string, BacktestHistoryPoint[]>();
    for (const row of scopedRows) {
      if (!byDate.has(row.date)) byDate.set(row.date, []);
      byDate.get(row.date)!.push(row);
    }
    return {
      points: [...byDate.entries()]
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([date, rows]) => [
          this.toChartTs(date),
          rows.reduce((sum, row) => sum + valueOf(row), 0) / rows.length,
        ]),
      seriesName: `${metricMeta.label} (media cartera)`,
    };
  }

  /** Gráfico de progresión de capital desplegado (% exposición) por ticker a lo largo del backtesting */
  private refreshExposureChart() {
    const scopedRows = this.pipelineScopedHistoryRows();
    if (!scopedRows.length) {
      this.exposureChartOptions = {};
      this.exposureChartUpdate = true;
      return;
    }

    const tickers = [...new Set(scopedRows.map(r => r.ticker))].sort();

    const colorMap: Record<string, string> = {
      SPY: '#1d4ed8', QQQ: '#7c3aed', GLD: '#d97706',
      IWM: '#16a34a', XLE: '#b91c1c', NVDA: '#0891b2',
    };
    const defaultColors = ['#1d4ed8','#7c3aed','#d97706','#16a34a','#b91c1c','#0891b2'];

    const series = tickers.map((ticker, idx) => {
      const rows = scopedRows
        .filter(r => r.ticker === ticker)
        .sort((a, b) => a.date.localeCompare(b.date));
      return {
        type: 'line' as const,
        name: ticker,
        data: rows.map(r => [this.toChartTs(r.date), +(r.avg_exposure).toFixed(1)]),
        color: colorMap[ticker] ?? defaultColors[idx % defaultColors.length],
        lineWidth: 2.5,
        marker: { enabled: false },
      };
    });

    this.exposureChartOptions = mergeStockOptions(
      demoStockChrome(380),
      {
        title: { text: undefined },
        legend: { enabled: true, align: 'right', verticalAlign: 'top' },
        xAxis: this.pipelineXAxis(),
        yAxis: {
          title: { text: '% Capital desplegado' },
          min: 0,
          max: 100,
          plotLines: [
            {
              value: 50,
              color: '#94a3b8',
              width: 1,
              dashStyle: 'Dash',
              label: { text: 'Floor 50% (régimen NEUTRAL)', style: { color: '#94a3b8', fontSize: '10px' } },
            },
            {
              value: 70,
              color: '#22c55e',
              width: 1,
              dashStyle: 'Dot',
              label: { text: '70% — Alta convicción', style: { color: '#16a34a', fontSize: '10px' } },
            },
          ],
          labels: { formatter: function () { return `${this.value}%`; } },
        },
        tooltip: { valueSuffix: '%', valueDecimals: 1, shared: true },
        plotOptions: {
          series: { dataGrouping: { enabled: false } } as Highcharts.PlotSeriesOptions,
        },
        series: series as Highcharts.SeriesOptionsType[],
      },
    );
    this.exposureChartUpdate = true;
  }

  private refreshCyclesChart() {
    const scopedCycles = this.pipelineScopedCycles();
    if (!scopedCycles.length) {
      this.cyclesChartOptions = {};
      this.cyclesChartUpdate = true;
      return;
    }

    const tickers = [...new Set(scopedCycles.map(r => r.ticker))].sort();
    const yIndex = new Map(tickers.map((t, i) => [t, i]));
    const series = EXPOSURE_CYCLE_SERIES
      .map(({ key, label, color }) => {
        const data = scopedCycles
          .filter(point => point.exposure_recommendation === key)
          .map(point => ({
            x: this.toChartTs(point.date),
            y: yIndex.get(point.ticker) ?? 0,
            name: point.ticker,
          }));
        return data.length
          ? { type: 'scatter' as const, name: label, data: data as Highcharts.PointOptionsObject[], color }
          : null;
      })
      .filter((s): s is NonNullable<typeof s> => s !== null);

    this.cyclesChartOptions = mergeStockOptions(
      demoStockChrome(Math.max(380, tickers.length * 64)),
      {
        chart: { type: 'scatter' },
        title: { text: undefined },
        xAxis: this.pipelineXAxis(),
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
        series: series as Highcharts.SeriesOptionsType[],
      },
    );
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

  expRecColor(rec: string): string {
    const m: Record<string, string> = {
      INCREASE_STRONG: '#15803d', INCREASE_MILD: '#22c55e', MAINTAIN: '#94a3b8',
      REDUCE_MILD: '#a78bfa', REDUCE_STRONG: '#7c3aed',
    };
    return m[rec] ?? '#94a3b8';
  }

  getCapitalView(ticker: string): TickerView | undefined {
    return this.tickerViews.find(v => v.ticker === ticker);
  }

  onHistoryTickerChange() {
    this.refreshAllHistoryCharts();
  }

  toggleAssetDetail(ticker: string) {
    if (this.expandedAssets.has(ticker)) {
      this.expandedAssets.delete(ticker);
      return;
    }
    this.expandedAssets.add(ticker);
    this.loadAssetPerformanceChart(ticker);
    this.refreshAssetMiniCharts(ticker);
  }

  isAssetExpanded(ticker: string): boolean {
    return this.expandedAssets.has(ticker);
  }

  bhFinalEquity(r: TickerView): number {
    return Math.round(10_000 * (1 + r.buy_hold_return));
  }

  alphaEur(r: TickerView): number {
    return Math.round(r.exp_final_equity - this.bhFinalEquity(r));
  }

  loadAssetPerformanceChart(ticker: string) {
    if (!this.selectedDate || this.assetPerformanceCache.has(ticker)) {
      const cached = this.assetPerformanceCache.get(ticker);
      if (cached) {
        this.assetPerformanceOptions[ticker] = this.buildAssetPerformanceOptions(cached, 300);
        this.assetPerformanceUpdates[ticker] = true;
      }
      return;
    }
    if (this.assetPerformanceLoading.has(ticker)) return;
    this.assetPerformanceLoading.add(ticker);
    this.apiSvc.getTickerPerformance(ticker, this.selectedDate, this.pipelineDayLimit()).pipe(
      catchError(() => of(null))
    ).subscribe(resp => {
      this.assetPerformanceLoading.delete(ticker);
      if (!resp) return;
      this.assetPerformanceCache.set(ticker, resp);
      this.assetPerformanceOptions[ticker] = this.buildAssetPerformanceOptions(resp, 300);
      this.assetPerformanceUpdates[ticker] = true;
    });
  }

  /** Rango temporal configurado del pipeline activo (startDate → endDate). */
  private pipelineBounds(): { start: string; end: string } | null {
    const p = this.pipelineCtx.selectedPipeline();
    if (!p) return null;
    return { start: p.startDate, end: p.endDate };
  }

  private pipelineDayLimit(): number {
    const bounds = this.pipelineBounds();
    if (!bounds) return 365;
    const startMs = new Date(`${bounds.start}T00:00:00Z`).getTime();
    const endMs = new Date(`${bounds.end}T00:00:00Z`).getTime();
    const days = Math.ceil((endMs - startMs) / 86_400_000) + 7;
    return Math.max(30, Math.min(365, days));
  }

  private inPipelineRange(date: string): boolean {
    const bounds = this.pipelineBounds();
    if (!bounds) return true;
    return date >= bounds.start && date <= bounds.end;
  }

  private toChartTs(date: string): number {
    return new Date(`${date}T00:00:00Z`).getTime();
  }

  /** Eje X acotado al gap temporal del pipeline ejecutado. */
  private pipelineXAxis(extra: Highcharts.XAxisOptions = {}): Highcharts.XAxisOptions {
    const bounds = this.pipelineBounds();
    return {
      type: 'datetime',
      ...(bounds ? { min: this.toChartTs(bounds.start), max: this.toChartTs(bounds.end) } : {}),
      ...extra,
    };
  }

  private pipelineScopedHistoryRows(): BacktestHistoryPoint[] {
    return this.historyRows.filter(r => this.inPipelineRange(r.date));
  }

  private pipelineScopedCycles(): SignalCyclePoint[] {
    return this.signalCycles.filter(r => this.inPipelineRange(r.date));
  }

  private buildAssetPerformanceOptions(resp: TickerPerformanceResponse, height: number): Highcharts.Options {
    const bounds = this.pipelineBounds();
    const points = resp.points.filter(p => this.inPipelineRange(p.date));
    const anchor = bounds
      ? (points.find(p => p.date >= bounds.start) ?? points[0])
      : points[0];
    const baseStrategy = anchor?.strategy_return ?? 0;
    const baseBuyHold = anchor?.buy_hold_return ?? 0;
    // API devuelve % acumulado desde el primer día del histórico pedido; rebaseamos al inicio del pipeline
    const strategy = points.map(p => [this.toChartTs(p.date), +(p.strategy_return - baseStrategy).toFixed(4)]);
    const buyHold = points.map(p => [this.toChartTs(p.date), +(p.buy_hold_return - baseBuyHold).toFixed(4)]);
    const exposure = this.pipelineScopedHistoryRows()
      .filter(r => r.ticker === resp.ticker)
      .sort((a, b) => a.date.localeCompare(b.date))
      .map(r => [this.toChartTs(r.date), r.avg_exposure]);

    return mergeStockOptions(
      demoStockChrome(height),
      {
        title: { text: undefined },
        legend: { enabled: true },
        xAxis: this.pipelineXAxis(),
        yAxis: [{
          title: { text: 'Rentabilidad acumulada (%)' },
          height: exposure.length ? '58%' : '100%',
          plotLines: [{ value: 0, color: '#94a3b8', width: 1 }],
        }, ...(exposure.length ? [{
          title: { text: 'Capital desplegado (%)' },
          top: '62%',
          height: '38%',
          min: 0,
          max: 100,
        }] : [])],
        tooltip: { shared: true, valueDecimals: 2, valueSuffix: '%' },
        plotOptions: { series: { dataGrouping: { enabled: false }, marker: { enabled: false } } as Highcharts.PlotSeriesOptions },
        series: [
          { type: 'line', name: 'Estrategia (exposición)', data: strategy, color: '#2563eb', lineWidth: 2.2, yAxis: 0 },
          { type: 'line', name: 'Buy & Hold', data: buyHold, color: '#94a3b8', lineWidth: 1.6, yAxis: 0 },
          ...(exposure.length ? [{ type: 'area', name: 'Capital desplegado', data: exposure, color: 'rgba(37,99,235,.25)', fillOpacity: 0.3, lineWidth: 1.5, yAxis: 1 }] : []),
        ] as Highcharts.SeriesOptionsType[],
      },
    );
  }

  private refreshAssetMiniCharts(ticker: string) {
    const rows = this.pipelineScopedHistoryRows()
      .filter(r => r.ticker === ticker)
      .sort((a, b) => a.date.localeCompare(b.date));
    if (!rows.length) return;

    const strategyEquity = rows.map(r => [this.toChartTs(r.date), r.final_equity]);
    const buyHoldEquity = rows.map(r => [this.toChartTs(r.date), Math.round(10_000 * (1 + r.buy_hold / 100))]);

    this.assetMiniChartOptions[ticker] = mergeStockOptions(
      demoStockChrome(300),
      {
        title: { text: undefined },
        legend: { enabled: true },
        xAxis: this.pipelineXAxis(),
        yAxis: {
          title: { text: 'Capital (€)' },
          plotLines: [{
            value: 10_000,
            color: '#94a3b8',
            width: 1,
            dashStyle: 'Dash',
            label: { text: 'Capital inicial 10.000 €', style: { color: '#64748b', fontSize: '10px' } },
          }],
          labels: {
            formatter: function () {
              return `€${Highcharts.numberFormat(Number(this.value), 0)}`;
            },
          },
        },
        tooltip: {
          shared: true,
          valueDecimals: 0,
          valuePrefix: '€',
        },
        plotOptions: { series: { marker: { enabled: false }, lineWidth: 2.2 } as Highcharts.PlotSeriesOptions },
        series: [
          { type: 'line', name: 'Capital (estrategia)', data: strategyEquity, color: '#2563eb' },
          { type: 'line', name: 'Capital (B&H)', data: buyHoldEquity, color: '#94a3b8', lineWidth: 1.6 },
        ] as Highcharts.SeriesOptionsType[],
      },
    );
    this.assetMiniChartUpdates[ticker] = true;
  }

  qualityLabel(s: number) {
    if (s >= 2) return 'Excelente';
    if (s >= 1) return 'Bueno';
    if (s >= 0) return 'Aceptable';
    return 'Deficiente';
  }
}
