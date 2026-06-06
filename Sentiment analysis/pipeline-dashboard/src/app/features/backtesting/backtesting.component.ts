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
import { NgxChartsModule, LegendPosition } from '@swimlane/ngx-charts';
import Highcharts from 'highcharts/highstock';
import { HighchartsChartModule } from 'highcharts-angular';
import { forkJoin, of, catchError, Subject, takeUntil } from 'rxjs';
import { ApiService, TickerPerformanceResponse } from '../../core/services/api.service';
import { ReportService } from '../../core/services/report.service';
import { PipelineContextService } from '../../core/services/pipeline-context.service';
import { DailyReport, TickerView, ReportDateEntry } from '../../core/models/report.model';
import { ChartDataPoint, ChartSeries } from '../../core/models/pipeline.model';
import {
  demoStockChrome,
  initDemoHighcharts,
  mergeStockOptions,
} from '../../core/charts/highcharts-stock-demo';

interface BacktestHistoryPoint {
  date: string;
  ticker: string;
  final_equity: number;
  avg_exposure: number;
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
  private apiSvc = inject(ApiService);
  private destroy$ = new Subject<void>();

  @ViewChild(MatSort) sort!: MatSort;

  Highcharts: typeof Highcharts = initDemoHighcharts();

  loading = true;
  selectedDate = '';
  tickerViews: TickerView[] = [];
  summary: any = { total_tickers: 0, avg_cumulative_return: 0, avg_sharpe_ratio: 0, avg_max_drawdown: 0, total_closed_trades: 0 };

  returnChart: ChartSeries[] = [];
  drawdownChart: ChartDataPoint[] = [];
  alphaChart: ChartDataPoint[] = [];
  legendPosition: LegendPosition = LegendPosition.Below;

  avgPortfolioExposure = 0;
  avgPortfolioCash = 0;

  historyLoading = false;
  historyRows: BacktestHistoryPoint[] = [];

  performanceTicker = '';
  performanceLoading = false;
  performanceError = '';
  performanceChartOptions: Highcharts.Options = {};
  performanceChartUpdate = false;
  performanceCache = new Map<string, TickerPerformanceResponse>();

  tableSource = new MatTableDataSource<TickerView>();
  tableCols = ['ticker', 'exp_return', 'exp_sharpe', 'exp_drawdown', 'exp_equity', 'avg_exp', 'bh', 'alpha'];

  compareScheme: any  = { domain: ['#2563EB', '#94A3B8'] }; 
  drawdownScheme: any = { domain: ['#EF4444'] };
  
  chartTooltips: Record<string, string> = {
    capitalDeployed: 'Porcentaje medio del capital que la IA decidió mantener invertido a lo largo del periodo, reteniendo el resto en cash por precaución.',
    returnComparison: 'Compara el retorno acumulado de la estrategia de exposición continua (modula posición día a día desde 0%) frente a comprar y mantener el 100% desde el día 1.',
    alpha: 'Exceso de retorno de la estrategia de exposición vs Buy & Hold. Positivo = la IA añade valor sobre el mercado pasivo.',
    drawdown: 'Mayor caída desde un máximo histórico. Un Drawdown profundo indica mala protección de capital.',
  };
  
  customColorsAlpha = (name: string) => {
    const item = this.alphaChart.find(d => d.name === name);
    return (item && item.value >= 0) ? '#22C55E' : '#EF4444'; 
  };

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

  ngAfterViewInit() {
    this.tableSource.sort = this.sort;
  }

  private loadPipelineData() {
    this.loading = true;
    this.performanceCache.clear();
    
    this.reportSvc.listAvailableDates().subscribe({
      next: (dates) => {
        this.loadBacktestingHistory(dates);
        if (!dates.length) {
          this.loading = false;
          return;
        }
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

  private processReport(report: DailyReport) {
    this.summary = report.summary;
    this.tickerViews = this.reportSvc.buildTickerViews(report)
      .sort((a, b) => a.ticker.localeCompare(b.ticker));
    
    this.tableSource.data = this.tickerViews;
    this.avgPortfolioExposure = this.reportSvc.avgPortfolioExposure(this.tickerViews);
    this.avgPortfolioCash = 100 - this.avgPortfolioExposure;
    
    if (this.sort) {
      this.tableSource.sort = this.sort;
    }

    this.returnChart = this.tickerViews.map(v => ({
      name: v.ticker,
      series: [
        { name: 'Estrategia IA', value: +(v.exp_cumulative_return * 100).toFixed(2) },
        { name: 'Buy & Hold', value: +(v.buy_hold_return * 100).toFixed(2) }
      ]
    }));

    this.drawdownChart = this.reportSvc.drawdownChart(this.tickerViews);
    this.alphaChart    = this.reportSvc.alphaChart(this.tickerViews);

    if (!this.tickerViews.some(v => v.ticker === this.performanceTicker)) {
      this.performanceTicker = this.tickerViews[0]?.ticker ?? '';
    }
    if (this.performanceTicker) {
      this.loadPerformanceChart(this.performanceTicker);
    }
  }

  private loadBacktestingHistory(dates: ReportDateEntry[]) {
    if (!dates.length) return;
    this.historyLoading = true;
    
    const pBounds = this.pipelineBounds();
    const sortedDates = [...dates]
      .filter(d => !pBounds || (d.date >= pBounds.start && d.date <= pBounds.end))
      .sort((a, b) => a.date.localeCompare(b.date));
      
    forkJoin(
      sortedDates.map(d => this.reportSvc.loadReport(d.date).pipe(catchError(() => of(null))))
    ).subscribe(reports => {
      const rows: BacktestHistoryPoint[] = [];
      reports.forEach((report, idx) => {
        if (!report) return;
        const date = sortedDates[idx].date;
        const views = this.reportSvc.buildTickerViews(report);
        for (const view of views) {
          rows.push({
            date,
            ticker: view.ticker,
            final_equity: view.exp_final_equity,
            avg_exposure: view.avg_exposure,
          });
        }
      });
      this.historyRows = rows;
      this.historyLoading = false;
      
      if (this.performanceTicker && this.performanceCache.has(`${this.selectedDate}:${this.performanceTicker}`)) {
        const cached = this.performanceCache.get(`${this.selectedDate}:${this.performanceTicker}`)!;
        this.performanceChartOptions = this.buildPerformanceChartOptions(cached);
        this.performanceChartUpdate = true;
      }
    });
  }

  onPerformanceTickerChange(ticker: string) {
    this.performanceTicker = ticker;
    this.loadPerformanceChart(ticker);
  }

  loadPerformanceChart(ticker: string) {
    if (!ticker || !this.selectedDate) return;
    const key = `${this.selectedDate}:${ticker}`;
    const cached = this.performanceCache.get(key);
    if (cached) {
      this.performanceChartOptions = this.buildPerformanceChartOptions(cached);
      this.performanceChartUpdate = true;
      return;
    }

    this.performanceLoading = true;
    this.performanceError = '';
    
    this.apiSvc.getTickerPerformance(ticker, this.selectedDate, 365).pipe(
      catchError(() => {
        this.performanceError = `No se pudo cargar el histórico de ${ticker}.`;
        this.performanceLoading = false;
        return of(null);
      })
    ).subscribe(resp => {
      this.performanceLoading = false;
      if (!resp) return;
      this.performanceCache.set(key, resp);
      this.performanceChartOptions = this.buildPerformanceChartOptions(resp);
      this.performanceChartUpdate = true;
    });
  }

  private pipelineBounds(): { start: string; end: string } | null {
    const p = this.pipelineCtx.selectedPipeline();
    if (!p) return null;
    return { start: p.startDate, end: p.endDate };
  }

  private buildPerformanceChartOptions(resp: TickerPerformanceResponse, height = 680): Highcharts.Options {
    const toTs = (date: string) => new Date(`${date}T00:00:00Z`).getTime();
    const points = resp.points;
    const targetTs = toTs(resp.target_date);
    const visibleStart = targetTs - 1000 * 60 * 60 * 24 * 120;

    const ohlc = points.map(p => [
      toTs(p.date), 
      +Number(p.open).toFixed(6), 
      +Number(p.high).toFixed(6), 
      +Number(p.low).toFixed(6), 
      +Number(p.close).toFixed(6)
    ]);
    const bbMiddle = points.map(p => [toTs(p.date), p.bb_middle]);
    
    // Tratamos los valores crudos directos de rendimiento acumulado tal como vienen de la API
    const strategy = points.map(p => [toTs(p.date), p.strategy_return]);
    const buyHold = points.map(p => [toTs(p.date), p.buy_hold_return]);

    const exposure = this.historyRows
      .filter(r => r.ticker === resp.ticker)
      .sort((a, b) => a.date.localeCompare(b.date))
      .map(r => [toTs(r.date), +Number(r.avg_exposure).toFixed(2)]);

    const stageBands = resp.stages.map(stage => ({
      from: toTs(stage.from),
      to: toTs(stage.to) + 1000 * 60 * 60 * 24,
      color: stage.stage === 'LONG' ? 'rgba(34,197,94,.055)' : 'rgba(124,58,237,.055)',
      label: {
        text: stage.stage,
        style: { color: stage.stage === 'LONG' ? '#15803d' : '#6d28d9', fontSize: '10px', fontWeight: '600' },
      },
    }));

    const flagColor = (rec: string) => rec.startsWith('INCREASE') ? '#16a34a' : rec.startsWith('REDUCE') ? '#7c3aed' : '#94a3b8';
    const flagTitle = (rec: string) => rec === 'INCREASE_STRONG' ? '↑↑' : rec === 'INCREASE_MILD' ? '↑' : rec === 'REDUCE_STRONG' ? '↓↓' : rec === 'REDUCE_MILD' ? '↓' : '→';
    
    const signalFlags = resp.recommendations
      .filter(s => s.exposure_recommendation !== 'MAINTAIN')
      .map(s => ({
        x: toTs(s.date),
        title: flagTitle(s.exposure_recommendation),
        text:  s.exposure_recommendation.replace(/_/g, ' '),
        fillColor: flagColor(s.exposure_recommendation),
      }));

    const drawdownPoint = points.find(p => p.date === resp.max_drawdown.date);

    return {
      chart: { 
        height, 
        backgroundColor: 'transparent', 
        zooming: { type: 'x' },
        events: {
          // Interceptamos los datos justo antes de exportar a CSV o Tabla
          exportData: function (event: any) {
            const dataRows = event.dataRows;
            if (!dataRows || dataRows.length <= 1) return;
            
            // 1. Renombramos la cabecera 'DateTime' a 'Fecha'
            dataRows[0][0] = 'Fecha';
            
            // 2. Extraemos la cabecera y el cuerpo de datos
            const header = dataRows[0];
            const body = dataRows.slice(1);
            
            // 3. Ordenamos el cuerpo de más reciente a más antiguo (descendente)
            body.sort((a: any, b: any) => {
              return new Date(b[0]).getTime() - new Date(a[0]).getTime();
            });
            
            // 4. Reconstruimos el array original inyectando la cabecera y los datos ordenados
            dataRows.length = 0; 
            dataRows.push(header, ...body);
          }
        }
      },
      title: { 
        text: 'Autopsia Operativa - ' + resp.ticker,
        style: { color: '#334155', fontSize: '15px', fontWeight: '700' }
      },
      credits: { enabled: false },
      
      exporting: {
        csv: {
          dateFormat: '%Y-%m-%d', // Fecha limpia sin horas
        },
        showTable: false // Controla que no se abra automáticamente
      },

      rangeSelector: {
        selected: 2,
        inputEnabled: height >= 500,
        enabled: height >= 500,
        buttons: height >= 500 ? [
          { type: 'month', count: 1, text: '1M' },
          { type: 'month', count: 3, text: '3M' },
          { type: 'month', count: 6, text: '6M' },
          { type: 'all', text: 'Todo' },
        ] : [],
      },
      navigator: { enabled: height >= 500 },
      scrollbar: { enabled: height >= 500 },
      legend: { enabled: true },
      xAxis: {
        type: 'datetime',
        min: Math.max(points.length ? toTs(points[0].date) : visibleStart, visibleStart),
        max: targetTs,
        plotBands: stageBands as any,
      },
      yAxis: [{
        title: { text: 'Precio del Activo' },
        height: '45%',
        resize: { enabled: true },
        gridLineColor: 'rgba(148,163,184,.18)',
      }, {
        title: { text: 'Rentabilidad Acum. (%)' },
        top: '50%',
        height: '25%',
        offset: 0,
        resize: { enabled: true },
        gridLineColor: 'rgba(148,163,184,.18)',
        plotLines: [{ value: 0, color: '#94a3b8', width: 1 }],
        labels: {
          formatter: function () {
            return `${Highcharts.numberFormat(Number(this.value), 0)}%`;
          }
        }
      }, {
        title: { text: 'Exposición IA (%)' },
        top: '80%',
        height: '20%',
        offset: 0,
        min: 0,
        max: 100,
        gridLineColor: 'rgba(148,163,184,.18)',
        plotLines: [{ value: 50, color: '#94a3b8', width: 1, dashStyle: 'Dash' }],
      }],
      
      // AL ACTIVAR split:true SIN FORMATTER PERSONALIZADO, HIGHCHARTS SEPARA LOS DESTELLOS DE TEXTO POR GRÁFICO AUTOMÁTICAMENTE
      tooltip: { 
        split: true,
        valueDecimals: 2
      },

      plotOptions: {
        series: { dataGrouping: { enabled: false } } as any,
        candlestick: { color: '#ef4444', upColor: '#22c55e', lineColor: '#dc2626', upLineColor: '#16a34a' } as any,
      },
      series: [
        { type: 'candlestick', id: 'ohlc', name: `${resp.ticker} Precio`, data: ohlc, yAxis: 0 },
        { type: 'line', name: 'Media Bollinger', data: bbMiddle, yAxis: 0, color: '#64748b', dashStyle: 'ShortDot', lineWidth: 1 },
        { type: 'line', name: 'Rendimiento IA', data: strategy, yAxis: 1, color: '#2563eb', lineWidth: 2.2, valueSuffix: '%' }, // Nombre simplificado
        { type: 'line', name: 'Buy & Hold', data: buyHold, yAxis: 1, color: '#94a3b8', lineWidth: 1.6, valueSuffix: '%' },
        {
          type: 'scatter',
          name: 'Pico de Caída (Max Drawdown)',
          data: drawdownPoint ? [[toTs(drawdownPoint.date), drawdownPoint.strategy_return]] : [],
          yAxis: 1,
          color: '#ef4444',
          marker: { enabled: true, symbol: 'triangle-down', radius: 7 },
          includeInCSVExport: false, // EXCLUIR DE TABLA DE DATOS
          tooltip: {
            // Añadimos 'this: any' como primer parámetro (es un truco de TypeScript)
            pointFormatter: function (this: any): string { 
              return `<span style="color:#ef4444">●</span> Max drawdown: <b>${Highcharts.numberFormat(Number(this.y), 2)}%</b><br/>`;
            },
          },
        },
        {
          type: 'area',
          name: 'Nivel de Exposición',
          data: exposure,
          yAxis: 2,
          color: '#10b981',
          fillColor: {
            linearGradient: { x1: 0, y1: 0, x2: 0, y2: 1 },
            stops: [[0, 'rgba(16, 185, 129, 0.3)'], [1, 'rgba(16, 185, 129, 0.05)']]
          },
          lineWidth: 2,
          valueSuffix: '%'
        },
        {
          type: 'flags',
          name: 'Decisiones IA',
          data: signalFlags,
          onSeries: 'ohlc',
          shape: 'squarepin',
          width: 18,
          includeInCSVExport: false, // EXPULSA POR COMPLETO ESTA COLUMNA DE LA DATA TABLE DESPLEGABLE
          style: { color: '#fff', fontSize: '9px', fontWeight: '700' },
        } as any,
      ] as any,
    };
  }
}