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
import { forkJoin, of, catchError, Subject, takeUntil, switchMap } from 'rxjs';
import { ApiService, TickerPerformanceResponse } from '../../core/services/api.service';
import { ReportService } from '../../core/services/report.service';
import { PipelineContextService } from '../../core/services/pipeline-context.service';
import { DailyReport, TickerView, ReportDateEntry } from '../../core/models/report.model';
import { ChartDataPoint, ChartSeries } from '../../core/models/pipeline.model';
import { initDemoHighcharts } from '../../core/charts/highcharts-stock-demo';

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
  
  availableDates: ReportDateEntry[] = [];
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

  performanceChartType: 'candlestick' | 'line' | 'area' = 'candlestick';

  setPerformanceChartType(type: 'candlestick' | 'line' | 'area'): void {
    this.performanceChartType = type;
    const cached = this.performanceCache.get(`${this.selectedDate}:${this.performanceTicker}`);
    if (cached) {
      this.performanceChartOptions = this.buildPerformanceChartOptions(cached, 540, type);
      this.performanceChartUpdate = true;
    }
  }

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
      this.loadDates();
    });
    this.loadDates();
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

  ngAfterViewInit() {
    this.tableSource.sort = this.sort;
  }

  private loadDates() {
    this.loading = true;
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        this.availableDates = dates;
        if (dates.length === 0) {
          this.loading = false;
          return [];
        }
        const endDate = this.pipelineCtx.pipelineEndDate() ?? dates[0].date;
        this.selectedDate = endDate;
        
        this.loadBacktestingHistory(dates);
        return this.reportSvc.loadReport(this.selectedDate);
      })
    ).subscribe({
      next: (report: any) => { if (report) this.processReport(report); this.loading = false; },
      error: () => { this.loading = false; },
    });
  }

  onDateChange(date: string) {
    this.loading = true;
    this.reportSvc.loadReport(date).subscribe({
      next: r => { 
        this.processReport(r); 
        this.loading = false; 
        
        if (this.performanceTicker) {
            this.loadPerformanceChart(this.performanceTicker);
        }
      },
      error: () => { this.loading = false; },
    });
  }

  refresh() { this.loading = true; this.loadDates(); }

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
        this.performanceChartOptions = this.buildPerformanceChartOptions(cached, 540, this.performanceChartType);
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
      this.performanceChartOptions = this.buildPerformanceChartOptions(cached, 540, this.performanceChartType);
      this.performanceChartUpdate = true;
      return;
    }

    this.performanceLoading = true;
    this.performanceError = '';
    
    // 1. REVERTIMOS AL LÍMITE SEGURO DE 365 DÍAS PARA EVITAR CRASHES DEL BACKEND
    this.apiSvc.getTickerPerformance(ticker, this.selectedDate, 365).pipe( 
      catchError((err) => {
        console.error("Error API Performance:", err);
        this.performanceError = `No se pudo cargar el histórico de ${ticker}.`;
        this.performanceLoading = false;
        return of(null);
      })
    ).subscribe(resp => {
      this.performanceLoading = false;
      if (!resp) return;
      this.performanceCache.set(key, resp);
      this.performanceChartOptions = this.buildPerformanceChartOptions(resp, 540, this.performanceChartType);
      this.performanceChartUpdate = true;
    });
  }

  private pipelineBounds(): { start: string; end: string } | null {
    const p = this.pipelineCtx.selectedPipeline();
    if (!p) return null;
    return { start: p.startDate, end: p.endDate };
  }

  private buildPerformanceChartOptions(resp: TickerPerformanceResponse, height = 540, chartType: 'candlestick' | 'line' | 'area' = 'candlestick'): Highcharts.Options {
    // 1. FORMATO BLINDADO: Cogemos siempre solo los 10 primeros caracteres (YYYY-MM-DD)
    const toTs = (dateStr: string) => {
      if (!dateStr) return 0;
      const iso = dateStr.substring(0, 10); 
      return new Date(`${iso}T00:00:00Z`).getTime();
    };
    
    const pBounds = this.pipelineBounds();
    const startTs = pBounds ? toTs(pBounds.start) : 0;
    const endTs = toTs(this.selectedDate);
    
    const validPoints = resp.points.filter(p => toTs(p.date) >= startTs);
    
    if (validPoints.length === 0) {
        console.warn("No hay puntos para el rango solicitado en buildPerformanceChartOptions");
        return {}; 
    }

    const ohlc = validPoints.map(p => [
      toTs(p.date), +Number(p.open).toFixed(6), +Number(p.high).toFixed(6), +Number(p.low).toFixed(6), +Number(p.close).toFixed(6)
    ]);
    const bbMiddle = validPoints.map(p => [toTs(p.date), p.bb_middle]);

    const initialPrice = validPoints[0].close;

    const buyHold = validPoints.map(p => {
      const ret = initialPrice > 0 ? (p.close - initialPrice) / initialPrice : 0;
      return [toTs(p.date), +(ret * 100).toFixed(2)];
    });

    const strategy = this.historyRows
      .filter(r => r.ticker === resp.ticker && toTs(r.date) >= startTs && toTs(r.date) <= endTs)
      .sort((a, b) => a.date.localeCompare(b.date))
      .map(r => [toTs(r.date), +(100 * (r.final_equity - 10000) / 10000).toFixed(2)]);

    const exposurePoints = this.historyRows
      .filter(r => r.ticker === resp.ticker && toTs(r.date) >= startTs && toTs(r.date) <= endTs)
      .sort((a, b) => a.date.localeCompare(b.date));

    const exposureSeriesData = exposurePoints.map(r => [toTs(r.date), +Number(r.avg_exposure).toFixed(2)]);

    // 2. CREAMOS EL DICCIONARIO DE COLORES (Tus 5 niveles)
    const colorMap: Record<string, string> = {
      'INCREASE_STRONG': '#15803d', // Verde oscuro
      'INCREASE_MILD': '#22c55e',   // Verde claro
      'MAINTAIN': '#3b82f6',        // Azul
      'REDUCE_MILD': '#f59e0b',     // Naranja
      'REDUCE_STRONG': '#b91c1c'    // Rojo
    };

    const rgbMap: Record<string, string> = {
      'INCREASE_STRONG': '21, 128, 61',
      'INCREASE_MILD': '34, 197, 94',
      'MAINTAIN': '59, 130, 246',
      'REDUCE_MILD': '245, 158, 11',
      'REDUCE_STRONG': '185, 28, 28'
    };

    // Mapeamos las recomendaciones limpiando la fecha
    const recMap = new Map<number, string>();
    resp.recommendations.forEach(r => recMap.set(toTs(r.date), r.exposure_recommendation));

    // 3. CONSTRUIMOS LAS ZONAS DINÁMICAS
    let lastRec = 'MAINTAIN'; // Memoria por si algún día no hay dato en la API
    const exposureZones = exposurePoints.map((pt, i) => {
      const ts = toTs(pt.date);
      
      // Si hoy no hay recomendación, heredamos la de ayer
      const rec = recMap.get(ts) || lastRec;
      lastRec = rec;

      const baseColor = colorMap[rec] || colorMap['MAINTAIN'];
      const rgb = rgbMap[rec] || rgbMap['MAINTAIN'];
      
      // La zona aplica el color hasta el "siguiente" día
      const nextTs = i < exposurePoints.length - 1 ? toTs(exposurePoints[i+1].date) : ts + 86400000;
      
      return {
        value: nextTs,
        color: baseColor,
        fillColor: {
          linearGradient: { x1: 0, y1: 0, x2: 0, y2: 1 },
          stops: [
            [0, `rgba(${rgb}, 0.35)`], // Degradado suave arriba
            [1, `rgba(${rgb}, 0.0)`]   // Transparente abajo
          ]
        }
      };
    });

    const flagColor = (rec: string) => rec.startsWith('INCREASE') ? '#16a34a' : rec.startsWith('REDUCE') ? '#7c3aed' : '#94a3b8';
    const flagTitle = (rec: string) => rec === 'INCREASE_STRONG' ? '↑↑' : rec === 'INCREASE_MILD' ? '↑' : rec === 'REDUCE_STRONG' ? '↓↓' : rec === 'REDUCE_MILD' ? '↓' : '→';
    
    const signalFlags = resp.recommendations
      .filter(s => s.exposure_recommendation !== 'MAINTAIN' && toTs(s.date) >= startTs && toTs(s.date) <= endTs)
      .map(s => ({
        x: toTs(s.date),
        title: flagTitle(s.exposure_recommendation),
        text:  s.exposure_recommendation.replace(/_/g, ' '),
        fillColor: flagColor(s.exposure_recommendation),
      }));

    const drawdownPoint = validPoints.find(p => p.date === resp.max_drawdown.date);

    return {
      chart: { 
        height, 
        backgroundColor: 'transparent', 
        zooming: { type: 'x' },
        events: {
          exportData: function (event: any) {
            const dataRows = event.dataRows;
            if (!dataRows || dataRows.length <= 1) return;
            
            dataRows[0][0] = 'Fecha'; 
            
            const headers = dataRows[0];
            const indicesToRemove: number[] = [];
            // Aquí ya no hace falta quitar 3 series, solo las flags y el dropdown
            for(let i=0; i<headers.length; i++) {
                if(headers[i] === 'Decisiones IA' || headers[i] === 'Pico de Caída (Max Drawdown)' || headers[i] === 'Media Bollinger') {
                    indicesToRemove.push(i);
                }
            }

            for (let r = 0; r < dataRows.length; r++) {
                dataRows[r] = dataRows[r].filter((_: any, idx: number) => !indicesToRemove.includes(idx));
            }

            const header = dataRows[0];
            const body = dataRows.slice(1);
            
            body.sort((a: any, b: any) => new Date(b[0]).getTime() - new Date(a[0]).getTime());
            
            dataRows.length = 0; 
            dataRows.push(header, ...body);
          }
        }
      },
      title: { text: 'Autopsia Operativa - ' + resp.ticker, style: { color: '#334155', fontSize: '15px', fontWeight: '700' } },
      credits: { enabled: false },
      exporting: { csv: { dateFormat: '%Y-%m-%d' }, showTable: false },
      rangeSelector: {
        selected: 2, inputEnabled: true, enabled: true,
        buttons: [{ type: 'month', count: 1, text: '1M' }, { type: 'month', count: 3, text: '3M' }, { type: 'month', count: 6, text: '6M' }, { type: 'all', text: 'Todo' }]
      },
      navigator: { enabled: true },
      scrollbar: { enabled: true },
      legend: { enabled: true },
      xAxis: { type: 'datetime', min: startTs },
      yAxis: [
        { title: { text: 'Precio' }, height: '45%', resize: { enabled: true }, gridLineColor: 'rgba(148,163,184,.18)' },
        { title: { text: 'Rentabilidad Acum. (%)' }, top: '50%', height: '25%', offset: 0, resize: { enabled: true }, gridLineColor: 'rgba(148,163,184,.18)', plotLines: [{ value: 0, color: '#94a3b8', width: 1 }] },
        { title: { text: 'Exposición IA (%)' }, top: '80%', height: '20%', offset: 0, min: 0, max: 100, gridLineColor: 'rgba(148,163,184,.18)', plotLines: [{ value: 50, color: '#94a3b8', width: 1, dashStyle: 'Dash' }] }
      ],
      tooltip: { split: true, valueDecimals: 2 },
      plotOptions: {
        series: { dataGrouping: { enabled: false } } as any,
        candlestick: { color: '#ef4444', upColor: '#22c55e', lineColor: '#dc2626', upLineColor: '#16a34a' } as any,
      },
      series: [
        (chartType === 'candlestick' ? { type: 'candlestick', id: 'ohlc', name: `${resp.ticker} Precio`, data: ohlc, yAxis: 0 } : chartType === 'line' ? { type: 'line', id: 'ohlc', name: `${resp.ticker} Precio`, data: ohlc.map(p => [p[0], p[4]]), yAxis: 0, color: '#2563eb', lineWidth: 2 } : { type: 'area', id: 'ohlc', name: `${resp.ticker} Precio`, data: ohlc.map(p => [p[0], p[4]]), yAxis: 0, color: '#2563eb', lineWidth: 2, fillColor: { linearGradient: { x1:0, y1:0, x2:0, y2:1 }, stops: [[0,'rgba(37,99,235,0.25)'],[1,'rgba(37,99,235,0.02)']] } }) as any,
        { type: 'line', name: 'Media Bollinger', data: bbMiddle, yAxis: 0, color: '#64748b', dashStyle: 'ShortDot', lineWidth: 1 },
        { type: 'line', name: 'Rendimiento Estrategia IA (%)', data: strategy, yAxis: 1, color: '#2563eb', lineWidth: 2.2, valueSuffix: '%' },
        { type: 'line', name: 'Rendimiento Buy & Hold (%)', data: buyHold, yAxis: 1, color: '#94a3b8', lineWidth: 1.6, valueSuffix: '%' },
        { type: 'scatter', name: 'Pico de Caída (Max Drawdown)', data: drawdownPoint && toTs(drawdownPoint.date) >= startTs && toTs(drawdownPoint.date) <= endTs ? [[toTs(drawdownPoint.date), drawdownPoint.strategy_return]] : [], yAxis: 1, color: '#ef4444', marker: { enabled: true, symbol: 'triangle-down', radius: 7 }, tooltip: { pointFormatter: function (this: any): string { return `<span style="color:#ef4444">●</span> Max drawdown: <b>${Highcharts.numberFormat(Number(this.y), 2)}%</b><br/>`; } } },
        
        // --- SERIE ÚNICA DE EXPOSICIÓN CON ZONAS ---
        {
          type: 'area',
          name: 'Nivel de Exposición',
          data: exposureSeriesData,
          yAxis: 2,
          zoneAxis: 'x',
          zones: exposureZones,
          lineWidth: 2,
          valueSuffix: '%'
        },

        { type: 'flags', name: 'Decisiones IA', data: signalFlags, onSeries: 'ohlc', shape: 'squarepin', width: 18, showInLegend: false, style: { color: '#fff', fontSize: '9px', fontWeight: '700' } } as any,
      ] as any,
    };
  }
}