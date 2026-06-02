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
import { switchMap, catchError, of, Subject, takeUntil } from 'rxjs';
import { ReportService } from '../../core/services/report.service';
import { PipelineContextService } from '../../core/services/pipeline-context.service';
import { TraceService } from '../../core/services/trace.service';
import {
  ApiService, NewsDetailResponse, NewsArticleDetail, MacroContext, MacroArticle,
  OhlcvPoint, TickerPerformanceResponse, FeatureSnapshot, InferenceModelId,
} from '../../core/services/api.service';
import {
  TickerView, ReportDateEntry, DailyReport,
  SentimentState, RsiState, TrendState, VolatilityState,
} from '../../core/models/report.model';
import { TickerTrace } from '../../core/models/trace.model';
import { ChartDataPoint } from '../../core/models/pipeline.model';

@Component({
  selector: 'app-signals',
  standalone: true,
  imports: [
    CommonModule, FormsModule,
    MatTableModule, MatSortModule,
    MatButtonModule, MatIconModule,
    MatProgressSpinnerModule, MatTooltipModule, MatExpansionModule,
    NgxChartsModule, HighchartsChartModule
  ],
  templateUrl: './signals.component.html',
  styleUrl: './signals.component.scss',
})
export class SignalsComponent implements OnInit, OnDestroy, AfterViewInit {
  private reportSvc = inject(ReportService);
  private traceSvc  = inject(TraceService);
  private apiSvc    = inject(ApiService);
  private pipelineCtx = inject(PipelineContextService);
  private destroy$ = new Subject<void>();

  @ViewChild(MatSort) sort!: MatSort;

  Highcharts: typeof Highcharts = Highcharts;

  loading = true;
  availableDates: ReportDateEntry[] = [];
  selectedDate = '';
  filterExposure = '';   // INCREASE_STRONG | INCREASE_MILD | MAINTAIN | REDUCE_MILD | REDUCE_STRONG | ''
  expandedRows = new Set<string>();

  // 1. SOLUCIÓN: Declaramos la variable que el HTML está buscando para los Smart Donuts
  tickerViews: TickerView[] = [];

  // Exposición primero, recomendación bayesiana como referencia secundaria
  displayedColumns = ['ticker', 'exposure', 'evidence', 'winrate', 'return', 'alpha', 'expand'];
  dataSource = new MatTableDataSource<TickerView>();

  // Gráficos de Resumen
  signalChart: ChartDataPoint[] = [];
  sentimentChart: ChartDataPoint[] = [];
  rsiChart: ChartDataPoint[] = [];
  trendChart: ChartDataPoint[] = [];
  volatilityChart: ChartDataPoint[] = [];

  increaseStrongCount = 0;
  increaseMildCount   = 0;
  maintainCount       = 0;
  reduceMildCount     = 0;
  reduceStrongCount   = 0;
  avgProbUp = 0;

  tickerTraceCache = new Map<string, TickerTrace | null>();
  tickerTraceLoading = new Set<string>();
  featureCache = new Map<string, FeatureSnapshot | null>();
  featureLoading = new Set<string>();
  selectedModelId: InferenceModelId = 'bayesian_v1.2';
  hasTraceForDate = false;

  // ── OHLCV Week chart ──────────────────────────────────────────────────────
  ohlcvWeekCache   = new Map<string, OhlcvPoint[]>();
  ohlcvWeekLoading = new Set<string>();
  weekChartOptionsCache = new Map<string, Highcharts.Options>();

  // ── Highcharts performance chart ──────────────────────────────────────────
  performanceTicker = '';
  performanceLoading = false;
  performanceError = '';
  performanceChartOptions: Highcharts.Options = {};
  performanceChartUpdate = false;
  performanceCache = new Map<string, TickerPerformanceResponse>();

  // Funciones de Coloreado Dinámico (Para NGX-Charts)
  customSignalColors = (name: string) => {
    if (name.startsWith('↑↑')) return '#15803d';
    if (name.startsWith('↑'))  return '#22c55e';
    if (name.startsWith('→'))  return '#3b82f6';
    if (name.startsWith('↓↓')) return '#b91c1c';
    return '#f59e0b';  // ↓ Reducir
  };
  customSentimentColors = (name: string) => {
    if (name === 'ALCISTA') return '#22C55E';
    if (name === 'BAJISTA') return '#EF4444';
    return '#94A3B8'; // NEUTRAL
  };
  customRsiColors = (name: string) => {
    if (name === 'SOBREVENTA') return '#22C55E';
    if (name === 'SOBRECOMPRA') return '#EF4444';
    return '#94A3B8'; // NEUTRAL
  };
  customTrendColors = (name: string) => name === 'ALCISTA' ? '#22C55E' : '#EF4444';
  customVolColors = (name: string) => name === 'BAJA' ? '#3B82F6' : '#F59E0B';

  ngOnInit() {
    this.pipelineCtx.pipelineChanged$.pipe(takeUntil(this.destroy$)).subscribe(() => {
      this.reportSvc.clearCache();
      this.loadInitial();
    });
    this.loadInitial();
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private loadInitial() {
    this.loading = true;
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        this.availableDates = dates;
        if (!dates.length) { this.loading = false; return []; }
        this.selectedDate = this.pipelineCtx.pipelineEndDate() ?? dates[0].date;
        const entry = dates.find(d => d.date === this.selectedDate) ?? dates[0];
        this.hasTraceForDate = !!(entry as any).has_trace;
        return this.reportSvc.loadReport(this.selectedDate);
      })
    ).subscribe({
      next: (r: any) => {
        if (r) this.processReport(r);
        this.loading = false;
        this.loadMacroContext(this.selectedDate);
      },
      error: () => { this.loading = false; },
    });
  }

  ngAfterViewInit() {
    this.dataSource.sort = this.sort;
  }

  onDateChange(date: string) {
    this.loading = true;
    this.tickerTraceCache.clear();
    this.ohlcvWeekCache.clear();
    this.ohlcvWeekLoading.clear();
    this.weekChartOptionsCache.clear();
    this.performanceCache.clear();
    this.performanceChartOptions = {};
    this.performanceError = '';
    this.expandedRows.clear();
    const entry = this.availableDates.find(d => d.date === date);
    this.hasTraceForDate = !!(entry as any)?.has_trace;
    this.reportSvc.loadReport(date).subscribe({
      next: r => {
        this.processReport(r);
        this.loading = false;
        this.loadMacroContext(date);
      },
      error: () => { this.loading = false; },
    });
  }

  private processReport(report: DailyReport) {
    const views = this.reportSvc.buildTickerViews(report);
    
    // 2. SOLUCIÓN: Guardamos los datos en la variable para que el HTML pueda contar la longitud
    this.tickerViews = views; 
    this.dataSource.data = views;
    
    if (this.sort) {
      this.dataSource.sort = this.sort;
    }

    this.dataSource.filterPredicate = (row, filter) => !filter || row.exposure_recommendation === filter;

    this.increaseStrongCount = views.filter(v => v.exposure_recommendation === 'INCREASE_STRONG').length;
    this.increaseMildCount   = views.filter(v => v.exposure_recommendation === 'INCREASE_MILD').length;
    this.maintainCount       = views.filter(v => v.exposure_recommendation === 'MAINTAIN').length;
    this.reduceMildCount     = views.filter(v => v.exposure_recommendation === 'REDUCE_MILD').length;
    this.reduceStrongCount   = views.filter(v => v.exposure_recommendation === 'REDUCE_STRONG').length;
    this.avgProbUp = views.length ? (views.reduce((s, v) => s + v.prob_up, 0) / views.length) * 100 : 0;

    // Procesar datos para gráficos de Nodos (Con nombres en castellano)
    const sent = { ALCISTA: 0, BAJISTA: 0, NEUTRAL: 0 };
    const rsi = { SOBREVENTA: 0, SOBRECOMPRA: 0, NEUTRAL: 0 };
    const trend = { ALCISTA: 0, BAJISTA: 0 };
    const vol = { ALTA: 0, BAJA: 0 };

    views.forEach(v => {
      const e = v.evidence;
      if (e.sentiment === 'bullish') sent.ALCISTA++;
      else if (e.sentiment === 'bearish') sent.BAJISTA++;
      else sent.NEUTRAL++;

      if (e.rsi === 'oversold') rsi.SOBREVENTA++;
      else if (e.rsi === 'overbought') rsi.SOBRECOMPRA++;
      else rsi.NEUTRAL++;

      if (e.trend === 'uptrend') trend.ALCISTA++;
      else trend.BAJISTA++;

      if (e.volatility === 'high') vol.ALTA++;
      else vol.BAJA++;
    });

    this.signalChart = [
      { name: '↑↑ Aumentar fuerte', value: this.increaseStrongCount },
      { name: '↑  Aumentar',        value: this.increaseMildCount   },
      { name: '→  Mantener',        value: this.maintainCount       },
      { name: '↓  Reducir',         value: this.reduceMildCount     },
      { name: '↓↓ Reducir fuerte',  value: this.reduceStrongCount   },
    ].filter(i => i.value > 0);

    this.sentimentChart = Object.entries(sent).map(([name, value]) => ({ name, value })).filter(i => i.value > 0);
    this.rsiChart = Object.entries(rsi).map(([name, value]) => ({ name, value })).filter(i => i.value > 0);
    this.trendChart = Object.entries(trend).map(([name, value]) => ({ name, value })).filter(i => i.value > 0);
    this.volatilityChart = Object.entries(vol).map(([name, value]) => ({ name, value })).filter(i => i.value > 0);

    if (!views.some(v => v.ticker === this.performanceTicker)) {
      this.performanceTicker = views[0]?.ticker ?? '';
    }
    if (this.performanceTicker) {
      this.loadPerformanceChart(this.performanceTicker);
    }
  }

  applyFilter() {
    this.dataSource.filter = this.filterExposure;
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
        this.performanceError = `No se pudo cargar el histórico de ${ticker}. Ejecuta el bootstrap hasta ${this.selectedDate}.`;
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

  private buildPerformanceChartOptions(resp: TickerPerformanceResponse): Highcharts.Options {
    const toTs = (date: string) => new Date(`${date}T00:00:00Z`).getTime();
    const points = resp.points;
    const targetTs = toTs(resp.target_date);
    const visibleStart = targetTs - 1000 * 60 * 60 * 24 * 120;

    const ohlc = points.map(p => [toTs(p.date), p.open, p.high, p.low, p.close]);
    const bbUpper = points.map(p => [toTs(p.date), p.bb_upper]);
    const bbMiddle = points.map(p => [toTs(p.date), p.bb_middle]);
    const bbLower = points.map(p => [toTs(p.date), p.bb_lower]);
    const strategy = points.map(p => [toTs(p.date), p.strategy_return]);
    const buyHold = points.map(p => [toTs(p.date), p.buy_hold_return]);
    const drawdownPoint = points.find(p => p.date === resp.max_drawdown.date);

    const stageBands = resp.stages.map(stage => ({
      from: toTs(stage.from),
      to: toTs(stage.to) + 1000 * 60 * 60 * 24,
      color: stage.stage === 'LONG' ? 'rgba(34,197,94,.055)' : 'rgba(124,58,237,.055)',
      label: {
        text: stage.stage,
        style: { color: stage.stage === 'LONG' ? '#15803d' : '#6d28d9', fontSize: '10px', fontWeight: '600' },
      },
    }));

    // Flags del gráfico de precio — traducimos la recomendación interna BN a lenguaje de exposición
    const flagColor = (signal: string) =>
      signal === 'BUY' ? '#16a34a' : signal === 'SELL' ? '#7c3aed' : '#94a3b8';
    const flagTitle = (signal: string) =>
      signal === 'BUY' ? '↑' : signal === 'SELL' ? '↓' : '→';
    const flagText  = (signal: string, probUp: number | null) => {
      const pct = probUp != null ? ` · P(↑) ${(probUp * 100).toFixed(1)}%` : '';
      return signal === 'BUY'  ? `Aumentar exposición${pct}`
           : signal === 'SELL' ? `Reducir exposición${pct}`
           : `Mantener exposición${pct}`;
    };

    const signalFlags = resp.signals
      .filter(s => s.signal !== 'HOLD')
      .map(s => ({
        x: toTs(s.date),
        title: flagTitle(s.signal),
        text:  flagText(s.signal, s.prob_up),
        fillColor: flagColor(s.signal),
      }));

    return {
      chart: {
        height: 540,
        backgroundColor: 'transparent',
        zooming: { type: 'x' },
      },
      title: { text: undefined },
      credits: { enabled: false },
      rangeSelector: {
        selected: 2,
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
      legend: { enabled: true },
      xAxis: {
        type: 'datetime',
        min: Math.max(points.length ? toTs(points[0].date) : visibleStart, visibleStart),
        max: targetTs,
        plotBands: stageBands as any,
        plotLines: [{
          value: targetTs,
          color: '#2563eb',
          width: 2,
          dashStyle: 'Dash',
          label: { text: `Fecha seleccionada: ${resp.target_date}`, rotation: 0, y: 14, style: { color: '#2563eb', fontWeight: '600' } },
        }],
      },
      yAxis: [{
        title: { text: 'Precio' },
        height: '62%',
        resize: { enabled: true },
        gridLineColor: 'rgba(148,163,184,.18)',
      }, {
        title: { text: 'Rendimiento (%)' },
        top: '68%',
        height: '32%',
        offset: 0,
        opposite: false,
        gridLineColor: 'rgba(148,163,184,.18)',
        plotLines: [{ value: 0, color: '#94a3b8', width: 1 }],
      }],
      tooltip: {
        split: true,
        valueDecimals: 2,
      },
      plotOptions: {
        series: {
          dataGrouping: { enabled: false },
          marker: { enabled: false },
        } as any,
        candlestick: {
          color: '#ef4444',
          upColor: '#22c55e',
          lineColor: '#dc2626',
          upLineColor: '#16a34a',
        } as any,
      },
      series: [
        { type: 'candlestick', id: 'ohlc', name: `${resp.ticker} OHLC`, data: ohlc, yAxis: 0 },
        { type: 'line', name: 'Bollinger superior', data: bbUpper, yAxis: 0, color: '#f59e0b', dashStyle: 'ShortDash', lineWidth: 1.4 },
        { type: 'line', name: 'Media Bollinger', data: bbMiddle, yAxis: 0, color: '#64748b', dashStyle: 'ShortDot', lineWidth: 1 },
        { type: 'line', name: 'Bollinger inferior', data: bbLower, yAxis: 0, color: '#f59e0b', dashStyle: 'ShortDash', lineWidth: 1.4 },
        { type: 'line', name: 'Rendimiento estrategia', data: strategy, yAxis: 1, color: '#2563eb', lineWidth: 2.2 },
        { type: 'line', name: 'Buy & Hold', data: buyHold, yAxis: 1, color: '#94a3b8', lineWidth: 1.6 },
        {
          type: 'scatter',
          name: 'Max drawdown',
          data: drawdownPoint ? [[toTs(drawdownPoint.date), drawdownPoint.strategy_return]] : [],
          yAxis: 1,
          color: '#ef4444',
          marker: { enabled: true, symbol: 'triangle-down', radius: 7 },
          tooltip: {
            pointFormatter: function () {
              return `<span style="color:#ef4444">●</span> Max drawdown: <b>${(resp.max_drawdown.drawdown * 100).toFixed(2)}%</b><br/>`;
            },
          },
        },
        {
          type: 'flags',
          name: 'Recomendaciones',
          data: signalFlags,
          onSeries: 'ohlc',
          shape: 'squarepin',
          width: 18,
          style: { color: '#fff', fontSize: '9px', fontWeight: '700' },
        } as any,
      ] as any,
    };
  }

  toggleRow(ticker: string) {
    if (this.expandedRows.has(ticker)) {
      this.expandedRows.delete(ticker);
    } else {
      this.expandedRows.add(ticker);
      this.loadTickerTrace(ticker);
      this.loadFeatureSnapshot(ticker);
      this.loadOhlcvWeek(ticker);
    }
  }

  loadFeatureSnapshot(ticker: string) {
    if (this.featureCache.has(ticker) || this.featureLoading.has(ticker)) return;
    this.featureLoading.add(ticker);
    this.apiSvc.getFeatures(this.selectedDate, ticker).pipe(
      catchError(() => of(null))
    ).subscribe(doc => {
      this.featureLoading.delete(ticker);
      this.featureCache.set(ticker, doc);
    });
  }

  getFeatureSnapshot(ticker: string): FeatureSnapshot | null {
    return this.featureCache.get(ticker) ?? null;
  }

  isFeatureLoading(ticker: string): boolean {
    return this.featureLoading.has(ticker);
  }

  getRecommendedExposure(ticker: string): number | null {
    const f = this.getFeatureSnapshot(ticker);
    const c = f?.exposure_constraints?.constrained_exposure;
    return c != null ? Math.round(c * 1000) / 10 : null;
  }

  loadOhlcvWeek(ticker: string) {
    if (this.ohlcvWeekCache.has(ticker) || this.ohlcvWeekLoading.has(ticker)) return;
    this.ohlcvWeekLoading.add(ticker);
    this.apiSvc.getOhlcvMonth(ticker, this.selectedDate).pipe(
      catchError(() => of(null))
    ).subscribe(resp => {
      this.ohlcvWeekLoading.delete(ticker);
      this.ohlcvWeekCache.set(ticker, resp?.points ?? []);
    });
  }

  getOhlcvPoints(ticker: string): OhlcvPoint[] {
    return this.ohlcvWeekCache.get(ticker) ?? [];
  }

  isOhlcvLoading(ticker: string): boolean {
    return this.ohlcvWeekLoading.has(ticker);
  }

  hasWeekChartData(ticker: string): boolean {
    return (this.ohlcvWeekCache.get(ticker)?.length ?? 0) > 0;
  }

  getWeekChartOptions(ticker: string): Highcharts.Options {
    const cached = this.weekChartOptionsCache.get(ticker);
    if (cached) return cached;
    const opts = this.buildWeekChartOptions(ticker);
    if (opts.series && (opts.series as any[]).length) {
      this.weekChartOptionsCache.set(ticker, opts);
    }
    return opts;
  }

  private buildWeekChartOptions(ticker: string): Highcharts.Options {
    const pts = this.ohlcvWeekCache.get(ticker) ?? [];
    if (pts.length === 0) return {};

    const toTs = (date: string) => new Date(`${date}T00:00:00Z`).getTime();
    const targetTs = toTs(this.selectedDate);

    const data = pts.map(p => {
      const isTarget = p.date === this.selectedDate;
      return {
        x: toTs(p.date),
        y: p.close,
        marker: isTarget
          ? { enabled: true, radius: 6, fillColor: '#2563eb', lineColor: '#ffffff', lineWidth: 2 }
          : { enabled: true, radius: 3, fillColor: '#ffffff', lineColor: '#2563eb', lineWidth: 1.5 },
      };
    });

    return {
      chart: {
        type: 'area',
        height: 280,
        backgroundColor: 'transparent',
        spacing: [16, 16, 12, 8],
        style: { fontFamily: 'inherit' },
      },
      title: { text: undefined },
      credits: { enabled: false },
      legend: { enabled: false },
      rangeSelector: { enabled: false, inputEnabled: false, buttonTheme: { visibility: 'hidden' }, labelStyle: { visibility: 'hidden' } } as any,
      navigator: { enabled: false },
      scrollbar: { enabled: false },
      xAxis: {
        type: 'datetime',
        tickPixelInterval: 90,
        lineColor: 'rgba(148,163,184,.3)',
        tickColor: 'rgba(148,163,184,.3)',
        labels: {
          style: { fontSize: '11px', color: '#94a3b8' },
          format: '{value:%d %b}',
        },
        plotLines: [{
          value: targetTs,
          color: '#2563eb',
          width: 1.5,
          dashStyle: 'Dash',
          zIndex: 5,
          label: {
            text: `Día analizado`,
            rotation: 0,
            y: 14,
            x: 6,
            style: { color: '#2563eb', fontWeight: '600', fontSize: '10px' },
          },
        }],
      },
      yAxis: {
        title: { text: undefined },
        gridLineColor: 'rgba(148,163,184,.18)',
        labels: {
          style: { fontSize: '11px', color: '#94a3b8' },
          formatter: function () { return '$' + (this.value as number).toFixed(2); },
        },
      },
      tooltip: {
        backgroundColor: 'rgba(15,23,42,.92)',
        borderWidth: 0,
        borderRadius: 6,
        shadow: false,
        style: { color: '#f8fafc', fontSize: '12px' },
        headerFormat: '<span style="font-size:10px;color:#94a3b8">{point.key}</span><br/>',
        pointFormat: '<b style="color:#60a5fa">${point.y:.2f}</b>',
        xDateFormat: '%A, %d %b %Y',
        useHTML: true,
      },
      plotOptions: {
        area: {
          lineWidth: 2.2,
          lineColor: '#2563eb',
          color: '#2563eb',
          fillColor: {
            linearGradient: { x1: 0, y1: 0, x2: 0, y2: 1 },
            stops: [
              [0, 'rgba(37,99,235,0.28)'],
              [1, 'rgba(37,99,235,0)'],
            ],
          },
          states: { hover: { lineWidth: 2.6 } },
          marker: { symbol: 'circle' },
          dataGrouping: { enabled: false } as any,
        },
      },
      series: [{
        type: 'area',
        name: 'Precio cierre',
        data,
      }],
    };
  }

  loadTickerTrace(ticker: string) {
    if (this.tickerTraceCache.has(ticker) || this.tickerTraceLoading.has(ticker)) return;
    this.tickerTraceLoading.add(ticker);
    this.traceSvc.getTickerTrace(this.selectedDate, ticker).pipe(
      catchError(() => of(null))
    ).subscribe(resp => {
      this.tickerTraceLoading.delete(ticker);
      this.tickerTraceCache.set(ticker, resp?.trace ?? null);
    });
  }

  getTickerTrace(ticker: string): TickerTrace | null {
    return this.tickerTraceCache.get(ticker) ?? null;
  }

  isTraceLoading(ticker: string): boolean {
    return this.tickerTraceLoading.has(ticker);
  }

  getSentimentDist(ticker: string): { key: string; count: number; pct: number }[] {
    const t = this.getTickerTrace(ticker);
    if (!t?.sentiment_detail?.distribution) return [];
    return Object.entries(t.sentiment_detail.distribution).map(([key, v]: [string, any]) => ({
      key, count: v.count, pct: v.pct,
    }));
  }

  // Utilidad de Traducción
  translateState(state: string): string {
    const dict: Record<string, string> = {
      bullish: 'Alcista', bearish: 'Bajista', neutral: 'Neutral',
      oversold: 'Sobreventa', overbought: 'Sobrecompra',
      uptrend: 'Alcista', downtrend: 'Bajista',
      low: 'Baja', high: 'Alta'
    };
    return dict[state] || state;
  }

  getProbClass(prob: number): string {
    if (prob >= 0.65) return 'high';
    if (prob <= 0.35) return 'low';
    return 'mid'; // Mantener (Amarillo)
  }

  getTextClass(prob: number): string {
    if (prob >= 0.65) return 'green';
    if (prob <= 0.35) return 'purple';
    return 'yellow'; 
  }

  signalIcon(s: string) {
    return ({ BUY: 'arrow_upward', SELL: 'arrow_downward', HOLD: 'remove' } as Record<string, string>)[s] ?? 'remove';
  }

  // ── Helpers de exposición ─────────────────────────────────────────────────
  expRecLabel(rec: string): string {
    const m: Record<string, string> = {
      INCREASE_STRONG: '↑↑ Aumentar fuerte',
      INCREASE_MILD:   '↑  Aumentar',
      MAINTAIN:        '→  Mantener posición',
      REDUCE_MILD:     '↓  Reducir',
      REDUCE_STRONG:   '↓↓ Reducir fuerte',
    };
    return m[rec] ?? rec;
  }

  expRecIcon(rec: string): string {
    const m: Record<string, string> = {
      INCREASE_STRONG: 'arrow_upward',
      INCREASE_MILD:   'trending_up',
      MAINTAIN:        'remove',
      REDUCE_MILD:     'trending_down',
      REDUCE_STRONG:   'arrow_downward',
    };
    return m[rec] ?? 'remove';
  }

  expBarClass(pct: number): string {
    if (pct >= 72) return 'exp-high';
    if (pct >= 58) return 'exp-mid';
    return 'exp-low';
  }

  expRecClass(rec: string): string {
    return (rec ?? '').toLowerCase().replace(/_/g, '-');
  }
  sentimentIcon(s: SentimentState) {
    return ({ bullish: 'sentiment_very_satisfied', bearish: 'sentiment_very_dissatisfied', neutral: 'sentiment_neutral' })[s];
  }

  sentimentClass(v: SentimentState)  { return `ev-${v}`; }
  rsiClass(v: RsiState)              {
    return v === 'oversold' ? 'ev-oversold' : v === 'overbought' ? 'ev-overbought' : 'ev-neutral-rsi';
  }
  trendClass(v: TrendState)          { return `ev-${v}`; }
  volClass(v: VolatilityState)       { return v === 'low' ? 'ev-low-vol' : 'ev-high-vol'; }

  // ─── Modal de detalle de noticias ───────────────────────────────────────────

  newsModalOpen    = false;
  newsModalTicker  = '';
  newsModalLoading = false;
  newsModalData:   NewsDetailResponse | null = null;
  newsModalError   = '';

  openNewsModal(ticker: string) {
    this.newsModalTicker  = ticker;
    this.newsModalOpen    = true;
    this.newsModalLoading = true;
    this.newsModalData    = null;
    this.newsModalError   = '';
    this.apiSvc.getNewsDetail(this.selectedDate, ticker).pipe(
      catchError(() => {
        this.newsModalError   = 'No se pudieron cargar las noticias. Comprueba que el pipeline se ha ejecutado para esta fecha.';
        this.newsModalLoading = false;
        return of(null);
      })
    ).subscribe(data => {
      this.newsModalData    = data;
      this.newsModalLoading = false;
    });
  }

  closeNewsModal() {
    this.newsModalOpen  = false;
    this.newsModalData  = null;
    this.newsModalError = '';
  }

  sentimentColor(s: string): string {
    if (s === 'bullish') return '#22C55E';
    if (s === 'bearish') return '#EF4444';
    return '#94A3B8';
  }

  sentimentBg(s: string): string {
    if (s === 'bullish') return 'rgba(34,197,94,.12)';
    if (s === 'bearish') return 'rgba(239,68,68,.12)';
    return 'rgba(148,163,184,.12)';
  }

  sentimentLabel(s: string): string {
    if (s === 'bullish') return 'Alcista';
    if (s === 'bearish') return 'Bajista';
    return 'Neutral';
  }

  // ─── Macro Context ──────────────────────────────────────────────────────────

  macroContext:     MacroContext | null = null;
  macroNews:        MacroArticle[]      = [];
  macroLoading      = false;
  macroNewsLoading  = false;
  macroError        = '';
  macroNewsExpanded = false;
  macroNewsCategory = '';

  get macroNewsByCategory(): Record<string, MacroArticle[]> {
    const grouped: Record<string, MacroArticle[]> = {};
    const filtered = this.macroNewsCategory
      ? this.macroNews.filter(a => a.category === this.macroNewsCategory)
      : this.macroNews;
    for (const art of filtered) {
      const cat = art.category || 'macro';
      if (!grouped[cat]) grouped[cat] = [];
      grouped[cat].push(art);
    }
    return grouped;
  }

  get macroCategories(): string[] {
    return [...new Set(this.macroNews.map(a => a.category || 'macro'))];
  }

  loadMacroContext(date: string) {
    this.macroLoading = true;
    this.macroContext = null;
    this.macroError   = '';
    this.apiSvc.getMacroContext(date).pipe(
      catchError(() => {
        this.macroError   = 'Sin datos macro para esta fecha. El pipeline debe haber ejecutado lambda_macro_context.';
        this.macroLoading = false;
        return of(null);
      })
    ).subscribe(data => {
      this.macroContext = data ? this.normalizeMacroContext(data) : null;
      this.macroLoading = false;
      if (data) this.loadMacroNews(date);
    });
  }

  private normalizeMacroContext(data: MacroContext): MacroContext {
    const raw = data as any;
    const detail = raw.detail ?? {};
    const events = detail.events ?? {};

    return {
      ...data,
      detail: {
        macro_score: detail.macro_score ?? 0,
        n_articles: detail.n_articles ?? 0,
        distribution: detail.distribution ?? {},
        vix: detail.vix ?? null,
        events: {
          geopolitical: Boolean(events.geopolitical),
          hawkish_fed: Boolean(events.hawkish_fed),
          dovish_fed: Boolean(events.dovish_fed),
          inflation_shock: Boolean(events.inflation_shock),
        },
        regime_reasoning: detail.regime_reasoning ?? {},
      },
    };
  }

  loadMacroNews(date: string) {
    this.macroNewsLoading = true;
    this.apiSvc.getMacroNews(date, 100).pipe(
      catchError(() => of(null))
    ).subscribe(resp => {
      this.macroNews        = resp?.articles ?? [];
      this.macroNewsLoading = false;
    });
  }

  macroSentimentColor(s: string): string {
    if (s === 'bullish') return '#22C55E';
    if (s === 'bearish') return '#EF4444';
    return '#94A3B8';
  }

  macroSentimentBg(s: string): string {
    if (s === 'bullish') return 'rgba(34,197,94,.12)';
    if (s === 'bearish') return 'rgba(239,68,68,.12)';
    return 'rgba(148,163,184,.12)';
  }

  regimeColor(r: string): string {
    if (r === 'RISK_ON')  return '#22C55E';
    if (r === 'RISK_OFF') return '#EF4444';
    return '#F59E0B';
  }

  regimeBg(r: string): string {
    if (r === 'RISK_ON')  return 'rgba(34,197,94,.12)';
    if (r === 'RISK_OFF') return 'rgba(239,68,68,.12)';
    return 'rgba(245,158,11,.12)';
  }

  regimeIcon(r: string): string {
    if (r === 'RISK_ON')  return 'trending_up';
    if (r === 'RISK_OFF') return 'trending_down';
    return 'remove';
  }

  categoryLabel(c: string): string {
    const map: Record<string,string> = {
      monetary_policy:    'Política Monetaria',
      inflation:          'Inflación',
      macro_economy:      'Macroeconomía',
      geopolitical:       'Geopolítica',
      commodities:        'Commodities',
      financial_stability:'Estabilidad Financiera',
      trade_tech:         'Comercio & Tecnología',
      macro:              'Macro General',
    };
    return map[c] || c;
  }

  categoryIcon(c: string): string {
    const map: Record<string,string> = {
      monetary_policy:    'account_balance',
      inflation:          'price_change',
      macro_economy:      'public',
      geopolitical:       'flag',
      commodities:        'oil_barrel',
      financial_stability:'security',
      trade_tech:         'devices',
    };
    return map[c] || 'article';
  }

  // ─── Decision-Ready Layer ────────────────────────────────────────────────

  getCompositeScore(row: TickerView): number {
    const probScore  = row.prob_up * 100;
    const wrScore    = row.win_rate * 100;
    const alphaScore = Math.min(Math.max(((row.alpha_vs_benchmark ?? 0) + 0.5) * 100, 0), 100);
    return Math.round(0.40 * probScore + 0.30 * wrScore + 0.30 * alphaScore);
  }

  getCompositeLabel(score: number): string {
    if (score >= 70) return 'Muy Alta';
    if (score >= 55) return 'Alta';
    if (score >= 40) return 'Media';
    return 'Baja';
  }

  getCompositeClass(score: number): string {
    if (score >= 70) return 'comp-high';
    if (score >= 55) return 'comp-mid-high';
    if (score >= 40) return 'comp-mid';
    return 'comp-low';
  }

  buildWhyNow(row: TickerView): string {
    const signalLabel = this.expRecLabel(row.exposure_recommendation);
    const bullishFlags = [
      row.evidence.sentiment === 'bullish',
      row.evidence.trend === 'uptrend',
      row.evidence.rsi === 'oversold',
      row.evidence.volatility === 'low',
    ];
    const bullishCount = bullishFlags.filter(Boolean).length;

    const factors: string[] = [];
    if (row.evidence.sentiment === 'bullish')  factors.push('sentimiento alcista en titulares financieros (FinBERT)');
    else if (row.evidence.sentiment === 'bearish') factors.push('sentimiento bajista en noticias del sector (FinBERT)');
    else factors.push('sentimiento neutral en medios financieros (FinBERT)');

    if (row.evidence.trend === 'uptrend')  factors.push('tendencia alcista confirmada por medias móviles (SMA20 > SMA50)');
    else factors.push('tendencia bajista en medias móviles (SMA20 < SMA50)');

    if (row.evidence.rsi === 'oversold')        factors.push('RSI en zona de sobreventa — posible rebote técnico inminente');
    else if (row.evidence.rsi === 'overbought') factors.push('RSI en sobrecompra — recomendación de precaución ante posible corrección');
    else factors.push('RSI en zona neutral sin presión técnica dominante');

    if (row.evidence.volatility === 'low') factors.push('volatilidad comprimida — condiciones de entrada favorables');
    else factors.push('volatilidad elevada — mayor incertidumbre en la ejecución');

    let text = `${row.ticker} genera recomendación ${signalLabel} con ${bullishCount} de 4 condiciones favorables alineadas: `;
    text += factors.slice(0, 3).join('; ') + '. ';

    if (row.trades_closed > 0) {
      const wr = Math.round(row.win_rate * 100);
      const alpha = row.alpha_vs_benchmark ?? 0;
      const alphaStr = alpha >= 0 ? `+${(alpha * 100).toFixed(1)}%` : `${(alpha * 100).toFixed(1)}%`;
      text += `En ${row.trades_closed} ciclos históricos, el modelo acertó el ${wr}% de las operaciones `;
      text += `con un alpha de ${alphaStr} frente al benchmark de mercado.`;
    } else {
      text += 'No hay ciclos históricos cerrados para este activo en el periodo analizado.';
    }
    return text;
  }

  getRiskProfiles(row: TickerView): { type: string; label: string; icon: string; color: string; action: string; rationale: string; suitable: boolean }[] {
    const score = this.getCompositeScore(row);
    // Exposure-based risk assessment (replaces binary BUY/SELL/HOLD)
    const isBuy  = ['INCREASE_STRONG', 'INCREASE_MILD'].includes(row.exposure_recommendation);
    const isHold = row.exposure_recommendation === 'MAINTAIN';
    const isSell = ['REDUCE_MILD', 'REDUCE_STRONG'].includes(row.exposure_recommendation);

    return [
      {
        type: 'conservative',
        label: 'Perfil Conservador',
        icon: 'shield',
        color: '#3B82F6',
        action: isBuy && score >= 70 && row.win_rate >= 0.55
          ? 'Entrada permitida — recomendación sólida'
          : isHold ? 'Mantener posición si ya invertido'
          : 'No actuar — esperar recomendación más clara',
        rationale: isBuy && score >= 70 && row.win_rate >= 0.55
          ? `Confianza compuesta ${score}/100 y tasa de acierto ${Math.round(row.win_rate*100)}% superan los umbrales mínimos para perfil conservador (≥70 y ≥55%).`
          : `La confianza compuesta (${score}/100) o la tasa de acierto (${Math.round(row.win_rate*100)}%) no alcanzan los umbrales requeridos para asumir riesgo.`,
        suitable: isBuy && score >= 70 && row.win_rate >= 0.55,
      },
      {
        type: 'moderate',
        label: 'Perfil Moderado',
        icon: 'balance',
        color: '#F59E0B',
        action: isBuy && score >= 55
          ? 'Entrada recomendada — relación riesgo/beneficio positiva'
          : isHold ? 'Mantener con stop-loss ajustado'
          : 'Reducir exposición o no entrar',
        rationale: isBuy && score >= 55
          ? `Confianza compuesta ${score}/100 con probabilidad alcista del ${Math.round(row.prob_up*100)}%. Adecuado para posición de tamaño estándar.`
          : `Recomendación ${this.expRecLabel(row.exposure_recommendation)} con confianza ${score}/100. El perfil moderado requiere ≥55 de confianza compuesta para abrir posición.`,
        suitable: isBuy && score >= 55,
      },
      {
        type: 'aggressive',
        label: 'Perfil Agresivo',
        icon: 'rocket_launch',
        color: '#EF4444',
        action: isBuy
          ? 'Entrada directa siguiendo recomendación del modelo'
          : isHold ? 'Mantener con trailing stop'
          : 'Salir del mercado y buscar alternativas',
        rationale: isBuy
          ? `Probabilidad alcista del ${Math.round(row.prob_up*100)}%. El perfil agresivo ejecuta cualquier recomendación de incremento de exposición sin filtros adicionales de confianza.`
          : isSell
          ? `El modelo recomienda reducir exposición. El perfil agresivo prioriza preservar capital y esperar mejor punto de reentrada.`
          : `Recomendación MANTENER — el perfil agresivo conserva posición existente pero activa trailing stop del 3%.`,
        suitable: isBuy,
      },
    ];
  }
}
