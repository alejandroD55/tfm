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
  OhlcvPoint, TickerPerformanceResponse,
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
  filterSignal = '';
  expandedRows = new Set<string>();

  // 1. SOLUCIÓN: Declaramos la variable que el HTML está buscando para los Smart Donuts
  tickerViews: TickerView[] = [];

  // Nombres Limpios y en Castellano para la Tabla
  displayedColumns = ['ticker', 'signal', 'prob_up', 'evidence', 'trades', 'winrate', 'return', 'alpha', 'expand'];
  dataSource = new MatTableDataSource<TickerView>();

  // Gráficos de Resumen
  signalChart: ChartDataPoint[] = [];
  sentimentChart: ChartDataPoint[] = [];
  rsiChart: ChartDataPoint[] = [];
  trendChart: ChartDataPoint[] = [];
  volatilityChart: ChartDataPoint[] = [];

  buyCount  = 0;
  sellCount = 0;
  holdCount = 0;
  avgProbUp = 0;

  tickerTraceCache = new Map<string, TickerTrace | null>();
  tickerTraceLoading = new Set<string>();
  hasTraceForDate = false;

  // ── OHLCV Week chart ──────────────────────────────────────────────────────
  ohlcvWeekCache   = new Map<string, OhlcvPoint[]>();
  ohlcvWeekLoading = new Set<string>();

  // ── Highcharts performance chart ──────────────────────────────────────────
  performanceTicker = '';
  performanceLoading = false;
  performanceError = '';
  performanceChartOptions: Highcharts.Options = {};
  performanceChartUpdate = false;
  performanceCache = new Map<string, TickerPerformanceResponse>();

  // Funciones de Coloreado Dinámico (Para NGX-Charts)
  customSignalColors = (name: string) => {
    if (name === 'COMPRAR') return '#22C55E';
    if (name === 'CASH') return '#7C3AED';
    return '#F59E0B'; // MANTENER
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

    this.dataSource.filterPredicate = (row, filter) => !filter || row.signal === filter;

    this.buyCount  = views.filter(v => v.signal === 'BUY').length;
    this.sellCount = views.filter(v => v.signal === 'SELL').length;
    this.holdCount = views.filter(v => v.signal === 'HOLD').length;
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
      { name: 'COMPRAR', value: this.buyCount },
      { name: 'CASH', value: this.sellCount },
      { name: 'MANTENER', value: this.holdCount }
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

  applyFilter() { this.dataSource.filter = this.filterSignal; }

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

    const flagColor = (signal: string) =>
      signal === 'BUY' ? '#16a34a' : signal === 'SELL' ? '#7c3aed' : '#f59e0b';

    const signalFlags = resp.signals
      .filter(s => s.signal !== 'HOLD')
      .map(s => ({
        x: toTs(s.date),
        title: s.signal,
        text: `${s.signal} · P(up) ${s.prob_up != null ? (s.prob_up * 100).toFixed(1) : '?'}%`,
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
          name: 'Señales',
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
      this.loadOhlcvWeek(ticker);
    }
  }

  loadOhlcvWeek(ticker: string) {
    if (this.ohlcvWeekCache.has(ticker) || this.ohlcvWeekLoading.has(ticker)) return;
    this.ohlcvWeekLoading.add(ticker);
    this.apiSvc.getOhlcvWeek(ticker, this.selectedDate).pipe(
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

  /** Returns SVG-ready data for the weekly price mini-chart */
  getWeekSvgData(ticker: string): {
    points: { x: number; y: number; date: string; close: number; isTarget: boolean }[];
    path:     string;
    areaPath: string;
    minClose: number;
    maxClose: number;
  } {
    const pts = this.ohlcvWeekCache.get(ticker) ?? [];
    if (pts.length === 0) return { points: [], path: '', areaPath: '', minClose: 0, maxClose: 0 };

    const W = 560, H = 140, padX = 44, padY = 18, bottomPad = 28;
    const chartW = W - padX * 2;
    const chartH = H - padY - bottomPad;

    const closes = pts.map(p => p.close);
    const minC   = Math.min(...closes);
    const maxC   = Math.max(...closes);
    const range  = maxC - minC || minC * 0.02 || 1;

    const xOf = (i: number) => pts.length > 1 ? padX + (i / (pts.length - 1)) * chartW : W / 2;
    const yOf = (c: number) => padY + (1 - (c - minC) / range) * chartH;
    const bottomY = padY + chartH;

    const svgPoints = pts.map((p, i) => ({
      x: xOf(i),
      y: yOf(p.close),
      date:     p.date,
      close:    p.close,
      isTarget: p.date === this.selectedDate,
    }));

    const path     = svgPoints.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ');
    const last     = svgPoints[svgPoints.length - 1];
    const first    = svgPoints[0];
    const areaPath = `${path} L${last.x.toFixed(1)},${bottomY} L${first.x.toFixed(1)},${bottomY} Z`;

    return { points: svgPoints, path, areaPath, minClose: minC, maxClose: maxC };
  }

  /** Formats "2024-06-15" → "15/06" */
  formatDateShort(dateStr: string): string {
    if (!dateStr) return '';
    const parts = dateStr.split('-');
    return parts.length === 3 ? `${parts[2]}/${parts[1]}` : dateStr;
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
    const signalLabel = row.signal === 'BUY' ? 'COMPRAR' : row.signal === 'SELL' ? 'CASH (Liquidez)' : 'MANTENER';
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
    else if (row.evidence.rsi === 'overbought') factors.push('RSI en sobrecompra — señal de precaución ante posible corrección');
    else factors.push('RSI en zona neutral sin presión técnica dominante');

    if (row.evidence.volatility === 'low') factors.push('volatilidad comprimida — condiciones de entrada favorables');
    else factors.push('volatilidad elevada — mayor incertidumbre en la ejecución');

    let text = `${row.ticker} genera señal ${signalLabel} con ${bullishCount} de 4 condiciones favorables alineadas: `;
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
    const isBuy  = row.signal === 'BUY';
    const isHold = row.signal === 'HOLD';
    const isSell = row.signal === 'SELL';

    return [
      {
        type: 'conservative',
        label: 'Perfil Conservador',
        icon: 'shield',
        color: '#3B82F6',
        action: isBuy && score >= 70 && row.win_rate >= 0.55
          ? 'Entrada permitida — señal sólida'
          : isHold ? 'Mantener posición si ya invertido'
          : 'No actuar — esperar señal más clara',
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
          : `Señal ${row.signal} con confianza ${score}/100. El perfil moderado requiere ≥55 de confianza compuesta para abrir posición.`,
        suitable: isBuy && score >= 55,
      },
      {
        type: 'aggressive',
        label: 'Perfil Agresivo',
        icon: 'rocket_launch',
        color: '#EF4444',
        action: isBuy
          ? 'Entrada directa siguiendo señal del modelo'
          : isHold ? 'Mantener con trailing stop'
          : 'Salir del mercado y buscar alternativas',
        rationale: isBuy
          ? `Probabilidad alcista del ${Math.round(row.prob_up*100)}%. El perfil agresivo ejecuta cualquier señal BUY del modelo sin filtros adicionales de confianza.`
          : isSell
          ? `El modelo recomienda CASH. El perfil agresivo prioriza preservar capital y esperar mejor punto de reentrada.`
          : `Señal MANTENER — el perfil agresivo conserva posición existente pero activa trailing stop del 3%.`,
        suitable: isBuy,
      },
    ];
  }
}
