import { Component, OnInit, OnDestroy, AfterViewInit, ViewChild, inject, ElementRef, NgZone } from '@angular/core';
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
import { switchMap, catchError, of, Subject, takeUntil } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import { ActivatedRoute } from '@angular/router';
import { ReportService } from '../../core/services/report.service';
import { PipelineContextService } from '../../core/services/pipeline-context.service';
import { TraceService } from '../../core/services/trace.service';
import {
  ApiService, NewsDetailResponse, MacroContext, MacroArticle,
  FeatureSnapshot, ExposurePositionPoint,
} from '../../core/services/api.service';
import {
  TickerView, ReportDateEntry, DailyReport, SignalExplanation,
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
    NgxChartsModule
  ],
  templateUrl: './signals.component.html',
  styleUrl: './signals.component.scss',
})
export class SignalsComponent implements OnInit, OnDestroy, AfterViewInit {
  private reportSvc = inject(ReportService);
  private route      = inject(ActivatedRoute);
  private ngZone     = inject(NgZone);
  private traceSvc  = inject(TraceService);
  private apiSvc    = inject(ApiService);
  private pipelineCtx = inject(PipelineContextService);
  private destroy$ = new Subject<void>();

  @ViewChild(MatSort) sort!: MatSort;

  loading = true;
  availableDates: ReportDateEntry[] = [];
  selectedDate = '';
  expandedRows = new Set<string>();
  currentReport: DailyReport | null = null;
  positionCache = new Map<string, ExposurePositionPoint | null>();
  positionLoading = new Set<string>();

  tickerViews: TickerView[] = [];

  // Se eliminan winrate y trades. Se mantiene el foco en evidencia y rentabilidad acumulada.
  displayedColumns = ['ticker', 'exposure', 'prob_up', 'evidence', 'return', 'alpha', 'expand'];
  dataSource = new MatTableDataSource<TickerView>();

  // Gráficos de Resumen
  signalChart: ChartDataPoint[] = [];
  sentimentChart: ChartDataPoint[] = [];
  rsiChart: ChartDataPoint[] = [];
  trendChart: ChartDataPoint[] = [];
  volatilityChart: ChartDataPoint[] = [];

  avgProbUp = 0;

  tickerTraceCache = new Map<string, TickerTrace | null>();
  tickerTraceLoading = new Set<string>();
  featureCache = new Map<string, FeatureSnapshot | null>();
  featureLoading = new Set<string>();

  // Funciones de Coloreado Dinámico
  customSignalColors = (name: string) => {
    if (name.includes('Aumentar Fuerte')) return '#15803d';
    if (name.includes('Aumentar'))  return '#22c55e';
    if (name.includes('Mantener'))  return '#3b82f6';
    if (name.includes('Reducir Fuerte')) return '#b91c1c';
    return '#f59e0b';
  };
  customSentimentColors = (name: string) => {
    if (name === 'ALCISTA') return '#22C55E';
    if (name === 'BAJISTA') return '#EF4444';
    return '#94A3B8';
  };
  customRsiColors = (name: string) => {
    if (name === 'SOBREVENTA') return '#22C55E';
    if (name === 'SOBRECOMPRA') return '#EF4444';
    return '#94A3B8';
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
    const dateFromUrl   = this.route.snapshot.queryParamMap.get('date');
    const tickerFromUrl = this.route.snapshot.queryParamMap.get('ticker');
    
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        this.availableDates = dates;
        if (!dates.length) { this.loading = false; return []; }
        const defaultDate = this.pipelineCtx.pipelineEndDate() ?? dates[0].date;
        this.selectedDate = (dateFromUrl && dates.find(d => d.date === dateFromUrl))
          ? dateFromUrl
          : defaultDate;
        return this.reportSvc.loadReport(this.selectedDate);
      })
    ).subscribe({
      next: (r: any) => {
        if (r) this.processReport(r);
        this.loading = false;
        this.loadMacroContext(this.selectedDate);
        if (tickerFromUrl) {
          this.ngZone.runOutsideAngular(() => {
            setTimeout(() => this.scrollAndExpandTicker(tickerFromUrl), 400);
          });
        }
      },
      error: () => { this.loading = false; },
    });
  }

  scrollAndExpandTicker(ticker: string): void {
    this.ngZone.run(() => {
      if (!this.expandedRows.has(ticker)) {
        this.toggleRow(ticker);
      }
    });

    let attempts = 0;
    const MAX_ATTEMPTS = 12;
    const tryScroll = () => {
      attempts++;
      let row = document.querySelector(`tr[data-ticker="${ticker}"]`) as HTMLElement | null;

      if (!row) {
        const allRows = document.querySelectorAll('tr.data-row');
        for (let i = 0; i < allRows.length; i++) {
          const cell = allRows[i].querySelector('.ticker-name');
          if (cell && cell.textContent?.trim() === ticker) {
            row = allRows[i] as HTMLElement;
            break;
          }
        }
      }

      if (row) {
        row.scrollIntoView({ behavior: 'smooth', block: 'center' });
        row.classList.add('row-highlight-flash');
        setTimeout(() => row!.classList.remove('row-highlight-flash'), 2200);
      } else if (attempts < MAX_ATTEMPTS) {
        setTimeout(tryScroll, 150);
      }
    };
    setTimeout(tryScroll, 300);
  }

  ngAfterViewInit() {
    this.dataSource.sort = this.sort;
  }

  onDateChange(date: string) {
    this.loading = true;
    this.tickerTraceCache.clear();
    this.expandedRows.clear();
    this.positionCache.clear();
    this.currentReport = null;
    
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
    this.currentReport = report;
    const views = this.reportSvc.buildTickerViews(report);
    
    this.tickerViews = views; 
    this.dataSource.data = views;
    
    if (this.sort) {
      this.dataSource.sort = this.sort;
    }

    let increaseStrongCount = 0;
    let increaseMildCount   = 0;
    let maintainCount       = 0;
    let reduceMildCount     = 0;
    let reduceStrongCount   = 0;

    const sent = { ALCISTA: 0, BAJISTA: 0, NEUTRAL: 0 };
    const rsi = { SOBREVENTA: 0, SOBRECOMPRA: 0, NEUTRAL: 0 };
    const trend = { ALCISTA: 0, BAJISTA: 0 };
    const vol = { ALTA: 0, BAJA: 0 };

    views.forEach(v => {
      // Recommendations Count
      if (v.exposure_recommendation === 'INCREASE_STRONG') increaseStrongCount++;
      else if (v.exposure_recommendation === 'INCREASE_MILD') increaseMildCount++;
      else if (v.exposure_recommendation === 'MAINTAIN') maintainCount++;
      else if (v.exposure_recommendation === 'REDUCE_MILD') reduceMildCount++;
      else if (v.exposure_recommendation === 'REDUCE_STRONG') reduceStrongCount++;

      // Evidence Count
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

    this.avgProbUp = views.length ? (views.reduce((s, v) => s + v.prob_up, 0) / views.length) * 100 : 0;

    this.signalChart = [
      { name: 'Aumentar Fuerte', value: increaseStrongCount },
      { name: 'Aumentar',        value: increaseMildCount   },
      { name: 'Mantener',        value: maintainCount       },
      { name: 'Reducir',         value: reduceMildCount     },
      { name: 'Reducir Fuerte',  value: reduceStrongCount   },
    ].filter(i => i.value > 0);

    this.sentimentChart = Object.entries(sent).map(([name, value]) => ({ name, value })).filter(i => i.value > 0);
    this.rsiChart = Object.entries(rsi).map(([name, value]) => ({ name, value })).filter(i => i.value > 0);
    this.trendChart = Object.entries(trend).map(([name, value]) => ({ name, value })).filter(i => i.value > 0);
    this.volatilityChart = Object.entries(vol).map(([name, value]) => ({ name, value })).filter(i => i.value > 0);
  }

  toggleRow(ticker: string) {
    if (this.expandedRows.has(ticker)) {
      this.expandedRows.delete(ticker);
    } else {
      this.expandedRows.add(ticker);
      this.loadTickerTrace(ticker);
      this.loadFeatureSnapshot(ticker);
      this.loadExposurePosition(ticker);
    }
  }

  get topDayExplanations(): SignalExplanation[] {
    return this.currentReport ? this.reportSvc.topSignalExplanations(this.currentReport) : [];
  }

  getDayExplanation(ticker: string): SignalExplanation | undefined {
    return this.currentReport ? this.reportSvc.explanationForTicker(this.currentReport, ticker) : undefined;
  }

  focusTickerExplanation(ticker: string) {
    if (!this.expandedRows.has(ticker)) {
      this.toggleRow(ticker);
    }
  }

  loadExposurePosition(ticker: string) {
    if (this.positionCache.has(ticker) || this.positionLoading.has(ticker)) return;
    this.positionLoading.add(ticker);
    this.apiSvc.getExposurePositions(ticker, 120).pipe(
      catchError((err: HttpErrorResponse) => {
        if (err?.status !== 404) console.warn('[ExposurePositions]', err.message);
        return of(null);
      })
    ).subscribe(resp => {
      this.positionLoading.delete(ticker);
      const pt = resp?.timeline?.find(p => p.date === this.selectedDate) ?? null;
      this.positionCache.set(ticker, pt);
    });
  }

  getExposurePosition(ticker: string): ExposurePositionPoint | null {
    return this.positionCache.get(ticker) ?? null;
  }

  isPositionLoading(ticker: string): boolean {
    return this.positionLoading.has(ticker);
  }

  smoothedExposurePct(ticker: string): number | null {
    const expl = this.getDayExplanation(ticker);
    if (expl?.smoothed_exposure != null) {
      return Math.round(expl.smoothed_exposure * 1000) / 10;
    }
    const pos = this.getExposurePosition(ticker);
    if (pos?.smoothed_exposure != null) {
      return Math.round(pos.smoothed_exposure * 1000) / 10;
    }
    const row = this.tickerViews.find(v => v.ticker === ticker);
    return row ? row.exposure_pct : null;
  }

  marketRegimeForTicker(ticker: string): string | null {
    const expl = this.getDayExplanation(ticker);
    if (expl?.market_regime) return expl.market_regime;
    const pos = this.getExposurePosition(ticker);
    if (pos?.confirmed_regime) return pos.confirmed_regime;
    if (pos?.raw_regime) return pos.raw_regime;
    return null;
  }

  macroRiskRegime(): string | null {
    return this.macroContext?.risk_regime
      ?? this.getFeatureSnapshot(this.tickerViews[0]?.ticker ?? '')?.macro?.risk_regime
      ?? null;
  }

  macroSentimentLabel(): string | null {
    return this.macroContext?.macro_sentiment
      ?? this.getFeatureSnapshot(this.tickerViews[0]?.ticker ?? '')?.macro?.macro_sentiment
      ?? null;
  }

  marketRegimeLabel(regime: string | null | undefined): string {
    if (!regime) return 'N/D';
    const m: Record<string, string> = {
      BULL: 'Alcista (BULL)', NEUTRAL: 'Neutral', HIGH_VOL: 'Alta volatilidad', BEAR: 'Bajista (BEAR)',
      RISK_ON: 'Risk-On', RISK_OFF: 'Risk-Off',
    };
    return m[regime] ?? regime;
  }

  expRecColor(rec: string | null | undefined): string {
    if (!rec) return '#94a3b8';
    const m: Record<string, string> = {
      INCREASE_STRONG: '#15803d', INCREASE_MILD: '#22c55e', MAINTAIN: '#94a3b8',
      REDUCE_MILD: '#a78bfa', REDUCE_STRONG: '#7c3aed',
    };
    return m[rec] ?? '#94a3b8';
  }

  loadFeatureSnapshot(ticker: string) {
    if (this.featureCache.has(ticker) || this.featureLoading.has(ticker)) return;
    this.featureLoading.add(ticker);
    this.apiSvc.getFeatures(this.selectedDate, ticker).pipe(
      catchError((err: HttpErrorResponse) => {
        if (err?.status !== 404) console.warn('[FeatureSnapshot]', err.message);
        return of(null);
      })
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

  /** Color de gradiente para Confianza Alcista: rojo (baja) → verde (alta) */
  probGradientColor(prob: number): string {
    const t = Math.max(0, Math.min(1, prob));
    const r = Math.round(220 - t * 180);
    const g = Math.round(50 + t * 170);
    const b = Math.round(60 - t * 20);
    return `rgb(${r}, ${g}, ${b})`;
  }

  expRecBadgeLabel(rec: string | null | undefined): string {
    if (!rec) return 'N/D';
    const m: Record<string, string> = {
      INCREASE_STRONG: 'Aumentar fuerte',
      INCREASE_MILD:   'Aumentar',
      MAINTAIN:        'Mantener',
      REDUCE_MILD:     'Reducir',
      REDUCE_STRONG:   'Reducir fuerte',
    };
    return m[rec] ?? rec.replace(/_/g, ' ');
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
    return 'mid'; 
  }

  getTextClass(prob: number): string {
    if (prob >= 0.65) return 'green';
    if (prob <= 0.35) return 'purple';
    return 'yellow'; 
  }

  signalIcon(rec: string) {
    return this.expRecIcon(rec);
  }

  expRecLabel(rec: string | null | undefined): string {
    if (!rec) return 'N/D';
    const m: Record<string, string> = {
      INCREASE_STRONG: 'Aumentar Fuerte',
      INCREASE_MILD:   'Aumentar',
      MAINTAIN:        'Mantener',
      REDUCE_MILD:     'Reducir',
      REDUCE_STRONG:   'Reducir Fuerte'
    };
    return m[rec] ?? rec;
  }

  expRecIcon(rec: string | null | undefined): string {
    if (!rec) return 'drag_handle';
    const m: Record<string, string> = {
      INCREASE_STRONG: 'keyboard_double_arrow_up',
      INCREASE_MILD:   'keyboard_arrow_up',
      MAINTAIN:        'drag_handle',
      REDUCE_MILD:     'keyboard_arrow_down',
      REDUCE_STRONG:   'keyboard_double_arrow_down',
    };
    return m[rec] ?? 'drag_handle';
  }

  expBarClass(pct: number): string {
    if (pct >= 72) return 'exp-high';
    if (pct >= 58) return 'exp-mid';
    return 'exp-low';
  }

  expRecClass(rec: string | null | undefined): string {
    return (rec ?? 'unknown').toLowerCase().replace(/_/g, '-');
  }

  sentimentIcon(s: SentimentState) {
    return ({ bullish: 'sentiment_very_satisfied', bearish: 'sentiment_very_dissatisfied', neutral: 'sentiment_neutral' })[s];
  }

  sentimentClass(v: SentimentState)  { return `ev-${v}`; }
  rsiClass(v: RsiState)              { return v === 'oversold' ? 'ev-oversold' : v === 'overbought' ? 'ev-overbought' : 'ev-neutral-rsi'; }
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
        this.macroError   = 'Sin datos macro para esta fecha.';
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
    // Eliminamos winrate de la ecuación del score compuesto
    const alphaScore = Math.min(Math.max(((row.alpha_vs_benchmark ?? 0) + 0.5) * 100, 0), 100);
    return Math.round(0.60 * probScore + 0.40 * alphaScore);
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

    const alpha = row.alpha_vs_benchmark ?? 0;
    const alphaStr = alpha >= 0 ? `+${(alpha * 100).toFixed(1)}%` : `${(alpha * 100).toFixed(1)}%`;
    text += ` El modelo ha logrado un Alpha histórico de ${alphaStr} frente al comportamiento pasivo del mercado para este activo.`;
    
    return text;
  }

  getRiskProfiles(row: TickerView): { type: string; label: string; icon: string; color: string; action: string; rationale: string; suitable: boolean }[] {
    const score = this.getCompositeScore(row);
    const isBuy  = ['INCREASE_STRONG', 'INCREASE_MILD'].includes(row.exposure_recommendation);
    const isHold = row.exposure_recommendation === 'MAINTAIN';
    const isSell = ['REDUCE_MILD', 'REDUCE_STRONG'].includes(row.exposure_recommendation);

    return [
      {
        type: 'conservative',
        label: 'Perfil Conservador',
        icon: 'shield',
        color: '#3B82F6',
        action: isBuy && score >= 70
          ? 'Entrada permitida — recomendación sólida'
          : isHold ? 'Mantener posición si ya invertido'
          : 'No actuar — esperar recomendación más clara',
        rationale: isBuy && score >= 70
          ? `Confianza compuesta ${score}/100 supera el umbral mínimo para perfil conservador (≥70).`
          : `La confianza compuesta (${score}/100) no alcanza el umbral requerido para asumir riesgo.`,
        suitable: isBuy && score >= 70,
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
          ? `Confianza compuesta ${score}/100 con probabilidad alcista del ${Math.round(row.prob_up*100)}%.`
          : `Recomendación ${this.expRecLabel(row.exposure_recommendation)} con confianza ${score}/100. El perfil moderado requiere ≥55 de confianza compuesta.`,
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
          ? `Probabilidad alcista del ${Math.round(row.prob_up*100)}%. El perfil agresivo ejecuta cualquier recomendación de incremento de exposición sin filtros adicionales.`
          : isSell
          ? `El modelo recomienda reducir exposición. Priorizar capital y esperar reentrada.`
          : `Recomendación MANTENER — el perfil agresivo conserva posición activa con stop del 3%.`,
        suitable: isBuy,
      },
    ];
  }
}