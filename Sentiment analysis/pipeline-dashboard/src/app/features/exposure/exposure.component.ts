import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatExpansionModule } from '@angular/material/expansion';
import { HighchartsChartModule } from 'highcharts-angular';
import Highcharts from 'highcharts/highstock';
import { forkJoin, of, catchError, Subject, takeUntil } from 'rxjs';

import {
  ApiService,
  ExposureTimelinePoint,
  ExposurePositionPoint,
  MarketRegime,
} from '../../core/services/api.service';
import { ReportService } from '../../core/services/report.service';
import { PipelineContextService } from '../../core/services/pipeline-context.service';
import { ReportDateEntry } from '../../core/models/report.model';

// ─── Constantes de régimen ────────────────────────────────────────────────────

const REGIME_COLORS: Record<string, string> = {
  BULL:     '#22c55e',
  NEUTRAL:  '#38bdf8',
  HIGH_VOL: '#f59e0b',
  BEAR:     '#ef4444',
};

const REGIME_LABELS: Record<string, string> = {
  BULL:     'Alcista (BULL)',
  NEUTRAL:  'Neutral',
  HIGH_VOL: 'Alta Volatilidad',
  BEAR:     'Bajista (BEAR)',
};

/** Floors y ceilings originales del modelo — se muestran en la vista */
const REGIME_FLOORS:   Record<string, number> = { BULL:0.60, NEUTRAL:0.35, HIGH_VOL:0.20, BEAR:0.10 };
const REGIME_CEILINGS: Record<string, number> = { BULL:1.00, NEUTRAL:0.80, HIGH_VOL:0.60, BEAR:0.45 };

/** Colores semáforo para el VIX regime label */
const VIX_REGIME_COLORS: Record<string, string> = {
  RISK_ON_STRONG: '#22c55e',
  RISK_ON:        '#4ade80',
  NEUTRAL:        '#38bdf8',
  RISK_OFF_MILD:  '#fbbf24',
  RISK_OFF:       '#f97316',
  FEAR:           '#ef4444',
  PANIC:          '#b91c1c',
};

interface RegimeStat {
  regime:  string;
  label:   string;
  color:   string;
  days:    number;
  pct:     number;
  floor:   number;
  ceiling: number;
}

@Component({
  selector: 'app-exposure',
  standalone: true,
  imports: [
    CommonModule, FormsModule,
    MatIconModule, MatButtonModule, MatProgressSpinnerModule,
    MatTooltipModule, MatExpansionModule,
    HighchartsChartModule,
  ],
  templateUrl: './exposure.component.html',
  styleUrl: './exposure.component.scss',
})
export class ExposureComponent implements OnInit, OnDestroy {
  private api       = inject(ApiService);
  private reportSvc = inject(ReportService);
  private pipelineCtx = inject(PipelineContextService);
  private destroy$ = new Subject<void>();

  Highcharts: typeof Highcharts = Highcharts;

  loading         = true;
  error           = '';
  availableDates: ReportDateEntry[] = [];
  selectedDate    = '';
  selectedTicker  = 'SPY';
  availableTickers: string[] = [];
  limitDays       = 90;

  // ── Fase 1: timeline de retornos/exposición (MongoDB reports)
  timelineAll: ExposureTimelinePoint[] = [];
  summaryData: any = null;

  // ── Fase 2A: posiciones enriquecidas (bayesian_reports)
  positionsTimeline: ExposurePositionPoint[] = [];
  loadingPositions = false;

  // ── Highcharts
  exposureChartOpts:  Highcharts.Options = {};
  returnCompareOpts:  Highcharts.Options = {};
  vtChartOpts:        Highcharts.Options = {};
  exposureChartUpdate  = false;
  returnCompareUpdate  = false;
  vtChartUpdate        = false;

  // ── Régimen stats
  regimeStats: RegimeStat[] = [];

  // ─── getters de la última fila de posiciones ──────────────────────────────

  get latestPosition(): ExposurePositionPoint | null {
    return this.positionsTimeline.length
      ? this.positionsTimeline[this.positionsTimeline.length - 1]
      : null;
  }

  get filteredTimeline(): ExposureTimelinePoint[] {
    return this.timelineAll.filter(p => p.ticker === this.selectedTicker);
  }

  get currentLatestExposure(): number | null {
    const pts = this.filteredTimeline;
    return pts.length ? (pts[pts.length - 1].avg_exposure ?? null) : null;
  }

  get exposureAlpha(): number | null {
    const pts = this.filteredTimeline;
    return pts.length ? (pts[pts.length - 1].exposure_alpha ?? null) : null;
  }

  get dominantRegime(): string {
    if (!this.regimeStats.length) return '—';
    return this.regimeStats.reduce((a, b) => b.days > a.days ? b : a).label;
  }

  /** Dispersión del sentimiento del último día: 0=consenso, 1=desacuerdo total */
  get sentimentDispersion(): number | null {
    return this.latestPosition?.sentiment_dispersion ?? null;
  }

  get dispersionLabel(): string {
    const d = this.sentimentDispersion;
    if (d === null) return '—';
    if (d < 0.30) return 'Consenso';
    if (d < 0.60) return 'Divergencia moderada';
    return 'Alta contradicción';
  }

  get dispersionColor(): string {
    const d = this.sentimentDispersion;
    if (d === null) return '#64748b';
    if (d < 0.30) return '#22c55e';
    if (d < 0.60) return '#f59e0b';
    return '#ef4444';
  }

  get volRatioLabel(): string {
    const r = this.latestPosition?.vol_ratio;
    if (r === null || r === undefined) return '—';
    if (r > 1.4) return 'Acelerando ↑';
    if (r < 0.7) return 'Comprimiendo ↓';
    return 'Estable';
  }

  get volRatioColor(): string {
    const r = this.latestPosition?.vol_ratio;
    if (r === null || r === undefined) return '#64748b';
    if (r > 1.4) return '#ef4444';
    if (r < 0.7) return '#22c55e';
    return '#38bdf8';
  }

  get regimeDiffersFromRaw(): boolean {
    const p = this.latestPosition;
    return !!p && p.confirmed_regime !== p.raw_regime && !!p.raw_regime;
  }

  get vixRegimeColor(): string {
    const label = this.latestPosition?.vix_regime_label ?? '';
    return VIX_REGIME_COLORS[label] ?? '#64748b';
  }

  // ─── Lifecycle ────────────────────────────────────────────────────────────

  ngOnInit() {
    this.pipelineCtx.pipelineChanged$.pipe(takeUntil(this.destroy$)).subscribe(() => {
      this.reportSvc.clearCache();
      this.initDates();
    });
    this.initDates();
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private initDates() {
    this.reportSvc.listAvailableDates().subscribe({
      next: dates => {
        this.availableDates = dates;
        if (dates.length) {
          this.selectedDate = this.pipelineCtx.pipelineEndDate() ?? dates[0].date;
          this.loadData();
        } else {
          this.loading = false;
          this.error = 'No hay reportes en este pipeline.';
        }
      },
      error: () => { this.loading = false; this.error = 'Error cargando fechas.'; },
    });
  }

  onTickerChange()  { this.rebuildCharts(); this.loadPositions(); }
  onDateChange()    { this.loadSummary(); }
  onLimitChange()   { this.loadData(); }

  // ─── Carga de datos ───────────────────────────────────────────────────────

  private loadData() {
    this.loading = true;
    this.error   = '';

    const { start, end } = this.pipelineCtx.dateFilter();
    const rangeLimit = start && end
      ? Math.min(this.limitDays, 500)
      : this.limitDays;

    forkJoin({
      history: this.api.getExposureHistory(undefined, rangeLimit, start, end).pipe(
        catchError(() => of({ total: 0, tickers: [], ticker_filter: null, days_requested: this.limitDays, timeline: [] }))
      ),
      summary: this.selectedDate
        ? this.api.getExposureSummary(this.selectedDate).pipe(catchError(() => of(null)))
        : of(null),
    }).subscribe({
      next: ({ history, summary }) => {
        this.timelineAll      = history.timeline;
        this.availableTickers = history.tickers;
        this.summaryData      = summary;

        if (!this.availableTickers.includes(this.selectedTicker) && this.availableTickers.length) {
          this.selectedTicker = this.availableTickers[0];
        }
        this.rebuildCharts();
        this.loading = false;
        this.loadPositions();
      },
      error: () => { this.loading = false; this.error = 'Error cargando datos de exposición.'; },
    });
  }

  private loadSummary() {
    if (!this.selectedDate) return;
    this.api.getExposureSummary(this.selectedDate).pipe(catchError(() => of(null)))
      .subscribe(s => { this.summaryData = s; });
  }

  /** Carga los datos enriquecidos de Fase 2A para el ticker seleccionado */
  private loadPositions() {
    if (!this.selectedTicker) return;
    this.loadingPositions = true;
    this.api.getExposurePositions(this.selectedTicker, this.limitDays)
      .pipe(catchError(() => of({ ticker: this.selectedTicker, total: 0, days_requested: this.limitDays, timeline: [] })))
      .subscribe(res => {
        this.positionsTimeline = res.timeline;
        this.loadingPositions  = false;
        this.buildVtChart();
      });
  }

  // ─── Highcharts ───────────────────────────────────────────────────────────

  private rebuildCharts() {
    const pts = this.filteredTimeline;
    if (!pts.length) {
      this.exposureChartOpts = {};
      this.returnCompareOpts = {};
      this.exposureChartUpdate = true;
      this.returnCompareUpdate = true;
      this.regimeStats = [];
      return;
    }

    const toTs = (d: string) => new Date(`${d}T00:00:00Z`).getTime();

    // ── Gráfico 1: exposición EWM ─────────────────────────────────────────
    const expSeries = pts
      .filter(p => p.avg_exposure !== null)
      .map(p => [toTs(p.date), Math.round((p.avg_exposure ?? 0) * 1000) / 10]);

    this.exposureChartOpts = {
      chart: { height: 320, backgroundColor: 'transparent', zooming: { type: 'x' } },
      title: { text: undefined },
      credits: { enabled: false },
      rangeSelector: {
        selected: 2, inputEnabled: false,
        buttons: [
          { type: 'month', count: 1, text: '1M' },
          { type: 'month', count: 3, text: '3M' },
          { type: 'all', text: 'Todo' },
        ],
      },
      navigator: { enabled: true },
      legend: { enabled: true, itemStyle: { color: '#94a3b8' } },
      xAxis: { type: 'datetime' },
      yAxis: {
        title: { text: 'Exposición (%)' }, min: 0, max: 100,
        labels: { formatter: function() { return `${this.value}%`; } },
        plotLines: [
          { value: 60, color: 'rgba(34,197,94,0.35)',  width: 1, dashStyle: 'Dash', label: { text: 'Floor BULL 60%',     style: { color: '#22c55e', fontSize: '10px' } } },
          { value: 35, color: 'rgba(56,189,248,0.35)', width: 1, dashStyle: 'Dash', label: { text: 'Floor NEUTRAL 35%',  style: { color: '#38bdf8', fontSize: '10px' } } },
          { value: 20, color: 'rgba(245,158,11,0.35)', width: 1, dashStyle: 'Dash', label: { text: 'Floor HIGH_VOL 20%', style: { color: '#f59e0b', fontSize: '10px' } } },
          { value: 10, color: 'rgba(239,68,68,0.35)',  width: 1, dashStyle: 'Dash', label: { text: 'Floor BEAR 10%',     style: { color: '#ef4444', fontSize: '10px' } } },
        ],
      },
      tooltip: { valueSuffix: '%', valueDecimals: 1 },
      plotOptions: { series: { dataGrouping: { enabled: false }, marker: { enabled: false } } as any },
      series: [{
        type: 'area', name: `${this.selectedTicker} — Exposición EWM`,
        data: expSeries, color: '#34d399',
        fillColor: { linearGradient: { x1:0, y1:0, x2:0, y2:1 },
          stops: [[0,'rgba(52,211,153,0.25)'], [1,'rgba(52,211,153,0.01)']] },
        lineWidth: 2, threshold: null,
      }] as any,
    };
    this.exposureChartUpdate = true;

    // ── Gráfico 2: retorno acumulado binario vs exposición ────────────────
    const binSeries = pts.filter(p => p.binary_cumulative_return !== null)
      .map(p => [toTs(p.date), Math.round((p.binary_cumulative_return ?? 0) * 10000) / 100]);
    const expRetSeries = pts.filter(p => p.exposure_cumulative_return !== null)
      .map(p => [toTs(p.date), Math.round((p.exposure_cumulative_return ?? 0) * 10000) / 100]);

    this.returnCompareOpts = {
      chart: { height: 320, backgroundColor: 'transparent', zooming: { type: 'x' } },
      title: { text: undefined },
      credits: { enabled: false },
      rangeSelector: {
        selected: 2, inputEnabled: false,
        buttons: [
          { type: 'month', count: 1, text: '1M' },
          { type: 'month', count: 3, text: '3M' },
          { type: 'all', text: 'Todo' },
        ],
      },
      navigator: { enabled: true },
      legend: { enabled: true, itemStyle: { color: '#94a3b8' } },
      xAxis: { type: 'datetime' },
      yAxis: {
        title: { text: 'Retorno acumulado (%)' },
        labels: { formatter: function() { return `${this.value}%`; } },
        plotLines: [{ value: 0, color: '#94a3b8', width: 1 }],
      },
      tooltip: { valueSuffix: '%', valueDecimals: 2, shared: true },
      plotOptions: { series: { dataGrouping: { enabled: false }, marker: { enabled: false } } as any },
      series: [
        { type: 'line', name: 'Sistema Binario (Long/Cash)',   data: binSeries,    color: '#94a3b8', lineWidth: 2 },
        { type: 'line', name: 'Exposición Continua (Fase 2A)', data: expRetSeries, color: '#a78bfa', lineWidth: 2.5 },
      ] as any,
    };
    this.returnCompareUpdate = true;

    this.buildRegimeStats(pts);
  }

  /**
   * Gráfico 3 (Fase 2A): Volatility Targeting vs Exposición final.
   * Muestra cómo el VT actúa como techo dinámico de la exposición.
   * Cuando vol sube → VT baja → exposición final se contrae automáticamente.
   */
  private buildVtChart() {
    const pts = this.positionsTimeline;
    if (!pts.length) { this.vtChartUpdate = true; return; }

    const toTs = (d: string) => new Date(`${d}T00:00:00Z`).getTime();

    const vtSeries = pts.filter(p => p.vt_exposure !== null)
      .map(p => [toTs(p.date), Math.round((p.vt_exposure ?? 0) * 1000) / 10]);

    const smoothedSeries = pts.filter(p => p.smoothed_exposure !== null)
      .map(p => [toTs(p.date), Math.round((p.smoothed_exposure ?? 0) * 1000) / 10]);

    const kellySeries = pts.filter(p => p.kelly_exposure !== null)
      .map(p => [toTs(p.date), Math.round((p.kelly_exposure ?? 0) * 1000) / 10]);

    this.vtChartOpts = {
      chart: { height: 300, backgroundColor: 'transparent', zooming: { type: 'x' } },
      title: { text: undefined },
      credits: { enabled: false },
      rangeSelector: {
        selected: 1, inputEnabled: false,
        buttons: [
          { type: 'month', count: 1, text: '1M' },
          { type: 'month', count: 3, text: '3M' },
          { type: 'all', text: 'Todo' },
        ],
      },
      navigator: { enabled: false },
      legend: { enabled: true, itemStyle: { color: '#94a3b8' } },
      xAxis: { type: 'datetime' },
      yAxis: {
        title: { text: 'Exposición (%)' }, min: 0, max: 100,
        labels: { formatter: function() { return `${this.value}%`; } },
      },
      tooltip: { valueSuffix: '%', valueDecimals: 1, shared: true },
      plotOptions: { series: { dataGrouping: { enabled: false }, marker: { enabled: false } } as any },
      series: [
        {
          type: 'line', name: 'VT Techo (Volatility Target)',
          data: vtSeries, color: '#60a5fa', lineWidth: 1.5,
          dashStyle: 'ShortDash' as any,
        },
        {
          type: 'line', name: 'Fractional Kelly',
          data: kellySeries, color: '#f59e0b', lineWidth: 1.5,
          dashStyle: 'Dot' as any,
        },
        {
          type: 'area', name: 'Exposición Final (EWM asimétrico)',
          data: smoothedSeries, color: '#34d399', lineWidth: 2,
          fillColor: { linearGradient: { x1:0, y1:0, x2:0, y2:1 },
            stops: [[0,'rgba(52,211,153,0.20)'], [1,'rgba(52,211,153,0.01)']] },
          threshold: null,
        },
      ] as any,
    };
    this.vtChartUpdate = true;
  }

  // ─── Stats de régimen ─────────────────────────────────────────────────────

  private buildRegimeStats(pts: ExposureTimelinePoint[]) {
    const counts: Record<string, number> = { BULL: 0, NEUTRAL: 0, HIGH_VOL: 0, BEAR: 0 };
    let totalDays = 0;

    for (const p of pts) {
      const dist = p.regime_distribution;
      if (!dist) continue;
      for (const [r, d] of Object.entries(dist)) {
        counts[r] = (counts[r] || 0) + d;
        totalDays += d;
      }
    }

    this.regimeStats = (['BULL', 'NEUTRAL', 'HIGH_VOL', 'BEAR'] as const).map(r => ({
      regime:  r,
      label:   REGIME_LABELS[r],
      color:   REGIME_COLORS[r],
      days:    counts[r] || 0,
      pct:     totalDays > 0 ? Math.round((counts[r] || 0) / totalDays * 100) : 0,
      floor:   REGIME_FLOORS[r],
      ceiling: REGIME_CEILINGS[r],
    })).filter(s => s.days > 0).sort((a, b) => b.days - a.days);
  }

  // ─── Helpers de formato ───────────────────────────────────────────────────

  fmtPct(v: number | null, decimals = 1): string {
    if (v === null || v === undefined) return '—';
    return `${(v * 100).toFixed(decimals)}%`;
  }

  fmtPctDirect(v: number | null, decimals = 1): string {
    if (v === null || v === undefined) return '—';
    return `${v.toFixed(decimals)}%`;
  }

  fmtPctRaw(v: number | null): string {
    if (v === null || v === undefined) return '—';
    const sign = v >= 0 ? '+' : '';
    return `${sign}${(v * 100).toFixed(2)}%`;
  }

  alphaColor(v: number | null): string {
    if (v === null) return '';
    return v >= 0 ? 'text-green' : 'text-red';
  }

  getDominantRegime(dist: Record<string, number>): string {
    if (!dist) return '—';
    return Object.entries(dist).sort(([,a],[,b]) => b - a)[0]?.[0] ?? '—';
  }

  getRegimeColor(regime: string | null | undefined): string {
    return REGIME_COLORS[regime ?? ''] ?? '#64748b';
  }

  getVixRegimeColor(label: string | null): string {
    return VIX_REGIME_COLORS[label ?? ''] ?? '#64748b';
  }

  /** Barra de dispersión de sentimiento: porcentaje [0-100] */
  dispersionBarWidth(): number {
    return Math.round((this.sentimentDispersion ?? 0) * 100);
  }

  /** Barra de vol_ratio normalizada para mostrar en %, centrada en 1× */
  volRatioBarWidth(): number {
    const r = this.latestPosition?.vol_ratio ?? 1;
    return Math.min(100, Math.round(r * 50));  // 1× = 50%, 2× = 100%
  }
}
