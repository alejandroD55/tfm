import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatExpansionModule } from '@angular/material/expansion';
import { HighchartsChartModule } from 'highcharts-angular';
import Highcharts from 'highcharts/highstock';
import { forkJoin, of, catchError } from 'rxjs';

import { ApiService, ExposureTimelinePoint, MarketRegime } from '../../core/services/api.service';
import { ReportService } from '../../core/services/report.service';
import { ReportDateEntry } from '../../core/models/report.model';

const REGIME_COLORS: Record<MarketRegime, string> = {
  BULL:     '#22c55e',
  NEUTRAL:  '#38bdf8',
  HIGH_VOL: '#f59e0b',
  BEAR:     '#ef4444',
};

const REGIME_LABELS: Record<MarketRegime, string> = {
  BULL:     'Alcista (BULL)',
  NEUTRAL:  'Neutral',
  HIGH_VOL: 'Alta Volatilidad',
  BEAR:     'Bajista (BEAR)',
};

/** Floors y ceilings del modelo — documentación inline en la vista */
const REGIME_FLOORS:    Record<MarketRegime, number> = { BULL:0.60, NEUTRAL:0.35, HIGH_VOL:0.20, BEAR:0.10 };
const REGIME_CEILINGS:  Record<MarketRegime, number> = { BULL:1.00, NEUTRAL:0.80, HIGH_VOL:0.60, BEAR:0.45 };

interface RegimeStat {
  regime:    MarketRegime;
  label:     string;
  color:     string;
  days:      number;
  pct:       number;
  floor:     number;
  ceiling:   number;
  avgReturn: number | null;
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
export class ExposureComponent implements OnInit {
  private api       = inject(ApiService);
  private reportSvc = inject(ReportService);

  Highcharts: typeof Highcharts = Highcharts;

  loading         = true;
  error           = '';
  availableDates: ReportDateEntry[] = [];
  selectedDate    = '';
  selectedTicker  = 'SPY';
  availableTickers: string[] = [];
  limitDays       = 90;

  // Timeline data
  timelineAll: ExposureTimelinePoint[] = [];

  // Highcharts
  exposureChartOpts:  Highcharts.Options = {};
  returnCompareOpts:  Highcharts.Options = {};
  exposureChartUpdate  = false;
  returnCompareUpdate  = false;

  // Régimen stats
  regimeStats: RegimeStat[] = [];

  // Summary para el ticker y fecha seleccionados
  summaryData: any = null;

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
    const top = this.regimeStats.reduce((a, b) => b.days > a.days ? b : a);
    return top.label;
  }

  ngOnInit() {
    this.reportSvc.listAvailableDates().subscribe({
      next: dates => {
        this.availableDates = dates;
        if (dates.length) {
          this.selectedDate = dates[0].date;
          this.loadData();
        } else {
          this.loading = false;
          this.error = 'No hay reportes disponibles todavía.';
        }
      },
      error: () => { this.loading = false; this.error = 'Error cargando fechas.'; },
    });
  }

  onTickerChange()  { this.rebuildCharts(); }
  onDateChange()    { this.loadSummary(); }
  onLimitChange()   { this.loadData(); }

  private loadData() {
    this.loading = true;
    this.error   = '';

    forkJoin({
      history: this.api.getExposureHistory(undefined, this.limitDays).pipe(
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
      },
      error: () => { this.loading = false; this.error = 'Error cargando datos de exposición.'; },
    });
  }

  private loadSummary() {
    if (!this.selectedDate) return;
    this.api.getExposureSummary(this.selectedDate).pipe(catchError(() => of(null)))
      .subscribe(s => { this.summaryData = s; });
  }

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

    // ── Gráfico 1: exposición continua con bandas de régimen ──────────────
    const expSeries = pts
      .filter(p => p.avg_exposure !== null)
      .map(p => [toTs(p.date), Math.round((p.avg_exposure ?? 0) * 1000) / 10]);

    this.exposureChartOpts = {
      chart: { height: 340, backgroundColor: 'transparent', zooming: { type: 'x' } },
      title: { text: undefined },
      credits: { enabled: false },
      rangeSelector: {
        selected: 2,
        inputEnabled: false,
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
        title: { text: 'Exposición (%)' },
        min: 0, max: 100,
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
        type: 'area',
        name: `${this.selectedTicker} — Exposición EWM`,
        data: expSeries,
        color: '#34d399',
        fillColor: { linearGradient: { x1:0, y1:0, x2:0, y2:1 },
          stops: [[0,'rgba(52,211,153,0.25)'], [1,'rgba(52,211,153,0.01)']] },
        lineWidth: 2,
        threshold: null,
      }] as any,
    };
    this.exposureChartUpdate = true;

    // ── Gráfico 2: retorno acumulado — binario vs exposición ─────────────
    const binSeries = pts
      .filter(p => p.binary_cumulative_return !== null)
      .map(p => [toTs(p.date), Math.round((p.binary_cumulative_return ?? 0) * 10000) / 100]);

    const expRetSeries = pts
      .filter(p => p.exposure_cumulative_return !== null)
      .map(p => [toTs(p.date), Math.round((p.exposure_cumulative_return ?? 0) * 10000) / 100]);

    this.returnCompareOpts = {
      chart: { height: 340, backgroundColor: 'transparent', zooming: { type: 'x' } },
      title: { text: undefined },
      credits: { enabled: false },
      rangeSelector: {
        selected: 2,
        inputEnabled: false,
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
        {
          type: 'line', name: 'Sistema Binario (Long/Cash)',
          data: binSeries, color: '#94a3b8', lineWidth: 2,
        },
        {
          type: 'line', name: 'Exposición Continua (Fase 1)',
          data: expRetSeries, color: '#a78bfa', lineWidth: 2.5,
        },
      ] as any,
    };
    this.returnCompareUpdate = true;

    // ── Régimen stats ─────────────────────────────────────────────────────
    this.buildRegimeStats(pts);
  }

  private buildRegimeStats(pts: ExposureTimelinePoint[]) {
    const counts: Record<string, number> = { BULL: 0, NEUTRAL: 0, HIGH_VOL: 0, BEAR: 0 };
    const totalByRegime: Record<string, number> = { BULL: 0, NEUTRAL: 0, HIGH_VOL: 0, BEAR: 0 };
    let totalDays = 0;

    for (const p of pts) {
      const dist = p.regime_distribution;
      if (!dist) continue;
      for (const [r, d] of Object.entries(dist)) {
        counts[r]      = (counts[r] || 0) + d;
        totalDays += d;
      }
      // avg return por régimen (aproximado, simplificado)
      const regime = this.dominantRegimeOfDay(dist);
      if (regime && p.exposure_cumulative_return !== null) {
        totalByRegime[regime] = (totalByRegime[regime] || 0) + (p.exposure_cumulative_return ?? 0);
      }
    }

    const regimes: MarketRegime[] = ['BULL', 'NEUTRAL', 'HIGH_VOL', 'BEAR'];
    this.regimeStats = regimes.map(r => ({
      regime:    r,
      label:     REGIME_LABELS[r],
      color:     REGIME_COLORS[r],
      days:      counts[r] || 0,
      pct:       totalDays > 0 ? Math.round((counts[r] || 0) / totalDays * 100) : 0,
      floor:     REGIME_FLOORS[r],
      ceiling:   REGIME_CEILINGS[r],
      avgReturn: null,
    })).filter(s => s.days > 0)
       .sort((a, b) => b.days - a.days);
  }

  private dominantRegimeOfDay(dist: Record<string, number>): string | null {
    if (!dist) return null;
    return Object.entries(dist).sort(([,a],[,b]) => b - a)[0]?.[0] ?? null;
  }

  fmtPct(v: number | null, decimals = 1): string {
    if (v === null || v === undefined) return '—';
    return `${(v * 100).toFixed(decimals)}%`;
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

  getRegimeColor(regime: string): string {
    return REGIME_COLORS[regime as MarketRegime] ?? '#64748b';
  }
}
