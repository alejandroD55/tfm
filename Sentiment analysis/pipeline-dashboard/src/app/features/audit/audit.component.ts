import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatSelectModule } from '@angular/material/select';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatChipsModule } from '@angular/material/chips';
import { Subject, switchMap, takeUntil } from 'rxjs';
import { TraceService } from '../../core/services/trace.service';
import { ReportService } from '../../core/services/report.service';
import { PipelineContextService } from '../../core/services/pipeline-context.service';
import { BayesianTrace, ModelConfig, TickerTrace } from '../../core/models/trace.model';
import { ReportDateEntry } from '../../core/models/report.model';

@Component({
  selector: 'app-audit',
  standalone: true,
  imports: [
    CommonModule, FormsModule, MatIconModule, MatButtonModule,
    MatProgressSpinnerModule, MatTooltipModule,
    MatExpansionModule, MatChipsModule
  ],
  templateUrl: './audit.component.html',
  styleUrl: './audit.component.scss',
})
export class AuditComponent implements OnInit, OnDestroy {
  private traceSvc  = inject(TraceService);
  private reportSvc = inject(ReportService);
  private pipelineCtx = inject(PipelineContextService);
  private destroy$ = new Subject<void>();

  loading       = true;
  trace: BayesianTrace | null = null;
  model: ModelConfig | null   = null;
  availableDates: ReportDateEntry[] = [];
  selectedDate  = '';

  // Walkthrough por ticker
  tickerKeys: string[] = [];
  expandedTicker = '';

  cptRows:            any[] = [];
  cptFilterSentiment  = '';
  cptFilterRsi        = '';
  cptFilterTrend      = '';

  limitations: string[] = [];
  priorNodes: { name: string; states: { key: string; value: number }[] }[] = [];

  get filteredCpt() {
    return this.cptRows
      .filter(r =>
        (!this.cptFilterSentiment || r.sentiment === this.cptFilterSentiment) &&
        (!this.cptFilterRsi       || r.rsi       === this.cptFilterRsi) &&
        (!this.cptFilterTrend     || r.trend     === this.cptFilterTrend)
      )
      .sort((a, b) => b.prob_up - a.prob_up);
  }

  ngOnInit() {
    this.pipelineCtx.pipelineChanged$.pipe(takeUntil(this.destroy$)).subscribe(() => {
      this.loadDates();
    });
    this.loadDates();
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private loadDates() {
    this.loading = true;
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        this.availableDates = dates;
        if (!dates.length) { this.loading = false; return []; }
        this.selectedDate = this.pipelineCtx.pipelineEndDate() ?? dates[0].date;
        return this.traceSvc.getTrace(this.selectedDate);
      })
    ).subscribe({
      next: (t: any) => { if (t) this.processTrace(t); this.loading = false; },
      error: () => { this.loading = false; },
    });
  }

  onDateChange(date: string) {
    this.loading = true;
    this.trace   = null;
    this.expandedTicker = '';
    this.traceSvc.getTrace(date).subscribe({
      next: t => { this.processTrace(t); this.loading = false; },
      error: () => { this.loading = false; },
    });
  }

  private processTrace(t: BayesianTrace) {
    this.trace = t;
    this.model = t.model_config;
    this.limitations = t.model_config.known_limitations || [];
    this.tickerKeys = Object.keys(t.tickers || {});
    if (this.tickerKeys.length > 0 && !this.expandedTicker) {
      this.expandedTicker = this.tickerKeys[0];
    }

    if (t.model_config.priors) {
      this.priorNodes = Object.entries(t.model_config.priors).map(([name, vals]) => ({
        name,
        states: Object.entries(vals)
          .filter(([k]) => k !== 'rationale')
          .map(([key, value]) => ({ key, value: value as number })),
      }));
    }

    if (t.model_config.cpt_market_direction.values_P_up) {
      this.cptRows = this.traceSvc.parseCptMatrix(t.model_config.cpt_market_direction.values_P_up);
    }
  }

  getTickerTrace(ticker: string): TickerTrace | null {
    return this.trace?.tickers?.[ticker] ?? null;
  }

  toggleTicker(ticker: string) {
    this.expandedTicker = this.expandedTicker === ticker ? '' : ticker;
  }

  // Porcentaje dominante del sentimiento FinBERT
  dominantSentimentPct(ticker: string): number {
    const t = this.getTickerTrace(ticker);
    if (!t?.sentiment_detail?.dominant) return 0;
    const sent = t.sentiment_detail.dominant.sentiment;
    return t.sentiment_detail.distribution?.[sent]?.pct ?? 0;
  }

  // Clase CSS para la recomendación de exposición
  signalClass(recommendation: string): string {
    if (recommendation?.startsWith('INCREASE')) return 'signal-buy';
    if (recommendation?.startsWith('REDUCE')) return 'signal-sell';
    return 'signal-hold';
  }

  signalLabel(recommendation: string): string {
    if (recommendation === 'INCREASE_STRONG') return '↑↑ Incrementar fuerte';
    if (recommendation === 'INCREASE_MILD') return '↑ Incrementar';
    if (recommendation === 'REDUCE_STRONG') return '↓↓ Reducir fuerte';
    if (recommendation === 'REDUCE_MILD') return '↓ Reducir';
    return '→ Mantener';
  }

  get buyThresholdPct(): number {
    const v = this.model?.signal_thresholds?.BUY?.prob_up_above;
    return typeof v === 'number' ? v * 100 : 52;
  }

  get buyThreshold(): number {
    const v = this.model?.signal_thresholds?.BUY?.prob_up_above;
    return typeof v === 'number' ? v : 0.52;
  }

  get sellThresholdPct(): number {
    const v = this.model?.signal_thresholds?.SELL?.prob_up_below;
    return typeof v === 'number' ? v * 100 : 28;
  }

  get sellThreshold(): number {
    const v = this.model?.signal_thresholds?.SELL?.prob_up_below;
    return typeof v === 'number' ? v : 0.28;
  }

  stateClass(state: string): string {
    const positive = ['bullish', 'oversold', 'uptrend', 'low'];
    const negative = ['bearish', 'overbought', 'downtrend', 'high'];
    if (positive.includes(state)) return 'state-positive';
    if (negative.includes(state)) return 'state-negative';
    return 'state-neutral';
  }

  // Traducción estricta para la UI
  translateState(val: string): string {
    const dict: Record<string, string> = {
      bullish: 'Alcista', bearish: 'Bajista', neutral: 'Neutral',
      oversold: 'Sobreventa', overbought: 'Sobrecompra',
      uptrend: 'Alcista', downtrend: 'Bajista',
      low: 'Baja', high: 'Alta'
    };
    return dict[val] || val;
  }

  translateNode(val: string): string {
    const dict: Record<string, string> = {
      Sentiment: 'Sentimiento FinBERT',
      RSI: 'Fuerza (RSI)',
      Trend: 'Tendencia General',
      Volatility: 'Volatilidad'
    };
    return dict[val] || val;
  }

  isPositiveState(state: string): boolean {
    return ['bullish', 'oversold', 'uptrend'].includes(state);
  }

  isNegativeState(state: string): boolean {
    return ['bearish', 'overbought', 'downtrend'].includes(state);
  }
}
