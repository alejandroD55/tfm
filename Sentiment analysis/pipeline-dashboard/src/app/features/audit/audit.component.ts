import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatSelectModule } from '@angular/material/select';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatChipsModule } from '@angular/material/chips';
import { switchMap } from 'rxjs';
import { TraceService } from '../../core/services/trace.service';
import { ReportService } from '../../core/services/report.service';
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
export class AuditComponent implements OnInit {
  private traceSvc  = inject(TraceService);
  private reportSvc = inject(ReportService);

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
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        this.availableDates = dates;
        if (!dates.length) { this.loading = false; return []; }
        this.selectedDate = dates[0].date;
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

  // Clase CSS para el estado de una señal
  signalClass(signal: string): string {
    return signal === 'BUY' ? 'signal-buy' : signal === 'SELL' ? 'signal-sell' : 'signal-hold';
  }

  signalLabel(signal: string): string {
    return signal === 'BUY' ? 'COMPRAR' : signal === 'SELL' ? 'CASH' : 'MANTENER';
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
