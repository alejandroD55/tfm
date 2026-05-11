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
import { BayesianTrace, ModelConfig } from '../../core/models/trace.model';
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
  selectedDate  = ''; // Lo dejamos solo para descargar internamente el último archivo, no se muestra

  cptRows:            any[] = [];
  cptFilterSentiment  = '';
  cptFilterRsi        = '';
  cptFilterTrend      = '';

  limitations: string[] = [];
  priorNodes: { name: string; states: { key: string; value: number }[] }[] = [];

  get filteredCpt() {
    return this.cptRows.filter(r =>
      (!this.cptFilterSentiment || r.sentiment === this.cptFilterSentiment) &&
      (!this.cptFilterRsi       || r.rsi       === this.cptFilterRsi) &&
      (!this.cptFilterTrend     || r.trend     === this.cptFilterTrend)
    );
  }

  ngOnInit() {
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        this.availableDates = dates;
        if (!dates.length) { this.loading = false; return []; }
        // Forzamos que siempre coja la última versión disponible
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
    this.traceSvc.getTrace(date).subscribe({
      next: t => { this.processTrace(t); this.loading = false; },
      error: () => { this.loading = false; },
    });
  }

  private processTrace(t: BayesianTrace) {
    this.trace = t;
    this.model = t.model_config;
    this.limitations = t.model_config.known_limitations || [];

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
