import { Component, OnDestroy, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatSelectModule } from '@angular/material/select';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatExpansionModule } from '@angular/material/expansion';
import { NgxChartsModule } from '@swimlane/ngx-charts';
import { Subject, switchMap, takeUntil } from 'rxjs';
import { ReportService } from '../../core/services/report.service';
import { PipelineContextService } from '../../core/services/pipeline-context.service';
import { DailyReport, TickerView, ReportDateEntry, ExposureRecommendation, SentimentState, TrendState } from '../../core/models/report.model';
import { ChartDataPoint } from '../../core/models/pipeline.model';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [
    CommonModule, FormsModule, MatIconModule, MatButtonModule,
    MatProgressSpinnerModule, MatSelectModule, MatTooltipModule,
    MatExpansionModule, NgxChartsModule,
  ],
  templateUrl: './dashboard.component.html',
  styleUrl: './dashboard.component.scss',
})
export class DashboardComponent implements OnInit, OnDestroy {
  private reportSvc = inject(ReportService);
  private pipelineCtx = inject(PipelineContextService);
  private destroy$ = new Subject<void>();

  loading = true;
  report: DailyReport | null = null;
  tickerViews: TickerView[] = [];
  availableDates: ReportDateEntry[] = [];
  selectedDate = '';

  probUpChart: ChartDataPoint[] = [];
  winRateChart: ChartDataPoint[] = [];
  
  avgBenchmark: number = 0; 

  customSignalColors = (name: string) => {
    if (name === 'COMPRAR') return '#22C55E';
    if (name === 'CASH') return '#7C3AED';
    return '#F59E0B'; 
  };

  customProbColors = (name: string) => {
    const item = this.probUpChart.find(d => d.name === name);
    if (!item) return '#3B82F6';
    if (item.value >= 65) return '#22C55E'; 
    if (item.value <= 35) return '#7C3AED'; 
    return '#F59E0B'; 
  };

  customWinRateColors = (name: string) => {
    const item = this.winRateChart.find(d => d.name === name);
    if (!item) return '#3B82F6';
    return item.value >= 50 ? '#06B6D4' : '#EF4444'; 
  };


  get pipelineLabel(): string {
    return this.pipelineCtx.selectedPipeline()?.label ?? '';
  }

  get initialCapital(): number {
    return this.pipelineCtx.selectedPipeline()?.initialCapital ?? 10_000;
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

  private loadDates() {
    this.loading = true;
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        this.availableDates = dates;
        if (dates.length === 0) { this.loading = false; return []; }
        const endDate = this.pipelineCtx.pipelineEndDate() ?? dates[0].date;
        this.selectedDate = endDate;
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
      next: r => { this.processReport(r); this.loading = false; },
      error: () => { this.loading = false; },
    });
  }

  refresh() { this.loading = true; this.loadDates(); }

  private processReport(report: DailyReport) {
    this.report = report;
    this.tickerViews = this.reportSvc.buildTickerViews(report).sort((a, b) => b.prob_up - a.prob_up);
    
    const sumBH = this.tickerViews.reduce((acc, curr) => acc + curr.buy_hold_return, 0);
    this.avgBenchmark = this.tickerViews.length > 0 ? (sumBH / this.tickerViews.length) : 0;

    this.probUpChart = this.reportSvc.probUpChart(this.tickerViews);

    this.winRateChart = this.tickerViews.map(t => ({
      name: t.ticker,
      value: t.win_rate * 100
    })).sort((a, b) => b.value - a.value);
  }

  qualityLabel(s: number) {
    if (s >= 2)  return 'Excelente';
    if (s >= 1)  return 'Bueno';
    if (s >= 0)  return 'Aceptable';
    return 'Deficiente';
  }

  // ── Helpers de exposición ─────────────────────────────────────────────────

  expRecClass(rec: ExposureRecommendation): string {
    return (rec ?? '').toLowerCase().replace(/_/g, '-');
  }

  expRecLabel(rec: ExposureRecommendation): string {
    const map: Record<ExposureRecommendation, string> = {
      INCREASE_STRONG: '↑↑ Aumentar fuerte',
      INCREASE_MILD:   '↑  Aumentar',
      MAINTAIN:        '→  Mantener',
      REDUCE_MILD:     '↓  Reducir',
      REDUCE_STRONG:   '↓↓ Reducir fuerte',
    };
    return map[rec] ?? rec;
  }

  expRecIcon(rec: ExposureRecommendation): string {
    const map: Record<ExposureRecommendation, string> = {
      INCREASE_STRONG: 'arrow_upward',
      INCREASE_MILD:   'trending_up',
      MAINTAIN:        'remove',
      REDUCE_MILD:     'trending_down',
      REDUCE_STRONG:   'arrow_downward',
    };
    return map[rec] ?? 'remove';
  }

  expBarClass(pct: number): string {
    if (pct >= 72) return 'exp-high';
    if (pct >= 58) return 'exp-mid';
    return 'exp-low';
  }

  sentimentClass(s: SentimentState): string {
    if (s === 'bullish') return 'chip-bull';
    if (s === 'bearish') return 'chip-bear';
    return 'chip-neu';
  }

  sentimentIcon(s: SentimentState): string {
    if (s === 'bullish') return 'sentiment_very_satisfied';
    if (s === 'bearish') return 'sentiment_very_dissatisfied';
    return 'sentiment_neutral';
  }

  trendClass(t: TrendState): string {
    return t === 'uptrend' ? 'chip-bull' : 'chip-bear';
  }

  translateState(s: string): string {
    const m: Record<string, string> = {
      bullish: 'Alcista', bearish: 'Bajista', neutral: 'Neutral',
      oversold: 'Sobrevendido', overbought: 'Sobrecomprado',
      uptrend: 'Tendencia ↑', downtrend: 'Tendencia ↓',
      low: 'Baja vol.', high: 'Alta vol.',
    };
    return m[s] ?? s;
  }
}
