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
import { ChartSeries } from '../../core/models/pipeline.model';

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

  avgBenchmark: number = 0; 
  returnComparisonChart: ChartSeries[] = [];

  colorScheme: any = {
    domain: ['#2563eb', '#94a3b8']
  };

  get pipelineLabel(): string {
    return this.pipelineCtx.rangeLabel();
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
    // Ordenamos por los que tienen más exposición actualmente
    this.tickerViews = this.reportSvc.buildTickerViews(report).sort((a, b) => b.exposure_pct - a.exposure_pct);
    
    const sumBH = this.tickerViews.reduce((acc, curr) => acc + curr.buy_hold_return, 0);
    this.avgBenchmark = this.tickerViews.length > 0 ? (sumBH / this.tickerViews.length) : 0;

    // Generar datos agrupando por Ticker en el eje X
    this.returnComparisonChart = this.tickerViews.map(v => ({
      name: v.ticker,
      series: [
        { name: 'Estrategia IA', value: +(v.exp_cumulative_return * 100).toFixed(2) },
        { name: 'Buy & Hold', value: +(v.buy_hold_return * 100).toFixed(2) }
      ]
    }));
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
      INCREASE_STRONG: 'Aumentar Fuerte',
      INCREASE_MILD:   'Aumentar',
      MAINTAIN:        'Mantener',
      REDUCE_MILD:     'Reducir',
      REDUCE_STRONG:   'Reducir Fuerte',
    };
    return map[rec] ?? rec;
  }

  expRecIcon(rec: ExposureRecommendation): string {
    const map: Record<ExposureRecommendation, string> = {
      INCREASE_STRONG: 'keyboard_double_arrow_up',
      INCREASE_MILD:   'keyboard_arrow_up',
      MAINTAIN:        'drag_handle',
      REDUCE_MILD:     'keyboard_arrow_down',
      REDUCE_STRONG:   'keyboard_double_arrow_down',
    };
    return map[rec] ?? 'drag_handle';
  }

  sentimentClass(s: SentimentState): string {
    if (s === 'bullish') return 'color-bull';
    if (s === 'bearish') return 'color-bear';
    return 'color-neu';
  }

  trendClass(t: TrendState): string {
    return t === 'uptrend' ? 'color-bull' : 'color-bear';
  }

  translateState(s: string): string {
    const m: Record<string, string> = {
      bullish: 'Alcista', bearish: 'Bajista', neutral: 'Neutral',
      oversold: 'Sobrevendido', overbought: 'Sobrecomprado',
      uptrend: 'Alcista', downtrend: 'Bajista',
      low: 'Baja', high: 'Alta',
    };
    return m[s] ?? s;
  }
}