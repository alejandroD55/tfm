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
import { DailyReport, TickerView, ReportDateEntry } from '../../core/models/report.model';
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

  signalPieChart: ChartDataPoint[] = [];
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

  get buyCount()  { return this.tickerViews.filter(t => t.signal === 'BUY').length;  }
  get sellCount() { return this.tickerViews.filter(t => t.signal === 'SELL').length; }
  get holdCount() { return this.tickerViews.filter(t => t.signal === 'HOLD').length; }

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

    this.signalPieChart = [
      { name: 'COMPRAR', value: this.buyCount },
      { name: 'CASH', value: this.sellCount },
      { name: 'MANTENER', value: this.holdCount }
    ].filter(item => item.value > 0);

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
}
