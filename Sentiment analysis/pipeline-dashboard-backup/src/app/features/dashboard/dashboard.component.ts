import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatSelectModule } from '@angular/material/select';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatChipsModule } from '@angular/material/chips';
import { NgxChartsModule } from '@swimlane/ngx-charts';
import { switchMap } from 'rxjs';
import { ReportService } from '../../core/services/report.service';
import { DailyReport, TickerView, ReportDateEntry } from '../../core/models/report.model';
import { ChartDataPoint, ChartSeries } from '../../core/models/pipeline.model';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [
    CommonModule, FormsModule, MatCardModule, MatIconModule, MatButtonModule,
    MatProgressSpinnerModule, MatSelectModule, MatTooltipModule,
    MatChipsModule, NgxChartsModule,
  ],
  template: `
    <div class="dashboard-page">

      <!-- Header -->
      <div class="page-header">
        <div>
          <h2 class="page-title"><mat-icon>dashboard</mat-icon> Resumen del Portfolio</h2>
          <p class="page-subtitle">Sistema de trading algorítmico ETF · Red bayesiana + FinBERT</p>
        </div>
        <div class="header-controls">
          <mat-form-field appearance="outline" class="date-selector">
            <mat-label>Fecha del report</mat-label>
            <mat-select [(ngModel)]="selectedDate" (ngModelChange)="onDateChange($event)">
              @for (d of availableDates; track d.date) {
                <mat-option [value]="d.date">{{ d.date }}</mat-option>
              }
            </mat-select>
          </mat-form-field>
          <button mat-stroked-button (click)="refresh()">
            <mat-icon>refresh</mat-icon>
          </button>
        </div>
      </div>

      @if (loading) {
        <div class="loading-center">
          <mat-spinner diameter="48"></mat-spinner>
          <p>Cargando report desde S3...</p>
        </div>
      } @else if (report) {

        <!-- Pipeline health banner -->
        <div class="health-banner" [class]="healthClass(report.pipeline_health.batch_status)">
          <mat-icon>{{ healthIcon(report.pipeline_health.batch_status) }}</mat-icon>
          <span>Pipeline <strong>{{ report.pipeline_health.batch_status }}</strong> ·
            {{ report.pipeline_health.tickers_with_signals }} / {{ report.pipeline_health.tickers_expected }} tickers ·
            {{ report.pipeline_health.headlines_scored }} titulares analizados ·
            Cobertura {{ (report.pipeline_health.coverage_ratio * 100) | number:'1.0-0' }}%
          </span>
          <span class="banner-period">Periodo: {{ report.data_period_days }} días</span>
        </div>

        <!-- KPI row -->
        <div class="kpi-grid">
          <div class="kpi-card">
            <div class="kpi-icon" style="background:#1a237e">
              <mat-icon>trending_up</mat-icon>
            </div>
            <div class="kpi-body">
              <div class="kpi-value" [class.positive]="report.summary.avg_cumulative_return > 0"
                   [class.negative]="report.summary.avg_cumulative_return < 0">
                {{ report.summary.avg_cumulative_return > 0 ? '+' : '' }}{{ (report.summary.avg_cumulative_return * 100) | number:'1.2-2' }}%
              </div>
              <div class="kpi-label">Retorno acumulado medio</div>
              <div class="kpi-sub">90 días · {{ report.summary.total_tickers }} ETFs</div>
            </div>
          </div>

          <div class="kpi-card">
            <div class="kpi-icon" style="background:#00695c">
              <mat-icon>speed</mat-icon>
            </div>
            <div class="kpi-body">
              <div class="kpi-value" [class.positive]="report.summary.avg_sharpe_ratio > 1">
                {{ report.summary.avg_sharpe_ratio | number:'1.2-2' }}
              </div>
              <div class="kpi-label">Sharpe Ratio medio</div>
              <div class="kpi-sub">Anualizado · rf=2%</div>
            </div>
          </div>

          <div class="kpi-card">
            <div class="kpi-icon" style="background:#b71c1c">
              <mat-icon>arrow_downward</mat-icon>
            </div>
            <div class="kpi-body">
              <div class="kpi-value negative">
                {{ (report.summary.avg_max_drawdown * 100) | number:'1.2-2' }}%
              </div>
              <div class="kpi-label">Max Drawdown medio</div>
              <div class="kpi-sub">Peor caída de capital</div>
            </div>
          </div>

          <div class="kpi-card">
            <div class="kpi-icon" style="background:#4527a0">
              <mat-icon>compare_arrows</mat-icon>
            </div>
            <div class="kpi-body">
              <div class="kpi-value">{{ report.summary.total_closed_trades }}</div>
              <div class="kpi-label">Trades cerrados</div>
              <div class="kpi-sub">Total portfolio</div>
            </div>
          </div>

          <div class="kpi-card">
            <div class="kpi-icon" style="background:#e65100">
              <mat-icon>psychology</mat-icon>
            </div>
            <div class="kpi-body">
              <div class="kpi-value">{{ buyCount }} <span class="signal-mini buy">BUY</span></div>
              <div class="kpi-label">Señales activas</div>
              <div class="kpi-sub">{{ sellCount }} SELL · {{ holdCount }} HOLD</div>
            </div>
          </div>
        </div>

        <!-- Charts row 1: señales pie + prob_up ranking -->
        <div class="charts-row">
          <div class="chart-box">
            <h3 class="section-title"><mat-icon>pie_chart</mat-icon> Distribución de señales hoy</h3>
            <ngx-charts-pie-chart
              [results]="signalPieChart"
              [legend]="true"
              [labels]="true"
              [doughnut]="true"
              [scheme]="signalScheme"
              [view]="[340, 260]"
            ></ngx-charts-pie-chart>
          </div>

          <div class="chart-box flex-1">
            <h3 class="section-title"><mat-icon>bar_chart</mat-icon> Probabilidad alcista por ETF (P_up)</h3>
            <ngx-charts-bar-vertical
              [results]="probUpChart"
              [xAxis]="true"
              [yAxis]="true"
              [showGridLines]="true"
              [scheme]="probScheme"
              [view]="[undefined, 260]"
              yAxisLabel="P(subida) %"
              [showYAxisLabel]="true"
              [rotateXAxisTicks]="false"
              [showDataLabel]="true"
            ></ngx-charts-bar-vertical>
          </div>
        </div>

        <!-- Charts row 2: retorno acumulado grouped bars -->
        <div class="chart-box">
          <h3 class="section-title">
            <mat-icon>show_chart</mat-icon> Estrategia vs Buy &amp; Hold — Retorno acumulado (%)
          </h3>
          <ngx-charts-bar-vertical-2d
            [results]="returnComparisonChart"
            [xAxis]="true"
            [yAxis]="true"
            [showGridLines]="true"
            [scheme]="compareScheme"
            [view]="[undefined, 280]"
            yAxisLabel="Retorno (%)"
            [showYAxisLabel]="true"
            [showDataLabel]="true"
            groupPadding="16"
          ></ngx-charts-bar-vertical-2d>
        </div>

        <!-- Top tickers cards -->
        <div class="tickers-section">
          <h3 class="section-title"><mat-icon>format_list_numbered</mat-icon> ETFs monitorizados hoy</h3>
          <div class="ticker-cards">
            @for (t of tickerViews; track t.ticker) {
              <div class="ticker-card">
                <div class="ticker-header">
                  <span class="ticker-symbol">{{ t.ticker }}</span>
                  <span class="signal-badge {{ t.signal.toLowerCase() }}">{{ t.signal }}</span>
                </div>
                <div class="ticker-prob">
                  <div class="prob-bar">
                    <div class="prob-up" [style.width.%]="t.prob_up * 100"
                         [matTooltip]="'P(up): ' + (t.prob_up*100|number:'1.1-1') + '%'"></div>
                    <div class="prob-down" [style.width.%]="t.prob_down * 100"
                         [matTooltip]="'P(down): ' + (t.prob_down*100|number:'1.1-1') + '%'"></div>
                  </div>
                  <div class="prob-labels">
                    <span class="up">↑{{ (t.prob_up*100)|number:'1.0-0' }}%</span>
                    <span class="down">↓{{ (t.prob_down*100)|number:'1.0-0' }}%</span>
                  </div>
                </div>
                <div class="ticker-metrics">
                  <div class="tm">
                    <span class="tl">Ret.</span>
                    <span class="tv" [class.positive]="t.cumulative_return>0" [class.negative]="t.cumulative_return<0">
                      {{ t.cumulative_return>0?'+':'' }}{{ (t.cumulative_return*100)|number:'1.1-1' }}%
                    </span>
                  </div>
                  <div class="tm">
                    <span class="tl">Sharpe</span>
                    <span class="tv">{{ t.sharpe_ratio|number:'1.2-2' }}</span>
                  </div>
                  <div class="tm">
                    <span class="tl">Alpha</span>
                    <span class="tv" [class.positive]="t.alpha_vs_benchmark>0" [class.negative]="t.alpha_vs_benchmark<0">
                      {{ t.alpha_vs_benchmark>0?'+':'' }}{{ (t.alpha_vs_benchmark*100)|number:'1.1-1' }}%
                    </span>
                  </div>
                </div>
              </div>
            }
          </div>
        </div>

      }
    </div>
  `,
  styles: [`
    .dashboard-page { max-width: 1400px; margin: 0 auto; }
    .page-header {
      display: flex; justify-content: space-between; align-items: flex-start;
      margin-bottom: 20px; flex-wrap: wrap; gap: 12px;
    }
    .page-title {
      display: flex; align-items: center; gap: 8px;
      font-size: 22px; font-weight: 700; color: #1a237e; margin: 0;
    }
    .page-subtitle { color: #666; font-size: 13px; margin-top: 4px; }
    .header-controls { display: flex; align-items: center; gap: 8px; }
    .date-selector { min-width: 160px; }
    .loading-center {
      display: flex; flex-direction: column; align-items: center;
      gap: 16px; padding: 80px; color: #666;
    }

    /* Health banner */
    .health-banner {
      display: flex; align-items: center; gap: 10px;
      padding: 12px 20px; border-radius: 10px;
      margin-bottom: 20px; font-size: 14px;
      mat-icon { font-size: 20px; }
    }
    .health-banner.completed { background: #e8f5e9; color: #1b5e20; }
    .health-banner.failed    { background: #ffebee; color: #b71c1c; }
    .health-banner.started   { background: #e3f2fd; color: #0d47a1; }
    .health-banner.unknown   { background: #f5f5f5; color: #424242; }
    .banner-period { margin-left: auto; font-size: 12px; opacity: .7; }

    /* KPI grid */
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 16px; margin-bottom: 20px;
    }
    .kpi-card {
      background: #fff; border-radius: 12px; padding: 18px;
      box-shadow: 0 2px 8px rgba(0,0,0,.07);
      display: flex; gap: 16px; align-items: flex-start;
    }
    .kpi-icon {
      width: 48px; height: 48px; border-radius: 12px;
      display: flex; align-items: center; justify-content: center; flex-shrink: 0;
      mat-icon { color: #fff; font-size: 24px; }
    }
    .kpi-body { flex: 1; min-width: 0; }
    .kpi-value {
      font-size: 26px; font-weight: 700; color: #1a237e; line-height: 1.1;
      &.positive { color: #2e7d32; }
      &.negative { color: #c62828; }
    }
    .kpi-label { font-size: 12px; color: #555; margin-top: 2px; font-weight: 500; }
    .kpi-sub   { font-size: 11px; color: #999; margin-top: 2px; }
    .signal-mini {
      font-size: 11px; padding: 1px 6px; border-radius: 4px; vertical-align: middle;
      &.buy  { background: #e8f5e9; color: #2e7d32; }
      &.sell { background: #ffebee; color: #c62828; }
      &.hold { background: #fff8e1; color: #f57f17; }
    }

    /* Charts */
    .charts-row {
      display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 16px;
    }
    .chart-box {
      background: #fff; border-radius: 12px; padding: 20px;
      box-shadow: 0 2px 8px rgba(0,0,0,.07);
      margin-bottom: 16px; min-width: 0;
    }
    .flex-1 { flex: 1; }
    .section-title {
      display: flex; align-items: center; gap: 8px;
      font-size: 15px; font-weight: 600; color: #1a237e;
      margin: 0 0 16px;
      mat-icon { font-size: 18px; height: 18px; width: 18px; }
    }

    /* Ticker cards */
    .tickers-section { margin-top: 4px; }
    .ticker-cards {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
      gap: 12px;
    }
    .ticker-card {
      background: #fff; border-radius: 12px; padding: 16px;
      box-shadow: 0 2px 8px rgba(0,0,0,.07);
      transition: box-shadow .2s;
      &:hover { box-shadow: 0 4px 16px rgba(0,0,0,.12); }
    }
    .ticker-header {
      display: flex; justify-content: space-between; align-items: center;
      margin-bottom: 10px;
    }
    .ticker-symbol { font-size: 18px; font-weight: 700; color: #1a237e; }
    .signal-badge {
      padding: 3px 10px; border-radius: 20px;
      font-size: 11px; font-weight: 700; letter-spacing: .5px;
      &.buy  { background: #e8f5e9; color: #2e7d32; }
      &.sell { background: #ffebee; color: #c62828; }
      &.hold { background: #fff8e1; color: #f57f17; }
    }
    .ticker-prob { margin-bottom: 10px; }
    .prob-bar {
      height: 8px; border-radius: 4px; background: #eee;
      display: flex; overflow: hidden; margin-bottom: 4px;
    }
    .prob-up   { background: #66bb6a; transition: width .5s; }
    .prob-down { background: #ef5350; transition: width .5s; }
    .prob-labels {
      display: flex; justify-content: space-between; font-size: 11px;
      .up   { color: #2e7d32; font-weight: 600; }
      .down { color: #c62828; font-weight: 600; }
    }
    .ticker-metrics {
      display: flex; gap: 4px; flex-wrap: wrap;
    }
    .tm {
      flex: 1; min-width: 60px; background: #f9f9fb; border-radius: 6px; padding: 6px;
      display: flex; flex-direction: column; align-items: center;
    }
    .tl { font-size: 10px; color: #999; }
    .tv {
      font-size: 13px; font-weight: 600; color: #333;
      &.positive { color: #2e7d32; }
      &.negative { color: #c62828; }
    }
  `],
})
export class DashboardComponent implements OnInit {
  private reportSvc = inject(ReportService);

  loading = true;
  report: DailyReport | null = null;
  tickerViews: TickerView[] = [];
  availableDates: ReportDateEntry[] = [];
  selectedDate = '';

  signalPieChart: ChartDataPoint[] = [];
  probUpChart: ChartDataPoint[] = [];
  returnComparisonChart: ChartSeries[] = [];

  signalScheme = { domain: ['#2e7d32', '#c62828', '#f57f17'] };
  probScheme   = { domain: ['#1a237e'] };
  compareScheme = { domain: ['#1a237e', '#ff7043'] };

  get buyCount()  { return this.tickerViews.filter(t => t.signal === 'BUY').length;  }
  get sellCount() { return this.tickerViews.filter(t => t.signal === 'SELL').length; }
  get holdCount() { return this.tickerViews.filter(t => t.signal === 'HOLD').length; }

  ngOnInit() {
    this.loadDates();
  }

  private loadDates() {
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        this.availableDates = dates;
        if (dates.length === 0) { this.loading = false; return []; }
        this.selectedDate = dates[0].date;
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
    this.tickerViews = this.reportSvc.buildTickerViews(report);
    this.signalPieChart     = this.reportSvc.signalDistributionChart(this.tickerViews);
    this.probUpChart        = this.reportSvc.probUpChart(this.tickerViews);
    this.returnComparisonChart = this.reportSvc.returnComparisonChart(this.tickerViews);
  }

  healthClass(status: string) { return status.toLowerCase(); }
  healthIcon(status: string) {
    return { COMPLETED: 'check_circle', FAILED: 'error', STARTED: 'pending', UNKNOWN: 'help' }[status] ?? 'help';
  }
}
