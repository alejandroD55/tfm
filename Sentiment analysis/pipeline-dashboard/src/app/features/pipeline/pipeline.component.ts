import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatChipsModule } from '@angular/material/chips';
import { MatExpansionModule } from '@angular/material/expansion';
import { NgxChartsModule } from '@swimlane/ngx-charts';
import { forkJoin, switchMap, of } from 'rxjs';
import { ReportService } from '../../core/services/report.service';
import { DailyReport, PipelineHealth, ReportDateEntry, BatchStatus } from '../../core/models/report.model';
import { ChartDataPoint } from '../../core/models/pipeline.model';

interface BatchSummary {
  date: string;
  status: BatchStatus;
  tickers_expected: number;
  tickers_with_signals: number;
  headlines_scored: number;
  coverage_ratio: number;
  stage_kpis: Record<string, any>;
}

@Component({
  selector: 'app-pipeline',
  standalone: true,
  imports: [
    CommonModule, FormsModule, MatButtonModule, MatIconModule,
    MatProgressSpinnerModule, MatTooltipModule, MatChipsModule,
    MatExpansionModule, NgxChartsModule,
  ],
  template: `
    <div class="pipeline-page">

      <!-- Header -->
      <div class="page-header">
        <div>
          <h2 class="page-title"><mat-icon>account_tree</mat-icon> Estado del Pipeline</h2>
          <p class="page-subtitle">5 Lambdas · Step Functions · Ingesta → Sentimiento → Indicadores → Bayesiana → Reporte</p>
        </div>
        <button mat-stroked-button (click)="refresh()"><mat-icon>refresh</mat-icon> Refrescar</button>
      </div>

      @if (loading) {
        <div class="loading-center">
          <mat-spinner diameter="48"></mat-spinner>
          <p>Cargando historial de batches...</p>
        </div>
      } @else {

        <!-- Pipeline architecture visual -->
        <div class="pipeline-arch">
          <div class="arch-title">
            <mat-icon>schema</mat-icon> Arquitectura de ejecución
          </div>
          <div class="arch-flow">
            @for (step of pipelineSteps; track step.name; let last = $last) {
              <div class="arch-step" [class.active]="latestHealth?.batch_status === 'COMPLETED'"
                   [matTooltip]="step.desc">
                <mat-icon>{{ step.icon }}</mat-icon>
                <span class="step-lambda">{{ step.lambda }}</span>
                <span class="step-name">{{ step.name }}</span>
              </div>
              @if (!last) {
                <mat-icon class="arch-arrow"
                  [class.parallel]="step.name === 'Ingesta'">
                  {{ step.name === 'Ingesta' ? 'call_split' : 'arrow_forward' }}
                </mat-icon>
              }
            }
          </div>
        </div>

        <!-- Coverage chart + status distribution -->
        <div class="charts-row">
          <div class="chart-box">
            <h3 class="section-title"><mat-icon>pie_chart</mat-icon> Estado de batches ({{ batches.length }} reportes)</h3>
            <ngx-charts-pie-chart
              [results]="statusPieChart"
              [legend]="true"
              [labels]="true"
              [doughnut]="false"
              [scheme]="statusScheme"
              [view]="[320, 220]"
            ></ngx-charts-pie-chart>
          </div>

          <div class="chart-box flex-1">
            <h3 class="section-title"><mat-icon>bar_chart</mat-icon> Cobertura de tickers por ejecución (%)</h3>
            <ngx-charts-bar-vertical
              [results]="coverageChart"
              [xAxis]="true"
              [yAxis]="true"
              [showGridLines]="true"
              [scheme]="coverageScheme"
              [view]="[900, 220]"
              yAxisLabel="Cobertura %"
              [showYAxisLabel]="true"
              [rotateXAxisTicks]="true"
              [showDataLabel]="true"
            ></ngx-charts-bar-vertical>
          </div>
        </div>

        <!-- Batch cards timeline -->
        <h3 class="section-title" style="margin-top: 8px; margin-bottom: 16px;">
          <mat-icon>history</mat-icon> Historial de ejecuciones
        </h3>
        <div class="batch-timeline">
          @for (batch of batches; track batch.date) {
            <mat-expansion-panel class="batch-panel {{ batch.status.toLowerCase() }}">
              <mat-expansion-panel-header>
                <div class="batch-header">
                  <span class="batch-status {{ batch.status.toLowerCase() }}">
                    <mat-icon>{{ statusIcon(batch.status) }}</mat-icon>
                    {{ batch.status }}
                  </span>
                  <span class="batch-date">{{ batch.date }}</span>
                  <div class="batch-meta">
                    <span matTooltip="Tickers procesados">
                      <mat-icon>storage</mat-icon> {{ batch.tickers_with_signals }}/{{ batch.tickers_expected }}
                    </span>
                    <span matTooltip="Titulares analizados">
                      <mat-icon>article</mat-icon> {{ batch.headlines_scored }}
                    </span>
                    <span matTooltip="Cobertura de señales">
                      <mat-icon>track_changes</mat-icon> {{ (batch.coverage_ratio*100)|number:'1.0-0' }}%
                    </span>
                  </div>
                  <!-- Coverage mini bar -->
                  <div class="cov-bar" matTooltip="Cobertura: {{ (batch.coverage_ratio*100)|number:'1.1-1' }}%">
                    <div class="cov-fill" [style.width.%]="batch.coverage_ratio*100"
                         [class.full]="batch.coverage_ratio >= 0.95"></div>
                  </div>
                </div>
              </mat-expansion-panel-header>

              <!-- Expanded: stage KPIs -->
              <div class="stage-kpis">
                <h4>KPIs por etapa Lambda</h4>
                <div class="stages-grid">
                  @for (stage of getStages(batch.stage_kpis); track stage.key) {
                    <div class="stage-card">
                      <div class="stage-name">
                        <mat-icon>{{ stageIcon(stage.key) }}</mat-icon>
                        {{ stageLabel(stage.key) }}
                      </div>
                      <div class="stage-metrics">
                        @for (kv of objectEntries(stage.value); track kv[0]) {
                          <div class="stage-metric">
                            <span class="sm-key">{{ kv[0] | titlecase }}</span>
                            <span class="sm-val">{{ kv[1] }}</span>
                          </div>
                        }
                      </div>
                    </div>
                  }

                  <!-- Fixed pipeline stages with context -->
                  <div class="stage-card">
                    <div class="stage-name"><mat-icon>download</mat-icon> λ1 Ingesta</div>
                    <div class="stage-metrics">
                      <div class="stage-metric">
                        <span class="sm-key">Tickers</span>
                        <span class="sm-val">{{ batch.tickers_expected }}</span>
                      </div>
                      <div class="stage-metric">
                        <span class="sm-key">Fuentes</span>
                        <span class="sm-val">yfinance + Finnhub</span>
                      </div>
                    </div>
                  </div>

                  <div class="stage-card">
                    <div class="stage-name"><mat-icon>sentiment_satisfied</mat-icon> λ2 Sentimiento</div>
                    <div class="stage-metrics">
                      <div class="stage-metric">
                        <span class="sm-key">Headlines</span>
                        <span class="sm-val">{{ batch.headlines_scored }}</span>
                      </div>
                      <div class="stage-metric">
                        <span class="sm-key">Modelo</span>
                        <span class="sm-val">FinBERT</span>
                      </div>
                    </div>
                  </div>

                  <div class="stage-card">
                    <div class="stage-name"><mat-icon>show_chart</mat-icon> λ3 Indicadores</div>
                    <div class="stage-metrics">
                      <div class="stage-metric">
                        <span class="sm-key">Tickers</span>
                        <span class="sm-val">{{ batch.tickers_with_signals }}</span>
                      </div>
                      <div class="stage-metric">
                        <span class="sm-key">Indicadores</span>
                        <span class="sm-val">RSI · SMA · BB</span>
                      </div>
                    </div>
                  </div>

                </div>
              </div>
            </mat-expansion-panel>
          }
        </div>

      }
    </div>
  `,
  styles: [`
    .pipeline-page { max-width: 1400px; margin: 0 auto; }
    .page-header {
      display: flex; justify-content: space-between; align-items: flex-start;
      margin-bottom: 20px; flex-wrap: wrap; gap: 12px;
    }
    .page-title {
      display: flex; align-items: center; gap: 8px;
      font-size: 22px; font-weight: 700; color: #1a237e; margin: 0;
    }
    .page-subtitle { color: #666; font-size: 13px; margin-top: 4px; }
    .loading-center {
      display: flex; flex-direction: column; align-items: center;
      gap: 16px; padding: 80px; color: #666;
    }

    /* Architecture visual */
    .pipeline-arch {
      background: #fff; border-radius: 12px; padding: 20px;
      box-shadow: 0 2px 8px rgba(0,0,0,.07); margin-bottom: 20px;
    }
    .arch-title {
      display: flex; align-items: center; gap: 8px;
      font-size: 14px; font-weight: 600; color: #1a237e; margin-bottom: 14px;
    }
    .arch-flow {
      display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
    }
    .arch-step {
      display: flex; flex-direction: column; align-items: center; gap: 2px;
      padding: 10px 16px; border-radius: 10px;
      background: #e8eaf6; border: 2px solid #c5cae9;
      min-width: 90px; text-align: center; transition: all .2s;
      mat-icon { color: #1a237e; font-size: 22px; }
      &.active { background: #e8f5e9; border-color: #a5d6a7;
        mat-icon { color: #2e7d32; } }
      .step-lambda { font-size: 10px; color: #888; font-family: monospace; }
      .step-name { font-size: 12px; font-weight: 600; color: #333; }
    }
    .arch-arrow {
      color: #bbb; font-size: 22px;
      &.parallel { color: #1a237e; transform: rotate(90deg); }
    }

    /* Charts */
    .charts-row { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 20px; }
    .chart-box {
      background: #fff; border-radius: 12px; padding: 20px;
      box-shadow: 0 2px 8px rgba(0,0,0,.07);
    }
    .flex-1 { flex: 1; min-width: 280px; }
    .section-title {
      display: flex; align-items: center; gap: 8px;
      font-size: 15px; font-weight: 600; color: #1a237e; margin: 0 0 16px;
      mat-icon { font-size: 18px; height: 18px; width: 18px; }
    }

    /* Batch timeline */
    .batch-timeline { display: flex; flex-direction: column; gap: 8px; }
    .batch-panel {
      border-radius: 10px !important; overflow: hidden;
      &.completed { border-left: 4px solid #43a047 !important; }
      &.failed    { border-left: 4px solid #e53935 !important; }
      &.started   { border-left: 4px solid #1e88e5 !important; }
    }
    .batch-header {
      display: flex; align-items: center; gap: 14px;
      flex: 1; flex-wrap: wrap;
    }
    .batch-status {
      display: flex; align-items: center; gap: 4px;
      padding: 2px 10px; border-radius: 12px; font-size: 12px; font-weight: 700;
      mat-icon { font-size: 16px; height: 16px; width: 16px; }
      &.completed { background: #e8f5e9; color: #2e7d32; }
      &.failed    { background: #ffebee; color: #c62828; }
      &.started   { background: #e3f2fd; color: #1565c0; }
    }
    .batch-date { font-weight: 600; font-size: 15px; color: #333; min-width: 100px; }
    .batch-meta {
      display: flex; gap: 14px;
      span { display: flex; align-items: center; gap: 4px; font-size: 13px; color: #555;
        mat-icon { font-size: 16px; height: 16px; width: 16px; color: #888; } }
    }
    .cov-bar {
      flex: 1; min-width: 80px; max-width: 140px;
      height: 6px; background: #eee; border-radius: 3px; overflow: hidden;
    }
    .cov-fill {
      height: 100%; background: #78909c; border-radius: 3px; transition: width .5s;
      &.full { background: #43a047; }
    }

    /* Stage KPIs */
    .stage-kpis { padding: 4px 0 8px; }
    .stage-kpis h4 {
      font-size: 14px; font-weight: 600; color: #1a237e; margin: 0 0 12px;
    }
    .stages-grid {
      display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
      gap: 10px;
    }
    .stage-card {
      background: #f9faff; border-radius: 8px; padding: 12px;
      border: 1px solid #e8eaf6;
    }
    .stage-name {
      display: flex; align-items: center; gap: 6px;
      font-size: 13px; font-weight: 600; color: #1a237e; margin-bottom: 8px;
      mat-icon { font-size: 16px; height: 16px; width: 16px; }
    }
    .stage-metrics { display: flex; flex-direction: column; gap: 4px; }
    .stage-metric {
      display: flex; justify-content: space-between;
      font-size: 12px;
    }
    .sm-key { color: #888; }
    .sm-val { font-weight: 600; color: #333; }
  `],
})
export class PipelineComponent implements OnInit {
  private reportSvc = inject(ReportService);

  loading = true;
  batches: BatchSummary[] = [];
  latestHealth: PipelineHealth | null = null;
  statusPieChart: ChartDataPoint[] = [];
  coverageChart: ChartDataPoint[] = [];

  statusScheme: any = { domain: ['#43a047', '#e53935', '#1e88e5'] };
  coverageScheme: any = { domain: ['#1a237e'] };

  pipelineSteps = [
    { name: 'Ingesta', lambda: 'λ1', icon: 'download', desc: 'Descarga OHLCV (yfinance) y noticias (Finnhub) a S3' },
    { name: 'Sentimiento', lambda: 'λ2', icon: 'sentiment_satisfied', desc: 'Análisis FinBERT vía Hugging Face API' },
    { name: 'Indicadores', lambda: 'λ3', icon: 'show_chart', desc: 'RSI 14, SMA 20/50, Bandas de Bollinger' },
    { name: 'Bayesiana', lambda: 'λ4', icon: 'account_tree', desc: 'Red bayesiana pgmpy: BUY/SELL/HOLD + P(up)' },
    { name: 'Reporte', lambda: 'λ5', icon: 'summarize', desc: 'Backtesting 90 días + guardado en S3' },
  ];

  ngOnInit() { this.loadData(); }
  refresh()  { this.loading = true; this.loadData(); }

  private loadData() {
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        if (!dates.length) { this.loading = false; return of([]); }
        const toLoad = dates.slice(0, 14); // last 14 reports
        return forkJoin(toLoad.map(d =>
          this.reportSvc.loadReport(d.date)
        ));
      })
    ).subscribe({
      next: (reports: any[]) => {
        this.batches = reports.map((r: DailyReport) => ({
          date: r.report_date,
          status: r.pipeline_health.batch_status,
          tickers_expected: r.pipeline_health.tickers_expected,
          tickers_with_signals: r.pipeline_health.tickers_with_signals,
          headlines_scored: r.pipeline_health.headlines_scored,
          coverage_ratio: r.pipeline_health.coverage_ratio,
          stage_kpis: r.pipeline_health.stage_kpis,
        }));
        this.latestHealth = reports[0]?.pipeline_health ?? null;
        this.buildCharts();
        this.loading = false;
      },
      error: () => { this.loading = false; },
    });
  }

  private buildCharts() {
    const counts: Record<string, number> = { COMPLETED: 0, FAILED: 0, STARTED: 0 };
    for (const b of this.batches) counts[b.status] = (counts[b.status] ?? 0) + 1;
    this.statusPieChart = Object.entries(counts)
      .filter(([, v]) => v > 0)
      .map(([name, value]) => ({ name, value }));

    this.coverageChart = this.batches.map(b => ({
      name: b.date.slice(5), // MM-DD
      value: +(b.coverage_ratio * 100).toFixed(1),
    }));
  }

  statusIcon(s: string) {
    return { COMPLETED: 'check_circle', FAILED: 'error', STARTED: 'pending' }[s] ?? 'help';
  }

  stageIcon(key: string) {
    const m: Record<string, string> = {
      ingestion: 'download', sentiment: 'sentiment_satisfied',
      indicators: 'show_chart', bayesian: 'account_tree', report: 'summarize',
    };
    return m[key] ?? 'memory';
  }

  stageLabel(key: string) {
    const m: Record<string, string> = {
      ingestion: 'λ1 Ingesta', sentiment: 'λ2 Sentimiento',
      indicators: 'λ3 Indicadores', bayesian: 'λ4 Bayesiana', report: 'λ5 Reporte',
    };
    return m[key] ?? key;
  }

  getStages(kpis: Record<string, any>) {
    return Object.entries(kpis).map(([key, value]) => ({ key, value }));
  }

  objectEntries(obj: Record<string, any>): [string, any][] {
    return Object.entries(obj);
  }
}
