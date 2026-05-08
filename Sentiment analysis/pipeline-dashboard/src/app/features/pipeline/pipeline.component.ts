import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatExpansionModule } from '@angular/material/expansion';
import { LegendPosition, NgxChartsModule } from '@swimlane/ngx-charts';
import { forkJoin, switchMap, of } from 'rxjs';
import { ReportService } from '../../core/services/report.service';
import { DailyReport, PipelineHealth, BatchStatus } from '../../core/models/report.model';
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
    MatProgressSpinnerModule, MatTooltipModule, MatExpansionModule, NgxChartsModule,
  ],
  template: `
    <div class="page">

      <!-- Header -->
      <header class="page-head">
        <div class="page-head-text">
          <div class="page-eyebrow">
            <mat-icon>account_tree</mat-icon>
            <span>Infrastructure</span>
          </div>
          <h1 class="page-title">Estado del pipeline</h1>
          <p class="page-sub">
            5 funciones Lambda orquestadas con AWS Step Functions:
            ingesta → sentimiento (FinBERT) → indicadores técnicos → red bayesiana → reporte.
          </p>
        </div>
        <button class="btn btn-ghost" (click)="refresh()">
          <mat-icon>refresh</mat-icon>
          <span>Refrescar</span>
        </button>
      </header>

      @if (loading) {
        <div class="loader">
          <mat-spinner diameter="40"></mat-spinner>
          <p>Cargando historial de batches…</p>
        </div>
      } @else {

        <!-- Pipeline status pills -->
        <div class="status-row">
          <div class="status-pill ok">
            <span class="led"></span>
            <div>
              <div class="pill-val">{{ completedCount }}</div>
              <div class="pill-lbl">Batches OK</div>
            </div>
          </div>
          <div class="status-pill err">
            <span class="led err-led"></span>
            <div>
              <div class="pill-val">{{ failedCount }}</div>
              <div class="pill-lbl">Batches con error</div>
            </div>
          </div>
          <div class="status-pill info">
            <span class="led info-led"></span>
            <div>
              <div class="pill-val">{{ avgCoverage | number:'1.0-0' }}%</div>
              <div class="pill-lbl">Cobertura media</div>
            </div>
          </div>
          <div class="status-pill neutral">
            <mat-icon>history</mat-icon>
            <div>
              <div class="pill-val">{{ batches.length }}</div>
              <div class="pill-lbl">Reports disponibles</div>
            </div>
          </div>
        </div>

        <!-- Architecture flow -->
        <section class="card arch-card">
          <div class="card-head">
            <div class="card-title">
              <mat-icon>schema</mat-icon>
              <span>Arquitectura de ejecución</span>
            </div>
            <span class="card-sub">Step Functions · ejecución diaria a las 22:00 UTC</span>
          </div>

          <div class="arch-flow">
            @for (step of pipelineSteps; track step.name; let i = $index; let last = $last) {
              <div class="arch-step"
                   [class.active]="latestHealth?.batch_status === 'COMPLETED'"
                   [matTooltip]="step.desc">
                <div class="arch-icon">
                  <mat-icon>{{ step.icon }}</mat-icon>
                </div>
                <div class="arch-meta">
                  <span class="arch-lambda">{{ step.lambda }}</span>
                  <span class="arch-name">{{ step.name }}</span>
                  <span class="arch-tech">{{ step.tech }}</span>
                </div>
                <span class="arch-num">{{ i + 1 }}</span>
              </div>
              @if (!last) {
                <div class="arch-link">
                  <mat-icon>{{ step.name === 'Ingesta' ? 'call_split' : 'east' }}</mat-icon>
                </div>
              }
            }
          </div>
        </section>

        <!-- Status pie + coverage chart -->
        <section class="row">
          <div class="card chart-card span-4">
            <div class="card-head">
              <div class="card-title">
                <mat-icon>donut_large</mat-icon>
                <span>Estado de los batches</span>
              </div>
              <span class="card-sub">{{ batches.length }} reports</span>
            </div>
            <div class="chart-host">
              <ngx-charts-pie-chart
                [results]="statusPieChart"
                [legend]="true"
                [legendPosition]="legendBelow"
                [labels]="false"
                [doughnut]="true"
                [arcWidth]="0.32"
                [scheme]="statusScheme"
                [view]="[340, 280]">
              </ngx-charts-pie-chart>
            </div>
          </div>

          <div class="card chart-card span-8">
            <div class="card-head">
              <div class="card-title">
                <mat-icon>show_chart</mat-icon>
                <span>Cobertura de tickers por ejecución</span>
              </div>
              <span class="card-sub">% de tickers con señal generada</span>
            </div>
            <div class="chart-host">
              <ngx-charts-bar-vertical
                [results]="coverageChart"
                [xAxis]="true" [yAxis]="true" [showGridLines]="true"
                [scheme]="coverageScheme"
                [view]="[760, 280]"
                yAxisLabel="Cobertura %" [showYAxisLabel]="true"
                [rotateXAxisTicks]="true" [showDataLabel]="true"
                [yScaleMax]="100" [roundEdges]="true">
              </ngx-charts-bar-vertical>
            </div>
          </div>
        </section>

        <!-- Batch timeline -->
        <section class="card timeline-card">
          <div class="card-head">
            <div class="card-title">
              <mat-icon>history</mat-icon>
              <span>Historial de ejecuciones</span>
            </div>
            <span class="card-sub">{{ batches.length }} batches · ordenados por fecha</span>
          </div>

          <div class="timeline">
            @for (batch of batches; track batch.date) {
              <mat-expansion-panel class="batch-panel">

                <mat-expansion-panel-header [collapsedHeight]="'76px'" [expandedHeight]="'76px'">
                  <div class="batch-row">
                    <span class="batch-status {{ batch.status.toLowerCase() }}">
                      <mat-icon>{{ statusIcon(batch.status) }}</mat-icon>
                      {{ batch.status }}
                    </span>
                    <span class="batch-date">{{ batch.date }}</span>

                    <div class="batch-meta">
                      <span class="m-item" matTooltip="Tickers procesados">
                        <mat-icon>storage</mat-icon>
                        {{ batch.tickers_with_signals }}/{{ batch.tickers_expected }}
                      </span>
                      <span class="m-item" matTooltip="Titulares analizados">
                        <mat-icon>article</mat-icon>{{ batch.headlines_scored }}
                      </span>
                      <span class="m-item" matTooltip="Cobertura de señales">
                        <mat-icon>track_changes</mat-icon>{{ (batch.coverage_ratio*100)|number:'1.0-0' }}%
                      </span>
                    </div>

                    <div class="cov-bar" [matTooltip]="'Cobertura: ' + (batch.coverage_ratio*100|number:'1.1-1') + '%'">
                      <div class="cov-fill"
                           [style.width.%]="batch.coverage_ratio*100"
                           [class.full]="batch.coverage_ratio >= 0.95"
                           [class.partial]="batch.coverage_ratio < 0.95 && batch.coverage_ratio >= 0.5"
                           [class.poor]="batch.coverage_ratio < 0.5"></div>
                    </div>
                  </div>
                </mat-expansion-panel-header>

                <div class="stages">
                  <h4>KPIs por etapa Lambda</h4>
                  <div class="stages-grid">

                    <!-- Stage cards from KPIs -->
                    @for (stage of getStages(batch.stage_kpis); track stage.key) {
                      <div class="stage">
                        <div class="stage-head">
                          <mat-icon>{{ stageIcon(stage.key) }}</mat-icon>
                          <span class="stage-title">{{ stageLabel(stage.key) }}</span>
                        </div>
                        <div class="stage-body">
                          @for (kv of objectEntries(stage.value); track kv[0]) {
                            <div class="stage-metric">
                              <span class="sm-key">{{ kv[0] | titlecase }}</span>
                              <span class="sm-val">{{ kv[1] }}</span>
                            </div>
                          }
                        </div>
                      </div>
                    }

                    <!-- Stages with fixed context -->
                    <div class="stage">
                      <div class="stage-head">
                        <mat-icon>download</mat-icon>
                        <span class="stage-title">λ1 Ingesta</span>
                      </div>
                      <div class="stage-body">
                        <div class="stage-metric"><span class="sm-key">Tickers</span><span class="sm-val">{{ batch.tickers_expected }}</span></div>
                        <div class="stage-metric"><span class="sm-key">Fuentes</span><span class="sm-val">yfinance · Finnhub</span></div>
                      </div>
                    </div>

                    <div class="stage">
                      <div class="stage-head">
                        <mat-icon>sentiment_satisfied</mat-icon>
                        <span class="stage-title">λ2 Sentimiento</span>
                      </div>
                      <div class="stage-body">
                        <div class="stage-metric"><span class="sm-key">Headlines</span><span class="sm-val">{{ batch.headlines_scored }}</span></div>
                        <div class="stage-metric"><span class="sm-key">Modelo</span><span class="sm-val">FinBERT (HF)</span></div>
                      </div>
                    </div>

                    <div class="stage">
                      <div class="stage-head">
                        <mat-icon>show_chart</mat-icon>
                        <span class="stage-title">λ3 Indicadores</span>
                      </div>
                      <div class="stage-body">
                        <div class="stage-metric"><span class="sm-key">Tickers</span><span class="sm-val">{{ batch.tickers_with_signals }}</span></div>
                        <div class="stage-metric"><span class="sm-key">Indicadores</span><span class="sm-val">RSI · SMA · BB</span></div>
                      </div>
                    </div>

                  </div>
                </div>
              </mat-expansion-panel>
            }
          </div>
        </section>

      }
    </div>
  `,
  styles: [`
    .page { max-width: var(--content-max); margin: 0 auto; }
    .page-head {
      display: flex; justify-content: space-between; align-items: flex-start;
      gap: 24px; flex-wrap: wrap; margin-bottom: 22px;
    }
    .page-eyebrow {
      display: inline-flex; align-items: center; gap: 6px;
      padding: 4px 10px;
      background: rgba(6, 182, 212, .12);
      color: var(--accent-cyan);
      border-radius: var(--r-pill);
      font-size: 11px; font-weight: 600;
      letter-spacing: .04em; text-transform: uppercase;
      margin-bottom: 10px;
      mat-icon { font-size: 14px; height: 14px; width: 14px; }
    }
    .page-title { font-size: 26px; font-weight: 700; color: var(--slate-900); letter-spacing: -.02em; }
    .page-sub { color: var(--slate-500); font-size: 13px; margin-top: 6px; max-width: 760px; }

    .btn {
      display: inline-flex; align-items: center; gap: 6px;
      padding: 8px 14px;
      border-radius: var(--r-sm);
      border: 1px solid var(--border);
      font-family: var(--font-sans); font-size: 13px; font-weight: 600;
      cursor: pointer; transition: all .15s;
      background: var(--bg-elevated); color: var(--slate-700);
      mat-icon { font-size: 18px; height: 18px; width: 18px; }
    }
    .btn:hover { border-color: var(--brand-300); color: var(--brand-600); background: var(--slate-50); }

    .loader {
      display: flex; flex-direction: column; align-items: center;
      gap: 14px; padding: 80px 16px; color: var(--slate-500);
    }

    /* Status pills */
    .status-row {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px; margin-bottom: 18px;
    }
    .status-pill {
      display: flex; align-items: center; gap: 12px;
      padding: 14px 18px;
      background: var(--bg-elevated);
      border: 1px solid var(--border);
      border-radius: var(--r-md);
      box-shadow: var(--shadow-sm);
      mat-icon { font-size: 20px; height: 20px; width: 20px; }
    }
    .status-pill .pill-val { font-size: 22px; font-weight: 700; color: var(--slate-900); font-variant-numeric: tabular-nums; }
    .status-pill .pill-lbl { font-size: 11px; color: var(--slate-500); }
    .led {
      width: 10px; height: 10px; border-radius: 50%;
      box-shadow: 0 0 0 3px rgba(34, 197, 94, .12);
      background: var(--success-500);
      animation: pulse-dot 2s ease-in-out infinite;
    }
    .err-led  { background: var(--danger-500);  box-shadow: 0 0 0 3px rgba(239,68,68,.12); }
    .info-led { background: var(--brand-500);   box-shadow: 0 0 0 3px rgba(59,130,246,.15); }
    @keyframes pulse-dot {
      0%,100% { transform: scale(1); }
      50%     { transform: scale(1.25); }
    }
    .status-pill.ok      { border-left: 3px solid var(--success-500); }
    .status-pill.err     { border-left: 3px solid var(--danger-500); }
    .status-pill.info    { border-left: 3px solid var(--brand-500); }
    .status-pill.neutral mat-icon { color: var(--slate-400); }

    /* Card baseline */
    .card {
      background: var(--bg-elevated);
      border: 1px solid var(--border);
      border-radius: var(--r-md);
      box-shadow: var(--shadow-sm);
      padding: 18px;
      margin-bottom: 18px;
    }
    .card-head {
      display: flex; align-items: center; justify-content: space-between;
      gap: 12px; margin-bottom: 14px; flex-wrap: wrap;
    }
    .card-title {
      display: flex; align-items: center; gap: 8px;
      font-size: 14px; font-weight: 600; color: var(--slate-900);
      mat-icon { font-size: 18px; height: 18px; width: 18px; color: var(--brand-600); }
    }
    .card-sub { font-size: 12px; color: var(--slate-400); }

    .row {
      display: grid;
      grid-template-columns: repeat(12, 1fr);
      gap: 16px; margin-bottom: 18px;
    }
    .row .card { margin-bottom: 0; }
    .span-4 { grid-column: span 4; }
    .span-8 { grid-column: span 8; }
    @media (max-width: 1100px) {
      .span-4, .span-8 { grid-column: span 12; }
    }
    .chart-host { width: 100%; overflow-x: auto; }
    .chart-card { padding: 18px 16px 12px; }

    /* Architecture flow */
    .arch-flow {
      display: flex; align-items: stretch; gap: 8px; flex-wrap: wrap;
      padding: 14px;
      background: var(--slate-50);
      border-radius: var(--r-md);
    }
    .arch-step {
      position: relative;
      display: flex; align-items: center; gap: 10px;
      padding: 14px 16px;
      background: var(--bg-elevated);
      border: 1px solid var(--border);
      border-radius: var(--r-md);
      flex: 1 1 180px;
      transition: transform .15s, box-shadow .15s, border-color .15s;
    }
    .arch-step:hover { box-shadow: var(--shadow-md); border-color: var(--brand-300); }
    .arch-step.active { border-color: var(--success-300, var(--success-500)); }
    .arch-icon {
      width: 36px; height: 36px;
      border-radius: 10px;
      background: var(--brand-100); color: var(--brand-700);
      display: inline-flex; align-items: center; justify-content: center;
      mat-icon { font-size: 20px; height: 20px; width: 20px; }
    }
    .arch-step.active .arch-icon { background: var(--success-100); color: var(--success-700); }
    .arch-meta { display: flex; flex-direction: column; line-height: 1.2; }
    .arch-lambda { font-size: 10px; color: var(--brand-500); font-weight: 700; letter-spacing: .04em; font-family: var(--font-mono); }
    .arch-name { font-size: 13px; font-weight: 700; color: var(--slate-900); }
    .arch-tech { font-size: 10px; color: var(--slate-400); }
    .arch-num {
      position: absolute; top: 6px; right: 8px;
      font-size: 9px; font-weight: 700; color: var(--slate-300);
      font-family: var(--font-mono);
    }
    .arch-link {
      display: flex; align-items: center; justify-content: center;
      color: var(--slate-300);
      mat-icon { font-size: 22px; height: 22px; width: 22px; }
    }

    /* Timeline */
    .timeline-card { padding: 18px; }
    .timeline {
      display: flex; flex-direction: column; gap: 10px;
    }
    .batch-panel {
      border: 1px solid var(--border) !important;
      border-radius: var(--r-md) !important;
      box-shadow: none !important;
      transition: border-color .15s, box-shadow .15s;
    }
    .batch-panel:hover {
      border-color: var(--brand-300) !important;
      box-shadow: var(--shadow-sm) !important;
    }
    .batch-row {
      display: flex; align-items: center; gap: 14px;
      flex: 1; flex-wrap: wrap;
    }
    .batch-status {
      display: inline-flex; align-items: center; gap: 5px;
      padding: 3px 10px; border-radius: var(--r-pill);
      font-size: 11px; font-weight: 700; letter-spacing: .03em;
      mat-icon { font-size: 14px; height: 14px; width: 14px; }
    }
    .batch-status.completed { background: var(--success-100); color: var(--success-700); }
    .batch-status.failed    { background: var(--danger-100);  color: var(--danger-700); }
    .batch-status.started   { background: var(--brand-100);    color: var(--brand-700); }
    .batch-status.unknown   { background: var(--slate-100);    color: var(--slate-600); }

    .batch-date {
      font-weight: 700; font-size: 14px; color: var(--slate-900);
      min-width: 100px; font-variant-numeric: tabular-nums;
    }
    .batch-meta {
      display: flex; gap: 14px;
    }
    .m-item {
      display: inline-flex; align-items: center; gap: 4px;
      font-size: 12px; color: var(--slate-600);
      mat-icon { font-size: 14px; height: 14px; width: 14px; color: var(--slate-400); }
    }
    .cov-bar {
      flex: 1; min-width: 100px; max-width: 220px;
      height: 6px; background: var(--slate-100);
      border-radius: var(--r-pill); overflow: hidden;
    }
    .cov-fill {
      height: 100%;
      border-radius: var(--r-pill);
      transition: width .5s;
      background: var(--slate-300);
      &.full    { background: linear-gradient(to right, var(--success-500), var(--success-600)); }
      &.partial { background: linear-gradient(to right, var(--warn-500), var(--warn-600)); }
      &.poor    { background: linear-gradient(to right, var(--danger-500), var(--danger-600)); }
    }

    /* Stage cards */
    .stages h4 {
      font-size: 13px; font-weight: 600; color: var(--slate-900);
      margin: 4px 0 12px;
    }
    .stages-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
      gap: 10px;
    }
    .stage {
      background: var(--slate-50);
      border-radius: var(--r-sm);
      padding: 12px;
      border: 1px solid var(--border);
    }
    .stage-head {
      display: flex; align-items: center; gap: 6px;
      margin-bottom: 10px;
      font-size: 13px; font-weight: 700; color: var(--brand-700);
      mat-icon { font-size: 16px; height: 16px; width: 16px; color: var(--brand-600); }
    }
    .stage-title { letter-spacing: .01em; }
    .stage-body { display: flex; flex-direction: column; gap: 4px; }
    .stage-metric {
      display: flex; justify-content: space-between;
      font-size: 12px;
    }
    .sm-key { color: var(--slate-500); }
    .sm-val { font-weight: 700; color: var(--slate-800); font-variant-numeric: tabular-nums; }
  `],
})
export class PipelineComponent implements OnInit {
  private reportSvc = inject(ReportService);

  legendBelow = LegendPosition.Below;

  loading = true;
  batches: BatchSummary[] = [];
  latestHealth: PipelineHealth | null = null;
  statusPieChart: ChartDataPoint[] = [];
  coverageChart: ChartDataPoint[] = [];

  statusScheme: any   = { domain: ['#22C55E', '#EF4444', '#3B82F6', '#94A3B8'] };
  coverageScheme: any = { domain: ['#06B6D4'] };

  pipelineSteps = [
    { name: 'Ingesta',     lambda: 'λ1', icon: 'download',           tech: 'yfinance + Finnhub', desc: 'Descarga OHLCV y noticias a S3' },
    { name: 'Sentimiento', lambda: 'λ2', icon: 'sentiment_satisfied', tech: 'FinBERT · HF',       desc: 'Análisis de sentimiento sobre titulares' },
    { name: 'Indicadores', lambda: 'λ3', icon: 'show_chart',          tech: 'RSI · SMA · BB',     desc: 'Cálculo de indicadores técnicos' },
    { name: 'Bayesiana',   lambda: 'λ4', icon: 'account_tree',        tech: 'pgmpy',              desc: 'Red bayesiana → BUY/SELL/HOLD + P(↑)' },
    { name: 'Reporte',     lambda: 'λ5', icon: 'summarize',           tech: 'Backtesting 90d',    desc: 'Cálculo de métricas y guardado en S3' },
  ];

  get completedCount() { return this.batches.filter(b => b.status === 'COMPLETED').length; }
  get failedCount()    { return this.batches.filter(b => b.status === 'FAILED').length; }
  get avgCoverage()    {
    if (!this.batches.length) return 0;
    return (this.batches.reduce((s, b) => s + b.coverage_ratio, 0) / this.batches.length) * 100;
  }

  ngOnInit() { this.loadData(); }
  refresh()  { this.loading = true; this.loadData(); }

  private loadData() {
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        if (!dates.length) { this.loading = false; return of([]); }
        const toLoad = dates.slice(0, 14);
        return forkJoin(toLoad.map(d => this.reportSvc.loadReport(d.date)));
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
    const counts: Record<string, number> = { COMPLETED: 0, FAILED: 0, STARTED: 0, UNKNOWN: 0 };
    for (const b of this.batches) counts[b.status] = (counts[b.status] ?? 0) + 1;
    this.statusPieChart = Object.entries(counts)
      .filter(([, v]) => v > 0)
      .map(([name, value]) => ({ name, value }));

    this.coverageChart = this.batches.map(b => ({
      name: b.date.slice(5),
      value: +(b.coverage_ratio * 100).toFixed(1),
    }));
  }

  statusIcon(s: string) {
    return ({
      COMPLETED: 'check_circle',
      FAILED: 'error',
      STARTED: 'pending',
      UNKNOWN: 'help_outline',
    } as Record<string, string>)[s] ?? 'help_outline';
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
