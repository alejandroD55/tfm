import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatTableModule, MatTableDataSource } from '@angular/material/table';
import { MatSortModule } from '@angular/material/sort';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { NgxChartsModule } from '@swimlane/ngx-charts';
import { switchMap } from 'rxjs';
import { ReportService } from '../../core/services/report.service';
import { DailyReport, TickerView, ReportDateEntry } from '../../core/models/report.model';
import { ChartDataPoint, ChartSeries } from '../../core/models/pipeline.model';

@Component({
  selector: 'app-backtesting',
  standalone: true,
  imports: [
    CommonModule, FormsModule, MatTableModule, MatSortModule, MatFormFieldModule,
    MatSelectModule, MatButtonModule, MatIconModule, MatProgressSpinnerModule,
    MatTooltipModule, NgxChartsModule,
  ],
  template: `
    <div class="page">

      <!-- Header -->
      <header class="page-head">
        <div class="page-head-text">
          <div class="page-eyebrow">
            <mat-icon>insights</mat-icon>
            <span>Performance & risk</span>
          </div>
          <h1 class="page-title">Backtesting & performance</h1>
          <p class="page-sub">
            Métricas de la estrategia <strong>Long/Short</strong> bayesiana frente a Buy &amp; Hold.
            Período: <strong>{{ summary.total_tickers ? 365 : 0 }} días</strong>.
            Capital inicial: <strong>$10,000</strong>.
          </p>
        </div>
        <div class="page-actions">
          <mat-form-field appearance="outline" class="date-input" subscriptSizing="dynamic">
            <mat-label>Fecha del report</mat-label>
            <mat-select [(ngModel)]="selectedDate" (ngModelChange)="onDateChange($event)">
              @for (d of availableDates; track d.date) {
                <mat-option [value]="d.date">{{ d.date }}</mat-option>
              }
            </mat-select>
          </mat-form-field>
        </div>
      </header>

      @if (loading) {
        <div class="loader">
          <mat-spinner diameter="40"></mat-spinner>
          <p>Calculando métricas de backtesting…</p>
        </div>
      } @else {

        <!-- KPI row -->
        <section class="kpi-grid">
          <article class="kpi"
                   [class.kpi-pos]="summary.avg_cumulative_return>0"
                   [class.kpi-neg]="summary.avg_cumulative_return<0">
            <div class="kpi-head">
              <span class="kpi-label">Retorno medio portfolio</span>
              <span class="kpi-icon"><mat-icon>trending_up</mat-icon></span>
            </div>
            <div class="kpi-value">
              {{ summary.avg_cumulative_return>0?'+':'' }}{{ (summary.avg_cumulative_return*100)|number:'1.2-2' }}<span class="unit">%</span>
            </div>
            <div class="kpi-foot">
              <span class="kpi-sub">365 días · {{ summary.total_tickers }} ETFs</span>
            </div>
          </article>

          <article class="kpi" [class.kpi-pos]="summary.avg_sharpe_ratio>1">
            <div class="kpi-head">
              <span class="kpi-label">Sharpe ratio medio</span>
              <span class="kpi-icon"><mat-icon>speed</mat-icon></span>
            </div>
            <div class="kpi-value">{{ summary.avg_sharpe_ratio|number:'1.2-2' }}</div>
            <div class="kpi-foot">
              <span class="quality" [class.good]="summary.avg_sharpe_ratio>1">{{ qualityLabel(summary.avg_sharpe_ratio) }}</span>
              <span class="kpi-sub">Anualizado</span>
            </div>
          </article>

          <article class="kpi kpi-neg">
            <div class="kpi-head">
              <span class="kpi-label">Max drawdown medio</span>
              <span class="kpi-icon"><mat-icon>arrow_downward</mat-icon></span>
            </div>
            <div class="kpi-value">{{ (summary.avg_max_drawdown*100)|number:'1.2-2' }}<span class="unit">%</span></div>
            <div class="kpi-foot">
              <span class="kpi-sub">Peor caída de capital</span>
            </div>
          </article>

          <article class="kpi">
            <div class="kpi-head">
              <span class="kpi-label">Trades cerrados</span>
              <span class="kpi-icon"><mat-icon>swap_horiz</mat-icon></span>
            </div>
            <div class="kpi-value">{{ summary.total_closed_trades }}</div>
            <div class="kpi-foot"><span class="kpi-sub">Total portfolio</span></div>
          </article>

          <article class="kpi" [class.kpi-pos]="winnersCount >= summary.total_tickers / 2">
            <div class="kpi-head">
              <span class="kpi-label">ETFs ganadores</span>
              <span class="kpi-icon"><mat-icon>emoji_events</mat-icon></span>
            </div>
            <div class="kpi-value">
              {{ winnersCount }}<span class="unit">/{{ summary.total_tickers }}</span>
            </div>
            <div class="kpi-foot"><span class="kpi-sub">Retorno positivo</span></div>
          </article>
        </section>

        <!-- Comparison chart -->
        <section class="card chart-card">
          <div class="card-head">
            <div class="card-title">
              <mat-icon>bar_chart</mat-icon>
              <span>Retorno acumulado: estrategia vs Buy &amp; Hold</span>
            </div>
            <div class="legend">
              <span class="leg-dot" style="background:var(--brand-600)"></span>
              <span>Estrategia bayesiana</span>
              <span class="leg-dot" style="background:var(--accent-cyan); margin-left:14px"></span>
              <span>Buy &amp; Hold</span>
            </div>
          </div>
          <div class="chart-host">
            <ngx-charts-bar-vertical-2d
              [results]="returnChart"
              [xAxis]="true" [yAxis]="true" [showGridLines]="true"
              [scheme]="compareScheme"
              [view]="[1180, 320]"
              yAxisLabel="Retorno (%)" [showYAxisLabel]="true"
              [showDataLabel]="true" [groupPadding]="14"
              [roundEdges]="true">
            </ngx-charts-bar-vertical-2d>
          </div>
        </section>

        <!-- Sharpe + Drawdown side by side -->
        <section class="row-2">
          <div class="card chart-card">
            <div class="card-head">
              <div class="card-title">
                <mat-icon>speed</mat-icon>
                <span>Sharpe ratio anualizado</span>
              </div>
              <span class="card-sub">por ETF</span>
            </div>
            <div class="chart-host">
              <ngx-charts-bar-horizontal
                [results]="sharpeChart"
                [xAxis]="true" [yAxis]="true" [showGridLines]="true"
                [scheme]="sharpeScheme"
                [view]="[580, 320]"
                xAxisLabel="Sharpe" [showXAxisLabel]="true"
                [showDataLabel]="true" [roundEdges]="true">
              </ngx-charts-bar-horizontal>
            </div>
            <div class="hint hint-info">
              <mat-icon>info</mat-icon>
              <span>Sharpe &gt; 1 = buena estrategia · &gt; 2 = excelente</span>
            </div>
          </div>

          <div class="card chart-card">
            <div class="card-head">
              <div class="card-title">
                <mat-icon>arrow_downward</mat-icon>
                <span>Max drawdown</span>
              </div>
              <span class="card-sub">por ETF</span>
            </div>
            <div class="chart-host">
              <ngx-charts-bar-horizontal
                [results]="drawdownChart"
                [xAxis]="true" [yAxis]="true" [showGridLines]="true"
                [scheme]="drawdownScheme"
                [view]="[580, 320]"
                xAxisLabel="Drawdown (%)" [showXAxisLabel]="true"
                [showDataLabel]="true" [roundEdges]="true">
              </ngx-charts-bar-horizontal>
            </div>
            <div class="hint hint-warn">
              <mat-icon>warning_amber</mat-icon>
              <span>Drawdown &gt; 20% indica un riesgo elevado</span>
            </div>
          </div>
        </section>

        <!-- Alpha chart -->
        <section class="card chart-card">
          <div class="card-head">
            <div class="card-title">
              <mat-icon>add_circle_outline</mat-icon>
              <span>Alpha vs Buy &amp; Hold</span>
            </div>
            <span class="card-sub">exceso de retorno · positivo = la estrategia gana</span>
          </div>
          <div class="chart-host">
            <ngx-charts-bar-vertical
              [results]="alphaChart"
              [xAxis]="true" [yAxis]="true" [showGridLines]="true"
              [customColors]="customColors"
              [view]="[1180, 280]"
              yAxisLabel="Alpha (%)" [showYAxisLabel]="true"
              [showDataLabel]="true" [roundEdges]="true">
            </ngx-charts-bar-vertical>
          </div>
        </section>

        <!-- Detailed table -->
        <section class="card table-card">
          <div class="card-head">
            <div class="card-title">
              <mat-icon>table_chart</mat-icon>
              <span>Detalle por ETF</span>
            </div>
            <span class="card-sub">{{ tickerViews.length }} instrumentos</span>
          </div>
          <table mat-table [dataSource]="tableSource" class="aurora-table">

            <ng-container matColumnDef="ticker">
              <th mat-header-cell *matHeaderCellDef>Ticker</th>
              <td mat-cell *matCellDef="let r">
                <div class="ticker-cell">
                  <span class="ticker-mark">{{ r.ticker[0] }}</span>
                  <strong class="ticker-lbl">{{ r.ticker }}</strong>
                </div>
              </td>
            </ng-container>

            <ng-container matColumnDef="signal">
              <th mat-header-cell *matHeaderCellDef>Señal de Hoy</th>
              <td mat-cell *matCellDef="let r">
                <span class="signal-pill {{ r.signal.toLowerCase() }}">
                  {{ r.signal === 'BUY' ? 'COMPRAR' : r.signal === 'SELL' ? 'VENDER' : 'MANTENER' }}
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="return">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>Retorno Estrategia</th>
              </ng-container>

            <ng-container matColumnDef="bh">
              <th mat-header-cell *matHeaderCellDef>Comprar y Mantener</th>
              </ng-container>

            <ng-container matColumnDef="alpha">
              <th mat-header-cell *matHeaderCellDef>Ventaja (Alpha)</th>
              </ng-container>

            <ng-container matColumnDef="sharpe">
              <th mat-header-cell *matHeaderCellDef>Ratio Sharpe</th>
              </ng-container>

            <ng-container matColumnDef="drawdown">
              <th mat-header-cell *matHeaderCellDef>Caída Máx.</th>
              </ng-container>

            <ng-container matColumnDef="equity">
              <th mat-header-cell *matHeaderCellDef>Capital Final</th>
              </ng-container>

            <ng-container matColumnDef="trades">
              <th mat-header-cell *matHeaderCellDef>Operaciones</th>
              </ng-container>

            <ng-container matColumnDef="winrate">
              <th mat-header-cell *matHeaderCellDef>Tasa Acierto</th>
              </ng-container>

            <ng-container matColumnDef="pf">
              <th mat-header-cell *matHeaderCellDef>F. Beneficio</th>
              </ng-container>

            <tr mat-header-row *matHeaderRowDef="tableCols; sticky: true"></tr>
            <tr mat-row *matRowDef="let r; columns: tableCols;"></tr>
          </table>
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
      background: rgba(20, 184, 166, .12);
      color: var(--accent-teal);
      border-radius: var(--r-pill);
      font-size: 11px; font-weight: 600;
      letter-spacing: .04em; text-transform: uppercase;
      margin-bottom: 10px;
      mat-icon { font-size: 14px; height: 14px; width: 14px; }
    }
    .page-title { font-size: 26px; font-weight: 700; color: var(--slate-900); letter-spacing: -.02em; }
    .page-sub { color: var(--slate-500); font-size: 13px; margin-top: 6px; max-width: 760px; }
    .page-actions { display: flex; align-items: center; gap: 10px; }
    .date-input { min-width: 200px; }

    .loader {
      display: flex; flex-direction: column; align-items: center;
      gap: 14px; padding: 80px 16px; color: var(--slate-500);
    }

    /* KPI grid (same vocab as dashboard) */
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 14px; margin-bottom: 18px;
    }
    .kpi {
      background: var(--bg-elevated);
      border: 1px solid var(--border);
      border-radius: var(--r-md);
      padding: 18px;
      box-shadow: var(--shadow-sm);
    }
    .kpi-head {
      display: flex; align-items: center; justify-content: space-between;
      margin-bottom: 12px;
    }
    .kpi-label {
      font-size: 11px; font-weight: 600;
      letter-spacing: .04em; text-transform: uppercase;
      color: var(--slate-500);
    }
    .kpi-icon {
      width: 30px; height: 30px;
      border-radius: 8px;
      background: var(--slate-50); color: var(--slate-500);
      display: inline-flex; align-items: center; justify-content: center;
      mat-icon { font-size: 16px; height: 16px; width: 16px; }
    }
    .kpi-pos .kpi-icon { background: var(--success-50); color: var(--success-600); }
    .kpi-neg .kpi-icon { background: var(--danger-50);  color: var(--danger-600); }

    .kpi-value {
      font-size: 28px; font-weight: 700;
      color: var(--slate-900);
      letter-spacing: -.02em; line-height: 1.1;
      font-variant-numeric: tabular-nums;
      .unit { font-size: 14px; font-weight: 500; color: var(--slate-400); margin-left: 2px; }
    }
    .kpi-pos .kpi-value { color: var(--success-700); }
    .kpi-neg .kpi-value { color: var(--danger-700); }
    .kpi-foot {
      margin-top: 10px;
      display: flex; align-items: center; justify-content: space-between;
      gap: 8px;
    }
    .kpi-sub { font-size: 11px; color: var(--slate-400); }
    .quality {
      font-size: 11px; font-weight: 700; padding: 2px 8px;
      background: var(--slate-100); color: var(--slate-500);
      border-radius: var(--r-pill);
    }
    .quality.good { background: var(--success-100); color: var(--success-700); }

    /* Cards / charts */
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
    .legend {
      display: inline-flex; align-items: center; gap: 6px;
      font-size: 12px; color: var(--slate-600);
    }
    .leg-dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; margin-right: 4px; }
    .chart-host { width: 100%; overflow-x: auto; }
    .chart-card { padding: 18px 16px 12px; }

    .row-2 {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      margin-bottom: 18px;
    }
    .row-2 .card { margin-bottom: 0; }
    @media (max-width: 1100px) { .row-2 { grid-template-columns: 1fr; } }

    .hint {
      display: flex; align-items: center; gap: 8px;
      margin-top: 10px;
      padding: 8px 12px;
      border-radius: var(--r-sm);
      font-size: 12px;
      mat-icon { font-size: 16px; height: 16px; width: 16px; }
    }
    .hint-info { background: var(--brand-100); color: var(--brand-700); mat-icon { color: var(--brand-600); } }
    .hint-warn { background: var(--warn-100);  color: var(--warn-700);  mat-icon { color: var(--warn-600); } }

    /* Table */
    .table-card { padding: 18px 0 6px; }
    .table-card .card-head { padding: 0 18px; }
    .aurora-table { width: 100%; }
    .ticker-cell { display: flex; align-items: center; gap: 10px; }
    .ticker-mark {
      width: 26px; height: 26px;
      border-radius: 8px;
      background: linear-gradient(135deg, var(--brand-600), var(--accent-cyan));
      color: #fff;
      display: inline-flex; align-items: center; justify-content: center;
      font-size: 12px; font-weight: 700;
    }
    .ticker-lbl { font-size: 14px; color: var(--slate-900); font-weight: 700; }

    .signal-pill {
      padding: 3px 10px; border-radius: var(--r-pill);
      font-size: 11px; font-weight: 700; letter-spacing: .03em;
      &.buy  { background: var(--success-100); color: var(--success-700); }
      &.sell { background: var(--danger-100);  color: var(--danger-700); }
      &.hold { background: var(--warn-100);    color: var(--warn-700); }
    }
    .num {
      font-variant-numeric: tabular-nums; font-weight: 600;
      &.bold { font-weight: 700; }
      &.pos  { color: var(--success-700); }
      &.neg  { color: var(--danger-700); }
    }

    .winrate-cell { display: flex; align-items: center; gap: 8px; min-width: 110px; }
    .wr-bar {
      flex: 1; height: 5px;
      background: var(--slate-100); border-radius: var(--r-pill);
      overflow: hidden;
    }
    .wr-fill {
      height: 100%; width: 0; background: var(--slate-400);
      border-radius: var(--r-pill); transition: width .5s;
      &.high { background: linear-gradient(to right, var(--success-500), var(--success-600)); }
    }
  `],
})
export class BacktestingComponent implements OnInit {
  private reportSvc = inject(ReportService);

  loading = true;
  availableDates: ReportDateEntry[] = [];
  selectedDate = '';
  tickerViews: TickerView[] = [];
  summary: any = { total_tickers: 0, avg_cumulative_return: 0, avg_sharpe_ratio: 0, avg_max_drawdown: 0, total_closed_trades: 0 };

  returnChart: ChartSeries[] = [];
  sharpeChart: ChartDataPoint[] = [];
  drawdownChart: ChartDataPoint[] = [];
  alphaChart: ChartDataPoint[] = [];

  tableSource = new MatTableDataSource<TickerView>();
  tableCols = ['ticker', 'signal', 'return', 'bh', 'alpha', 'sharpe', 'drawdown', 'equity', 'trades', 'winrate', 'pf'];

  compareScheme: any  = { domain: ['#60A5FA', '#94A3B8'] }; // Azul claro (Estrategia) vs Gris neutro (B&H)
  sharpeScheme: any   = { domain: ['#14B8A6'] };
  drawdownScheme: any = { domain: ['#EF4444'] };
  
  // Hacemos que el color de Alpha dependa de si es positivo o negativo
  customColors = (name: string) => {
    const item = this.alphaChart.find(d => d.name === name);
    return (item && item.value >= 0) ? '#22C55E' : '#EF4444'; // Verde si ganamos, Rojo si perdemos
  };

  get winnersCount() { return this.tickerViews.filter(t => t.cumulative_return > 0).length; }

  ngOnInit() {
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        this.availableDates = dates;
        if (!dates.length) { this.loading = false; return []; }
        this.selectedDate = dates[0].date;
        return this.reportSvc.loadReport(this.selectedDate);
      })
    ).subscribe({
      next: (r: any) => { if (r) this.processReport(r); this.loading = false; },
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

  private processReport(report: DailyReport) {
    this.summary = report.summary;
    this.tickerViews = this.reportSvc.buildTickerViews(report);
    this.tableSource.data = this.tickerViews;
    this.returnChart   = this.reportSvc.returnComparisonChart(this.tickerViews);
    this.sharpeChart   = this.reportSvc.sharpeChart(this.tickerViews);
    this.drawdownChart = this.reportSvc.drawdownChart(this.tickerViews);
    this.alphaChart    = this.reportSvc.alphaChart(this.tickerViews);
  }

  qualityLabel(s: number) {
    if (s >= 2) return 'Excelente';
    if (s >= 1) return 'Buena';
    if (s >= 0) return 'Aceptable';
    return 'Pobre';
  }
}
