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
import { MatCardModule } from '@angular/material/card';
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
    MatTooltipModule, MatCardModule, NgxChartsModule,
  ],
  template: `
    <div class="bt-page">

      <!-- Header -->
      <div class="page-header">
        <div>
          <h2 class="page-title"><mat-icon>analytics</mat-icon> Backtesting &amp; Performance</h2>
          <p class="page-subtitle">Métricas estrategia vs Buy &amp; Hold · Últimos 90 días · Capital inicial: $10,000</p>
        </div>
        <div class="header-controls">
          <mat-form-field appearance="outline" class="date-sel">
            <mat-label>Fecha del report</mat-label>
            <mat-select [(ngModel)]="selectedDate" (ngModelChange)="onDateChange($event)">
              @for (d of availableDates; track d.date) {
                <mat-option [value]="d.date">{{ d.date }}</mat-option>
              }
            </mat-select>
          </mat-form-field>
        </div>
      </div>

      @if (loading) {
        <div class="loading-center">
          <mat-spinner diameter="48"></mat-spinner><p>Calculando métricas...</p>
        </div>
      } @else {

        <!-- Portfolio KPIs -->
        <div class="kpi-row">
          <div class="kpi-card" matTooltip="Retorno acumulado medio de la estrategia en todos los ETFs">
            <mat-icon style="color:#1a237e">trending_up</mat-icon>
            <div>
              <div class="kv" [class.pos]="summary.avg_cumulative_return>0" [class.neg]="summary.avg_cumulative_return<0">
                {{ summary.avg_cumulative_return>0?'+':'' }}{{ (summary.avg_cumulative_return*100)|number:'1.2-2' }}%
              </div>
              <div class="kl">Retorno medio portfolio</div>
            </div>
          </div>
          <div class="kpi-card" matTooltip="Ratio de Sharpe anualizado medio (rf = 2%)">
            <mat-icon style="color:#00695c">speed</mat-icon>
            <div>
              <div class="kv" [class.pos]="summary.avg_sharpe_ratio>1">{{ summary.avg_sharpe_ratio|number:'1.2-2' }}</div>
              <div class="kl">Sharpe ratio medio</div>
            </div>
          </div>
          <div class="kpi-card" matTooltip="Peor caída de capital media en el período">
            <mat-icon style="color:#b71c1c">arrow_downward</mat-icon>
            <div>
              <div class="kv neg">{{ (summary.avg_max_drawdown*100)|number:'1.2-2' }}%</div>
              <div class="kl">Max drawdown medio</div>
            </div>
          </div>
          <div class="kpi-card" matTooltip="Trades completados en todos los ETFs">
            <mat-icon style="color:#4527a0">swap_horiz</mat-icon>
            <div>
              <div class="kv">{{ summary.total_closed_trades }}</div>
              <div class="kl">Trades cerrados</div>
            </div>
          </div>
          <div class="kpi-card" matTooltip="Nº de ETFs con retorno positivo">
            <mat-icon style="color:#e65100">emoji_events</mat-icon>
            <div>
              <div class="kv">{{ winnersCount }}/{{ summary.total_tickers }}</div>
              <div class="kl">ETFs ganadores</div>
            </div>
          </div>
        </div>

        <!-- Chart row 1: Return comparison grouped bars -->
        <div class="chart-box">
          <h3 class="section-title"><mat-icon>bar_chart</mat-icon> Retorno acumulado: Estrategia vs Buy &amp; Hold (%)</h3>
          <ngx-charts-bar-vertical-2d
            [results]="returnChart"
            [xAxis]="true"
            [yAxis]="true"
            [showGridLines]="true"
            [scheme]="compareScheme"
            [view]="[950, 300]"
            yAxisLabel="Retorno (%)"
            [showYAxisLabel]="true"
            [showDataLabel]="true"
            [groupPadding]="12"
          ></ngx-charts-bar-vertical-2d>
          <div class="chart-legend">
            <span class="leg-item strat">Estrategia bayesiana</span>
            <span class="leg-item bh">Buy &amp; Hold</span>
          </div>
        </div>

        <!-- Chart row 2: Sharpe + Drawdown side by side -->
        <div class="charts-2col">
          <div class="chart-box">
            <h3 class="section-title"><mat-icon>speed</mat-icon> Sharpe Ratio anualizado por ETF</h3>
            <ngx-charts-bar-horizontal
              [results]="sharpeChart"
              [xAxis]="true"
              [yAxis]="true"
              [showGridLines]="true"
              [scheme]="sharpeScheme"
              [view]="[700, 280]"
              xAxisLabel="Sharpe"
              [showXAxisLabel]="true"
              [showDataLabel]="true"
            ></ngx-charts-bar-horizontal>
            <div class="threshold-note">
              <mat-icon>info_outline</mat-icon> Sharpe &gt; 1 = buena estrategia · &gt; 2 = excelente
            </div>
          </div>

          <div class="chart-box">
            <h3 class="section-title"><mat-icon>arrow_downward</mat-icon> Max Drawdown por ETF (%)</h3>
            <ngx-charts-bar-horizontal
              [results]="drawdownChart"
              [xAxis]="true"
              [yAxis]="true"
              [showGridLines]="true"
              [scheme]="drawdownScheme"
              [view]="[700, 280]"
              xAxisLabel="Drawdown (%)"
              [showXAxisLabel]="true"
              [showDataLabel]="true"
            ></ngx-charts-bar-horizontal>
            <div class="threshold-note danger">
              <mat-icon>warning</mat-icon> Drawdown &gt; 20% = riesgo elevado
            </div>
          </div>
        </div>

        <!-- Alpha chart -->
        <div class="chart-box">
          <h3 class="section-title"><mat-icon>add_circle_outline</mat-icon> Alpha vs Buy &amp; Hold (exceso de retorno estrategia)</h3>
          <ngx-charts-bar-vertical
            [results]="alphaChart"
            [xAxis]="true"
            [yAxis]="true"
            [showGridLines]="true"
            [scheme]="alphaScheme"
            [view]="[950, 240]"
            yAxisLabel="Alpha (%)"
            [showYAxisLabel]="true"
            [showDataLabel]="true"
          ></ngx-charts-bar-vertical>
          <div class="threshold-note">
            <mat-icon>info_outline</mat-icon> Alpha positivo = estrategia supera al mercado
          </div>
        </div>

        <!-- Detail table -->
        <div class="table-wrapper">
          <h3 class="section-title" style="padding: 20px 20px 0">
            <mat-icon>table_chart</mat-icon> Detalle por ETF
          </h3>
          <table mat-table [dataSource]="tableSource">

            <ng-container matColumnDef="ticker">
              <th mat-header-cell *matHeaderCellDef>Ticker</th>
              <td mat-cell *matCellDef="let r"><strong class="ticker-lbl">{{ r.ticker }}</strong></td>
            </ng-container>

            <ng-container matColumnDef="signal">
              <th mat-header-cell *matHeaderCellDef>Señal hoy</th>
              <td mat-cell *matCellDef="let r">
                <span class="sig-badge {{ r.signal.toLowerCase() }}">{{ r.signal }}</span>
              </td>
            </ng-container>

            <ng-container matColumnDef="return">
              <th mat-header-cell *matHeaderCellDef>Retorno est.</th>
              <td mat-cell *matCellDef="let r">
                <span [class.pos]="r.cumulative_return>0" [class.neg]="r.cumulative_return<0" class="num-val">
                  {{ r.cumulative_return>0?'+':'' }}{{ (r.cumulative_return*100)|number:'1.2-2' }}%
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="bh">
              <th mat-header-cell *matHeaderCellDef>Buy&amp;Hold</th>
              <td mat-cell *matCellDef="let r">
                <span class="num-val">{{ (r.buy_hold_return*100)|number:'1.2-2' }}%</span>
              </td>
            </ng-container>

            <ng-container matColumnDef="alpha">
              <th mat-header-cell *matHeaderCellDef>Alpha</th>
              <td mat-cell *matCellDef="let r">
                <span [class.pos]="r.alpha_vs_benchmark>0" [class.neg]="r.alpha_vs_benchmark<0" class="num-val bold">
                  {{ r.alpha_vs_benchmark>0?'+':'' }}{{ (r.alpha_vs_benchmark*100)|number:'1.2-2' }}%
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="sharpe">
              <th mat-header-cell *matHeaderCellDef>Sharpe</th>
              <td mat-cell *matCellDef="let r">
                <span [class.pos]="r.sharpe_ratio>1" class="num-val">{{ r.sharpe_ratio|number:'1.2-2' }}</span>
              </td>
            </ng-container>

            <ng-container matColumnDef="drawdown">
              <th mat-header-cell *matHeaderCellDef>Max DD</th>
              <td mat-cell *matCellDef="let r">
                <span class="neg num-val">{{ (r.max_drawdown*100)|number:'1.2-2' }}%</span>
              </td>
            </ng-container>

            <ng-container matColumnDef="equity">
              <th mat-header-cell *matHeaderCellDef>Capital final</th>
              <td mat-cell *matCellDef="let r">
                <span [class.pos]="r.final_equity>10000" [class.neg]="r.final_equity<10000" class="num-val">
                  {{ '$' }}{{ r.final_equity|number:'1.0-0' }}
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="trades">
              <th mat-header-cell *matHeaderCellDef>Trades</th>
              <td mat-cell *matCellDef="let r">{{ r.trades_closed }}</td>
            </ng-container>

            <ng-container matColumnDef="winrate">
              <th mat-header-cell *matHeaderCellDef>Win rate</th>
              <td mat-cell *matCellDef="let r">
                <span [class.pos]="r.win_rate>0.5">{{ (r.win_rate*100)|number:'1.0-0' }}%</span>
              </td>
            </ng-container>

            <ng-container matColumnDef="pf">
              <th mat-header-cell *matHeaderCellDef>Profit factor</th>
              <td mat-cell *matCellDef="let r">
                <span [class.pos]="r.profit_factor>1">{{ r.profit_factor|number:'1.2-2' }}</span>
              </td>
            </ng-container>

            <tr mat-header-row *matHeaderRowDef="tableCols; sticky: true"></tr>
            <tr mat-row *matRowDef="let r; columns: tableCols;"></tr>
          </table>
        </div>

      }
    </div>
  `,
  styles: [`
    .bt-page { max-width: 1400px; margin: 0 auto; }
    .page-header {
      display: flex; justify-content: space-between; align-items: flex-start;
      margin-bottom: 20px; flex-wrap: wrap; gap: 12px;
    }
    .page-title {
      display: flex; align-items: center; gap: 8px;
      font-size: 22px; font-weight: 700; color: #1a237e; margin: 0;
    }
    .page-subtitle { color: #666; font-size: 13px; margin-top: 4px; }
    .header-controls { display: flex; gap: 8px; }
    .date-sel { min-width: 160px; }
    .loading-center {
      display: flex; flex-direction: column; align-items: center;
      gap: 16px; padding: 80px; color: #666;
    }

    .kpi-row {
      display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px;
    }
    .kpi-card {
      background: #fff; border-radius: 12px; padding: 16px 20px;
      box-shadow: 0 2px 8px rgba(0,0,0,.07);
      display: flex; align-items: center; gap: 12px; flex: 1; min-width: 160px;
      mat-icon { font-size: 28px; height: 28px; width: 28px; }
    }
    .kv {
      font-size: 22px; font-weight: 700; color: #1a237e;
      &.pos { color: #2e7d32; } &.neg { color: #c62828; }
    }
    .kl { font-size: 11px; color: #666; margin-top: 2px; }

    .chart-box {
      background: #fff; border-radius: 12px; padding: 20px;
      box-shadow: 0 2px 8px rgba(0,0,0,.07); margin-bottom: 16px;
    }
    .charts-2col {
      display: grid; grid-template-columns: 1fr 1fr; gap: 16px;
      margin-bottom: 16px;
    }
    @media (max-width: 900px) { .charts-2col { grid-template-columns: 1fr; } }

    .section-title {
      display: flex; align-items: center; gap: 8px;
      font-size: 15px; font-weight: 600; color: #1a237e; margin: 0 0 16px;
      mat-icon { font-size: 18px; height: 18px; width: 18px; }
    }
    .chart-legend {
      display: flex; gap: 16px; margin-top: 8px; font-size: 12px;
    }
    .leg-item { display: flex; align-items: center; gap: 6px; }
    .leg-item::before { content: ''; width: 12px; height: 12px; border-radius: 2px; }
    .leg-item.strat::before { background: #1a237e; }
    .leg-item.bh::before    { background: #ff7043; }
    .threshold-note {
      display: flex; align-items: center; gap: 6px;
      margin-top: 8px; font-size: 12px; color: #666; background: #f9f9fb;
      padding: 6px 10px; border-radius: 6px;
      mat-icon { font-size: 16px; color: #1565c0; }
      &.danger { color: #b71c1c; mat-icon { color: #e53935; } }
    }

    .table-wrapper {
      background: #fff; border-radius: 12px;
      box-shadow: 0 2px 8px rgba(0,0,0,.07);
      overflow: hidden;
    }
    table { width: 100%; }
    .ticker-lbl { font-size: 15px; color: #1a237e; }
    .sig-badge {
      padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 700;
      &.buy  { background: #e8f5e9; color: #2e7d32; }
      &.sell { background: #ffebee; color: #c62828; }
      &.hold { background: #fff8e1; color: #f57f17; }
    }
    .num-val { font-weight: 500;
      &.pos, &.green { color: #2e7d32; }
      &.neg, &.red   { color: #c62828; }
      &.bold { font-weight: 700; }
    }
    .pos { color: #2e7d32; font-weight: 600; }
    .neg { color: #c62828; }
  `],
})
export class BacktestingComponent implements OnInit {
  private reportSvc = inject(ReportService);

  loading = true;
  availableDates: ReportDateEntry[] = [];
  selectedDate = '';
  tickerViews: TickerView[] = [];
  summary: any = {};

  returnChart: ChartSeries[] = [];
  sharpeChart: ChartDataPoint[] = [];
  drawdownChart: ChartDataPoint[] = [];
  alphaChart: ChartDataPoint[] = [];

  tableSource = new MatTableDataSource<TickerView>();
  tableCols = ['ticker', 'signal', 'return', 'bh', 'alpha', 'sharpe', 'drawdown', 'equity', 'trades', 'winrate', 'pf'];

  compareScheme: any = { domain: ['#1a237e', '#ff7043'] };
  sharpeScheme: any = { domain: ['#00695c'] };
  drawdownScheme: any = { domain: ['#b71c1c'] };
  alphaScheme: any = { domain: ['#4527a0', '#e53935'] };

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
    this.returnChart  = this.reportSvc.returnComparisonChart(this.tickerViews);
    this.sharpeChart  = this.reportSvc.sharpeChart(this.tickerViews);
    this.drawdownChart = this.reportSvc.drawdownChart(this.tickerViews);
    this.alphaChart    = this.reportSvc.alphaChart(this.tickerViews);
  }
}
