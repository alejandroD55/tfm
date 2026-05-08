import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatSelectModule } from '@angular/material/select';
import { MatTooltipModule } from '@angular/material/tooltip';
import { LegendPosition, NgxChartsModule } from '@swimlane/ngx-charts';
import { switchMap } from 'rxjs';
import { ReportService } from '../../core/services/report.service';
import { DailyReport, TickerView, ReportDateEntry } from '../../core/models/report.model';
import { ChartDataPoint, ChartSeries } from '../../core/models/pipeline.model';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [
    CommonModule, FormsModule, MatIconModule, MatButtonModule,
    MatProgressSpinnerModule, MatSelectModule, MatTooltipModule, NgxChartsModule,
  ],
  template: `
    <div class="page">

      <!-- ─── Page header ─── -->
      <header class="page-head">
        <div class="page-head-text">
          <div class="page-eyebrow">
            <mat-icon>space_dashboard</mat-icon>
            <span>Portfolio overview</span>
          </div>
          <h1 class="page-title">Resumen del portfolio</h1>
          <p class="page-sub">Sistema de trading algorítmico ETF · Red bayesiana + FinBERT · Datos de los últimos {{ report?.data_period_days || 90 }} días</p>
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
          <button class="btn btn-ghost" (click)="refresh()" matTooltip="Recargar último report">
            <mat-icon>refresh</mat-icon>
            <span>Refrescar</span>
          </button>
        </div>
      </header>

      <!-- Loader -->
      @if (loading) {
        <div class="loader">
          <mat-spinner diameter="40"></mat-spinner>
          <p>Cargando report desde el data lake…</p>
        </div>
      } @else if (report) {

        <!-- Pipeline health banner -->
        <div class="health" [class]="'health-' + healthClass(report.pipeline_health.batch_status)">
          <div class="health-icon">
            <mat-icon>{{ healthIcon(report.pipeline_health.batch_status) }}</mat-icon>
          </div>
          <div class="health-body">
            <div class="health-title">
              Pipeline <strong>{{ report.pipeline_health.batch_status }}</strong>
              <span class="health-dot">·</span>
              {{ report.pipeline_health.tickers_with_signals }}/{{ report.pipeline_health.tickers_expected }} tickers procesados
            </div>
            <div class="health-meta">
              {{ report.pipeline_health.headlines_scored }} titulares analizados ·
              cobertura {{ (report.pipeline_health.coverage_ratio * 100) | number:'1.0-0' }}%
            </div>
          </div>
          <div class="health-period">
            <mat-icon>event</mat-icon>
            {{ report.report_date }}
          </div>
        </div>

        <!-- KPI grid -->
        <section class="kpi-grid">

          <article class="kpi"
                   [class.kpi-pos]="report.summary.avg_cumulative_return > 0"
                   [class.kpi-neg]="report.summary.avg_cumulative_return < 0">
            <div class="kpi-head">
              <span class="kpi-label">Retorno acumulado medio</span>
              <span class="kpi-icon"><mat-icon>trending_up</mat-icon></span>
            </div>
            <div class="kpi-value">
              {{ report.summary.avg_cumulative_return > 0 ? '+' : '' }}{{ (report.summary.avg_cumulative_return * 100) | number:'1.2-2' }}<span class="unit">%</span>
            </div>
            <div class="kpi-foot">
              <span class="delta" [class.up]="report.summary.avg_cumulative_return > 0" [class.down]="report.summary.avg_cumulative_return < 0">
                <mat-icon>{{ report.summary.avg_cumulative_return >= 0 ? 'arrow_upward' : 'arrow_downward' }}</mat-icon>
                {{ report.data_period_days }}d
              </span>
              <span class="kpi-sub">{{ report.summary.total_tickers }} ETFs</span>
            </div>
          </article>

          <article class="kpi" [class.kpi-pos]="report.summary.avg_sharpe_ratio > 1">
            <div class="kpi-head">
              <span class="kpi-label">Sharpe ratio medio</span>
              <span class="kpi-icon"><mat-icon>speed</mat-icon></span>
            </div>
            <div class="kpi-value">{{ report.summary.avg_sharpe_ratio | number:'1.2-2' }}</div>
            <div class="kpi-foot">
              <span class="quality" [class.good]="report.summary.avg_sharpe_ratio > 1" [class.bad]="report.summary.avg_sharpe_ratio < 0">
                {{ qualityLabel(report.summary.avg_sharpe_ratio) }}
              </span>
              <span class="kpi-sub">Anualizado · rf 2%</span>
            </div>
          </article>

          <article class="kpi kpi-neg">
            <div class="kpi-head">
              <span class="kpi-label">Max drawdown medio</span>
              <span class="kpi-icon"><mat-icon>arrow_downward</mat-icon></span>
            </div>
            <div class="kpi-value">{{ (report.summary.avg_max_drawdown * 100) | number:'1.2-2' }}<span class="unit">%</span></div>
            <div class="kpi-foot">
              <span class="delta down"><mat-icon>warning_amber</mat-icon> Riesgo</span>
              <span class="kpi-sub">Peor caída del capital</span>
            </div>
          </article>

          <article class="kpi">
            <div class="kpi-head">
              <span class="kpi-label">Trades cerrados</span>
              <span class="kpi-icon"><mat-icon>compare_arrows</mat-icon></span>
            </div>
            <div class="kpi-value">{{ report.summary.total_closed_trades }}</div>
            <div class="kpi-foot">
              <span class="kpi-sub">Total portfolio · {{ report.data_period_days }}d</span>
            </div>
          </article>

          <article class="kpi kpi-signals">
            <div class="kpi-head">
              <span class="kpi-label">Señales activas hoy</span>
              <span class="kpi-icon"><mat-icon>psychology</mat-icon></span>
            </div>
            <div class="kpi-value">
              {{ tickerViews.length }}
              <span class="unit">tickers</span>
            </div>
            <div class="signals-bar">
              <span class="seg buy"   [style.flex]="buyCount  || 0">{{ buyCount  }} BUY</span>
              <span class="seg sell"  [style.flex]="sellCount || 0">{{ sellCount }} SELL</span>
              <span class="seg hold"  [style.flex]="holdCount || 0">{{ holdCount }} HOLD</span>
            </div>
          </article>

        </section>

        <!-- ─── Charts row 1 ─── -->
        <section class="row">
          <div class="card chart-card span-4">
            <div class="card-head">
              <div class="card-title">
                <mat-icon>donut_large</mat-icon>
                <span>Distribución de señales</span>
              </div>
              <span class="card-sub">{{ report.report_date }}</span>
            </div>
            <div class="chart-host">
              <ngx-charts-pie-chart
                [results]="signalPieChart"
                [legend]="true"
                [legendPosition]="legendBelow"
                [labels]="false"
                [doughnut]="true"
                [arcWidth]="0.32"
                [scheme]="signalScheme"
                [view]="[360, 280]">
              </ngx-charts-pie-chart>
            </div>
          </div>

          <div class="card chart-card span-8">
            <div class="card-head">
              <div class="card-title">
                <mat-icon>bar_chart</mat-icon>
                <span>Probabilidad alcista P(↑) por ETF</span>
              </div>
              <span class="card-sub">Bayesian · Markov-blanket inferred</span>
            </div>
            <div class="chart-host">
              <ngx-charts-bar-vertical
                [results]="probUpChart"
                [xAxis]="true"
                [yAxis]="true"
                [showGridLines]="true"
                [scheme]="probScheme"
                [view]="[760, 280]"
                yAxisLabel="P(subida) %"
                [showYAxisLabel]="true"
                [rotateXAxisTicks]="false"
                [showDataLabel]="true"
                [yScaleMax]="100"
                [barPadding]="14"
                [roundEdges]="true">
              </ngx-charts-bar-vertical>
            </div>
            <div class="thresholds">
              <span class="th-buy">BUY ≥ 65%</span>
              <span class="th-hold">HOLD 35–65%</span>
              <span class="th-sell">SELL ≤ 35%</span>
            </div>
          </div>
        </section>

        <!-- ─── Strategy vs B&H ─── -->
        <section class="row">
          <div class="card chart-card span-12">
            <div class="card-head">
              <div class="card-title">
                <mat-icon>show_chart</mat-icon>
                <span>Estrategia bayesiana vs Buy &amp; Hold</span>
              </div>
              <div class="legend">
                <span class="leg-dot" style="background:var(--brand-600)"></span>
                <span>Estrategia</span>
                <span class="leg-dot" style="background:var(--accent-cyan); margin-left: 14px"></span>
                <span>Buy &amp; Hold</span>
              </div>
            </div>
            <div class="chart-host">
              <ngx-charts-bar-vertical-2d
                [results]="returnComparisonChart"
                [xAxis]="true"
                [yAxis]="true"
                [showGridLines]="true"
                [scheme]="compareScheme"
                [view]="[1180, 320]"
                yAxisLabel="Retorno acumulado (%)"
                [showYAxisLabel]="true"
                [showDataLabel]="true"
                [groupPadding]="14"
                [roundEdges]="true">
              </ngx-charts-bar-vertical-2d>
            </div>
          </div>
        </section>

        <!-- ─── Ticker watchlist ─── -->
        <section class="card watchlist">
          <div class="card-head">
            <div class="card-title">
              <mat-icon>format_list_numbered</mat-icon>
              <span>ETFs monitorizados</span>
            </div>
            <span class="card-sub">{{ tickerViews.length }} instrumentos · ranking por P(↑)</span>
          </div>

          <div class="ticker-grid">
            @for (t of tickerViews; track t.ticker) {
              <article class="ticker"
                       [class.ticker-buy]="t.signal === 'BUY'"
                       [class.ticker-sell]="t.signal === 'SELL'"
                       [class.ticker-hold]="t.signal === 'HOLD'">
                <header class="ticker-head">
                  <div class="ticker-symbol">
                    <span class="ticker-mark">{{ t.ticker[0] }}</span>
                    <span>{{ t.ticker }}</span>
                  </div>
                  <span class="signal-pill {{ t.signal.toLowerCase() }}">
                    <mat-icon>{{ signalIcon(t.signal) }}</mat-icon>
                    {{ t.signal }}
                  </span>
                </header>

                <div class="prob-block">
                  <div class="prob-bar"
                       [matTooltip]="'P↑ ' + (t.prob_up*100|number:'1.1-1') + '% · P↓ ' + (t.prob_down*100|number:'1.1-1') + '%'">
                    <div class="prob-up"   [style.width.%]="t.prob_up * 100"></div>
                    <div class="prob-down" [style.width.%]="t.prob_down * 100"></div>
                  </div>
                  <div class="prob-labels">
                    <span class="up">↑ {{ (t.prob_up*100)|number:'1.0-0' }}%</span>
                    <span class="down">↓ {{ (t.prob_down*100)|number:'1.0-0' }}%</span>
                  </div>
                </div>

                <div class="ticker-metrics">
                  <div class="metric">
                    <span class="metric-label">Retorno</span>
                    <span class="metric-value"
                          [class.pos]="t.cumulative_return>0"
                          [class.neg]="t.cumulative_return<0">
                      {{ t.cumulative_return>0?'+':'' }}{{ (t.cumulative_return*100)|number:'1.1-1' }}%
                    </span>
                  </div>
                  <div class="metric">
                    <span class="metric-label">Sharpe</span>
                    <span class="metric-value" [class.pos]="t.sharpe_ratio>1">
                      {{ t.sharpe_ratio|number:'1.2-2' }}
                    </span>
                  </div>
                  <div class="metric">
                    <span class="metric-label">α vs B&amp;H</span>
                    <span class="metric-value"
                          [class.pos]="t.alpha_vs_benchmark>0"
                          [class.neg]="t.alpha_vs_benchmark<0">
                      {{ t.alpha_vs_benchmark>0?'+':'' }}{{ (t.alpha_vs_benchmark*100)|number:'1.1-1' }}%
                    </span>
                  </div>
                </div>
              </article>
            }
          </div>
        </section>

      } @else {
        <div class="empty">
          <mat-icon>inbox</mat-icon>
          <p>No hay reports disponibles todavía.</p>
        </div>
      }
    </div>
  `,
  styles: [`
    /* ─── Page chrome (shared shape) ─── */
    .page { max-width: var(--content-max); margin: 0 auto; }
    .page-head {
      display: flex; justify-content: space-between; align-items: flex-start;
      gap: 24px; flex-wrap: wrap; margin-bottom: 24px;
    }
    .page-eyebrow {
      display: inline-flex; align-items: center; gap: 6px;
      padding: 4px 10px;
      background: var(--brand-100);
      color: var(--brand-700);
      border-radius: var(--r-pill);
      font-size: 11px; font-weight: 600;
      letter-spacing: .04em; text-transform: uppercase;
      margin-bottom: 10px;
      mat-icon { font-size: 14px; height: 14px; width: 14px; }
    }
    .page-title {
      font-size: 26px; font-weight: 700;
      color: var(--slate-900); letter-spacing: -.02em; line-height: 1.2;
    }
    .page-sub { color: var(--slate-500); font-size: 13px; margin-top: 6px; max-width: 740px; }

    .page-actions { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
    .date-input { min-width: 200px; }

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
    .btn-ghost { background: var(--bg-elevated); }

    .loader {
      display: flex; flex-direction: column; align-items: center;
      gap: 14px; padding: 80px 16px; color: var(--slate-500);
    }
    .empty {
      display: flex; flex-direction: column; align-items: center; gap: 8px;
      padding: 80px 16px; color: var(--slate-400);
      mat-icon { font-size: 48px; height: 48px; width: 48px; opacity: .5; }
    }

    /* ─── Health banner ─── */
    .health {
      display: flex; align-items: center; gap: 16px;
      padding: 14px 18px;
      background: var(--bg-elevated);
      border: 1px solid var(--border);
      border-left-width: 4px;
      border-radius: var(--r-md);
      box-shadow: var(--shadow-sm);
      margin-bottom: 22px;
    }
    .health-completed { border-left-color: var(--success-500); }
    .health-failed    { border-left-color: var(--danger-500); }
    .health-started   { border-left-color: var(--brand-500); }
    .health-unknown   { border-left-color: var(--slate-400); }

    .health-icon {
      width: 38px; height: 38px;
      border-radius: 10px;
      display: inline-flex; align-items: center; justify-content: center;
      background: var(--slate-50);
      mat-icon { font-size: 22px; height: 22px; width: 22px; }
    }
    .health-completed .health-icon { background: var(--success-50); color: var(--success-600); }
    .health-failed    .health-icon { background: var(--danger-50);  color: var(--danger-600); }
    .health-started   .health-icon { background: var(--brand-100);   color: var(--brand-600); }
    .health-unknown   .health-icon { background: var(--slate-100);   color: var(--slate-500); }

    .health-body { flex: 1; min-width: 0; }
    .health-title { font-size: 14px; font-weight: 600; color: var(--slate-900); }
    .health-title strong {
      padding: 1px 8px;
      background: var(--slate-100);
      border-radius: var(--r-pill);
      font-size: 11px; letter-spacing: .04em;
      vertical-align: middle;
      margin: 0 4px;
    }
    .health-completed .health-title strong { background: var(--success-100); color: var(--success-700); }
    .health-failed    .health-title strong { background: var(--danger-100);  color: var(--danger-700); }
    .health-started   .health-title strong { background: var(--brand-100);    color: var(--brand-700); }
    .health-dot { color: var(--slate-300); margin: 0 4px; }
    .health-meta { font-size: 12px; color: var(--slate-500); margin-top: 2px; }
    .health-period {
      display: inline-flex; align-items: center; gap: 6px;
      padding: 6px 12px;
      background: var(--slate-50);
      border: 1px solid var(--border);
      border-radius: var(--r-pill);
      color: var(--slate-600);
      font-size: 12px; font-weight: 600; font-variant-numeric: tabular-nums;
      mat-icon { font-size: 14px; height: 14px; width: 14px; color: var(--slate-400); }
    }

    /* ─── KPI grid ─── */
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px;
      margin-bottom: 22px;
    }
    .kpi {
      position: relative;
      background: var(--bg-elevated);
      border: 1px solid var(--border);
      border-radius: var(--r-md);
      padding: 18px;
      box-shadow: var(--shadow-sm);
      transition: transform .15s, box-shadow .15s, border-color .15s;
      overflow: hidden;
    }
    .kpi::after {
      content: ''; position: absolute; inset: auto 0 0 0; height: 3px;
      background: linear-gradient(to right, transparent, var(--brand-300), transparent);
      opacity: 0; transition: opacity .2s;
    }
    .kpi:hover { box-shadow: var(--shadow-md); border-color: var(--brand-300); }
    .kpi:hover::after { opacity: .5; }

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
      background: var(--slate-50);
      display: inline-flex; align-items: center; justify-content: center;
      color: var(--slate-500);
      mat-icon { font-size: 16px; height: 16px; width: 16px; }
    }
    .kpi-pos .kpi-icon { background: var(--success-50); color: var(--success-600); }
    .kpi-neg .kpi-icon { background: var(--danger-50);  color: var(--danger-600); }
    .kpi-signals .kpi-icon { background: var(--brand-100); color: var(--brand-600); }

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

    .delta {
      display: inline-flex; align-items: center; gap: 3px;
      padding: 2px 8px; border-radius: var(--r-pill);
      font-size: 11px; font-weight: 700;
      background: var(--slate-100); color: var(--slate-500);
      mat-icon { font-size: 14px; height: 14px; width: 14px; }
    }
    .delta.up   { background: var(--success-100); color: var(--success-700); }
    .delta.down { background: var(--danger-100);  color: var(--danger-700); }

    .quality {
      font-size: 11px; font-weight: 700; padding: 2px 8px;
      background: var(--slate-100); color: var(--slate-500);
      border-radius: var(--r-pill);
    }
    .quality.good { background: var(--success-100); color: var(--success-700); }
    .quality.bad  { background: var(--danger-100); color: var(--danger-700); }

    .signals-bar {
      margin-top: 10px;
      display: flex; gap: 4px;
      height: 28px;
      border-radius: var(--r-sm);
      overflow: hidden;
    }
    .signals-bar .seg {
      display: flex; align-items: center; justify-content: center;
      font-size: 11px; font-weight: 700; letter-spacing: .03em;
      color: #fff;
    }
    .signals-bar .buy  { background: var(--success-500); }
    .signals-bar .sell { background: var(--danger-500); }
    .signals-bar .hold { background: var(--warn-500); }

    /* ─── Cards / chart-cards ─── */
    .row {
      display: grid;
      grid-template-columns: repeat(12, 1fr);
      gap: 16px;
      margin-bottom: 18px;
    }
    .span-4  { grid-column: span 4; }
    .span-8  { grid-column: span 8; }
    .span-12 { grid-column: span 12; }
    @media (max-width: 1100px) {
      .span-4, .span-8 { grid-column: span 12; }
    }

    .card {
      background: var(--bg-elevated);
      border: 1px solid var(--border);
      border-radius: var(--r-md);
      box-shadow: var(--shadow-sm);
      padding: 18px;
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
    .chart-card { padding: 18px 16px 8px; }

    .thresholds {
      display: flex; gap: 8px; padding: 8px 6px 0;
      span { font-size: 11px; padding: 3px 10px; border-radius: var(--r-pill); font-weight: 600; }
      .th-buy  { background: var(--success-100); color: var(--success-700); }
      .th-hold { background: var(--warn-100); color: var(--warn-700); }
      .th-sell { background: var(--danger-100); color: var(--danger-700); }
    }

    /* ─── Watchlist ticker cards ─── */
    .watchlist { padding: 18px 18px 22px; }
    .ticker-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
      gap: 12px;
      margin-top: 6px;
    }
    .ticker {
      position: relative;
      background: var(--bg-elevated);
      border: 1px solid var(--border);
      border-radius: var(--r-md);
      padding: 14px;
      transition: transform .15s, box-shadow .15s, border-color .15s;
    }
    .ticker:hover {
      box-shadow: var(--shadow-md);
      transform: translateY(-1px);
      border-color: var(--brand-300);
    }
    .ticker::before {
      content: ''; position: absolute; left: 0; top: 14px; bottom: 14px;
      width: 3px; border-radius: 0 3px 3px 0;
      background: var(--slate-200);
    }
    .ticker-buy::before  { background: var(--success-500); }
    .ticker-sell::before { background: var(--danger-500); }
    .ticker-hold::before { background: var(--warn-500); }

    .ticker-head {
      display: flex; justify-content: space-between; align-items: center;
      margin-bottom: 12px;
    }
    .ticker-symbol {
      display: flex; align-items: center; gap: 8px;
      font-size: 16px; font-weight: 700; color: var(--slate-900);
    }
    .ticker-mark {
      width: 28px; height: 28px;
      border-radius: 8px;
      background: linear-gradient(135deg, var(--brand-600), var(--accent-cyan));
      color: #fff;
      display: inline-flex; align-items: center; justify-content: center;
      font-size: 13px; font-weight: 700;
    }
    .signal-pill {
      display: inline-flex; align-items: center; gap: 4px;
      padding: 3px 10px; border-radius: var(--r-pill);
      font-size: 11px; font-weight: 700; letter-spacing: .03em;
      mat-icon { font-size: 14px; height: 14px; width: 14px; }
    }
    .signal-pill.buy  { background: var(--success-100); color: var(--success-700); }
    .signal-pill.sell { background: var(--danger-100);  color: var(--danger-700); }
    .signal-pill.hold { background: var(--warn-100);    color: var(--warn-700); }

    .prob-block { margin-bottom: 12px; }
    .prob-bar {
      height: 8px; background: var(--slate-100);
      border-radius: var(--r-pill); overflow: hidden;
      display: flex; gap: 1px; cursor: help;
    }
    .prob-up   { background: linear-gradient(to right, var(--success-500), var(--success-600)); transition: width .5s; }
    .prob-down { background: linear-gradient(to right, var(--danger-500), var(--danger-600));   transition: width .5s; }
    .prob-labels {
      display: flex; justify-content: space-between;
      margin-top: 5px; font-size: 11px;
      .up   { color: var(--success-700); font-weight: 700; }
      .down { color: var(--danger-700);  font-weight: 700; }
    }

    .ticker-metrics {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 6px;
    }
    .metric {
      background: var(--slate-50);
      border-radius: var(--r-sm);
      padding: 6px 8px;
      display: flex; flex-direction: column; align-items: center;
      gap: 2px;
    }
    .metric-label {
      font-size: 10px; color: var(--slate-400);
      letter-spacing: .04em; text-transform: uppercase; font-weight: 600;
    }
    .metric-value {
      font-size: 13px; font-weight: 700; color: var(--slate-800);
      font-variant-numeric: tabular-nums;
    }
    .metric-value.pos { color: var(--success-700); }
    .metric-value.neg { color: var(--danger-700); }
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

  legendBelow = LegendPosition.Below;

  signalScheme: any = { domain: ['#22C55E', '#EF4444', '#F59E0B'] };
  probScheme: any   = { domain: ['#2563EB'] };
  compareScheme: any = { domain: ['#2563EB', '#06B6D4'] };

  get buyCount()  { return this.tickerViews.filter(t => t.signal === 'BUY').length;  }
  get sellCount() { return this.tickerViews.filter(t => t.signal === 'SELL').length; }
  get holdCount() { return this.tickerViews.filter(t => t.signal === 'HOLD').length; }

  ngOnInit() { this.loadDates(); }

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
    this.tickerViews = this.reportSvc.buildTickerViews(report)
      .sort((a, b) => b.prob_up - a.prob_up);
    this.signalPieChart        = this.reportSvc.signalDistributionChart(this.tickerViews);
    this.probUpChart           = this.reportSvc.probUpChart(this.tickerViews);
    this.returnComparisonChart = this.reportSvc.returnComparisonChart(this.tickerViews);
  }

  healthClass(status: string) { return status.toLowerCase(); }
  healthIcon(status: string)  {
    return ({
      COMPLETED: 'check_circle',
      FAILED:    'error',
      STARTED:   'pending',
      UNKNOWN:   'help_outline',
    } as Record<string, string>)[status] ?? 'help_outline';
  }
  signalIcon(s: string) {
    return ({ BUY: 'arrow_upward', SELL: 'arrow_downward', HOLD: 'remove' } as Record<string, string>)[s] ?? 'remove';
  }
  qualityLabel(s: number) {
    if (s >= 2)  return 'Excelente';
    if (s >= 1)  return 'Buena';
    if (s >= 0)  return 'Aceptable';
    return 'Pobre';
  }
}
