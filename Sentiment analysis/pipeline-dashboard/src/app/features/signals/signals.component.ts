import { Component, OnInit, ViewChild, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatTableModule, MatTableDataSource } from '@angular/material/table';
import { MatSortModule, MatSort } from '@angular/material/sort';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { RouterModule } from '@angular/router';
import { switchMap, catchError, of } from 'rxjs';
import { ReportService } from '../../core/services/report.service';
import { TraceService } from '../../core/services/trace.service';
import {
  TickerView, ReportDateEntry, DailyReport,
  SentimentState, RsiState, TrendState, VolatilityState,
} from '../../core/models/report.model';
import { TickerTrace } from '../../core/models/trace.model';

@Component({
  selector: 'app-signals',
  standalone: true,
  imports: [
    CommonModule, FormsModule, RouterModule,
    MatTableModule, MatSortModule, MatFormFieldModule,
    MatSelectModule, MatInputModule, MatButtonModule, MatIconModule,
    MatProgressSpinnerModule, MatTooltipModule,
  ],
  template: `
    <div class="page">

      <!-- Header -->
      <header class="page-head">
        <div class="page-head-text">
          <div class="page-eyebrow">
            <mat-icon>psychology</mat-icon>
            <span>Bayesian decisions</span>
          </div>
          <h1 class="page-title">Señales &amp; decisiones explicables</h1>
          <p class="page-sub">
            Cada señal se explica como una cadena de evidencia bayesiana sobre 4 nodos:
            sentimiento (FinBERT), RSI 14, tendencia (SMA20 vs SMA50) y volatilidad (Bollinger).
          </p>
        </div>
        <div class="page-actions">
          <mat-form-field appearance="outline" class="date-input" subscriptSizing="dynamic">
            <mat-label>Fecha</mat-label>
            <mat-select [(ngModel)]="selectedDate" (ngModelChange)="onDateChange($event)">
              @for (d of availableDates; track d.date) {
                <mat-option [value]="d.date">{{ d.date }}</mat-option>
              }
            </mat-select>
          </mat-form-field>
          <mat-form-field appearance="outline" subscriptSizing="dynamic">
            <mat-label>Filtrar señal</mat-label>
            <mat-select [(ngModel)]="filterSignal" (ngModelChange)="applyFilter()">
              <mat-option value="">Todas</mat-option>
              <mat-option value="BUY">BUY</mat-option>
              <mat-option value="SELL">SELL</mat-option>
              <mat-option value="HOLD">HOLD</mat-option>
            </mat-select>
          </mat-form-field>
        </div>
      </header>

      <!-- Bayesian network legend -->
      <section class="bn">
        <div class="bn-head">
          <div class="card-title">
            <mat-icon>account_tree</mat-icon>
            <span>Modelo bayesiano · 4 nodos de evidencia → MarketDirection</span>
          </div>
          <div class="bn-thresholds">
            <span class="th th-buy">BUY · P(↑) ≥ 65%</span>
            <span class="th th-hold">HOLD · 35–65%</span>
            <span class="th th-sell">SELL · P(↑) ≤ 35%</span>
          </div>
        </div>
        <div class="bn-flow">
          <div class="bn-node bn-sentiment">
            <mat-icon>sentiment_satisfied</mat-icon>
            <div>
              <div class="bn-label">Sentimiento</div>
              <small>FinBERT · titulares</small>
            </div>
          </div>
          <span class="bn-arrow">+</span>
          <div class="bn-node bn-rsi">
            <mat-icon>show_chart</mat-icon>
            <div>
              <div class="bn-label">RSI 14</div>
              <small>&lt;30 / 30–70 / &gt;70</small>
            </div>
          </div>
          <span class="bn-arrow">+</span>
          <div class="bn-node bn-trend">
            <mat-icon>trending_up</mat-icon>
            <div>
              <div class="bn-label">Tendencia</div>
              <small>SMA20 vs SMA50</small>
            </div>
          </div>
          <span class="bn-arrow">+</span>
          <div class="bn-node bn-vol">
            <mat-icon>swap_vert</mat-icon>
            <div>
              <div class="bn-label">Volatilidad</div>
              <small>Bollinger Width</small>
            </div>
          </div>
          <span class="bn-arrow bn-arrow-result">→</span>
          <div class="bn-node bn-target">
            <mat-icon>query_stats</mat-icon>
            <div>
              <div class="bn-label">Market Direction</div>
              <small>P(↑) · P(↓)</small>
            </div>
          </div>
        </div>
      </section>

      @if (loading) {
        <div class="loader">
          <mat-spinner diameter="40"></mat-spinner>
          <p>Calculando señales bayesianas…</p>
        </div>
      } @else {

        <!-- Summary chips -->
        <div class="summary-chips">
          <span class="schip buy"  ><mat-icon>arrow_upward</mat-icon> {{ buyCount }} BUY</span>
          <span class="schip sell" ><mat-icon>arrow_downward</mat-icon> {{ sellCount }} SELL</span>
          <span class="schip hold" ><mat-icon>remove</mat-icon> {{ holdCount }} HOLD</span>
          <span class="schip avg"  ><mat-icon>insights</mat-icon> Avg P(↑): {{ avgProbUp | number:'1.1-1' }}%</span>
        </div>

        <!-- Table card -->
        <div class="card table-card">
          <table mat-table [dataSource]="dataSource" matSort class="aurora-table">

            <ng-container matColumnDef="signal">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>Señal</th>
              <td mat-cell *matCellDef="let row">
                <span class="signal-pill {{ row.signal.toLowerCase() }}">
                  <mat-icon>{{ signalIcon(row.signal) }}</mat-icon>
                  {{ row.signal }}
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="ticker">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>Ticker</th>
              <td mat-cell *matCellDef="let row">
                <div class="ticker-cell">
                  <span class="ticker-mark">{{ row.ticker[0] }}</span>
                  <span class="ticker-name">{{ row.ticker }}</span>
                </div>
              </td>
            </ng-container>

            <ng-container matColumnDef="prob_up">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>P(subida)</th>
              <td mat-cell *matCellDef="let row">
                <div class="prob-cell">
                  <div class="prob-gauge">
                    <div class="gauge-fill"
                         [style.width.%]="row.prob_up*100"
                         [class.high]="row.prob_up > 0.65"
                         [class.low]="row.prob_up < 0.35"></div>
                  </div>
                  <span class="prob-pct" [class.green]="row.prob_up>0.65" [class.red]="row.prob_up<0.35">
                    {{ (row.prob_up*100)|number:'1.1-1' }}%
                  </span>
                </div>
              </td>
            </ng-container>

            <ng-container matColumnDef="prob_down">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>P(bajada)</th>
              <td mat-cell *matCellDef="let row">
                <span class="muted-num">{{ (row.prob_down*100)|number:'1.1-1' }}%</span>
              </td>
            </ng-container>

            <ng-container matColumnDef="evidence">
              <th mat-header-cell *matHeaderCellDef>Evidencia bayesiana</th>
              <td mat-cell *matCellDef="let row">
                <div class="ev-row">
                  <span class="ev-chip {{ sentimentClass(row.evidence.sentiment) }}"
                        [matTooltip]="'Sentimiento FinBERT: ' + row.evidence.sentiment">
                    <mat-icon>{{ sentimentIcon(row.evidence.sentiment) }}</mat-icon>
                    {{ row.evidence.sentiment }}
                  </span>
                  <span class="ev-chip {{ rsiClass(row.evidence.rsi) }}"
                        [matTooltip]="'RSI 14: ' + row.evidence.rsi">
                    <mat-icon>show_chart</mat-icon>
                    {{ row.evidence.rsi }}
                  </span>
                  <span class="ev-chip {{ trendClass(row.evidence.trend) }}"
                        [matTooltip]="'Tendencia SMA20 vs SMA50: ' + row.evidence.trend">
                    <mat-icon>{{ row.evidence.trend === 'uptrend' ? 'trending_up' : 'trending_down' }}</mat-icon>
                    {{ row.evidence.trend }}
                  </span>
                  <span class="ev-chip {{ volClass(row.evidence.volatility) }}"
                        [matTooltip]="'Volatilidad: ' + row.evidence.volatility">
                    <mat-icon>swap_vert</mat-icon>
                    {{ row.evidence.volatility }}
                  </span>
                </div>
              </td>
            </ng-container>

            <ng-container matColumnDef="trades">
              <th mat-header-cell *matHeaderCellDef>Trades</th>
              <td mat-cell *matCellDef="let row">
                <div class="trade-stats">
                  <span class="ts-chip" matTooltip="Trades cerrados (90d)">
                    <mat-icon>swap_horiz</mat-icon>{{ row.trades_closed }}
                  </span>
                  <span class="ts-chip wr"
                        [class.good]="row.win_rate>=0.5"
                        matTooltip="Win rate (90d)">
                    <mat-icon>track_changes</mat-icon>{{ (row.win_rate*100)|number:'1.0-0' }}%
                  </span>
                </div>
              </td>
            </ng-container>

            <ng-container matColumnDef="return">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>Retorno</th>
              <td mat-cell *matCellDef="let row">
                <span class="ret-val" [class.pos]="row.cumulative_return>0" [class.neg]="row.cumulative_return<0">
                  {{ row.cumulative_return>0?'+':'' }}{{ (row.cumulative_return*100)|number:'1.2-2' }}%
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="alpha">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>α vs B&amp;H</th>
              <td mat-cell *matCellDef="let row">
                <span class="ret-val" [class.pos]="row.alpha_vs_benchmark>0" [class.neg]="row.alpha_vs_benchmark<0">
                  {{ row.alpha_vs_benchmark>0?'+':'' }}{{ (row.alpha_vs_benchmark*100)|number:'1.2-2' }}%
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="expand">
              <th mat-header-cell *matHeaderCellDef></th>
              <td mat-cell *matCellDef="let row">
                <button mat-icon-button (click)="toggleRow(row.ticker); $event.stopPropagation()"
                        [matTooltip]="expandedRows.has(row.ticker) ? 'Cerrar detalle' : 'Ver decisión completa'">
                  <mat-icon>{{ expandedRows.has(row.ticker) ? 'expand_less' : 'expand_more' }}</mat-icon>
                </button>
              </td>
            </ng-container>

            <ng-container matColumnDef="expandedDetail">
              <td mat-cell *matCellDef="let row" [attr.colspan]="displayedColumns.length">
                @if (expandedRows.has(row.ticker)) {
                  <div class="exp-panel">

                    <div class="exp-grid">

                      <!-- Decision chain -->
                      <div class="exp-card">
                        <h4><mat-icon>account_tree</mat-icon> Cadena de decisión bayesiana — {{ row.ticker }}</h4>

                        <div class="dec-chain">
                          <div class="dec-node {{ sentimentClass(row.evidence.sentiment) }}">
                            <span class="dec-cap">Sentimiento</span>
                            <span class="dec-val">{{ row.evidence.sentiment | uppercase }}</span>
                            <span class="dec-sub">FinBERT sobre titulares</span>
                          </div>
                          <span class="dec-plus">+</span>
                          <div class="dec-node {{ rsiClass(row.evidence.rsi) }}">
                            <span class="dec-cap">RSI 14</span>
                            <span class="dec-val">{{ row.evidence.rsi | uppercase }}</span>
                            <span class="dec-sub">&lt;30 oversold / &gt;70 overbought</span>
                          </div>
                          <span class="dec-plus">+</span>
                          <div class="dec-node {{ trendClass(row.evidence.trend) }}">
                            <span class="dec-cap">Tendencia</span>
                            <span class="dec-val">{{ row.evidence.trend | uppercase }}</span>
                            <span class="dec-sub">SMA20 {{ row.evidence.trend === 'uptrend' ? '>' : '<' }} SMA50</span>
                          </div>
                          <span class="dec-plus">+</span>
                          <div class="dec-node {{ volClass(row.evidence.volatility) }}">
                            <span class="dec-cap">Volatilidad</span>
                            <span class="dec-val">{{ row.evidence.volatility | uppercase }}</span>
                            <span class="dec-sub">Bollinger Width</span>
                          </div>
                          <span class="dec-arrow">→</span>
                          <div class="dec-result {{ row.signal.toLowerCase() }}">
                            <span class="dec-cap">Señal</span>
                            <span class="dec-val">{{ row.signal }}</span>
                            <span class="dec-sub">P↑ {{ (row.prob_up*100)|number:'1.1-1' }}% · P↓ {{ (row.prob_down*100)|number:'1.1-1' }}%</span>
                          </div>
                        </div>

                        <div class="explainer">
                          <mat-icon>info</mat-icon>
                          <span>
                            La señal <strong>{{ row.signal }}</strong> se genera porque
                            @if (row.signal === 'BUY') {
                              la combinación de evidencias produce P(↑) = <strong>{{ (row.prob_up*100)|number:'1.1-1' }}%</strong> &gt; umbral BUY (65%).
                            }
                            @if (row.signal === 'SELL') {
                              la combinación de evidencias produce P(↑) = <strong>{{ (row.prob_up*100)|number:'1.1-1' }}%</strong> &lt; umbral SELL (35%).
                            }
                            @if (row.signal === 'HOLD') {
                              P(↑) = <strong>{{ (row.prob_up*100)|number:'1.1-1' }}%</strong> está entre los umbrales (35–65%): señal de espera.
                            }
                          </span>
                        </div>
                      </div>

                      <!-- Diagnostics -->
                      <div class="exp-card">
                        <h4><mat-icon>insights</mat-icon> Diagnóstico de la operativa (90 días)</h4>
                        <div class="diag-grid">
                          <div class="diag-cell">
                            <span class="diag-label">Trades cerrados</span>
                            <span class="diag-value">{{ row.trades_closed }}</span>
                          </div>
                          <div class="diag-cell">
                            <span class="diag-label">Win rate</span>
                            <span class="diag-value" [class.pos]="row.win_rate>=0.5">{{ (row.win_rate*100)|number:'1.1-1' }}%</span>
                          </div>
                          <div class="diag-cell">
                            <span class="diag-label">Profit factor</span>
                            <span class="diag-value" [class.pos]="row.profit_factor>1">{{ row.profit_factor|number:'1.2-2' }}</span>
                          </div>
                          <div class="diag-cell">
                            <span class="diag-label">α vs B&amp;H</span>
                            <span class="diag-value"
                                  [class.pos]="row.alpha_vs_benchmark>0"
                                  [class.neg]="row.alpha_vs_benchmark<0">
                              {{ row.alpha_vs_benchmark>0?'+':'' }}{{ (row.alpha_vs_benchmark*100)|number:'1.2-2' }}%
                            </span>
                          </div>

                          <div class="signals-mix">
                            <div class="mix-label">Mix de señales emitidas</div>
                            <div class="mix-bars">
                              <div class="mix buy"
                                   [style.flex]="row.signals_count.BUY || 0"
                                   [matTooltip]="row.signals_count.BUY + ' señales BUY'">
                                {{ row.signals_count.BUY }}
                              </div>
                              <div class="mix sell"
                                   [style.flex]="row.signals_count.SELL || 0"
                                   [matTooltip]="row.signals_count.SELL + ' señales SELL'">
                                {{ row.signals_count.SELL }}
                              </div>
                              <div class="mix hold"
                                   [style.flex]="row.signals_count.HOLD || 0"
                                   [matTooltip]="row.signals_count.HOLD + ' señales HOLD'">
                                {{ row.signals_count.HOLD }}
                              </div>
                            </div>
                          </div>
                        </div>
                      </div>

                    </div>
                  </div>
                }
              </td>
            </ng-container>

            <tr mat-header-row *matHeaderRowDef="displayedColumns; sticky: true"></tr>
            <tr mat-row *matRowDef="let row; columns: displayedColumns;" class="data-row"
                (click)="toggleRow(row.ticker)"></tr>
            <tr mat-row *matRowDef="let row; columns: ['expandedDetail']" class="detail-row"></tr>

            <tr *matNoDataRow>
              <td [attr.colspan]="displayedColumns.length" class="no-data">
                <mat-icon>filter_alt_off</mat-icon>
                <p>Sin tickers para los filtros aplicados</p>
              </td>
            </tr>
          </table>
        </div>

      }
    </div>
  `,
  styles: [`
    /* shared page chrome */
    .page { max-width: var(--content-max); margin: 0 auto; }
    .page-head {
      display: flex; justify-content: space-between; align-items: flex-start;
      gap: 24px; flex-wrap: wrap; margin-bottom: 22px;
    }
    .page-eyebrow {
      display: inline-flex; align-items: center; gap: 6px;
      padding: 4px 10px;
      background: rgba(124, 58, 237, .1);
      color: var(--accent-violet);
      border-radius: var(--r-pill);
      font-size: 11px; font-weight: 600;
      letter-spacing: .04em; text-transform: uppercase;
      margin-bottom: 10px;
      mat-icon { font-size: 14px; height: 14px; width: 14px; }
    }
    .page-title { font-size: 26px; font-weight: 700; color: var(--slate-900); letter-spacing: -.02em; }
    .page-sub { color: var(--slate-500); font-size: 13px; margin-top: 6px; max-width: 760px; }
    .page-actions { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
    .date-input, mat-form-field { min-width: 170px; }

    .card {
      background: var(--bg-elevated);
      border: 1px solid var(--border);
      border-radius: var(--r-md);
      box-shadow: var(--shadow-sm);
    }
    .card-title {
      display: flex; align-items: center; gap: 8px;
      font-size: 14px; font-weight: 600; color: var(--slate-900);
      mat-icon { font-size: 18px; height: 18px; width: 18px; color: var(--brand-600); }
    }
    .loader {
      display: flex; flex-direction: column; align-items: center;
      gap: 14px; padding: 80px 16px; color: var(--slate-500);
    }

    /* ─── Bayesian network legend ─── */
    .bn {
      background: var(--bg-elevated);
      border: 1px solid var(--border);
      border-radius: var(--r-md);
      box-shadow: var(--shadow-sm);
      padding: 18px;
      margin-bottom: 18px;
    }
    .bn-head {
      display: flex; justify-content: space-between; align-items: center;
      gap: 12px; flex-wrap: wrap; margin-bottom: 14px;
    }
    .bn-thresholds { display: flex; gap: 6px; }
    .th {
      font-size: 11px; font-weight: 700;
      padding: 3px 10px; border-radius: var(--r-pill); letter-spacing: .03em;
    }
    .th-buy  { background: var(--success-100); color: var(--success-700); }
    .th-hold { background: var(--warn-100);    color: var(--warn-700); }
    .th-sell { background: var(--danger-100);  color: var(--danger-700); }

    .bn-flow {
      display: flex; align-items: stretch; gap: 8px;
      flex-wrap: wrap;
      padding: 8px;
      background: var(--slate-50);
      border-radius: var(--r-md);
    }
    .bn-node {
      display: flex; align-items: center; gap: 10px;
      padding: 12px 14px;
      border-radius: var(--r-sm);
      background: var(--bg-elevated);
      border: 1px solid var(--border);
      flex: 1 1 160px; min-width: 160px;
      mat-icon { font-size: 20px; height: 20px; width: 20px; }
    }
    .bn-label { font-size: 12px; font-weight: 700; color: var(--slate-900); }
    .bn-node small { font-size: 10px; color: var(--slate-500); }
    .bn-sentiment mat-icon { color: var(--brand-600); }
    .bn-rsi       mat-icon { color: var(--accent-violet); }
    .bn-trend     mat-icon { color: var(--success-600); }
    .bn-vol       mat-icon { color: var(--warn-600); }
    .bn-target {
      background: linear-gradient(135deg, var(--brand-700), var(--brand-600));
      border-color: transparent;
      color: #fff;
      mat-icon { color: #fff; }
      .bn-label { color: #fff; }
      small     { color: rgba(255,255,255,.7); }
    }
    .bn-arrow {
      display: flex; align-items: center; justify-content: center;
      color: var(--slate-400);
      font-size: 18px; font-weight: 700;
      padding: 0 4px;
    }
    .bn-arrow-result {
      color: var(--brand-600);
      font-size: 24px;
    }

    /* ─── Summary chips ─── */
    .summary-chips {
      display: flex; gap: 8px; flex-wrap: wrap;
      margin-bottom: 14px;
    }
    .schip {
      display: inline-flex; align-items: center; gap: 6px;
      padding: 6px 14px;
      border-radius: var(--r-pill);
      font-size: 12px; font-weight: 700;
      background: var(--slate-100); color: var(--slate-700);
      mat-icon { font-size: 14px; height: 14px; width: 14px; }
    }
    .schip.buy  { background: var(--success-100); color: var(--success-700); }
    .schip.sell { background: var(--danger-100);  color: var(--danger-700); }
    .schip.hold { background: var(--warn-100);    color: var(--warn-700); }
    .schip.avg  { background: var(--brand-100);   color: var(--brand-700); }

    /* ─── Table ─── */
    .table-card { overflow: hidden; }
    .aurora-table { width: 100%; }
    .data-row { cursor: pointer; transition: background .15s; }
    .data-row:hover { background: var(--slate-50); }
    .detail-row { height: 0; }
    .no-data {
      text-align: center; padding: 40px;
      color: var(--slate-400);
      mat-icon { font-size: 36px; height: 36px; width: 36px; opacity: .5; }
      p { margin-top: 6px; }
    }

    /* ticker cell */
    .ticker-cell { display: flex; align-items: center; gap: 10px; }
    .ticker-mark {
      width: 28px; height: 28px;
      border-radius: 8px;
      background: linear-gradient(135deg, var(--brand-600), var(--accent-cyan));
      color: #fff;
      display: inline-flex; align-items: center; justify-content: center;
      font-size: 12px; font-weight: 700;
    }
    .ticker-name { font-size: 14px; font-weight: 700; color: var(--slate-900); letter-spacing: -.01em; }

    .signal-pill {
      display: inline-flex; align-items: center; gap: 4px;
      padding: 3px 10px; border-radius: var(--r-pill);
      font-size: 11px; font-weight: 700; letter-spacing: .03em;
      mat-icon { font-size: 13px; height: 13px; width: 13px; }
    }
    .signal-pill.buy  { background: var(--success-100); color: var(--success-700); }
    .signal-pill.sell { background: rgba(124, 58, 237, .15); color: #7C3AED; }
    .signal-pill.hold { background: var(--warn-100);    color: var(--warn-700); }

    /* prob gauge */
    .prob-cell { display: flex; align-items: center; gap: 10px; min-width: 130px; }
    .prob-gauge {
      flex: 1; height: 6px;
      background: var(--slate-100);
      border-radius: var(--r-pill); overflow: hidden;
    }
    .gauge-fill {
      height: 100%;
      background: var(--slate-400);
      border-radius: var(--r-pill);
      transition: width .5s;
      &.high { background: linear-gradient(to right, var(--success-500), var(--success-600)); }
      &.low  { background: linear-gradient(to right, var(--danger-500),  var(--danger-600)); }
    }
    .prob-pct {
      font-size: 13px; font-weight: 700; min-width: 50px; text-align: right;
      font-variant-numeric: tabular-nums;
      color: var(--slate-700);
      &.green { color: var(--success-700); }
      &.red   { color: var(--danger-700); }
    }
    .muted-num { font-size: 13px; color: var(--slate-500); font-variant-numeric: tabular-nums; }

    /* evidence chips */
    .ev-row { display: flex; gap: 6px; flex-wrap: wrap; }
    .ev-chip {
      display: inline-flex; align-items: center; gap: 3px;
      padding: 3px 8px;
      border-radius: var(--r-pill);
      font-size: 11px; font-weight: 600;
      mat-icon { font-size: 13px; height: 13px; width: 13px; }
    }
    /* sentiment */
    .ev-bullish  { background: var(--success-100); color: var(--success-700); }
    .ev-bearish  { background: var(--danger-100);  color: var(--danger-700); }
    .ev-neutral  { background: var(--slate-100);   color: var(--slate-600); }
    /* rsi */
    .ev-oversold   { background: var(--success-100); color: var(--success-700); }
    .ev-overbought { background: var(--danger-100);  color: var(--danger-700); }
    .ev-neutral-rsi{ background: var(--slate-100);   color: var(--slate-600); }
    /* trend */
    .ev-uptrend   { background: var(--success-100); color: var(--success-700); }
    .ev-downtrend { background: var(--danger-100);  color: var(--danger-700); }
    /* volatility */
    .ev-low-vol  { background: var(--brand-100); color: var(--brand-700); }
    .ev-high-vol { background: var(--warn-100);  color: var(--warn-700); }

    .trade-stats { display: flex; gap: 4px; flex-wrap: wrap; }
    .ts-chip {
      display: inline-flex; align-items: center; gap: 4px;
      padding: 2px 8px; border-radius: var(--r-pill);
      background: var(--slate-100); color: var(--slate-600);
      font-size: 12px; font-weight: 600;
      mat-icon { font-size: 13px; height: 13px; width: 13px; }
      &.wr.good { background: var(--success-100); color: var(--success-700); }
    }

    .ret-val {
      font-size: 13px; font-weight: 700;
      font-variant-numeric: tabular-nums;
      color: var(--slate-700);
      &.pos { color: var(--success-700); }
      &.neg { color: var(--danger-700); }
    }

    /* ─── Expanded detail ─── */
    .exp-panel {
      background: var(--slate-50);
      padding: 16px;
      border-top: 1px solid var(--border);
    }
    .exp-grid {
      display: grid;
      grid-template-columns: 2fr 1fr;
      gap: 14px;
    }
    @media (max-width: 1100px) { .exp-grid { grid-template-columns: 1fr; } }

    .exp-card {
      background: var(--bg-elevated);
      border: 1px solid var(--border);
      border-radius: var(--r-md);
      padding: 16px;
      h4 {
        display: flex; align-items: center; gap: 6px;
        font-size: 13px; font-weight: 600; color: var(--slate-900);
        margin-bottom: 14px;
        mat-icon { font-size: 16px; height: 16px; width: 16px; color: var(--brand-600); }
      }
    }

    .dec-chain {
      display: flex; align-items: center; flex-wrap: wrap;
      gap: 8px; margin-bottom: 14px;
    }
    .dec-node {
      display: flex; flex-direction: column; gap: 2px;
      padding: 10px 14px;
      border-radius: var(--r-sm);
      border: 1px solid var(--border);
      min-width: 130px; flex: 1 1 130px;
    }
    .dec-cap {
      font-size: 10px; font-weight: 700; letter-spacing: .04em;
      text-transform: uppercase; opacity: .65;
    }
    .dec-val {
      font-size: 13px; font-weight: 700; letter-spacing: -.01em;
    }
    .dec-sub { font-size: 10px; opacity: .65; }
    .dec-plus, .dec-arrow {
      color: var(--slate-400);
      font-weight: 700; font-size: 16px;
      padding: 0 4px;
    }
    .dec-arrow { color: var(--brand-600); font-size: 22px; }

    .dec-node.ev-bullish, .dec-node.ev-uptrend, .dec-node.ev-oversold, .dec-node.ev-low-vol {
      background: var(--success-50); color: var(--success-700); border-color: var(--success-100);
    }
    .dec-node.ev-bearish, .dec-node.ev-downtrend, .dec-node.ev-overbought, .dec-node.ev-high-vol {
      background: var(--danger-50); color: var(--danger-700); border-color: var(--danger-100);
    }
    .dec-node.ev-neutral, .dec-node.ev-neutral-rsi {
      background: var(--slate-50); color: var(--slate-600); border-color: var(--border);
    }

    .dec-result {
      display: flex; flex-direction: column; gap: 2px;
      padding: 10px 14px;
      min-width: 150px; flex: 1 1 150px;
      border-radius: var(--r-sm);
      border: 2px solid;
      .dec-cap { opacity: .7; }
    }
    .dec-result.buy  { background: var(--success-50); color: var(--success-700); border-color: var(--success-500); }
    .dec-result.sell { background: var(--danger-50);  color: var(--danger-700);  border-color: var(--danger-500); }
    .dec-result.hold { background: var(--warn-50);    color: var(--warn-700);    border-color: var(--warn-500); }

    .explainer {
      display: flex; align-items: flex-start; gap: 8px;
      background: var(--brand-100);
      color: var(--brand-700);
      padding: 10px 14px;
      border-radius: var(--r-sm);
      font-size: 13px;
      mat-icon { font-size: 18px; height: 18px; width: 18px; flex-shrink: 0; margin-top: 1px; }
    }

    .diag-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }
    .diag-cell {
      background: var(--slate-50);
      border-radius: var(--r-sm);
      padding: 10px 12px;
      display: flex; flex-direction: column; gap: 2px;
    }
    .diag-label {
      font-size: 10px; font-weight: 600; letter-spacing: .04em;
      text-transform: uppercase; color: var(--slate-400);
    }
    .diag-value {
      font-size: 16px; font-weight: 700; color: var(--slate-900);
      font-variant-numeric: tabular-nums;
      &.pos { color: var(--success-700); }
      &.neg { color: var(--danger-700); }
    }
    .signals-mix {
      grid-column: 1 / -1;
      background: var(--slate-50);
      border-radius: var(--r-sm);
      padding: 10px 12px;
    }
    .mix-label {
      font-size: 10px; font-weight: 600; letter-spacing: .04em;
      text-transform: uppercase; color: var(--slate-400); margin-bottom: 6px;
    }
    .mix-bars {
      display: flex; gap: 3px;
      height: 22px; border-radius: var(--r-pill);
      overflow: hidden;
    }
    .mix {
      display: flex; align-items: center; justify-content: center;
      color: #fff; font-size: 11px; font-weight: 700;
    }
    .mix.buy  { background: var(--success-500); }
    .mix.sell { background: var(--danger-500); }
    .mix.hold { background: var(--warn-500); }
  `],
})
export class SignalsComponent implements OnInit {
  private reportSvc = inject(ReportService);
  private traceSvc  = inject(TraceService);

  @ViewChild(MatSort) sort!: MatSort;

  loading = true;
  availableDates: ReportDateEntry[] = [];
  selectedDate = '';
  filterSignal = '';
  expandedRows = new Set<string>();

  displayedColumns = ['signal', 'ticker', 'prob_up', 'prob_down', 'evidence', 'trades', 'return', 'alpha', 'expand'];
  dataSource = new MatTableDataSource<TickerView>();

  buyCount  = 0;
  sellCount = 0;
  holdCount = 0;
  avgProbUp = 0;

  // ─── Trace lazy loading ───────────────────────────────────────────
  /** Cache de trazas por ticker. Se carga al expandir la fila. */
  tickerTraceCache = new Map<string, TickerTrace | null>();
  tickerTraceLoading = new Set<string>();
  hasTraceForDate = false;

  ngOnInit() {
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        this.availableDates = dates;
        if (!dates.length) { this.loading = false; return []; }
        this.selectedDate = dates[0].date;
        this.hasTraceForDate = !!(dates[0] as any).has_trace;
        return this.reportSvc.loadReport(this.selectedDate);
      })
    ).subscribe({
      next: (r: any) => { if (r) this.processReport(r); this.loading = false; },
      error: () => { this.loading = false; },
    });
  }

  onDateChange(date: string) {
    this.loading = true;
    this.tickerTraceCache.clear();
    this.expandedRows.clear();
    const entry = this.availableDates.find(d => d.date === date);
    this.hasTraceForDate = !!(entry as any)?.has_trace;
    this.reportSvc.loadReport(date).subscribe({
      next: r => { this.processReport(r); this.loading = false; },
      error: () => { this.loading = false; },
    });
  }

  private processReport(report: DailyReport) {
    const views = this.reportSvc.buildTickerViews(report);
    this.dataSource.data = views;
    this.dataSource.sort = this.sort;
    this.dataSource.filterPredicate = (row, filter) => !filter || row.signal === filter;

    this.buyCount  = views.filter(v => v.signal === 'BUY').length;
    this.sellCount = views.filter(v => v.signal === 'SELL').length;
    this.holdCount = views.filter(v => v.signal === 'HOLD').length;
    this.avgProbUp = views.length ? (views.reduce((s, v) => s + v.prob_up, 0) / views.length) * 100 : 0;
  }

  applyFilter() { this.dataSource.filter = this.filterSignal; }

  toggleRow(ticker: string) {
    if (this.expandedRows.has(ticker)) {
      this.expandedRows.delete(ticker);
    } else {
      this.expandedRows.add(ticker);
      this.loadTickerTrace(ticker);
    }
  }

  // ─── Carga lazy del trace por ticker ─────────────────────────────
  loadTickerTrace(ticker: string) {
    if (this.tickerTraceCache.has(ticker) || this.tickerTraceLoading.has(ticker)) return;
    this.tickerTraceLoading.add(ticker);
    this.traceSvc.getTickerTrace(this.selectedDate, ticker).pipe(
      catchError(() => of(null))
    ).subscribe(resp => {
      this.tickerTraceLoading.delete(ticker);
      this.tickerTraceCache.set(ticker, resp?.trace ?? null);
    });
  }

  getTickerTrace(ticker: string): TickerTrace | null {
    return this.tickerTraceCache.get(ticker) ?? null;
  }

  isTraceLoading(ticker: string): boolean {
    return this.tickerTraceLoading.has(ticker);
  }

  /** Distribución de sentimientos ordenada para visualización */
  getSentimentDist(ticker: string): { key: string; count: number; pct: number }[] {
    const t = this.getTickerTrace(ticker);
    if (!t?.sentiment_detail?.distribution) return [];
    return Object.entries(t.sentiment_detail.distribution).map(([key, v]: [string, any]) => ({
      key, count: v.count, pct: v.pct,
    }));
  }

  signalIcon(s: string) {
    return ({ BUY: 'arrow_upward', SELL: 'arrow_downward', HOLD: 'remove' } as Record<string, string>)[s] ?? 'remove';
  }
  sentimentIcon(s: SentimentState) {
    return ({ bullish: 'sentiment_very_satisfied', bearish: 'sentiment_very_dissatisfied', neutral: 'sentiment_neutral' })[s];
  }

  sentimentClass(v: SentimentState)  { return `ev-${v}`; }
  rsiClass(v: RsiState)              {
    return v === 'oversold' ? 'ev-oversold' : v === 'overbought' ? 'ev-overbought' : 'ev-neutral-rsi';
  }
  trendClass(v: TrendState)          { return `ev-${v}`; }
  volClass(v: VolatilityState)       { return v === 'low' ? 'ev-low-vol' : 'ev-high-vol'; }
}
