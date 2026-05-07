import { Component, OnInit, inject } from '@angular/core';
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
import { MatChipsModule } from '@angular/material/chips';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatDividerModule } from '@angular/material/divider';
import { NgxChartsModule } from '@swimlane/ngx-charts';
import { ViewChild } from '@angular/core';
import { ReportService } from '../../core/services/report.service';
import {
  DailyReport, TickerView, SignalExplanation, ReportDateEntry,
  BayesianEvidence, SentimentState, RsiState, TrendState, VolatilityState,
} from '../../core/models/report.model';
import { switchMap } from 'rxjs';

@Component({
  selector: 'app-signals',
  standalone: true,
  imports: [
    CommonModule, FormsModule, MatTableModule, MatSortModule, MatFormFieldModule,
    MatSelectModule, MatInputModule, MatButtonModule, MatIconModule,
    MatProgressSpinnerModule, MatTooltipModule, MatChipsModule,
    MatExpansionModule, MatDividerModule, NgxChartsModule,
  ],
  template: `
    <div class="signals-page">

      <!-- Header -->
      <div class="page-header">
        <div>
          <h2 class="page-title"><mat-icon>psychology</mat-icon> Señales &amp; Decisiones Bayesianas</h2>
          <p class="page-subtitle">Toma de decisiones explicable · Red bayesiana con 4 nodos de evidencia</p>
        </div>
        <div class="header-controls">
          <mat-form-field appearance="outline" class="date-sel">
            <mat-label>Fecha</mat-label>
            <mat-select [(ngModel)]="selectedDate" (ngModelChange)="onDateChange($event)">
              @for (d of availableDates; track d.date) {
                <mat-option [value]="d.date">{{ d.date }}</mat-option>
              }
            </mat-select>
          </mat-form-field>
          <mat-form-field appearance="outline">
            <mat-label>Señal</mat-label>
            <mat-select [(ngModel)]="filterSignal" (ngModelChange)="applyFilter()">
              <mat-option value="">Todas</mat-option>
              <mat-option value="BUY">BUY</mat-option>
              <mat-option value="SELL">SELL</mat-option>
              <mat-option value="HOLD">HOLD</mat-option>
            </mat-select>
          </mat-form-field>
        </div>
      </div>

      <!-- Bayesian network legend -->
      <div class="bn-legend">
        <div class="bn-title"><mat-icon>account_tree</mat-icon> Modelo bayesiano: 4 nodos de evidencia → MarketDirection</div>
        <div class="bn-nodes">
          <div class="bn-node sentiment">
            <mat-icon>sentiment_satisfied</mat-icon>
            <span>Sentimiento</span>
            <small>FinBERT</small>
          </div>
          <mat-icon class="arrow">arrow_forward</mat-icon>
          <div class="bn-node rsi">
            <mat-icon>show_chart</mat-icon>
            <span>RSI 14</span>
            <small>&lt;30 / 30-70 / &gt;70</small>
          </div>
          <mat-icon class="arrow">arrow_forward</mat-icon>
          <div class="bn-node trend">
            <mat-icon>trending_up</mat-icon>
            <span>Tendencia</span>
            <small>SMA20 vs SMA50</small>
          </div>
          <mat-icon class="arrow">arrow_forward</mat-icon>
          <div class="bn-node volatility">
            <mat-icon>swap_vert</mat-icon>
            <span>Volatilidad</span>
            <small>Bollinger Width</small>
          </div>
          <mat-icon class="arrow big">double_arrow</mat-icon>
          <div class="bn-node target">
            <mat-icon>query_stats</mat-icon>
            <span>Market Direction</span>
            <small>P(up) / P(down)</small>
          </div>
        </div>
        <div class="bn-thresholds">
          Umbrales: <strong>BUY</strong> si P(up) &gt; 65% · <strong>SELL</strong> si P(up) &lt; 35% · <strong>HOLD</strong> en otro caso
        </div>
      </div>

      @if (loading) {
        <div class="loading-center">
          <mat-spinner diameter="48"></mat-spinner>
          <p>Cargando señales...</p>
        </div>
      } @else {

        <!-- Table -->
        <div class="table-wrapper">
          <table mat-table [dataSource]="dataSource" matSort>

            <!-- Signal -->
            <ng-container matColumnDef="signal">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>Señal</th>
              <td mat-cell *matCellDef="let row">
                <span class="signal-badge {{ row.signal.toLowerCase() }}">
                  <mat-icon class="sig-icon">{{ signalIcon(row.signal) }}</mat-icon>
                  {{ row.signal }}
                </span>
              </td>
            </ng-container>

            <!-- Ticker -->
            <ng-container matColumnDef="ticker">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>Ticker</th>
              <td mat-cell *matCellDef="let row">
                <span class="ticker-name">{{ row.ticker }}</span>
              </td>
            </ng-container>

            <!-- Prob up gauge -->
            <ng-container matColumnDef="prob_up">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>P(subida)</th>
              <td mat-cell *matCellDef="let row">
                <div class="prob-cell">
                  <div class="prob-gauge">
                    <div class="gauge-fill" [style.width.%]="row.prob_up*100"
                         [class.high]="row.prob_up > 0.65"
                         [class.low]="row.prob_up < 0.35"></div>
                  </div>
                  <span class="prob-pct" [class.green]="row.prob_up>0.65" [class.red]="row.prob_up<0.35">
                    {{ (row.prob_up*100)|number:'1.1-1' }}%
                  </span>
                </div>
              </td>
            </ng-container>

            <!-- Prob down -->
            <ng-container matColumnDef="prob_down">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>P(bajada)</th>
              <td mat-cell *matCellDef="let row">
                <span class="prob-down-val">{{ (row.prob_down*100)|number:'1.1-1' }}%</span>
              </td>
            </ng-container>

            <!-- Evidence chips (Bayesian) -->
            <ng-container matColumnDef="evidence">
              <th mat-header-cell *matHeaderCellDef>Evidencia bayesiana</th>
              <td mat-cell *matCellDef="let row">
                <div class="evidence-row">
                  <span class="ev-chip {{ sentimentClass(row.evidence.sentiment) }}"
                        [matTooltip]="'Sentimiento FinBERT: ' + row.evidence.sentiment">
                    <mat-icon>sentiment_satisfied</mat-icon>
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
                        [matTooltip]="'Volatilidad Bollinger: ' + row.evidence.volatility">
                    <mat-icon>swap_vert</mat-icon>
                    {{ row.evidence.volatility }}
                  </span>
                </div>
              </td>
            </ng-container>

            <!-- Trade stats -->
            <ng-container matColumnDef="trades">
              <th mat-header-cell *matHeaderCellDef>Trades (90d)</th>
              <td mat-cell *matCellDef="let row">
                <div class="trade-stats">
                  <span class="ts-item" matTooltip="Trades cerrados">🔄 {{ row.trades_closed }}</span>
                  <span class="ts-item" matTooltip="Win rate">
                    🎯 {{ (row.win_rate*100)|number:'1.0-0' }}%
                  </span>
                </div>
              </td>
            </ng-container>

            <!-- Return -->
            <ng-container matColumnDef="return">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>Retorno</th>
              <td mat-cell *matCellDef="let row">
                <span [class.pos]="row.cumulative_return>0" [class.neg]="row.cumulative_return<0" class="ret-val">
                  {{ row.cumulative_return>0?'+':'' }}{{ (row.cumulative_return*100)|number:'1.2-2' }}%
                </span>
              </td>
            </ng-container>

            <!-- Alpha -->
            <ng-container matColumnDef="alpha">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>Alpha</th>
              <td mat-cell *matCellDef="let row">
                <span [class.pos]="row.alpha_vs_benchmark>0" [class.neg]="row.alpha_vs_benchmark<0" class="ret-val">
                  {{ row.alpha_vs_benchmark>0?'+':'' }}{{ (row.alpha_vs_benchmark*100)|number:'1.2-2' }}%
                </span>
              </td>
            </ng-container>

            <!-- Expand button -->
            <ng-container matColumnDef="expand">
              <th mat-header-cell *matHeaderCellDef></th>
              <td mat-cell *matCellDef="let row">
                <button mat-icon-button (click)="toggleRow(row.ticker); $event.stopPropagation()">
                  <mat-icon>{{ expandedRows.has(row.ticker) ? 'expand_less' : 'expand_more' }}</mat-icon>
                </button>
              </td>
            </ng-container>

            <!-- Expanded detail row -->
            <ng-container matColumnDef="expandedDetail">
              <td mat-cell *matCellDef="let row" [attr.colspan]="displayedColumns.length">
                @if (expandedRows.has(row.ticker)) {
                  <div class="expanded-panel">
                    <div class="exp-grid">

                      <!-- Bayesian decision path -->
                      <div class="exp-card decision-card">
                        <h4><mat-icon>account_tree</mat-icon> Cadena de decisión bayesiana</h4>
                        <div class="decision-path">
                          <div class="dp-node" [class]="sentimentClass(row.evidence.sentiment)">
                            <div class="dp-label">Sentimiento</div>
                            <div class="dp-value">{{ row.evidence.sentiment | uppercase }}</div>
                            <div class="dp-sub">FinBERT sobre titulares</div>
                          </div>
                          <mat-icon class="dp-arrow">add</mat-icon>
                          <div class="dp-node" [class]="rsiClass(row.evidence.rsi)">
                            <div class="dp-label">RSI 14</div>
                            <div class="dp-value">{{ row.evidence.rsi | uppercase }}</div>
                            <div class="dp-sub">&lt;30 sobrevendido / &gt;70 sobrecomprado</div>
                          </div>
                          <mat-icon class="dp-arrow">add</mat-icon>
                          <div class="dp-node" [class]="trendClass(row.evidence.trend)">
                            <div class="dp-label">Tendencia</div>
                            <div class="dp-value">{{ row.evidence.trend | uppercase }}</div>
                            <div class="dp-sub">SMA20 {{ row.evidence.trend === 'uptrend' ? '>' : '<' }} SMA50</div>
                          </div>
                          <mat-icon class="dp-arrow">add</mat-icon>
                          <div class="dp-node" [class]="volClass(row.evidence.volatility)">
                            <div class="dp-label">Volatilidad</div>
                            <div class="dp-value">{{ row.evidence.volatility | uppercase }}</div>
                            <div class="dp-sub">Ancho bandas Bollinger</div>
                          </div>
                          <mat-icon class="dp-arrow result-arrow">double_arrow</mat-icon>
                          <div class="dp-node result {{ row.signal.toLowerCase() }}">
                            <div class="dp-label">Señal</div>
                            <div class="dp-value">{{ row.signal }}</div>
                            <div class="dp-sub">P↑={{ (row.prob_up*100)|number:'1.1-1' }}% P↓={{ (row.prob_down*100)|number:'1.1-1' }}%</div>
                          </div>
                        </div>

                        <!-- CPT explanation -->
                        <div class="cpt-note">
                          <mat-icon>info_outline</mat-icon>
                          <span>
                            La señal <strong>{{ row.signal }}</strong> se genera porque
                            @if (row.signal === 'BUY') {
                              la combinación de evidencias produce P(subida) = {{ (row.prob_up*100)|number:'1.1-1' }}% &gt; umbral BUY (65%).
                            }
                            @if (row.signal === 'SELL') {
                              la combinación de evidencias produce P(subida) = {{ (row.prob_up*100)|number:'1.1-1' }}% &lt; umbral SELL (35%).
                            }
                            @if (row.signal === 'HOLD') {
                              P(subida) = {{ (row.prob_up*100)|number:'1.1-1' }}% está entre umbrales (35%–65%), señal de espera.
                            }
                          </span>
                        </div>
                      </div>

                      <!-- Trade stats detail -->
                      <div class="exp-card">
                        <h4><mat-icon>bar_chart</mat-icon> Diagnóstico de operativas (90d)</h4>
                        <div class="stat-grid">
                          <div class="stat-item">
                            <span class="sl">Trades cerrados</span>
                            <span class="sv">{{ row.trades_closed }}</span>
                          </div>
                          <div class="stat-item">
                            <span class="sl">Win rate</span>
                            <span class="sv" [class.green]="row.win_rate>0.5">{{ (row.win_rate*100)|number:'1.1-1' }}%</span>
                          </div>
                          <div class="stat-item">
                            <span class="sl">Profit factor</span>
                            <span class="sv" [class.green]="row.profit_factor>1">{{ row.profit_factor|number:'1.2-2' }}</span>
                          </div>
                          <div class="stat-item">
                            <span class="sl">Ret. medio/trade</span>
                            <span class="sv">—</span>
                          </div>
                          <div class="stat-item">
                            <span class="sl">Señales BUY</span>
                            <span class="sv buy-txt">{{ row.signals_count.BUY }}</span>
                          </div>
                          <div class="stat-item">
                            <span class="sl">Señales SELL</span>
                            <span class="sv sell-txt">{{ row.signals_count.SELL }}</span>
                          </div>
                          <div class="stat-item">
                            <span class="sl">Señales HOLD</span>
                            <span class="sv hold-txt">{{ row.signals_count.HOLD }}</span>
                          </div>
                          <div class="stat-item">
                            <span class="sl">Alpha vs B&H</span>
                            <span class="sv" [class.green]="row.alpha_vs_benchmark>0" [class.red]="row.alpha_vs_benchmark<0">
                              {{ row.alpha_vs_benchmark>0?'+':'' }}{{ (row.alpha_vs_benchmark*100)|number:'1.2-2' }}%
                            </span>
                          </div>
                        </div>
                      </div>

                    </div>
                  </div>
                }
              </td>
            </ng-container>

            <tr mat-header-row *matHeaderRowDef="displayedColumns; sticky: true"></tr>
            <tr mat-row *matRowDef="let row; columns: displayedColumns;"
                class="data-row" (click)="toggleRow(row.ticker)"></tr>
            <tr mat-row *matRowDef="let row; columns: ['expandedDetail']" class="detail-row"></tr>

            <tr *matNoDataRow>
              <td [attr.colspan]="displayedColumns.length" class="no-data">Sin datos para los filtros seleccionados</td>
            </tr>
          </table>
        </div>

      }
    </div>
  `,
  styles: [`
    .signals-page { max-width: 1400px; margin: 0 auto; }
    .page-header {
      display: flex; justify-content: space-between; align-items: flex-start;
      margin-bottom: 20px; flex-wrap: wrap; gap: 12px;
    }
    .page-title {
      display: flex; align-items: center; gap: 8px;
      font-size: 22px; font-weight: 700; color: #1a237e; margin: 0;
    }
    .page-subtitle { color: #666; font-size: 13px; margin-top: 4px; }
    .header-controls { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    .date-sel { min-width: 150px; }

    /* BN legend */
    .bn-legend {
      background: #fff; border-radius: 12px; padding: 16px 20px;
      box-shadow: 0 2px 8px rgba(0,0,0,.07); margin-bottom: 20px;
    }
    .bn-title {
      display: flex; align-items: center; gap: 8px;
      font-size: 14px; font-weight: 600; color: #1a237e; margin-bottom: 12px;
      mat-icon { font-size: 18px; }
    }
    .bn-nodes {
      display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
    }
    .bn-node {
      display: flex; flex-direction: column; align-items: center; gap: 2px;
      padding: 8px 14px; border-radius: 8px; min-width: 100px; text-align: center;
      mat-icon { font-size: 20px; margin-bottom: 2px; }
      span { font-size: 13px; font-weight: 600; }
      small { font-size: 10px; opacity: .7; }
      &.sentiment { background: #e3f2fd; color: #1565c0; }
      &.rsi { background: #f3e5f5; color: #6a1b9a; }
      &.trend { background: #e8f5e9; color: #1b5e20; }
      &.volatility { background: #fff8e1; color: #f57f17; }
      &.target { background: #1a237e; color: #fff; }
    }
    .arrow { color: #bbb; font-size: 20px; }
    .big { font-size: 28px; color: #1a237e; }
    .bn-thresholds {
      margin-top: 10px; font-size: 12px; color: #666;
      padding: 6px 12px; background: #f5f5f5; border-radius: 6px; display: inline-block;
    }

    .loading-center {
      display: flex; flex-direction: column; align-items: center;
      gap: 16px; padding: 80px; color: #666;
    }

    /* Table */
    .table-wrapper {
      border-radius: 12px; overflow: hidden;
      box-shadow: 0 2px 8px rgba(0,0,0,.08);
    }
    table { width: 100%; }
    .data-row { cursor: pointer; transition: background .15s; }
    .data-row:hover { background: #f5f7ff; }
    .detail-row { height: 0; }

    .ticker-name { font-size: 16px; font-weight: 700; color: #1a237e; }

    .signal-badge {
      display: inline-flex; align-items: center; gap: 4px;
      padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: 700;
      .sig-icon { font-size: 14px; height: 14px; width: 14px; }
      &.buy  { background: #e8f5e9; color: #2e7d32; }
      &.sell { background: #ffebee; color: #c62828; }
      &.hold { background: #fff8e1; color: #f57f17; }
    }

    .prob-cell { display: flex; align-items: center; gap: 8px; min-width: 120px; }
    .prob-gauge {
      flex: 1; height: 8px; background: #eee; border-radius: 4px; overflow: hidden;
    }
    .gauge-fill {
      height: 100%; background: #78909c; border-radius: 4px; transition: width .5s;
      &.high { background: #43a047; }
      &.low  { background: #e53935; }
    }
    .prob-pct { font-size: 13px; font-weight: 600; min-width: 40px; text-align: right;
      &.green { color: #2e7d32; } &.red { color: #c62828; } }
    .prob-down-val { font-size: 13px; color: #888; }

    .evidence-row { display: flex; gap: 4px; flex-wrap: wrap; }
    .ev-chip {
      display: inline-flex; align-items: center; gap: 3px;
      padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 500; cursor: default;
      mat-icon { font-size: 13px; height: 13px; width: 13px; }
    }
    /* sentiment */
    .ev-bullish  { background: #e8f5e9; color: #2e7d32; }
    .ev-bearish  { background: #ffebee; color: #c62828; }
    .ev-neutral  { background: #f5f5f5; color: #616161; }
    /* rsi */
    .ev-oversold   { background: #e8f5e9; color: #2e7d32; }
    .ev-overbought { background: #ffebee; color: #c62828; }
    .ev-neutral-rsi{ background: #f5f5f5; color: #616161; }
    /* trend */
    .ev-uptrend   { background: #e8f5e9; color: #2e7d32; }
    .ev-downtrend { background: #ffebee; color: #c62828; }
    /* volatility */
    .ev-low-vol  { background: #e3f2fd; color: #1565c0; }
    .ev-high-vol { background: #fff8e1; color: #f57f17; }

    .trade-stats { display: flex; gap: 8px; }
    .ts-item { font-size: 12px; color: #555; }

    .ret-val { font-size: 14px; font-weight: 600; color: #333;
      &.pos { color: #2e7d32; } &.neg { color: #c62828; } }

    /* Expanded panel */
    .expanded-panel {
      background: #f9faff; padding: 16px;
      border-top: 1px solid #e8eaf6;
    }
    .exp-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
    }
    @media (max-width: 900px) { .exp-grid { grid-template-columns: 1fr; } }
    .exp-card {
      background: #fff; border-radius: 10px; padding: 16px;
      box-shadow: 0 1px 4px rgba(0,0,0,.06);
      h4 {
        display: flex; align-items: center; gap: 6px;
        font-size: 14px; font-weight: 600; color: #1a237e;
        margin: 0 0 14px;
        mat-icon { font-size: 16px; }
      }
    }
    .decision-card { grid-column: 1 / -1; }

    /* Decision path */
    .decision-path {
      display: flex; align-items: center; gap: 8px;
      flex-wrap: wrap; margin-bottom: 14px;
    }
    .dp-node {
      display: flex; flex-direction: column; align-items: center; gap: 2px;
      padding: 10px 14px; border-radius: 10px; min-width: 100px;
      text-align: center; border: 2px solid transparent;
      .dp-label { font-size: 10px; text-transform: uppercase; letter-spacing: .5px; opacity: .7; }
      .dp-value { font-size: 15px; font-weight: 700; }
      .dp-sub   { font-size: 10px; opacity: .65; }

      &.ev-bullish, &.ev-uptrend, &.ev-oversold, &.ev-low-vol
                   { background: #e8f5e9; color: #2e7d32; border-color: #a5d6a7; }
      &.ev-bearish, &.ev-downtrend, &.ev-overbought, &.ev-high-vol
                   { background: #ffebee; color: #c62828; border-color: #ef9a9a; }
      &.ev-neutral  { background: #f5f5f5; color: #616161; border-color: #e0e0e0; }
      &.result.buy  { background: #e8f5e9; color: #2e7d32; border-color: #2e7d32; }
      &.result.sell { background: #ffebee; color: #c62828; border-color: #c62828; }
      &.result.hold { background: #fff8e1; color: #f57f17; border-color: #f57f17; }
    }
    .dp-arrow { color: #bbb; }
    .result-arrow { color: #1a237e; font-size: 28px; }

    .cpt-note {
      display: flex; align-items: flex-start; gap: 8px;
      background: #e8eaf6; border-radius: 8px; padding: 10px 14px;
      font-size: 13px; color: #283593;
      mat-icon { font-size: 18px; flex-shrink: 0; margin-top: 1px; }
    }

    /* Stat grid */
    .stat-grid {
      display: grid; grid-template-columns: 1fr 1fr; gap: 8px;
    }
    .stat-item {
      display: flex; flex-direction: column; gap: 2px;
      background: #f9f9fb; border-radius: 6px; padding: 8px 10px;
    }
    .sl { font-size: 11px; color: #999; }
    .sv { font-size: 16px; font-weight: 600; color: #333;
      &.green { color: #2e7d32; } &.red { color: #c62828; } }
    .buy-txt  { color: #2e7d32; }
    .sell-txt { color: #c62828; }
    .hold-txt { color: #f57f17; }

    .no-data { text-align: center; padding: 32px; color: #999; }
  `],
})
export class SignalsComponent implements OnInit {
  private reportSvc = inject(ReportService);

  @ViewChild(MatSort) sort!: MatSort;

  loading = true;
  availableDates: ReportDateEntry[] = [];
  selectedDate = '';
  filterSignal = '';
  expandedRows = new Set<string>();

  displayedColumns = ['signal', 'ticker', 'prob_up', 'prob_down', 'evidence', 'trades', 'return', 'alpha', 'expand'];
  dataSource = new MatTableDataSource<TickerView>();

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
    const views = this.reportSvc.buildTickerViews(report);
    this.dataSource.data = views;
    this.dataSource.sort = this.sort;
    this.dataSource.filterPredicate = (row, filter) =>
      !filter || row.signal === filter;
  }

  applyFilter() {
    this.dataSource.filter = this.filterSignal;
  }

  toggleRow(ticker: string) {
    if (this.expandedRows.has(ticker)) this.expandedRows.delete(ticker);
    else this.expandedRows.add(ticker);
  }

  signalIcon(s: string) {
    return { BUY: 'arrow_upward', SELL: 'arrow_downward', HOLD: 'pause' }[s] ?? 'help';
  }

  sentimentClass(v: SentimentState)  { return `ev-${v}`; }
  rsiClass(v: RsiState) {
    return v === 'oversold' ? 'ev-oversold' : v === 'overbought' ? 'ev-overbought' : 'ev-neutral';
  }
  trendClass(v: TrendState)     { return `ev-${v}`; }
  volClass(v: VolatilityState)  { return v === 'low' ? 'ev-low-vol' : 'ev-high-vol'; }
}
