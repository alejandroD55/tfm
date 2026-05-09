import { Component, OnInit, AfterViewInit, ViewChild, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatTableModule, MatTableDataSource } from '@angular/material/table';
import { MatSortModule, MatSort } from '@angular/material/sort';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatExpansionModule } from '@angular/material/expansion';
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
    CommonModule, FormsModule,
    MatTableModule, MatSortModule,
    MatButtonModule, MatIconModule,
    MatProgressSpinnerModule, MatTooltipModule, MatExpansionModule
  ],
  template: `
    <div class="page">

      <header class="page-head">
        <div class="page-head-text">
          <div class="page-eyebrow">
            <mat-icon>psychology</mat-icon>
            <span>Decisiones de Inteligencia Artificial</span>
          </div>
          <h1 class="page-title">Señales & Explicabilidad</h1>
          <p class="page-sub">
            Descubre el razonamiento exacto de la IA detrás de cada decisión.
          </p>
        </div>
        
        <div class="page-actions">
          <div class="filter-group">
            <label>Fecha del Informe</label>
            <select class="aurora-select" [(ngModel)]="selectedDate" (change)="onDateChange(selectedDate)">
              @for (d of availableDates; track d.date) {
                <option [value]="d.date">{{ d.date }}</option>
              }
            </select>
          </div>
          
          <div class="filter-group">
            <label>Filtrar por Señal</label>
            <select class="aurora-select" [(ngModel)]="filterSignal" (change)="applyFilter()">
              <option value="">Todas las señales</option>
              <option value="BUY">COMPRAR</option>
              <option value="SELL">LIQUIDEZ (CASH)</option>
              <option value="HOLD">MANTENER</option>
            </select>
          </div>
        </div>
      </header>

      <mat-accordion class="glossary-accordion">
        <mat-expansion-panel class="glossary-panel">
          <mat-expansion-panel-header>
            <mat-panel-title>
              <mat-icon>lightbulb</mat-icon>
              <span>¿Cómo toma las decisiones la Inteligencia Artificial? (Glosario)</span>
            </mat-panel-title>
          </mat-expansion-panel-header>
          <div class="glossary-content">
            <p style="grid-column: 1 / -1; margin-bottom: 8px; color: var(--slate-600);">
              Nuestra Red Bayesiana evalúa 4 "nodos" o evidencias del mercado para calcular la probabilidad de que un activo suba:
            </p>
            <div class="g-col">
              <strong>1. Sentimiento (Noticias):</strong> El algoritmo FinBERT lee titulares financieros reales y clasifica si el tono del día es Positivo, Negativo o Neutral.
            </div>
            <div class="g-col">
              <strong>2. RSI 14 (Fuerza):</strong> Mide la velocidad del precio. Un RSI alto suele indicar "Sobrecompra" (peligro de caída o fuerte impulso), y bajo "Sobreventa" (oportunidad).
            </div>
            <div class="g-col">
              <strong>3. Tendencia (Medias):</strong> Compara el precio a corto plazo frente al largo plazo. Si la media corta supera a la larga, estamos en "Tendencia Alcista".
            </div>
            <div class="g-col">
              <strong>4. Volatilidad (Bandas):</strong> Analiza la anchura de las Bandas de Bollinger. Una alta volatilidad indica mucha incertidumbre y nerviosismo en el mercado.
            </div>
          </div>
        </mat-expansion-panel>
      </mat-accordion>

      <section class="bn">
        <div class="bn-head">
          <div class="card-title">
            <mat-icon>account_tree</mat-icon>
            <span>Flujo de Inferencia Bayesiana</span>
          </div>
          <div class="bn-thresholds">
            <span class="th th-buy">COMPRAR · P(↑) ≥ 65%</span>
            <span class="th th-hold">MANTENER · 35–65%</span>
            <span class="th th-sell">CASH · P(↑) ≤ 35%</span>
          </div>
        </div>
        <div class="bn-flow">
          <div class="bn-node bn-sentiment">
            <mat-icon>sentiment_satisfied</mat-icon>
            <div>
              <div class="bn-label">Sentimiento</div>
              <small>NLP Titulares</small>
            </div>
          </div>
          <span class="bn-arrow">+</span>
          <div class="bn-node bn-rsi">
            <mat-icon>show_chart</mat-icon>
            <div>
              <div class="bn-label">Fuerza (RSI)</div>
              <small>Impulso del precio</small>
            </div>
          </div>
          <span class="bn-arrow">+</span>
          <div class="bn-node bn-trend">
            <mat-icon>trending_up</mat-icon>
            <div>
              <div class="bn-label">Tendencia</div>
              <small>Medias Móviles</small>
            </div>
          </div>
          <span class="bn-arrow">+</span>
          <div class="bn-node bn-vol">
            <mat-icon>swap_vert</mat-icon>
            <div>
              <div class="bn-label">Volatilidad</div>
              <small>Incertidumbre</small>
            </div>
          </div>
          <span class="bn-arrow bn-arrow-result">→</span>
          <div class="bn-node bn-target">
            <mat-icon>query_stats</mat-icon>
            <div>
              <div class="bn-label">Probabilidad P(↑)</div>
              <small>Decisión Final</small>
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

        <div class="summary-chips">
          <span class="schip buy"  ><mat-icon>arrow_upward</mat-icon> {{ buyCount }} COMPRAR</span>
          <span class="schip sell" ><mat-icon>arrow_downward</mat-icon> {{ sellCount }} CASH (LIQUIDEZ)</span>
          <span class="schip hold" ><mat-icon>remove</mat-icon> {{ holdCount }} MANTENER</span>
          <span class="schip avg"  ><mat-icon>insights</mat-icon> Media Prob. Alcista: {{ avgProbUp | number:'1.1-1' }}%</span>
        </div>

        <div class="card table-card">
          <table mat-table [dataSource]="dataSource" matSort class="aurora-table">

            <ng-container matColumnDef="signal">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>Señal</th>
              <td mat-cell *matCellDef="let row">
                <span class="signal-pill {{ row.signal.toLowerCase() }}">
                  <mat-icon>{{ signalIcon(row.signal) }}</mat-icon>
                  {{ row.signal === 'BUY' ? 'COMPRAR' : row.signal === 'SELL' ? 'CASH' : 'MANTENER' }}
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="ticker">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>Activo</th>
              <td mat-cell *matCellDef="let row">
                <div class="ticker-cell">
                  <span class="ticker-name">{{ row.ticker }}</span>
                </div>
              </td>
            </ng-container>

            <ng-container matColumnDef="prob_up">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>P(Subida)</th>
              <td mat-cell *matCellDef="let row">
                <div class="prob-cell">
                  <div class="prob-gauge">
                    <div class="gauge-fill"
                         [style.width.%]="row.prob_up*100"
                         [class.high]="row.prob_up > 0.65"
                         [class.low]="row.prob_up < 0.35"></div>
                  </div>
                  <span class="prob-pct" [class.green]="row.prob_up>=0.65" [class.purple]="row.prob_up<=0.35" [class.yellow]="row.prob_up>0.35 && row.prob_up<0.65">
                    {{ (row.prob_up*100)|number:'1.1-1' }}%
                  </span>
                </div>
              </td>
            </ng-container>

            <ng-container matColumnDef="evidence">
              <th mat-header-cell *matHeaderCellDef>Evidencia (Nodos)</th>
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
              <th mat-header-cell *matHeaderCellDef mat-sort-header="trades_closed">Operaciones</th>
              <td mat-cell *matCellDef="let row">
                <div class="trade-stats">
                  <span class="ts-chip" matTooltip="Operaciones cerradas (Último Año)">
                    <mat-icon>swap_horiz</mat-icon>{{ row.trades_closed }}
                  </span>
                  <span class="ts-chip wr"
                        [class.good]="row.win_rate>=0.5"
                        matTooltip="Porcentaje de acierto">
                    <mat-icon>track_changes</mat-icon>{{ (row.win_rate*100)|number:'1.0-0' }}%
                  </span>
                </div>
              </td>
            </ng-container>

            <ng-container matColumnDef="return">
              <th mat-header-cell *matHeaderCellDef mat-sort-header="cumulative_return">Retorno IA</th>
              <td mat-cell *matCellDef="let row">
                <span class="ret-val" [class.pos]="row.cumulative_return>0" [class.neg]="row.cumulative_return<0">
                  {{ row.cumulative_return>0?'+':'' }}{{ (row.cumulative_return*100)|number:'1.2-2' }}%
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="alpha">
              <th mat-header-cell *matHeaderCellDef mat-sort-header="alpha_vs_benchmark">Ventaja (α)</th>
              <td mat-cell *matCellDef="let row">
                <span class="ret-val" [class.pos]="row.alpha_vs_benchmark>0" [class.neg]="row.alpha_vs_benchmark<0" matTooltip="Diferencia frente a Buy & Hold">
                  {{ row.alpha_vs_benchmark>0?'+':'' }}{{ (row.alpha_vs_benchmark*100)|number:'1.2-2' }}%
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="expand">
              <th mat-header-cell *matHeaderCellDef></th>
              <td mat-cell *matCellDef="let row">
                <button mat-icon-button (click)="toggleRow(row.ticker); $event.stopPropagation()"
                        [matTooltip]="expandedRows.has(row.ticker) ? 'Cerrar detalle' : 'Ver justificación de la IA'">
                  <mat-icon>{{ expandedRows.has(row.ticker) ? 'expand_less' : 'expand_more' }}</mat-icon>
                </button>
              </td>
            </ng-container>

            <ng-container matColumnDef="expandedDetail">
              <td mat-cell *matCellDef="let row" [attr.colspan]="displayedColumns.length" class="detail-cell">
                @if (expandedRows.has(row.ticker)) {
                  <div class="exp-panel">
                    <div class="exp-grid">

                      <div class="exp-card">
                        <h4><mat-icon>account_tree</mat-icon> Justificación de la IA para {{ row.ticker }}</h4>

                        <div class="dec-chain">
                          <div class="dec-node {{ sentimentClass(row.evidence.sentiment) }}">
                            <span class="dec-cap">Sentimiento</span>
                            <span class="dec-val">{{ row.evidence.sentiment | uppercase }}</span>
                          </div>
                          <span class="dec-plus">+</span>
                          <div class="dec-node {{ rsiClass(row.evidence.rsi) }}">
                            <span class="dec-cap">Fuerza RSI</span>
                            <span class="dec-val">{{ row.evidence.rsi | uppercase }}</span>
                          </div>
                          <span class="dec-plus">+</span>
                          <div class="dec-node {{ trendClass(row.evidence.trend) }}">
                            <span class="dec-cap">Tendencia</span>
                            <span class="dec-val">{{ row.evidence.trend | uppercase }}</span>
                          </div>
                          <span class="dec-plus">+</span>
                          <div class="dec-node {{ volClass(row.evidence.volatility) }}">
                            <span class="dec-cap">Volatilidad</span>
                            <span class="dec-val">{{ row.evidence.volatility | uppercase }}</span>
                          </div>
                          <span class="dec-arrow">→</span>
                          <div class="dec-result {{ row.signal.toLowerCase() }}">
                            <span class="dec-cap">Decisión</span>
                            <span class="dec-val">{{ row.signal === 'BUY' ? 'COMPRAR' : row.signal === 'SELL' ? 'CASH' : 'MANTENER' }}</span>
                            <span class="dec-sub">Confianza Alcista: {{ (row.prob_up*100)|number:'1.1-1' }}%</span>
                          </div>
                        </div>

                        <div class="explainer">
                          <mat-icon>info</mat-icon>
                          <span>
                            <strong>Razonamiento Bayesiano:</strong> Se emite orden de 
                            {{ row.signal === 'BUY' ? 'COMPRAR' : row.signal === 'SELL' ? 'PASAR A LIQUIDEZ' : 'MANTENER' }} 
                            porque la combinación actual de indicadores y noticias produce una probabilidad alcista del 
                            <strong>{{ (row.prob_up*100)|number:'1.1-1' }}%</strong>.
                          </span>
                        </div>
                      </div>

                      <div class="exp-card">
                        <h4><mat-icon>article</mat-icon> Análisis de Titulares de Hoy (FinBERT)</h4>
                        
                        @if (isTraceLoading(row.ticker)) {
                          <div class="trace-loading">
                            <mat-spinner diameter="30"></mat-spinner>
                            <span>Descargando titulares desde AWS...</span>
                          </div>
                        } @else if (getSentimentDist(row.ticker).length > 0) {
                          <div class="sentiment-dist">
                            @for (entry of getSentimentDist(row.ticker); track entry.key) {
                              <div class="sd-row">
                                <span class="sd-label ev-chip ev-{{ entry.key }}">{{ entry.key | uppercase }}</span>
                                <div class="sd-bar-wrap">
                                  <div class="sd-bar {{ entry.key }}" [style.width.%]="entry.pct"></div>
                                </div>
                                <span class="sd-num">{{ entry.count }} ({{ entry.pct }}%)</span>
                              </div>
                            }
                          </div>
                          
                          <div class="headlines-list">
                            <p class="hl-title">Titulares más relevantes leídos por la IA:</p>
                            @for (h of getTickerTrace(row.ticker)?.sentiment_detail?.headlines_sample?.slice(0, 3); track $index) {
                              <div class="hl-row">
                                <span class="ev-chip ev-{{ h.sentiment }} small">{{ h.sentiment | uppercase }}</span>
                                <span class="hl-text">{{ h.headline }}</span>
                                <span class="hl-conf">{{ (h.confidence * 100) | number:'1.0-0' }}% cert.</span>
                              </div>
                            }
                          </div>
                        } @else {
                          <p class="muted">No hay titulares relevantes para este activo hoy.</p>
                        }
                      </div>

                    </div>
                  </div>
                }
              </td>
            </ng-container>

            <tr mat-header-row *matHeaderRowDef="displayedColumns; sticky: true"></tr>
            <tr mat-row *matRowDef="let row; columns: displayedColumns;" class="data-row"
                (click)="toggleRow(row.ticker)"></tr>
            <tr mat-row *matRowDef="let row; columns: ['expandedDetail']" class="detail-row" [class.expanded]="expandedRows.has(row.ticker)"></tr>

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
    .page { max-width: var(--content-max); margin: 0 auto; padding-bottom: 40px;}
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
    
    /* Filtros Personalizados nativos */
    .page-actions { display: flex; gap: 16px; flex-wrap: wrap; align-items: center; }
    .filter-group { display: flex; flex-direction: column; gap: 4px; }
    .filter-group label { font-size: 11px; font-weight: 600; color: var(--slate-500); text-transform: uppercase; letter-spacing: 0.05em; }
    .aurora-select {
      appearance: none;
      background-color: var(--bg-elevated);
      border: 1px solid var(--border);
      border-radius: var(--r-sm);
      padding: 10px 32px 10px 14px;
      font-family: var(--font-sans);
      font-size: 14px; font-weight: 600;
      color: var(--slate-700);
      cursor: pointer;
      min-width: 180px;
      background-image: url('data:image/svg+xml;charset=US-ASCII,<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="%2364748B" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"></polyline></svg>');
      background-repeat: no-repeat;
      background-position: right 8px center;
      background-size: 16px;
      transition: all 0.2s ease;
    }
    .aurora-select:hover { border-color: var(--brand-400); }
    .aurora-select:focus { outline: none; border-color: var(--brand-600); box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1); }

    /* Glosario */
    .glossary-accordion { display: block; margin-bottom: 24px; }
    .glossary-panel { background: rgba(124, 58, 237, 0.05) !important; border: 1px solid rgba(124, 58, 237, 0.2) !important; border-radius: 8px !important; box-shadow: none !important; }
    .glossary-panel mat-panel-title { color: var(--accent-violet); font-size: 13px; font-weight: 600; display: flex; align-items: center; gap: 8px; }
    .glossary-panel mat-icon { font-size: 18px; height: 18px; width: 18px; color: var(--accent-violet); }
    .glossary-content { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; padding-top: 10px; font-size: 12.5px; color: var(--slate-700); line-height: 1.5; }

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
    .th-sell { background: rgba(124, 58, 237, .15); color: #7C3AED; }

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
      flex: 1 1 140px; min-width: 140px;
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
    .schip.sell { background: rgba(124, 58, 237, .15); color: #7C3AED; }
    .schip.hold { background: var(--warn-100);    color: var(--warn-700); }
    .schip.avg  { background: var(--brand-100);   color: var(--brand-700); }

    /* ─── Table ─── */
    .table-card { overflow: hidden; }
    .aurora-table { width: 100%; }
    .data-row { cursor: pointer; transition: background .15s; }
    .data-row:hover { background: var(--slate-50); }
    
    /* Configuración para fila colapsable */
    .detail-row { height: 0; }
    .detail-row.expanded { height: auto; }
    .detail-cell { padding: 0 !important; border-bottom: none !important; }

    .no-data {
      text-align: center; padding: 40px;
      color: var(--slate-400);
      mat-icon { font-size: 36px; height: 36px; width: 36px; opacity: .5; }
      p { margin-top: 6px; }
    }

    /* ticker cell */
    .ticker-cell { display: flex; align-items: center; gap: 10px; }
    .ticker-name { font-size: 15px; font-weight: 700; color: var(--slate-900); letter-spacing: -.01em; }

    .signal-pill {
      display: inline-flex; align-items: center; gap: 4px;
      padding: 4px 12px; border-radius: var(--r-pill);
      font-size: 11px; font-weight: 700; letter-spacing: .03em;
      mat-icon { font-size: 14px; height: 14px; width: 14px; }
    }
    .signal-pill.buy  { background: var(--success-100); color: var(--success-700); }
    .signal-pill.sell { background: rgba(124, 58, 237, .15); color: #7C3AED; } /* Violeta */
    .signal-pill.hold { background: var(--warn-100);    color: var(--warn-700); }

    /* prob gauge */
    .prob-cell { display: flex; align-items: center; gap: 10px; min-width: 140px; }
    .prob-gauge {
      flex: 1; height: 6px;
      background: var(--slate-100);
      border-radius: var(--r-pill); overflow: hidden;
    }
    .gauge-fill {
      height: 100%;
      background: var(--warn-400); /* Amarillo por defecto (HOLD) */
      border-radius: var(--r-pill);
      transition: width .5s;
      &.high { background: linear-gradient(to right, var(--success-500), var(--success-600)); } /* Verde (BUY) */
      &.low  { background: linear-gradient(to right, var(--accent-violet), #5b21b6); } /* Violeta (SELL) */
    }
    .prob-pct {
      font-size: 13px; font-weight: 700; min-width: 50px; text-align: right;
      font-variant-numeric: tabular-nums;
      color: var(--warn-600);
      &.green { color: var(--success-700); }
      &.purple { color: #7C3AED; }
    }
    .muted-num { font-size: 13px; color: var(--slate-500); font-variant-numeric: tabular-nums; }

    /* evidence chips */
    .ev-row { display: flex; gap: 6px; flex-wrap: wrap; }
    .ev-chip {
      display: inline-flex; align-items: center; gap: 3px;
      padding: 3px 8px;
      border-radius: var(--r-pill);
      font-size: 11px; font-weight: 600; text-transform: capitalize;
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
      border-bottom: 2px solid var(--border);
    }
    .exp-grid {
      display: grid;
      grid-template-columns: 3fr 2fr; /* Ajustado para dar más espacio al análisis de noticias */
      gap: 16px;
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
      gap: 8px; margin-bottom: 16px;
    }
    .dec-node {
      display: flex; flex-direction: column; gap: 2px;
      padding: 10px 14px;
      border-radius: var(--r-sm);
      border: 1px solid var(--border);
      flex: 1 1 110px;
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
      padding: 0 2px;
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
      min-width: 140px; flex: 1 1 140px;
      border-radius: var(--r-sm);
      border: 2px solid;
      .dec-cap { opacity: .7; }
    }
    .dec-result.buy  { background: var(--success-50); color: var(--success-700); border-color: var(--success-500); }
    .dec-result.sell { background: rgba(124, 58, 237, .05);  color: #7C3AED;  border-color: #7C3AED; }
    .dec-result.hold { background: var(--warn-50);    color: var(--warn-700);    border-color: var(--warn-500); }

    .explainer {
      display: flex; align-items: flex-start; gap: 8px;
      background: var(--brand-100);
      color: var(--brand-700);
      padding: 10px 14px;
      border-radius: var(--r-sm);
      font-size: 13px; line-height: 1.5;
      mat-icon { font-size: 18px; height: 18px; width: 18px; flex-shrink: 0; margin-top: 1px; }
    }

    /* Trace Loading & Headlines */
    .trace-loading {
      display: flex; align-items: center; gap: 12px; padding: 20px;
      color: var(--slate-500); font-size: 13px; font-weight: 600;
    }
    .sentiment-dist { display: flex; flex-direction: column; gap: 6px; margin-bottom: 16px; }
    .sd-row { display: flex; align-items: center; gap: 8px; font-size: 12px; }
    .sd-label { min-width: 70px; text-align: center; }
    .sd-bar-wrap { flex: 1; height: 6px; background: var(--slate-100); border-radius: 3px; overflow: hidden; }
    .sd-bar { height: 100%; border-radius: 3px; }
    .sd-bar.bullish { background: var(--success-500); }
    .sd-bar.bearish { background: var(--danger-500); }
    .sd-bar.neutral { background: var(--slate-400); }
    .sd-num { min-width: 60px; text-align: right; color: var(--slate-500); font-weight: 600; font-variant-numeric: tabular-nums; }

    .hl-title { font-size: 12px; font-weight: 700; color: var(--slate-700); margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.03em;}
    .headlines-list { display: flex; flex-direction: column; gap: 8px; }
    .hl-row { display: flex; align-items: flex-start; gap: 8px; font-size: 12px; padding: 6px; background: var(--slate-50); border-radius: 6px; border: 1px solid var(--border); }
    .hl-text { flex: 1; color: var(--slate-800); line-height: 1.4; font-style: italic; }
    .hl-conf { color: var(--brand-600); font-weight: 700; white-space: nowrap; }
    .ev-chip.small { font-size: 9px; padding: 2px 6px; }
  `],
})
export class SignalsComponent implements OnInit, AfterViewInit {
  private reportSvc = inject(ReportService);
  private traceSvc  = inject(TraceService);

  @ViewChild(MatSort) sort!: MatSort;

  loading = true;
  availableDates: ReportDateEntry[] = [];
  selectedDate = '';
  filterSignal = '';
  expandedRows = new Set<string>();

  // Eliminamos prob_down e incorporamos nombres limpios en español
  displayedColumns = ['signal', 'ticker', 'prob_up', 'evidence', 'trades', 'return', 'alpha', 'expand'];
  dataSource = new MatTableDataSource<TickerView>();

  buyCount  = 0;
  sellCount = 0;
  holdCount = 0;
  avgProbUp = 0;

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

  // SOLUCIÓN AL BUG DE LAS FLECHAS DE ORDENACIÓN
  ngAfterViewInit() {
    this.dataSource.sort = this.sort;
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
    
    // Volvemos a asignar el sort por si los datos cambian
    if (this.sort) {
      this.dataSource.sort = this.sort;
    }

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