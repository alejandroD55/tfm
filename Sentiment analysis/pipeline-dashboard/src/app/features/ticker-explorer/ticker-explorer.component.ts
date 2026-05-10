import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatChipsModule } from '@angular/material/chips';
import { MatExpansionModule } from '@angular/material/expansion';
import { NgxChartsModule } from '@swimlane/ngx-charts';
import { catchError, interval, of, switchMap, takeWhile } from 'rxjs';
import { ApiService } from '../../core/services/api.service';
import { TraceService } from '../../core/services/trace.service';
import { ReportService } from '../../core/services/report.service';
import { ReportDateEntry } from '../../core/models/report.model';
import { TickerTrace } from '../../core/models/trace.model';

interface OhlcvRow {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}
interface Article {
  headline: string;
  url?: string;
  datetime?: number;
  source?: string;
  sentiment?: string;
  confidence?: number;
}

@Component({
  selector: 'app-ticker-explorer',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatIconModule,
    MatButtonModule,
    MatProgressSpinnerModule,
    MatTooltipModule,
    MatChipsModule,
    MatExpansionModule,
    NgxChartsModule,
  ],
  template: `
    <div class="page">
      <!-- ─── Header ─── -->
      <header class="page-head">
        <div class="page-head-text">
          <div class="page-eyebrow">
            <mat-icon>search</mat-icon><span>Ticker Explorer</span>
          </div>
          <h1 class="page-title">Explorador de datos por ticker</h1>
          <p class="page-sub">
            Visualiza las noticias ingestadas, los datos OHLCV y la cadena
            completa de decisión para cualquier ETF del universo. Lanza el
            pipeline para un ticker concreto.
          </p>
        </div>
      </header>

      <!-- ─── Buscador ─── -->
      <section class="search-section">
        <div class="search-row">
          <!-- Ticker input con autocompletado -->
          <div class="search-box">
            <mat-icon class="search-icon">search</mat-icon>
            <input
              class="search-input"
              [(ngModel)]="tickerInput"
              (ngModelChange)="onTickerInput($event)"
              (keyup.enter)="loadTicker()"
              placeholder="Ej: SPY, QQQ, IWM..."
              list="ticker-list"
            />
            <datalist id="ticker-list">
              @for (t of filteredTickers; track t) {
                <option [value]="t"></option>
              }
            </datalist>
          </div>

          <!-- Fecha -->
          <div class="date-box">
            <mat-icon>event</mat-icon>
            <select class="date-select" [(ngModel)]="selectedDate">
              @for (d of availableDates; track d.date) {
                <option [value]="d.date">{{ d.date }}</option>
              }
            </select>
          </div>

          <!-- Buscar -->
          <button
            class="btn btn-primary"
            (click)="loadTicker()"
            [disabled]="!tickerInput.trim()"
          >
            <mat-icon>manage_search</mat-icon>
            <span>Explorar</span>
          </button>

          <!-- Pipeline trigger -->
          <button
            class="btn btn-accent"
            (click)="triggerPipeline()"
            [disabled]="!tickerInput.trim() || pipelineRunning"
            [matTooltip]="'Ejecutar el pipeline completo solo para este ticker'"
          >
            @if (pipelineRunning) {
              <mat-spinner diameter="16"></mat-spinner>
            } @else {
              <mat-icon>play_circle</mat-icon>
            }
            <span>{{
              pipelineRunning ? 'Ejecutando...' : 'Lanzar pipeline'
            }}</span>
          </button>
        </div>

        <!-- Chips de tickers rápidos -->
        @if (availableTickers.length) {
          <div class="quick-chips">
            <span class="chips-label">Acceso rápido:</span>
            @for (t of availableTickers.slice(0, 12); track t) {
              <button
                class="ticker-chip"
                [class.active]="currentTicker === t"
                (click)="selectTicker(t)"
              >
                {{ t }}
              </button>
            }
          </div>
        }
      </section>

      <!-- ─── Pipeline status ─── -->
      @if (pipelineExecution) {
        <div
          class="pipeline-status"
          [class]="'ps-' + pipelineExecution.status.toLowerCase()"
        >
          <mat-icon>{{
            pipelineStatusIcon(pipelineExecution.status)
          }}</mat-icon>
          <div>
            <strong>Pipeline {{ pipelineExecution.status }}</strong>
            @if (pipelineExecution.status === 'RUNNING') {
              <span>
                — ejecutando para <strong>{{ currentTicker }}</strong
                >...</span
              >
            } @else if (pipelineExecution.status === 'SUCCEEDED') {
              <span>
                — completado. Refresca la página para ver los nuevos
                datos.</span
              >
            } @else if (pipelineExecution.status === 'FAILED') {
              <span> — error en la ejecución. Revisa CloudWatch Logs.</span>
            }
          </div>
          @if (pipelineExecution.status === 'RUNNING') {
            <mat-spinner diameter="16"></mat-spinner>
          }
        </div>
      }

      <!-- ─── Contenido del ticker ─── -->
      @if (loading) {
        <div class="loader">
          <mat-spinner diameter="40"></mat-spinner>
          <p>Cargando datos de {{ tickerInput }}...</p>
        </div>
      } @else if (currentTicker) {
        <!-- Banner del ticker -->
        <section class="ticker-banner">
          <div class="tb-left">
            <div class="tb-symbol">{{ currentTicker }}</div>
            <div class="tb-date">{{ selectedDate }}</div>
          </div>
          @if (tickerTrace) {
            <div
              class="tb-signal {{ tickerTrace.inference.signal.toLowerCase() }}"
            >
              {{ tickerTrace.inference.signal }}
            </div>
            <div class="tb-prob">
              <div class="tbp-label">P(subida)</div>
              <div class="tbp-val">
                {{ tickerTrace.inference.prob_up * 100 | number: '1.1-1' }}%
              </div>
              <div class="tbp-bar">
                <div
                  class="tbp-fill"
                  [style.width.%]="tickerTrace.inference.prob_up * 100"
                  [class.buy]="tickerTrace.inference.prob_up > 0.65"
                  [class.sell]="tickerTrace.inference.prob_up < 0.35"
                ></div>
              </div>
            </div>
            <div class="tb-states">
              @for (s of getTraceStates(); track s.label) {
                <div class="tbs-item">
                  <span class="tbs-label">{{ s.label }}</span>
                  <span class="ev-chip {{ s.cls }}">{{ s.value }}</span>
                </div>
              }
            </div>
          } @else {
            <div class="no-trace-note">
              <mat-icon>info_outline</mat-icon>
              Sin traza bayesiana para esta fecha. Ejecuta el pipeline primero.
            </div>
          }
        </section>

        <div class="explorer-grid">
          <!-- ─── OHLCV Chart ─── -->
          <section class="card chart-section">
            <div class="card-head">
              <div class="card-title">
                <mat-icon>candlestick_chart</mat-icon
                ><span>Precio de cierre — {{ currentTicker }}</span>
              </div>
              <span class="card-sub"
                >{{ ohlcvData.length }} sesiones · últimos 90 días</span
              >
            </div>
            @if (ohlcvData.length) {
              <div class="chart-host">
                <ngx-charts-line-chart
                  [results]="ohlcvChartData"
                  [xAxis]="true"
                  [yAxis]="true"
                  [showGridLines]="true"
                  [scheme]="lineScheme"
                  [view]="chartView"
                  [autoScale]="true"
                  [legend]="false"
                  yAxisLabel="Precio (USD)"
                  [showYAxisLabel]="true"
                >
                </ngx-charts-line-chart>
              </div>

              <!-- OHLCV tabla últimas sesiones -->
              <div class="ohlcv-table-wrap">
                <table class="ohlcv-table">
                  <thead>
                    <tr>
                      <th>Fecha</th>
                      <th>Open</th>
                      <th>High</th>
                      <th>Low</th>
                      <th>Close</th>
                      <th>Volumen</th>
                    </tr>
                  </thead>
                  <tbody>
                    @for (
                      row of ohlcvData.slice(-10).reverse();
                      track row.date
                    ) {
                      <tr [class.today-row]="row.date === selectedDate">
                        <td>{{ row.date }}</td>
                        <td>{{ row.open | number: '1.2-2' }}</td>
                        <td class="high">{{ row.high | number: '1.2-2' }}</td>
                        <td class="low">{{ row.low | number: '1.2-2' }}</td>
                        <td class="close">{{ row.close | number: '1.2-2' }}</td>
                        <td>{{ row.volume | number: '1.0-0' }}</td>
                      </tr>
                    }
                  </tbody>
                </table>
              </div>
            } @else {
              <div class="empty-box">
                <mat-icon>bar_chart</mat-icon>
                <p>
                  No hay datos OHLCV para {{ currentTicker }} en
                  {{ selectedDate }}
                </p>
              </div>
            }
          </section>

          <!-- ─── Noticias ─── -->
          <section class="card news-section">
            <div class="card-head">
              <div class="card-title">
                <mat-icon>newspaper</mat-icon>
                <span>Noticias Finnhub — {{ currentTicker }}</span>
              </div>
              <span class="card-sub"
                >{{ articles.length }} titulares · fuente de sentimiento
                FinBERT</span
              >
            </div>

            @if (articles.length) {
              <!-- Distribución de sentimiento -->
              @if (tickerTrace?.sentiment_detail) {
                <div class="sent-dist">
                  @for (e of getSentimentDist(); track e.key) {
                    <div class="sd-item">
                      <span class="ev-chip {{ e.key }}">{{ e.key }}</span>
                      <div class="sd-bar-wrap">
                        <div
                          class="sd-bar {{ e.key }}"
                          [style.width.%]="e.pct"
                        ></div>
                      </div>
                      <span class="sd-num">{{ e.count }} ({{ e.pct }}%)</span>
                    </div>
                  }
                  <div class="sd-note">
                    <mat-icon>info_outline</mat-icon>
                    Dominante:
                    <strong>{{
                      tickerTrace?.sentiment_detail?.dominant?.sentiment
                    }}</strong>
                    ({{
                      (tickerTrace?.sentiment_detail?.dominant?.confidence ||
                        0) * 100 | number: '1.0-0'
                    }}% confidence) → señal
                    <strong>{{ tickerTrace?.inference?.signal }}</strong>
                  </div>
                </div>
              }

              <!-- Lista de artículos -->
              <div class="articles-list">
                @for (art of articles; track $index) {
                  <div
                    class="article-card"
                    [class.has-sentiment]="art.sentiment"
                  >
                    <div class="art-head">
                      @if (art.sentiment) {
                        <span class="ev-chip {{ art.sentiment }} small">
                          {{ art.sentiment }}
                          {{ (art.confidence || 0) * 100 | number: '1.0-0' }}%
                        </span>
                      }
                      @if (art.source) {
                        <span class="art-source">{{ art.source }}</span>
                      }
                      @if (art.datetime) {
                        <span class="art-date">{{
                          art.datetime * 1000 | date: 'dd/MM HH:mm'
                        }}</span>
                      }
                    </div>
                    <div class="art-headline">{{ art.headline }}</div>
                    @if (art.url) {
                      <a
                        class="art-link"
                        [href]="art.url"
                        target="_blank"
                        rel="noopener"
                      >
                        <mat-icon>open_in_new</mat-icon> Ver artículo
                      </a>
                    }
                  </div>
                }
              </div>
            } @else {
              <div class="empty-box">
                <mat-icon>newspaper</mat-icon>
                <p>
                  No hay noticias para {{ currentTicker }} en {{ selectedDate }}
                </p>
                <p class="empty-sub">
                  Puede que la ingesta aún no se haya ejecutado para esta fecha.
                </p>
              </div>
            }
          </section>
        </div>

        <!-- ─── Decisión bayesiana completa ─── -->
        @if (tickerTrace) {
          <section class="card decision-section">
            <div class="card-head">
              <div class="card-title">
                <mat-icon>account_tree</mat-icon
                ><span>Cadena de decisión bayesiana</span>
              </div>
              <span class="card-sub"
                >De los datos ingestados a la señal final</span
              >
            </div>

            <!-- Reasoning -->
            <div class="reasoning-box">
              <mat-icon>lightbulb</mat-icon>
              <span>{{ tickerTrace.reasoning }}</span>
            </div>

            <!-- Raw → discretizado → inferencia -->
            <div class="decision-flow">
              <div class="df-stage">
                <div class="df-stage-title">
                  <mat-icon>numbers</mat-icon> 1 · Datos crudos
                </div>
                <div class="df-kv-grid">
                  <div class="dfkv">
                    <span>Precio cierre</span
                    ><strong>{{
                      tickerTrace.raw_values.close_price | number: '1.2-2'
                    }}</strong>
                  </div>
                  <div class="dfkv">
                    <span>RSI 14</span>
                    <strong
                      [class.green]="tickerTrace.raw_values.rsi_14 < 30"
                      [class.red]="tickerTrace.raw_values.rsi_14 > 70"
                    >
                      {{ tickerTrace.raw_values.rsi_14 | number: '1.2-2' }}
                    </strong>
                  </div>
                  <div class="dfkv">
                    <span>SMA 20</span
                    ><strong>{{
                      tickerTrace.raw_values.sma_20 | number: '1.2-2'
                    }}</strong>
                  </div>
                  <div class="dfkv">
                    <span>SMA 50</span
                    ><strong>{{
                      tickerTrace.raw_values.sma_50 | number: '1.2-2'
                    }}</strong>
                  </div>
                  <div class="dfkv">
                    <span>SMA spread</span>
                    <strong
                      [class.green]="
                        (tickerTrace.raw_values.sma_spread || 0) > 0
                      "
                      [class.red]="(tickerTrace.raw_values.sma_spread || 0) < 0"
                    >
                      {{ tickerTrace.raw_values.sma_spread | number: '1.2-2' }}
                    </strong>
                  </div>
                  <div class="dfkv">
                    <span>BB width</span>
                    <strong
                      [class.orange]="
                        tickerTrace.raw_values.bb_width_ratio > 0.05
                      "
                    >
                      {{
                        tickerTrace.raw_values.bb_width_ratio * 100
                          | number: '1.2-2'
                      }}%
                    </strong>
                  </div>
                </div>
              </div>

              <div class="df-arrow"><mat-icon>arrow_forward</mat-icon></div>

              <div class="df-stage">
                <div class="df-stage-title">
                  <mat-icon>compress</mat-icon> 2 · Discretización
                </div>
                <div class="df-chips">
                  <div class="dfc-row">
                    <span class="dfc-label">Sentimiento</span>
                    <span
                      class="ev-chip {{
                        tickerTrace.discretization.sentiment_state
                      }}"
                    >
                      {{ tickerTrace.discretization.sentiment_state }}
                    </span>
                    <span class="dfc-raw"
                      >(FinBERT {{ tickerTrace.discretization.sentiment_raw }}
                      {{
                        tickerTrace.discretization.sentiment_conf * 100
                          | number: '1.0-0'
                      }}%)</span
                    >
                  </div>
                  <div class="dfc-row">
                    <span class="dfc-label">RSI 14</span>
                    <span
                      class="ev-chip {{ tickerTrace.discretization.rsi_state }}"
                      >{{ tickerTrace.discretization.rsi_state }}</span
                    >
                    <span class="dfc-raw"
                      >({{
                        tickerTrace.raw_values.rsi_14 | number: '1.1-1'
                      }})</span
                    >
                  </div>
                  <div class="dfc-row">
                    <span class="dfc-label">Tendencia</span>
                    <span
                      class="ev-chip {{
                        tickerTrace.discretization.trend_state
                      }}"
                      >{{ tickerTrace.discretization.trend_state }}</span
                    >
                    <span class="dfc-raw">(SMA20 vs SMA50)</span>
                  </div>
                  <div class="dfc-row">
                    <span class="dfc-label">Volatilidad</span>
                    <span
                      class="ev-chip vol-{{
                        tickerTrace.discretization.volatility_state
                      }}"
                      >{{ tickerTrace.discretization.volatility_state }}</span
                    >
                    <span class="dfc-raw"
                      >(BB
                      {{
                        tickerTrace.raw_values.bb_width_ratio * 100
                          | number: '1.2-2'
                      }}%)</span
                    >
                  </div>
                </div>
              </div>

              <div class="df-arrow"><mat-icon>arrow_forward</mat-icon></div>

              <div class="df-stage result-stage">
                <div class="df-stage-title">
                  <mat-icon>query_stats</mat-icon> 3 · Inferencia bayesiana
                </div>
                <div
                  class="result-signal {{
                    tickerTrace.inference.signal.toLowerCase()
                  }}"
                >
                  {{ tickerTrace.inference.signal }}
                </div>
                <div class="result-probs">
                  <div>
                    P(subida) =
                    <strong
                      >{{
                        tickerTrace.inference.prob_up * 100 | number: '1.1-1'
                      }}%</strong
                    >
                  </div>
                  <div>
                    P(bajada) =
                    <strong
                      >{{
                        tickerTrace.inference.prob_down * 100 | number: '1.1-1'
                      }}%</strong
                    >
                  </div>
                </div>
                <div class="result-threshold">
                  Umbral: {{ tickerTrace.inference.threshold_used }}
                </div>
              </div>
            </div>
          </section>
        }
      } @else if (!loading) {
        <div class="welcome-state">
          <mat-icon>manage_search</mat-icon>
          <h3>Busca un ticker para explorar</h3>
          <p>
            Introduce un símbolo (SPY, QQQ, IWM...) y selecciona una fecha para
            ver las noticias ingestadas, los datos OHLCV y la decisión bayesiana
            completa.
          </p>
        </div>
      }
    </div>
  `,
  styles: [
    `
      .page {
        max-width: 1400px;
        margin: 0 auto;
      }

      /* Header */
      .page-head {
        margin-bottom: 24px;
      }
      .page-eyebrow {
        display: flex;
        align-items: center;
        gap: 6px;
        font-size: 12px;
        font-weight: 600;
        color: var(--brand-400, #60a5fa);
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-bottom: 4px;
        mat-icon {
          font-size: 14px;
        }
      }
      .page-title {
        font-size: 22px;
        font-weight: 700;
        color: var(--fg, #fff);
        margin: 0 0 4px;
      }
      .page-sub {
        font-size: 13px;
        color: var(--fg-muted, #888);
        margin: 0;
        max-width: 640px;
      }

      /* Search */
      .search-section {
        background: var(--surface, #1f2937);
        border-radius: 16px;
        padding: 20px;
        margin-bottom: 20px;
        border: 1px solid rgba(255, 255, 255, 0.06);
      }
      .search-row {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        align-items: center;
        margin-bottom: 14px;
      }
      .search-box {
        flex: 1;
        min-width: 200px;
        display: flex;
        align-items: center;
        gap: 8px;
        background: rgba(255, 255, 255, 0.06);
        border: 1px solid rgba(255, 255, 255, 0.12);
        border-radius: 10px;
        padding: 0 14px;
      }
      .search-icon {
        color: var(--fg-muted, #888);
        font-size: 20px;
      }
      .search-input {
        flex: 1;
        background: transparent;
        border: none;
        outline: none;
        color: var(--fg, #fff);
        font-size: 16px;
        font-weight: 600;
        padding: 12px 0;
        &::placeholder {
          color: var(--fg-muted, #888);
          font-weight: 400;
        }
      }
      .date-box {
        display: flex;
        align-items: center;
        gap: 8px;
        background: rgba(255, 255, 255, 0.06);
        border: 1px solid rgba(255, 255, 255, 0.12);
        border-radius: 10px;
        padding: 0 14px;
        mat-icon {
          color: var(--fg-muted, #888);
        }
      }
      .date-select {
        background: transparent;
        border: none;
        outline: none;
        color: var(--fg, #fff);
        font-size: 13px;
        padding: 12px 0;
        cursor: pointer;
        option {
          background: #1f2937;
        }
      }
      .btn {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 10px 18px;
        border-radius: 10px;
        border: none;
        cursor: pointer;
        font-size: 13px;
        font-weight: 600;
        transition: opacity 0.2s;
        &:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }
      }
      .btn-primary {
        background: var(--brand-600, #2563eb);
        color: #fff;
      }
      .btn-accent {
        background: linear-gradient(135deg, #7c3aed, #2563eb);
        color: #fff;
      }
      .btn-ghost {
        background: rgba(255, 255, 255, 0.08);
        color: var(--fg, #fff);
      }
      .quick-chips {
        display: flex;
        gap: 6px;
        flex-wrap: wrap;
        align-items: center;
      }
      .chips-label {
        font-size: 11px;
        color: var(--fg-muted, #888);
        font-weight: 600;
        margin-right: 4px;
      }
      .ticker-chip {
        background: rgba(255, 255, 255, 0.06);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 8px;
        padding: 4px 10px;
        color: var(--fg-muted, #aaa);
        font-size: 12px;
        font-weight: 600;
        cursor: pointer;
        transition: all 0.15s;
        &:hover {
          background: rgba(255, 255, 255, 0.12);
          color: #fff;
        }
        &.active {
          background: var(--brand-600, #2563eb);
          color: #fff;
          border-color: transparent;
        }
      }

      /* Pipeline status */
      .pipeline-status {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 14px 20px;
        border-radius: 12px;
        margin-bottom: 16px;
        font-size: 13px;
        &.ps-running {
          background: rgba(59, 130, 246, 0.08);
          border: 1px solid rgba(59, 130, 246, 0.2);
          color: #93c5fd;
          mat-icon {
            color: #60a5fa;
          }
        }
        &.ps-succeeded {
          background: rgba(34, 197, 94, 0.08);
          border: 1px solid rgba(34, 197, 94, 0.2);
          color: #86efac;
          mat-icon {
            color: #22c55e;
          }
        }
        &.ps-failed {
          background: rgba(239, 68, 68, 0.08);
          border: 1px solid rgba(239, 68, 68, 0.2);
          color: #fca5a5;
          mat-icon {
            color: #ef4444;
          }
        }
      }

      /* Loader & welcome */
      .loader {
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 16px;
        padding: 80px;
        color: var(--fg-muted, #888);
      }
      .welcome-state {
        text-align: center;
        padding: 80px 40px;
        color: var(--fg-muted, #888);
        mat-icon {
          font-size: 56px;
          height: 56px;
          width: 56px;
          margin-bottom: 16px;
          display: block;
          margin-inline: auto;
        }
        h3 {
          margin: 0 0 8px;
          font-size: 20px;
          color: var(--fg, #fff);
        }
      }

      /* Ticker banner */
      .ticker-banner {
        display: flex;
        align-items: center;
        gap: 20px;
        background: var(--surface, #1f2937);
        border-radius: 16px;
        padding: 20px 24px;
        margin-bottom: 16px;
        border: 1px solid rgba(255, 255, 255, 0.06);
        flex-wrap: wrap;
      }
      .tb-left {
        min-width: 80px;
      }
      .tb-symbol {
        font-size: 28px;
        font-weight: 800;
        color: var(--fg, #fff);
        letter-spacing: -0.02em;
      }
      .tb-date {
        font-size: 12px;
        color: var(--fg-muted, #888);
        margin-top: 2px;
      }
      .tb-signal {
        padding: 6px 18px;
        border-radius: 20px;
        font-size: 16px;
        font-weight: 700;
        &.buy {
          background: rgba(34, 197, 94, 0.15);
          color: #86efac;
        }
        &.sell {
          background: rgba(239, 68, 68, 0.15);
          color: #fca5a5;
        }
        &.hold {
          background: rgba(245, 158, 11, 0.15);
          color: #fde68a;
        }
      }
      .tb-prob {
        display: flex;
        flex-direction: column;
        gap: 4px;
        min-width: 120px;
      }
      .tbp-label {
        font-size: 11px;
        color: var(--fg-muted, #888);
        font-weight: 600;
      }
      .tbp-val {
        font-size: 20px;
        font-weight: 700;
        color: var(--fg, #fff);
      }
      .tbp-bar {
        height: 6px;
        background: rgba(255, 255, 255, 0.1);
        border-radius: 3px;
        overflow: hidden;
      }
      .tbp-fill {
        height: 100%;
        background: #60a5fa;
        border-radius: 3px;
        transition: width 0.5s;
        &.buy {
          background: #22c55e;
        }
        &.sell {
          background: #ef4444;
        }
      }
      .tb-states {
        display: flex;
        gap: 12px;
        flex-wrap: wrap;
      }
      .tbs-item {
        display: flex;
        flex-direction: column;
        gap: 3px;
        align-items: center;
      }
      .tbs-label {
        font-size: 10px;
        color: var(--fg-muted, #888);
        font-weight: 600;
        text-transform: uppercase;
      }
      .no-trace-note {
        display: flex;
        align-items: center;
        gap: 8px;
        font-size: 13px;
        color: var(--fg-muted, #888);
        mat-icon {
          color: #60a5fa;
        }
      }

      /* Evidence chips */
      .ev-chip {
        display: inline-flex;
        align-items: center;
        padding: 2px 8px;
        border-radius: 10px;
        font-size: 11px;
        font-weight: 600;
      }
      .bullish,
      .oversold,
      .uptrend {
        background: rgba(34, 197, 94, 0.15);
        color: #86efac;
      }
      .bearish,
      .overbought,
      .downtrend {
        background: rgba(239, 68, 68, 0.15);
        color: #fca5a5;
      }
      .neutral {
        background: rgba(255, 255, 255, 0.08);
        color: #d1d5db;
      }
      .vol-low {
        background: rgba(96, 165, 250, 0.12);
        color: #93c5fd;
      }
      .vol-high {
        background: rgba(251, 146, 60, 0.12);
        color: #fdba74;
      }
      .small {
        font-size: 10px;
        padding: 1px 6px;
      }

      /* Explorer grid */
      .explorer-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 16px;
        margin-bottom: 16px;
      }
      @media (max-width: 1100px) {
        .explorer-grid {
          grid-template-columns: 1fr;
        }
      }

      .card {
        background: var(--surface, #1f2937);
        border-radius: 12px;
        padding: 20px;
        border: 1px solid rgba(255, 255, 255, 0.06);
      }
      .card-head {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        margin-bottom: 16px;
        flex-wrap: wrap;
        gap: 6px;
      }
      .card-title {
        display: flex;
        align-items: center;
        gap: 8px;
        font-size: 15px;
        font-weight: 600;
        color: var(--fg, #fff);
        mat-icon {
          font-size: 18px;
          height: 18px;
          width: 18px;
        }
      }
      .card-sub {
        font-size: 12px;
        color: var(--fg-muted, #888);
      }

      /* OHLCV */
      .chart-host {
        margin-bottom: 16px;
      }
      .ohlcv-table-wrap {
        overflow-x: auto;
      }
      .ohlcv-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 12px;
      }
      .ohlcv-table th {
        background: rgba(255, 255, 255, 0.06);
        padding: 8px 10px;
        text-align: right;
        color: var(--fg-muted, #888);
        font-weight: 600;
        font-size: 11px;
        text-transform: uppercase;
        &:first-child {
          text-align: left;
        }
      }
      .ohlcv-table td {
        padding: 6px 10px;
        border-top: 1px solid rgba(255, 255, 255, 0.04);
        text-align: right;
        color: var(--fg, #fff);
        font-variant-numeric: tabular-nums;
        &:first-child {
          text-align: left;
          color: var(--fg-muted, #888);
        }
      }
      .ohlcv-table .high {
        color: #86efac;
      }
      .ohlcv-table .low {
        color: #fca5a5;
      }
      .ohlcv-table .close {
        font-weight: 700;
      }
      .ohlcv-table .today-row {
        background: rgba(37, 99, 235, 0.08);
      }
      .empty-box {
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 8px;
        padding: 40px;
        color: var(--fg-muted, #888);
        mat-icon {
          font-size: 36px;
        }
        .empty-sub {
          font-size: 12px;
        }
      }

      /* News */
      .sent-dist {
        display: flex;
        flex-direction: column;
        gap: 6px;
        padding: 12px;
        background: rgba(255, 255, 255, 0.03);
        border-radius: 8px;
        margin-bottom: 14px;
      }
      .sd-item {
        display: flex;
        align-items: center;
        gap: 8px;
        font-size: 12px;
      }
      .sd-bar-wrap {
        flex: 1;
        height: 5px;
        background: rgba(255, 255, 255, 0.08);
        border-radius: 3px;
        overflow: hidden;
      }
      .sd-bar {
        height: 100%;
        border-radius: 3px;
        &.bullish {
          background: #22c55e;
        }
        &.bearish {
          background: #ef4444;
        }
        &.neutral {
          background: #6b7280;
        }
      }
      .sd-num {
        min-width: 60px;
        text-align: right;
        color: var(--fg-muted, #888);
      }
      .sd-note {
        display: flex;
        align-items: center;
        gap: 6px;
        font-size: 11px;
        color: var(--fg-muted, #aaa);
        padding-top: 4px;
        mat-icon {
          font-size: 14px;
          color: #60a5fa;
        }
        strong {
          color: var(--fg, #fff);
        }
      }

      .articles-list {
        display: flex;
        flex-direction: column;
        gap: 8px;
        max-height: 480px;
        overflow-y: auto;
        padding-right: 4px;
      }
      .article-card {
        background: rgba(255, 255, 255, 0.03);
        border-radius: 8px;
        padding: 12px;
        border: 1px solid rgba(255, 255, 255, 0.05);
        &.has-sentiment {
          border-left-width: 3px;
        }
        &.has-sentiment .ev-chip.bullish {
        }
        &.has-sentiment .ev-chip.bearish {
        }
      }
      .art-head {
        display: flex;
        align-items: center;
        gap: 8px;
        flex-wrap: wrap;
        margin-bottom: 6px;
      }
      .art-source {
        font-size: 11px;
        color: var(--fg-muted, #888);
      }
      .art-date {
        font-size: 11px;
        color: var(--fg-muted, #777);
        margin-left: auto;
      }
      .art-headline {
        font-size: 13px;
        color: var(--fg, #ddd);
        line-height: 1.45;
      }
      .art-link {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        margin-top: 6px;
        font-size: 11px;
        color: #60a5fa;
        text-decoration: none;
        mat-icon {
          font-size: 13px;
          height: 13px;
          width: 13px;
        }
        &:hover {
          text-decoration: underline;
        }
      }

      /* Decision flow */
      .decision-section {
        margin-bottom: 0;
      }
      .reasoning-box {
        display: flex;
        align-items: flex-start;
        gap: 10px;
        background: rgba(96, 165, 250, 0.06);
        border-radius: 8px;
        padding: 12px 16px;
        margin-bottom: 20px;
        font-size: 13px;
        color: #bfdbfe;
        border-left: 3px solid #60a5fa;
        mat-icon {
          color: #60a5fa;
          flex-shrink: 0;
          margin-top: 1px;
        }
      }
      .decision-flow {
        display: flex;
        align-items: flex-start;
        gap: 12px;
        flex-wrap: wrap;
      }
      .df-stage {
        background: rgba(255, 255, 255, 0.03);
        border-radius: 10px;
        padding: 16px;
        flex: 1;
        min-width: 200px;
      }
      .df-stage-title {
        display: flex;
        align-items: center;
        gap: 6px;
        font-size: 13px;
        font-weight: 600;
        color: var(--brand-400, #60a5fa);
        margin-bottom: 12px;
        mat-icon {
          font-size: 16px;
        }
      }
      .df-kv-grid {
        display: flex;
        flex-direction: column;
        gap: 6px;
      }
      .dfkv {
        display: flex;
        justify-content: space-between;
        font-size: 12px;
        span {
          color: var(--fg-muted, #888);
        }
        strong {
          font-weight: 600;
          color: var(--fg, #fff);
        }
      }
      .green {
        color: #86efac !important;
      }
      .red {
        color: #fca5a5 !important;
      }
      .orange {
        color: #fdba74 !important;
      }
      .df-chips {
        display: flex;
        flex-direction: column;
        gap: 8px;
      }
      .dfc-row {
        display: flex;
        align-items: center;
        gap: 8px;
        font-size: 12px;
      }
      .dfc-label {
        min-width: 75px;
        color: var(--fg-muted, #888);
        font-weight: 600;
      }
      .dfc-raw {
        color: var(--fg-muted, #777);
        font-family: monospace;
      }
      .df-arrow {
        color: var(--fg-muted, #555);
        display: flex;
        align-items: center;
        padding-top: 40px;
      }
      .result-stage {
        text-align: center;
        min-width: 160px;
        flex: 0 0 auto;
      }
      .result-signal {
        font-size: 28px;
        font-weight: 800;
        padding: 12px 24px;
        border-radius: 12px;
        margin-bottom: 12px;
        &.buy {
          background: rgba(34, 197, 94, 0.15);
          color: #86efac;
        }
        &.sell {
          background: rgba(239, 68, 68, 0.15);
          color: #fca5a5;
        }
        &.hold {
          background: rgba(245, 158, 11, 0.15);
          color: #fde68a;
        }
      }
      .result-probs {
        font-size: 13px;
        line-height: 1.8;
        strong {
          color: var(--fg, #fff);
        }
      }
      .result-threshold {
        font-size: 11px;
        color: var(--fg-muted, #888);
        margin-top: 6px;
      }
    `,
  ],
})
export class TickerExplorerComponent implements OnInit {
  private api = inject(ApiService);
  private traceSvc = inject(TraceService);
  private reportSvc = inject(ReportService);

  // ─── State ────────────────────────────────────────────────────────
  tickerInput = '';
  currentTicker = '';
  selectedDate = '';
  loading = false;

  availableTickers: string[] = [];
  filteredTickers: string[] = [];
  availableDates: ReportDateEntry[] = [];

  ohlcvData: OhlcvRow[] = [];
  ohlcvChartData: any[] = [];
  articles: Article[] = [];
  tickerTrace: TickerTrace | null = null;

  pipelineRunning = false;
  pipelineExecution: any = null;

  lineScheme: any = { domain: ['#3b82f6'] };
  chartView: [number, number] = [800, 260];

  // ─── Init ─────────────────────────────────────────────────────────
  ngOnInit() {
    // Cargar fechas y tickers disponibles en paralelo
    this.reportSvc.listAvailableDates().subscribe((dates) => {
      this.availableDates = dates;
      if (dates.length) this.selectedDate = dates[0].date;
    });

    this.api.getTickers().subscribe({
      next: (resp) => {
        this.availableTickers = resp.tickers;
        this.filteredTickers = resp.tickers;
      },
      error: () => {},
    });
  }

  // ─── Ticker input ─────────────────────────────────────────────────
  onTickerInput(value: string) {
    const q = value.toUpperCase();
    this.filteredTickers = this.availableTickers.filter((t) => t.startsWith(q));
  }

  selectTicker(ticker: string) {
    this.tickerInput = ticker;
    this.loadTicker();
  }

  // ─── Cargar datos del ticker ──────────────────────────────────────
  loadTicker() {
    const ticker = this.tickerInput.trim().toUpperCase();
    if (!ticker || !this.selectedDate) return;

    this.currentTicker = ticker;
    this.loading = true;
    this.ohlcvData = [];
    this.articles = [];
    this.tickerTrace = null;

    // Cargar en paralelo: OHLCV + Noticias + Trace
    Promise.all([
      this.loadOhlcv(ticker),
      this.loadNews(ticker),
      this.loadTrace(ticker),
    ]).finally(() => {
      this.loading = false;
    });
  }

  private loadOhlcv(ticker: string): Promise<void> {
    return new Promise((resolve) => {
      this.api
        .getRawOhlcv(this.selectedDate, ticker)
        .pipe(catchError(() => of(null)))
        .subscribe((resp) => {
          if (resp?.data) {
            this.ohlcvData = resp.data;
            this.ohlcvChartData = [
              {
                name: ticker,
                series: resp.data.map((r: OhlcvRow) => ({
                  name: r.date,
                  value: r.close,
                })),
              },
            ];
          }
          resolve();
        });
    });
  }

  private loadNews(ticker: string): Promise<void> {
    return new Promise((resolve) => {
      this.api
        .getRawNews(this.selectedDate, ticker)
        .pipe(catchError(() => of(null)))
        .subscribe((resp) => {
          this.articles = resp?.articles ?? [];
          resolve();
        });
    });
  }

  private loadTrace(ticker: string): Promise<void> {
    return new Promise((resolve) => {
      this.api
        .getTickerTrace(this.selectedDate, ticker)
        .pipe(catchError(() => of(null)))
        .subscribe((resp) => {
          this.tickerTrace = resp?.trace ?? null;
          // Enriquecer artículos con sentiment del trace si está disponible
          if (this.tickerTrace?.sentiment_detail?.headlines_sample) {
            const sample = this.tickerTrace.sentiment_detail.headlines_sample;
            this.articles = this.articles.map((art) => {
              const match = sample.find((s: any) =>
                art.headline?.includes(s.headline?.slice(0, 40)),
              );
              return match
                ? {
                    ...art,
                    sentiment: match.sentiment,
                    confidence: match.confidence,
                  }
                : art;
            });
          }
          resolve();
        });
    });
  }

  // ─── Pipeline trigger ─────────────────────────────────────────────
  triggerPipeline() {
    const ticker = this.tickerInput.trim().toUpperCase();
    if (!ticker || this.pipelineRunning) return;

    this.pipelineRunning = true;
    this.pipelineExecution = null;

    this.api.runPipeline({ ticker, batch_date: this.selectedDate }).subscribe({
      next: (exec) => {
        this.pipelineExecution = exec;
        this.pollPipelineStatus(exec.executionArn);
      },
      error: (err) => {
        this.pipelineRunning = false;
        this.pipelineExecution = {
          status: 'FAILED',
          message: err.error?.detail ?? 'Error al lanzar',
        };
      },
    });
  }

  private pollPipelineStatus(arn: string) {
    interval(5000)
      .pipe(
        switchMap(() =>
          this.api.getPipelineStatus(arn).pipe(catchError(() => of(null))),
        ),
        takeWhile((s) => s?.status === 'RUNNING', true),
      )
      .subscribe((status) => {
        if (status) this.pipelineExecution = status;
        if (status?.status !== 'RUNNING') {
          this.pipelineRunning = false;
        }
      });
  }

  pipelineStatusIcon(status: string): string {
    return (
      {
        RUNNING: 'pending',
        SUCCEEDED: 'check_circle',
        FAILED: 'error',
        ABORTED: 'cancel',
      }[status] ?? 'help'
    );
  }

  // ─── Helpers de presentación ──────────────────────────────────────
  getTraceStates() {
    const d = this.tickerTrace?.discretization;
    if (!d) return [];
    return [
      {
        label: 'Sentimiento',
        value: d.sentiment_state,
        cls: d.sentiment_state,
      },
      { label: 'RSI', value: d.rsi_state, cls: d.rsi_state },
      { label: 'Tendencia', value: d.trend_state, cls: d.trend_state },
      {
        label: 'Volatilidad',
        value: d.volatility_state,
        cls: `vol-${d.volatility_state}`,
      },
    ];
  }

  getSentimentDist() {
    const dist = this.tickerTrace?.sentiment_detail?.distribution;
    if (!dist) return [];
    return Object.entries(dist).map(([key, v]: [string, any]) => ({
      key,
      count: v.count,
      pct: v.pct,
    }));
  }
}
