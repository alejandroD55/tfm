import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatSelectModule } from '@angular/material/select';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatTableModule } from '@angular/material/table';
import { MatChipsModule } from '@angular/material/chips';
import { NgxChartsModule } from '@swimlane/ngx-charts';
import { switchMap } from 'rxjs';
import { TraceService } from '../../core/services/trace.service';
import { ReportService } from '../../core/services/report.service';
import {
  BayesianTrace, TickerTrace, ModelConfig,
  ExecutionMeta, SentimentDetail,
} from '../../core/models/trace.model';
import { ReportDateEntry } from '../../core/models/report.model';

@Component({
  selector: 'app-audit',
  standalone: true,
  imports: [
    CommonModule, FormsModule, MatIconModule, MatButtonModule,
    MatProgressSpinnerModule, MatSelectModule, MatTooltipModule,
    MatExpansionModule, MatTableModule, MatChipsModule, NgxChartsModule,
  ],
  template: `
    <div class="page">

      <!-- ─── Header ─── -->
      <header class="page-head">
        <div class="page-head-text">
          <div class="page-eyebrow">
            <mat-icon>manage_search</mat-icon>
            <span>Observabilidad · Trazabilidad</span>
          </div>
          <h1 class="page-title">Auditoría del pipeline bayesiano</h1>
          <p class="page-sub">
            Configuración completa del modelo, decisiones implícitas documentadas,
            cadena de razonamiento por ticker y distribución de sentimientos.
          </p>
        </div>
        <div class="page-actions">
          <mat-form-field appearance="outline" class="date-input" subscriptSizing="dynamic">
            <mat-label>Fecha</mat-label>
            <mat-select [(ngModel)]="selectedDate" (ngModelChange)="onDateChange($event)">
              @for (d of availableDates; track d.date) {
                <mat-option [value]="d.date">
                  {{ d.date }}
                  @if (!d.has_trace) { <span class="no-trace"> (sin trace)</span> }
                </mat-option>
              }
            </mat-select>
          </mat-form-field>
        </div>
      </header>

      @if (loading) {
        <div class="loader">
          <mat-spinner diameter="40"></mat-spinner>
          <p>Cargando traza bayesiana...</p>
        </div>
      } @else if (!trace) {
        <div class="empty-state">
          <mat-icon>info_outline</mat-icon>
          <h3>No hay traza disponible para esta fecha</h3>
          <p>La traza se genera a partir de la versión 2.0 de lambda_bayesian.<br>
             Ejecuta el pipeline para generar datos auditables.</p>
        </div>
      } @else {

        <!-- ─── Ejecución metadata ─── -->
        <section class="exec-banner" [class.ok]="trace.execution?.signals_generated > 0">
          <div class="exec-icon">
            <mat-icon>play_circle</mat-icon>
          </div>
          <div class="exec-body">
            <div class="exec-title">
              lambda_bayesian · {{ trace.batch_date }} · v{{ trace.model_config?.version }}
            </div>
            <div class="exec-meta">
              {{ trace.execution?.signals_generated }} señales generadas en
              {{ trace.execution?.duration_seconds }}s ·
              {{ trace.execution?.tickers_skipped }} tickers omitidos ·
              generado {{ trace.generated_at | date:'dd/MM HH:mm' }}
            </div>
          </div>
          <div class="exec-stats">
            <span class="es ok">{{ trace.execution?.signals_generated }} OK</span>
            <span class="es warn">{{ trace.execution?.tickers_skipped }} omitidos</span>
          </div>
        </section>

        <!-- ─── Advertencias sobre decisiones implícitas ─── -->
        <section class="card limitations-card">
          <div class="card-head">
            <div class="card-title warn-title">
              <mat-icon>warning_amber</mat-icon>
              <span>Decisiones implícitas documentadas ({{ limitations.length }})</span>
            </div>
            <span class="card-sub">Aspectos del modelo que no son visibles en la señal final</span>
          </div>
          <div class="limit-list">
            @for (lim of limitations; track $index) {
              <div class="limit-item">
                <mat-icon class="lim-icon">info</mat-icon>
                <span>{{ lim }}</span>
              </div>
            }
          </div>
        </section>

        <!-- ─── Configuración del modelo ─── -->
        <section class="card">
          <div class="card-head">
            <div class="card-title">
              <mat-icon>tune</mat-icon>
              <span>Configuración del modelo bayesiano</span>
            </div>
            <span class="card-sub">Todos los parámetros que determinan las señales</span>
          </div>

          <div class="config-grid">

            <!-- Thresholds de discretización -->
            <div class="config-box">
              <h4 class="config-box-title"><mat-icon>compress</mat-icon> Discretización</h4>
              <div class="config-rows">
                <div class="cr">
                  <span class="cr-label">RSI sobrevendido si</span>
                  <span class="cr-val badge-blue">RSI &lt; {{ model?.discretization?.rsi?.oversold_below }}</span>
                </div>
                <div class="cr">
                  <span class="cr-label">RSI sobrecomprado si</span>
                  <span class="cr-val badge-red">RSI &gt; {{ model?.discretization?.rsi?.overbought_above }}</span>
                </div>
                <div class="cr">
                  <span class="cr-label">Tendencia</span>
                  <span class="cr-val">{{ model?.discretization?.trend?.rule }}</span>
                </div>
                <div class="cr">
                  <span class="cr-label">Volatilidad alta si</span>
                  <span class="cr-val badge-orange">BB width &gt; {{ (model?.discretization?.volatility?.high_if_band_width_ratio_above || 0) * 100 }}% precio</span>
                </div>
              </div>
            </div>

            <!-- Thresholds de señal -->
            <div class="config-box">
              <h4 class="config-box-title"><mat-icon>call_split</mat-icon> Umbrales de señal</h4>
              <div class="config-rows">
                <div class="cr">
                  <span class="signal-badge buy">BUY</span>
                  <span class="cr-val">si P(↑) &gt; {{ model?.signal_thresholds?.BUY?.prob_up_above }}</span>
                </div>
                <div class="cr">
                  <span class="signal-badge sell">SELL</span>
                  <span class="cr-val">si P(↑) &lt; {{ model?.signal_thresholds?.SELL?.prob_up_below }}</span>
                </div>
                <div class="cr">
                  <span class="signal-badge hold">HOLD</span>
                  <span class="cr-val">si P(↑) entre {{ model?.signal_thresholds?.HOLD?.range?.[0] }} y {{ model?.signal_thresholds?.HOLD?.range?.[1] }}</span>
                </div>
              </div>
            </div>

            <!-- Priors -->
            <div class="config-box">
              <h4 class="config-box-title"><mat-icon>donut_small</mat-icon> Distribuciones prior</h4>
              @for (node of priorNodes; track node.name) {
                <div class="prior-node">
                  <span class="pn-name">{{ node.name }}</span>
                  <div class="pn-bars">
                    @for (state of node.states; track state.key) {
                      <div class="pn-bar-row">
                        <span class="pn-state">{{ state.key }}</span>
                        <div class="pn-bar-wrap">
                          <div class="pn-bar" [style.width.%]="state.value * 100"
                               [class.green]="isPositiveState(state.key)"
                               [class.red]="isNegativeState(state.key)"></div>
                        </div>
                        <span class="pn-pct">{{ (state.value * 100) | number:'1.0-0' }}%</span>
                      </div>
                    }
                  </div>
                </div>
              }
            </div>

          </div>
        </section>

        <!-- ─── CPT selector ─── -->
        <section class="card">
          <div class="card-head">
            <div class="card-title">
              <mat-icon>grid_on</mat-icon>
              <span>Tabla de Probabilidades Condicionales — MarketDirection</span>
            </div>
            <span class="card-sub">36 combinaciones · filtra para ver el razonamiento en contexto</span>
          </div>

          <div class="cpt-filters">
            <select [(ngModel)]="cptFilterSentiment" class="cpt-select">
              <option value="">Sentimiento: todos</option>
              <option value="bullish">bullish</option>
              <option value="bearish">bearish</option>
              <option value="neutral">neutral</option>
            </select>
            <select [(ngModel)]="cptFilterRsi" class="cpt-select">
              <option value="">RSI: todos</option>
              <option value="oversold">oversold</option>
              <option value="neutral">neutral</option>
              <option value="overbought">overbought</option>
            </select>
            <select [(ngModel)]="cptFilterTrend" class="cpt-select">
              <option value="">Tendencia: todos</option>
              <option value="uptrend">uptrend</option>
              <option value="downtrend">downtrend</option>
            </select>
          </div>

          <div class="cpt-table-wrap">
            <table class="cpt-table">
              <thead>
                <tr>
                  <th>Sentimiento</th><th>RSI</th><th>Tendencia</th><th>Volatilidad</th>
                  <th>P(↑ subida)</th><th>P(↓ bajada)</th><th>Señal implícita</th>
                </tr>
              </thead>
              <tbody>
                @for (row of filteredCpt; track $index) {
                  <tr [class.row-buy]="row.prob_up > 0.65"
                      [class.row-sell]="row.prob_up < 0.35">
                    <td><span class="ev-chip {{ row.sentiment }}">{{ row.sentiment }}</span></td>
                    <td><span class="ev-chip {{ row.rsi }}">{{ row.rsi }}</span></td>
                    <td><span class="ev-chip {{ row.trend }}">{{ row.trend }}</span></td>
                    <td><span class="ev-chip vol-{{ row.volatility }}">{{ row.volatility }}</span></td>
                    <td>
                      <div class="prob-bar-cell">
                        <div class="pb-fill" [style.width.%]="row.prob_up * 100"
                             [class.high]="row.prob_up > 0.65"
                             [class.low]="row.prob_up < 0.35"></div>
                        <span>{{ (row.prob_up * 100) | number:'1.0-0' }}%</span>
                      </div>
                    </td>
                    <td class="muted">{{ ((1 - row.prob_up) * 100) | number:'1.0-0' }}%</td>
                    <td>
                      @if (row.prob_up > 0.65) { <span class="signal-badge buy">BUY</span> }
                      @else if (row.prob_up < 0.35) { <span class="signal-badge sell">SELL</span> }
                      @else { <span class="signal-badge hold">HOLD</span> }
                    </td>
                  </tr>
                }
              </tbody>
            </table>
          </div>
        </section>

        <!-- ─── Trazas por ticker ─── -->
        <section class="card">
          <div class="card-head">
            <div class="card-title">
              <mat-icon>account_tree</mat-icon>
              <span>Cadena de decisión por ETF ({{ tickerList.length }} tickers)</span>
            </div>
            <span class="card-sub">Valores raw → estados discretos → probabilidades → señal → razonamiento</span>
          </div>

          <div class="ticker-traces">
            @for (ticker of tickerList; track ticker) {
              <mat-expansion-panel class="trace-panel">
                <mat-expansion-panel-header [collapsedHeight]="'68px'" [expandedHeight]="'68px'">
                  <div class="tp-header">
                    <span class="tp-ticker">{{ ticker }}</span>
                    <span class="signal-badge {{ getSignal(ticker).toLowerCase() }}">{{ getSignal(ticker) }}</span>
                    <div class="tp-prob">
                      <div class="tp-bar">
                        <div class="tp-fill" [style.width.%]="getProbUp(ticker) * 100"
                             [class.buy-fill]="getProbUp(ticker) > 0.65"
                             [class.sell-fill]="getProbUp(ticker) < 0.35"></div>
                      </div>
                      <span class="tp-pct">P↑={{ (getProbUp(ticker) * 100) | number:'1.1-1' }}%</span>
                    </div>
                    <div class="tp-states">
                      @for (state of getStates(ticker); track state.label) {
                        <span class="ev-chip {{ state.cls }}" [matTooltip]="state.tooltip">{{ state.value }}</span>
                      }
                    </div>
                  </div>
                </mat-expansion-panel-header>

                <!-- Contenido expandido -->
                @let t = getTrace(ticker);
                @if (t) {
                  <div class="trace-detail">

                    <!-- Razonamiento textual -->
                    <div class="reasoning-box">
                      <mat-icon>lightbulb</mat-icon>
                      <span>{{ t.reasoning }}</span>
                    </div>

                    <!-- Grid de valores -->
                    <div class="detail-grid">

                      <!-- Valores raw -->
                      <div class="detail-box">
                        <h5><mat-icon>numbers</mat-icon> Valores crudos</h5>
                        <div class="kv-list">
                          <div class="kv"><span>Precio cierre</span><strong>${{ t.raw_values.close_price | number:'1.2-2' }}</strong></div>
                          <div class="kv"><span>RSI 14</span>
                            <strong [class.kv-green]="t.raw_values.rsi_14 < 30"
                                    [class.kv-red]="t.raw_values.rsi_14 > 70">
                              {{ t.raw_values.rsi_14 | number:'1.1-1' }}
                            </strong>
                          </div>
                          <div class="kv"><span>SMA 20</span><strong>{{ t.raw_values.sma_20 | number:'1.2-2' }}</strong></div>
                          <div class="kv"><span>SMA 50</span><strong>{{ t.raw_values.sma_50 | number:'1.2-2' }}</strong></div>
                          <div class="kv"><span>SMA spread</span>
                            <strong [class.kv-green]="(t.raw_values.sma_spread || 0) > 0"
                                    [class.kv-red]="(t.raw_values.sma_spread || 0) < 0">
                              {{ (t.raw_values.sma_spread || 0) | number:'1.2-2' }}
                            </strong>
                          </div>
                          <div class="kv"><span>BB width ratio</span>
                            <strong [class.kv-orange]="t.raw_values.bb_width_ratio > 0.05">
                              {{ (t.raw_values.bb_width_ratio * 100) | number:'1.2-2' }}%
                            </strong>
                          </div>
                        </div>
                      </div>

                      <!-- Discretización -->
                      <div class="detail-box">
                        <h5><mat-icon>compress</mat-icon> Discretización</h5>
                        <div class="disc-chain">
                          <div class="dc-step">
                            <span class="dc-node">Sentimiento</span>
                            <span class="dc-raw">{{ t.discretization.sentiment_raw }} ({{ (t.discretization.sentiment_conf * 100) | number:'1.0-0' }}%)</span>
                            <mat-icon class="dc-arrow">arrow_forward</mat-icon>
                            <span class="ev-chip {{ t.discretization.sentiment_state }}">{{ t.discretization.sentiment_state }}</span>
                          </div>
                          <div class="dc-step">
                            <span class="dc-node">RSI</span>
                            <span class="dc-raw">{{ t.raw_values.rsi_14 | number:'1.1-1' }}</span>
                            <mat-icon class="dc-arrow">arrow_forward</mat-icon>
                            <span class="ev-chip {{ t.discretization.rsi_state }}">{{ t.discretization.rsi_state }}</span>
                          </div>
                          <div class="dc-step">
                            <span class="dc-node">Tendencia</span>
                            <span class="dc-raw">SMA20={{ t.raw_values.sma_20 | number:'1.0-0' }} vs SMA50={{ t.raw_values.sma_50 | number:'1.0-0' }}</span>
                            <mat-icon class="dc-arrow">arrow_forward</mat-icon>
                            <span class="ev-chip {{ t.discretization.trend_state }}">{{ t.discretization.trend_state }}</span>
                          </div>
                          <div class="dc-step">
                            <span class="dc-node">Volatilidad</span>
                            <span class="dc-raw">{{ (t.raw_values.bb_width_ratio * 100) | number:'1.2-2' }}% (umbral 5%)</span>
                            <mat-icon class="dc-arrow">arrow_forward</mat-icon>
                            <span class="ev-chip vol-{{ t.discretization.volatility_state }}">{{ t.discretization.volatility_state }}</span>
                          </div>
                        </div>
                      </div>

                      <!-- Sentimiento multi-headline -->
                      <div class="detail-box">
                        <h5><mat-icon>article</mat-icon> Sentimiento FinBERT ({{ t.sentiment_detail.total_headlines }} titulares)</h5>
                        <div class="sentiment-dist">
                          @for (entry of getSentimentDist(t.sentiment_detail); track entry.key) {
                            <div class="sd-row">
                              <span class="sd-label ev-chip {{ entry.key }}">{{ entry.key }}</span>
                              <div class="sd-bar-wrap">
                                <div class="sd-bar {{ entry.key }}" [style.width.%]="entry.pct"></div>
                              </div>
                              <span class="sd-num">{{ entry.count }} ({{ entry.pct }}%)</span>
                            </div>
                          }
                        </div>
                        @if (t.sentiment_detail.headlines_sample?.length > 0) {
                          <div class="headlines-list">
                            @for (h of t.sentiment_detail.headlines_sample?.slice(0,5); track $index) {
                              <div class="hl-row">
                                <span class="ev-chip {{ h.sentiment }} small">{{ h.sentiment }}</span>
                                <span class="hl-text">{{ h.headline }}</span>
                                <span class="hl-conf">{{ (h.confidence * 100) | number:'1.0-0' }}%</span>
                              </div>
                            }
                          </div>
                        }
                        <div class="limitation-note">
                          <mat-icon>warning_amber</mat-icon>
                          {{ t.sentiment_detail.limitation }}
                        </div>
                      </div>

                    </div>
                  </div>
                }
              </mat-expansion-panel>
            }
          </div>
        </section>

      }
    </div>
  `,
  styles: [`
    .page { max-width: 1400px; margin: 0 auto; }

    /* Header */
    .page-head { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 20px; flex-wrap: wrap; gap: 12px; }
    .page-eyebrow { display: flex; align-items: center; gap: 6px; font-size: 12px; font-weight: 600; color: var(--brand-400,#60a5fa); letter-spacing: .08em; text-transform: uppercase; margin-bottom: 4px; mat-icon { font-size: 14px; height: 14px; width: 14px; } }
    .page-title { font-size: 22px; font-weight: 700; color: var(--fg,#fff); margin: 0 0 4px; }
    .page-sub { font-size: 13px; color: var(--fg-muted,#888); margin: 0; }
    .date-input { min-width: 160px; }
    .no-trace { color: #888; font-size: 11px; }
    .loader { display: flex; flex-direction: column; align-items: center; gap: 16px; padding: 80px; color: var(--fg-muted,#888); }
    .empty-state { text-align: center; padding: 60px; color: var(--fg-muted,#888); mat-icon { font-size: 48px; height: 48px; width: 48px; margin-bottom: 12px; } h3 { margin: 0 0 8px; font-size: 18px; } }

    /* Execution banner */
    .exec-banner { display: flex; align-items: center; gap: 16px; background: rgba(255,255,255,.04); border-radius: 12px; padding: 16px 20px; margin-bottom: 20px; border-left: 4px solid #666; &.ok { border-color: #22c55e; } }
    .exec-icon mat-icon { font-size: 28px; color: #22c55e; }
    .exec-body { flex: 1; }
    .exec-title { font-weight: 600; font-size: 14px; }
    .exec-meta { font-size: 12px; color: var(--fg-muted,#888); margin-top: 2px; }
    .exec-stats { display: flex; gap: 8px; }
    .es { padding: 3px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; &.ok { background: rgba(34,197,94,.12); color: #86efac; } &.warn { background: rgba(245,158,11,.12); color: #fcd34d; } }

    /* Card base */
    .card { background: var(--surface,#111827); border-radius: 12px; padding: 20px; margin-bottom: 16px; border: 1px solid rgba(255,255,255,.06); }
    .card-head { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 16px; flex-wrap: wrap; gap: 8px; }
    .card-title { display: flex; align-items: center; gap: 8px; font-size: 15px; font-weight: 600; color: var(--fg,#fff); mat-icon { font-size: 18px; height: 18px; width: 18px; } }
    .warn-title { color: #fcd34d; mat-icon { color: #fcd34d; } }
    .card-sub { font-size: 12px; color: var(--fg-muted,#888); }

    /* Limitations */
    .limit-list { display: flex; flex-direction: column; gap: 8px; }
    .limit-item { display: flex; align-items: flex-start; gap: 10px; padding: 10px 14px; background: rgba(245,158,11,.06); border-radius: 8px; border-left: 3px solid #f59e0b; font-size: 13px; color: #fde68a; }
    .lim-icon { color: #f59e0b; font-size: 18px; flex-shrink: 0; margin-top: 1px; }

    /* Config grid */
    .config-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }
    .config-box { background: rgba(255,255,255,.03); border-radius: 10px; padding: 16px; }
    .config-box-title { display: flex; align-items: center; gap: 6px; font-size: 13px; font-weight: 600; color: var(--brand-400,#60a5fa); margin: 0 0 12px; mat-icon { font-size: 16px; } }
    .config-rows { display: flex; flex-direction: column; gap: 8px; }
    .cr { display: flex; align-items: center; justify-content: space-between; gap: 8px; font-size: 13px; }
    .cr-label { color: var(--fg-muted,#888); }
    .cr-val { font-weight: 600; }
    .badge-blue { color: #60a5fa; } .badge-red { color: #f87171; } .badge-orange { color: #fb923c; }

    /* Priors */
    .prior-node { margin-bottom: 12px; }
    .pn-name { font-size: 12px; font-weight: 600; color: var(--fg-muted,#aaa); text-transform: uppercase; letter-spacing: .05em; margin-bottom: 4px; display: block; }
    .pn-bars { display: flex; flex-direction: column; gap: 4px; }
    .pn-bar-row { display: flex; align-items: center; gap: 8px; font-size: 12px; }
    .pn-state { width: 80px; color: var(--fg-muted,#888); }
    .pn-bar-wrap { flex: 1; height: 6px; background: rgba(255,255,255,.08); border-radius: 3px; overflow: hidden; }
    .pn-bar { height: 100%; background: #60a5fa; border-radius: 3px; &.green { background: #22c55e; } &.red { background: #ef4444; } }
    .pn-pct { width: 32px; text-align: right; font-weight: 600; }

    /* Signal badges */
    .signal-badge { padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 700; &.buy { background: rgba(34,197,94,.15); color: #86efac; } &.sell { background: rgba(239,68,68,.15); color: #fca5a5; } &.hold { background: rgba(245,158,11,.15); color: #fde68a; } }

    /* CPT */
    .cpt-filters { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; }
    .cpt-select { background: rgba(255,255,255,.06); border: 1px solid rgba(255,255,255,.12); color: var(--fg,#fff); border-radius: 6px; padding: 6px 10px; font-size: 13px; outline: none; cursor: pointer; }
    .cpt-table-wrap { overflow-x: auto; }
    .cpt-table { width: 100%; border-collapse: collapse; font-size: 13px; }
    .cpt-table th { background: rgba(255,255,255,.06); padding: 10px 12px; text-align: left; font-size: 11px; color: var(--fg-muted,#888); font-weight: 600; text-transform: uppercase; letter-spacing: .05em; }
    .cpt-table td { padding: 8px 12px; border-top: 1px solid rgba(255,255,255,.04); }
    .cpt-table tr.row-buy { background: rgba(34,197,94,.04); }
    .cpt-table tr.row-sell { background: rgba(239,68,68,.04); }
    .prob-bar-cell { display: flex; align-items: center; gap: 8px; }
    .pb-fill { height: 6px; background: #60a5fa; border-radius: 3px; min-width: 4px; &.high { background: #22c55e; } &.low { background: #ef4444; } }
    .muted { color: var(--fg-muted,#888); }

    /* Evidence chips */
    .ev-chip { display: inline-flex; align-items: center; gap: 3px; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }
    .bullish, .oversold, .uptrend { background: rgba(34,197,94,.15); color: #86efac; }
    .bearish, .overbought, .downtrend { background: rgba(239,68,68,.15); color: #fca5a5; }
    .neutral { background: rgba(255,255,255,.08); color: #d1d5db; }
    .vol-low { background: rgba(96,165,250,.12); color: #93c5fd; }
    .vol-high { background: rgba(251,146,60,.12); color: #fdba74; }
    .small { font-size: 10px; padding: 1px 6px; }

    /* Ticker traces */
    .ticker-traces { display: flex; flex-direction: column; gap: 6px; }
    .trace-panel { background: rgba(255,255,255,.02) !important; border-radius: 10px !important; border: 1px solid rgba(255,255,255,.06) !important; }
    .tp-header { display: flex; align-items: center; gap: 12px; flex: 1; flex-wrap: wrap; }
    .tp-ticker { font-size: 16px; font-weight: 700; min-width: 50px; }
    .tp-prob { display: flex; align-items: center; gap: 8px; min-width: 160px; }
    .tp-bar { flex: 1; height: 6px; background: rgba(255,255,255,.08); border-radius: 3px; overflow: hidden; }
    .tp-fill { height: 100%; background: #60a5fa; border-radius: 3px; &.buy-fill { background: #22c55e; } &.sell-fill { background: #ef4444; } }
    .tp-pct { font-size: 12px; font-weight: 600; min-width: 55px; }
    .tp-states { display: flex; gap: 4px; flex-wrap: wrap; }

    /* Trace detail */
    .trace-detail { padding: 16px 4px 8px; }
    .reasoning-box { display: flex; align-items: flex-start; gap: 10px; background: rgba(96,165,250,.06); border-radius: 8px; padding: 12px 16px; margin-bottom: 16px; font-size: 13px; color: #bfdbfe; border-left: 3px solid #60a5fa; mat-icon { color: #60a5fa; flex-shrink: 0; margin-top: 1px; } }
    .detail-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 14px; }
    .detail-box { background: rgba(255,255,255,.03); border-radius: 10px; padding: 14px; h5 { display: flex; align-items: center; gap: 6px; font-size: 13px; font-weight: 600; color: var(--brand-400,#60a5fa); margin: 0 0 12px; mat-icon { font-size: 16px; } } }
    .kv-list { display: flex; flex-direction: column; gap: 6px; }
    .kv { display: flex; justify-content: space-between; font-size: 13px; span { color: var(--fg-muted,#888); } strong { font-weight: 600; } }
    .kv-green { color: #86efac; } .kv-red { color: #fca5a5; } .kv-orange { color: #fdba74; }

    /* Discretization chain */
    .disc-chain { display: flex; flex-direction: column; gap: 8px; }
    .dc-step { display: flex; align-items: center; gap: 8px; font-size: 12px; flex-wrap: wrap; }
    .dc-node { font-weight: 600; color: var(--fg-muted,#aaa); min-width: 80px; }
    .dc-raw { color: var(--fg-muted,#777); font-family: monospace; flex: 1; }
    .dc-arrow { font-size: 16px; color: #555; }

    /* Sentiment distribution */
    .sentiment-dist { display: flex; flex-direction: column; gap: 6px; margin-bottom: 12px; }
    .sd-row { display: flex; align-items: center; gap: 8px; font-size: 12px; }
    .sd-label { min-width: 70px; }
    .sd-bar-wrap { flex: 1; height: 6px; background: rgba(255,255,255,.08); border-radius: 3px; overflow: hidden; }
    .sd-bar { height: 100%; border-radius: 3px; &.bullish { background: #22c55e; } &.bearish { background: #ef4444; } &.neutral { background: #6b7280; } }
    .sd-num { min-width: 60px; text-align: right; color: var(--fg-muted,#888); }

    /* Headlines */
    .headlines-list { display: flex; flex-direction: column; gap: 4px; margin-bottom: 10px; }
    .hl-row { display: flex; align-items: flex-start; gap: 8px; font-size: 11px; padding: 4px 0; }
    .hl-text { flex: 1; color: var(--fg-muted,#aaa); line-height: 1.4; }
    .hl-conf { color: var(--fg-muted,#888); white-space: nowrap; }

    /* Limitation note */
    .limitation-note { display: flex; align-items: flex-start; gap: 6px; font-size: 11px; color: #fde68a; background: rgba(245,158,11,.06); border-radius: 6px; padding: 8px 10px; mat-icon { font-size: 14px; color: #f59e0b; flex-shrink: 0; margin-top: 1px; } }
  `],
})
export class AuditComponent implements OnInit {
  private traceSvc  = inject(TraceService);
  private reportSvc = inject(ReportService);

  loading       = true;
  trace: BayesianTrace | null = null;
  model: ModelConfig | null   = null;
  availableDates: (ReportDateEntry & { has_trace?: boolean })[] = [];
  selectedDate  = '';

  cptRows: any[]         = [];
  cptFilterSentiment     = '';
  cptFilterRsi           = '';
  cptFilterTrend         = '';

  limitations: string[]  = [];
  priorNodes: { name: string; states: { key: string; value: number }[] }[] = [];

  get tickerList()   { return Object.keys(this.trace?.tickers ?? {}); }
  get filteredCpt() {
    return this.cptRows.filter(r =>
      (!this.cptFilterSentiment || r.sentiment === this.cptFilterSentiment) &&
      (!this.cptFilterRsi       || r.rsi       === this.cptFilterRsi) &&
      (!this.cptFilterTrend     || r.trend     === this.cptFilterTrend)
    );
  }

  ngOnInit() {
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        this.availableDates = dates as any[];
        if (!dates.length) { this.loading = false; return []; }
        this.selectedDate = dates[0].date;
        return this.traceSvc.getTrace(this.selectedDate);
      })
    ).subscribe({
      next: (t: any) => { if (t) this.processTrace(t); this.loading = false; },
      error: () => { this.loading = false; },
    });
  }

  onDateChange(date: string) {
    this.loading = true;
    this.trace   = null;
    this.traceSvc.getTrace(date).subscribe({
      next: t => { this.processTrace(t); this.loading = false; },
      error: () => { this.loading = false; },
    });
  }

  private processTrace(t: BayesianTrace) {
    this.trace = t;
    this.model = t.model_config;
    this.limitations = t.model_config?.known_limitations ?? [];

    // Priors para visualización
    if (t.model_config?.priors) {
      this.priorNodes = Object.entries(t.model_config.priors).map(([name, vals]) => ({
        name,
        states: Object.entries(vals)
          .filter(([k]) => k !== 'rationale')
          .map(([key, value]) => ({ key, value: value as number })),
      }));
    }

    // CPT matrix
    if (t.model_config?.cpt_market_direction?.values_P_up) {
      this.cptRows = this.traceSvc.parseCptMatrix(
        t.model_config.cpt_market_direction.values_P_up
      );
    }
  }

  getTrace(ticker: string): TickerTrace | null {
    return this.trace?.tickers?.[ticker] ?? null;
  }

  getSignal(ticker: string): string {
    return this.trace?.tickers?.[ticker]?.inference?.signal ?? '—';
  }

  getProbUp(ticker: string): number {
    return this.trace?.tickers?.[ticker]?.inference?.prob_up ?? 0.5;
  }

  getStates(ticker: string) {
    const d = this.trace?.tickers?.[ticker]?.discretization;
    if (!d) return [];
    return [
      { label: 'Sentimiento', value: d.sentiment_state, cls: d.sentiment_state,          tooltip: `FinBERT: ${d.sentiment_raw} (${(d.sentiment_conf*100).toFixed(0)}%)` },
      { label: 'RSI',         value: d.rsi_state,        cls: d.rsi_state,                tooltip: `RSI 14 = ${this.trace?.tickers?.[ticker]?.raw_values?.rsi_14?.toFixed(1)}` },
      { label: 'Tendencia',   value: d.trend_state,      cls: d.trend_state,              tooltip: `SMA20 vs SMA50` },
      { label: 'Volatilidad', value: d.volatility_state, cls: `vol-${d.volatility_state}`, tooltip: `BB width = ${((this.trace?.tickers?.[ticker]?.raw_values?.bb_width_ratio ?? 0)*100).toFixed(2)}%` },
    ];
  }

  getSentimentDist(sd: SentimentDetail): { key: string; count: number; pct: number }[] {
    if (!sd?.distribution) return [];
    return Object.entries(sd.distribution).map(([key, v]) => ({
      key, count: v.count, pct: v.pct
    }));
  }

  isPositiveState(state: string): boolean {
    return ['bullish', 'oversold', 'uptrend'].includes(state);
  }
  isNegativeState(state: string): boolean {
    return ['bearish', 'overbought', 'downtrend'].includes(state);
  }
}
