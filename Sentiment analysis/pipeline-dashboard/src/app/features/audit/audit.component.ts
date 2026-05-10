import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatSelectModule } from '@angular/material/select';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatChipsModule } from '@angular/material/chips';
import { switchMap } from 'rxjs';
import { TraceService } from '../../core/services/trace.service';
import { ReportService } from '../../core/services/report.service';
import { BayesianTrace, ModelConfig } from '../../core/models/trace.model';
import { ReportDateEntry } from '../../core/models/report.model';

@Component({
  selector: 'app-audit',
  standalone: true,
  imports: [
    CommonModule, FormsModule, MatIconModule, MatButtonModule,
    MatProgressSpinnerModule, MatSelectModule, MatTooltipModule,
    MatExpansionModule, MatChipsModule
  ],
  template: `
    <div class="page">

      <header class="page-head">
        <div class="page-head-text">
          <div class="page-eyebrow">
            <mat-icon>manage_search</mat-icon>
            <span>Observabilidad y Trazabilidad MLOps</span>
          </div>
          <h1 class="page-title">Auditoría del Modelo Matemático</h1>
          <p class="page-sub">
            Inspección de caja blanca (Whitebox) de la Red Bayesiana. Consulta los pesos, distribuciones y umbrales exactos usados en la versión del modelo de este día.
          </p>
        </div>
        <div class="page-actions">
          <div class="filter-group">
            <label>Fecha de la Traza</label>
            <select class="aurora-select" [(ngModel)]="selectedDate" (change)="onDateChange(selectedDate)">
              @for (d of availableDates; track d.date) {
                <option [value]="d.date">{{ d.date }}</option>
              }
            </select>
          </div>
        </div>
      </header>

      <mat-accordion class="glossary-accordion">
        <mat-expansion-panel class="glossary-panel">
          <mat-expansion-panel-header>
            <mat-panel-title>
              <mat-icon>lightbulb</mat-icon>
              <span>¿Cómo auditar el comportamiento del algoritmo? (Glosario)</span>
            </mat-panel-title>
          </mat-expansion-panel-header>
          
          <div class="glossary-content">
            <div class="g-col">
              <strong>1. Reglas de Discretización:</strong> Los modelos bayesianos no entienden de números continuos. Aquí documentamos cómo convertimos un número (ej. RSI = 25) en un estado discreto entendible por la IA (ej. "Sobreventa").
            </div>
            <div class="g-col">
              <strong>2. Distribuciones Prior:</strong> Los sesgos matemáticos base. Antes de analizar ningún ETF, la IA asume estas probabilidades estadísticas (ej: Asume que el mercado está en tendencia alcista el 50% de las veces).
            </div>
            <div class="g-col">
              <strong>3. Matriz CPT (Tabla Condicional):</strong> El núcleo de la IA. Muestra las 36 combinaciones posibles del mercado y qué probabilidad exacta de subida se le asigna a cada escenario.
            </div>

            <div class="g-col full-width limitation-box">
              <strong><mat-icon>engineering</mat-icon> Decisiones Implícitas (Hardcoded) de esta versión:</strong>
              <ul class="limit-list">
                @for (lim of limitations; track $index) {
                  <li>{{ lim }}</li>
                }
              </ul>
            </div>
          </div>
        </mat-expansion-panel>
      </mat-accordion>

      @if (loading) {
        <div class="loader">
          <mat-spinner diameter="40"></mat-spinner>
          <p>Cargando pesos matemáticos desde AWS S3...</p>
        </div>
      } @else if (!trace) {
        <div class="empty-state">
          <mat-icon>info_outline</mat-icon>
          <h3>No hay traza auditable para esta fecha</h3>
          <p>La traza de caja blanca se genera a partir de la versión 2.0 del algoritmo.</p>
        </div>
      } @else {

        <section class="exec-banner">
          <div class="exec-icon"><mat-icon>verified_user</mat-icon></div>
          <div class="exec-body">
            <div class="exec-title">
              Firma de Ejecución: lambda_bayesian · {{ trace.batch_date }} · Versión del Modelo: v{{ model?.version || '1.0.0' }}
            </div>
            <div class="exec-meta">
              {{ trace.execution.signals_generated }} decisiones generadas en
              {{ trace.execution.duration_seconds }}s ·
              {{ trace.execution.tickers_skipped }} activos omitidos por falta de datos
            </div>
          </div>
          <div class="exec-stats">
            <span class="es ok">Trazabilidad OK</span>
          </div>
        </section>

        <section class="card">
          <div class="card-head">
            <div class="card-title">
              <mat-icon>tune</mat-icon>
              <span>Parámetros de Configuración del Algoritmo</span>
            </div>
            <span class="card-sub">Reglas deterministas aplicadas a los datos crudos</span>
          </div>

          <div class="config-grid">

            <div class="config-box">
              <h4 class="config-box-title"><mat-icon>compress</mat-icon> Reglas de Discretización</h4>
              <div class="config-rows">
                <div class="cr">
                  <span class="cr-label">Considerar Sobreventa si:</span>
                  <span class="cr-val badge-blue">RSI &lt; {{ model?.discretization?.rsi?.oversold_below }}</span>
                </div>
                <div class="cr">
                  <span class="cr-label">Considerar Sobrecompra si:</span>
                  <span class="cr-val badge-red">RSI &gt; {{ model?.discretization?.rsi?.overbought_above }}</span>
                </div>
                <div class="cr">
                  <span class="cr-label">Considerar Tendencia Alcista si:</span>
                  <span class="cr-val badge-green">SMA 20 &gt; SMA 50</span>
                </div>
                <div class="cr">
                  <span class="cr-label">Considerar Alta Volatilidad si:</span>
                  <span class="cr-val badge-orange">Bandas Bollinger &gt; {{ (model?.discretization?.volatility?.high_if_band_width_ratio_above || 0.05) * 100 }}%</span>
                </div>
              </div>
            </div>

            <div class="config-box">
              <h4 class="config-box-title"><mat-icon>call_split</mat-icon> Umbrales de Inversión</h4>
              <div class="config-rows">
                <div class="cr">
                  <span class="signal-badge buy">COMPRAR</span>
                  <span class="cr-val">Si Confianza &gt; {{ (model?.signal_thresholds?.BUY?.prob_up_above || 0.65) * 100 }}%</span>
                </div>
                <div class="cr">
                  <span class="signal-badge sell">CASH (CORTOS)</span>
                  <span class="cr-val">Si Confianza &lt; {{ (model?.signal_thresholds?.SELL?.prob_up_below || 0.35) * 100 }}%</span>
                </div>
                <div class="cr">
                  <span class="signal-badge hold">MANTENER</span>
                  <span class="cr-val">
                    Si Confianza está entre {{ (model?.signal_thresholds?.HOLD?.range?.[0] || 0.35) * 100 }}% y {{ (model?.signal_thresholds?.HOLD?.range?.[1] || 0.65) * 100 }}%
                  </span>
                </div>
              </div>
            </div>

            <div class="config-box">
              <h4 class="config-box-title"><mat-icon>donut_small</mat-icon> Distribuciones Estadísticas Base (Priors)</h4>
              @for (node of priorNodes; track node.name) {
                <div class="prior-node">
                  <span class="pn-name">{{ translateNode(node.name) }}</span>
                  <div class="pn-bars">
                    @for (state of node.states; track state.key) {
                      <div class="pn-bar-row">
                        <span class="pn-state">{{ translateState(state.key) }}</span>
                        <div class="pn-bar-wrap">
                          <div class="pn-bar"
                               [style.width.%]="state.value * 100"
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

        <section class="card">
          <div class="card-head">
            <div class="card-title">
              <mat-icon>grid_on</mat-icon>
              <span>Matriz de Probabilidad Condicional (CPT)</span>
            </div>
            <span class="card-sub">Tabla completa de las 36 combinaciones matemáticas que rigen la decisión final del algoritmo.</span>
          </div>

          <div class="cpt-filters">
            <select class="aurora-select" [(ngModel)]="cptFilterSentiment">
              <option value="">Sentimiento (FinBERT): Todos</option>
              <option value="bullish">Alcista</option>
              <option value="neutral">Neutral</option>
              <option value="bearish">Bajista</option>
            </select>

            <select class="aurora-select" [(ngModel)]="cptFilterRsi">
              <option value="">Fuerza (RSI): Todos</option>
              <option value="overbought">Sobrecompra</option>
              <option value="neutral">Neutral</option>
              <option value="oversold">Sobreventa</option>
            </select>

            <select class="aurora-select" [(ngModel)]="cptFilterTrend">
              <option value="">Tendencia: Todos</option>
              <option value="uptrend">Alcista</option>
              <option value="downtrend">Bajista</option>
            </select>
          </div>

          <div class="cpt-table-wrap">
            <table class="cpt-table">
              <thead>
                <tr>
                  <th>Sentimiento</th><th>Fuerza (RSI)</th><th>Tendencia</th><th>Volatilidad</th>
                  <th>Confianza Alcista P(↑)</th><th>Confianza Bajista P(↓)</th><th>Señal Implícita</th>
                </tr>
              </thead>
              <tbody>
                @for (row of filteredCpt; track $index) {
                  <tr>
                    <td><span class="ev-chip {{ row.sentiment }}">{{ translateState(row.sentiment) }}</span></td>
                    <td><span class="ev-chip {{ row.rsi }}">{{ translateState(row.rsi) }}</span></td>
                    <td><span class="ev-chip {{ row.trend }}">{{ translateState(row.trend) }}</span></td>
                    <td><span class="ev-chip vol-{{ row.volatility }}">{{ translateState(row.volatility) }}</span></td>
                    <td>
                      <div class="prob-bar-cell">
                        <div class="pb-fill"
                             [style.width.%]="row.prob_up * 100"
                             [class.high]="row.prob_up >= 0.65"
                             [class.mid]="row.prob_up > 0.35 && row.prob_up < 0.65"
                             [class.low]="row.prob_up <= 0.35"></div>
                        <span class="prob-pct-text" 
                              [class.txt-green]="row.prob_up >= 0.65"
                              [class.txt-yellow]="row.prob_up > 0.35 && row.prob_up < 0.65"
                              [class.txt-purple]="row.prob_up <= 0.35">
                          {{ (row.prob_up * 100) | number:'1.0-0' }}%
                        </span>
                      </div>
                    </td>
                    <td class="muted">{{ ((1 - row.prob_up) * 100) | number:'1.0-0' }}%</td>
                    <td>
                      @if (row.prob_up >= 0.65) { <span class="signal-badge buy">COMPRAR</span> }
                      @else if (row.prob_up <= 0.35) { <span class="signal-badge sell">CASH</span> }
                      @else { <span class="signal-badge hold">MANTENER</span> }
                    </td>
                  </tr>
                }
              </tbody>
            </table>
          </div>
        </section>

      }
    </div>
  `,
  styles: [`
    /* shared page chrome */
    .page { max-width: var(--content-max); margin: 0 auto; padding-bottom: 40px;}
    .page-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 24px; flex-wrap: wrap; margin-bottom: 22px; }
    .page-eyebrow { display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px; background: rgba(59, 130, 246, .12); color: var(--brand-600); border-radius: var(--r-pill); font-size: 11px; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; margin-bottom: 10px; mat-icon { font-size: 14px; height: 14px; width: 14px; } }
    .page-title { font-size: 26px; font-weight: 700; color: var(--slate-900); letter-spacing: -.02em; }
    .page-sub { color: var(--slate-500); font-size: 13px; margin-top: 6px; max-width: 760px; }

    /* Filtros Personalizados nativos */
    .page-actions { display: flex; gap: 16px; flex-wrap: wrap; align-items: flex-end; }
    .filter-group { display: flex; flex-direction: column; gap: 4px; }
    .filter-group label { font-size: 11px; font-weight: 600; color: var(--slate-500); text-transform: uppercase; letter-spacing: 0.05em; }
    .aurora-select {
      height: 40px; appearance: none; background-color: var(--bg-elevated); border: 1px solid var(--border); border-radius: var(--r-sm); 
      padding: 0 32px 0 14px; font-family: var(--font-sans); font-size: 13px; font-weight: 600; color: var(--slate-700); cursor: pointer; min-width: 180px; 
      background-image: url('data:image/svg+xml;charset=US-ASCII,<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="%2364748B" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"></polyline></svg>'); 
      background-repeat: no-repeat; background-position: right 8px center; background-size: 16px; transition: all 0.2s ease; 
    }
    .aurora-select:hover { border-color: var(--brand-400); }
    .aurora-select:focus { outline: none; border-color: var(--brand-600); box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1); }

    .loader { display: flex; flex-direction: column; align-items: center; gap: 14px; padding: 80px 16px; color: var(--slate-500); }
    .empty-state { text-align: center; padding: 60px; color: var(--slate-500); mat-icon { font-size: 48px; height: 48px; width: 48px; display: block; margin: 0 auto 12px; opacity: 0.5;} h3 { margin: 0 0 8px; font-size: 18px; color: var(--slate-700);} }

    /* Glosario Integrado */
    .glossary-accordion { display: block; margin-bottom: 24px; }
    .glossary-panel { background: rgba(59, 130, 246, 0.03) !important; border: 1px solid rgba(59, 130, 246, 0.2) !important; border-radius: 8px !important; box-shadow: none !important; }
    .glossary-panel mat-panel-title { color: var(--brand-600); font-size: 13px; font-weight: 600; display: flex; align-items: center; gap: 8px; }
    .glossary-panel mat-icon { font-size: 18px; height: 18px; width: 18px; color: var(--brand-600); }
    
    .glossary-content { padding-top: 10px; display: flex; flex-wrap: wrap; gap: 16px;}
    .g-col { flex: 1 1 200px; font-size: 12.5px; color: var(--slate-700); line-height: 1.5; }
    .g-col strong { color: var(--slate-900); display: block; margin-bottom: 4px; }
    .full-width { flex: 1 1 100%; }

    /* Decisiones implícitas integradas */
    .limitation-box { background: rgba(245, 158, 11, 0.05); border: 1px dashed rgba(245, 158, 11, 0.3); border-radius: var(--r-sm); padding: 14px;}
    .limitation-box strong { color: var(--warn-700); display: flex; align-items: center; gap: 6px; mat-icon {font-size: 18px; height: 18px; width: 18px;}}
    .limit-list { margin: 8px 0 0 0; padding-left: 20px; font-size: 12.5px;}
    .limit-list li { margin-bottom: 4px;}

    /* Banner Ejecución */
    .exec-banner { display: flex; align-items: center; gap: 16px; background: var(--success-50); border-radius: var(--r-md); padding: 16px 20px; margin-bottom: 24px; border-left: 4px solid var(--success-500); }
    .exec-icon mat-icon { font-size: 28px; height: 28px; width: 28px; color: var(--success-600); }
    .exec-body { flex: 1; }
    .exec-title { font-weight: 700; font-size: 14px; color: var(--slate-900); font-family: var(--font-mono);}
    .exec-meta { font-size: 12px; color: var(--slate-600); margin-top: 4px; }
    .exec-stats { display: flex; gap: 8px; }
    .es { padding: 4px 12px; border-radius: var(--r-pill); font-size: 12px; font-weight: 700; &.ok { background: var(--success-100); color: var(--success-700); } }

    /* Tarjetas y Rejillas */
    .card { background: var(--bg-elevated); border: 1px solid var(--border); border-radius: var(--r-md); box-shadow: var(--shadow-sm); padding: 18px; margin-bottom: 24px; }
    .card-head { display: flex; flex-direction: column; justify-content: flex-start; margin-bottom: 18px; }
    .card-title { display: flex; align-items: center; gap: 8px; font-size: 15px; font-weight: 700; color: var(--slate-900); mat-icon { font-size: 18px; height: 18px; width: 18px; color: var(--brand-600); } }
    .card-sub { font-size: 12px; color: var(--slate-500); margin-left: 26px;}

    .config-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }
    .config-box { background: var(--slate-50); border-radius: var(--r-md); padding: 16px; border: 1px solid var(--border);}
    .config-box-title { display: flex; align-items: center; gap: 6px; font-size: 13px; font-weight: 700; color: var(--brand-700); margin: 0 0 14px; text-transform: uppercase; letter-spacing: 0.03em; mat-icon { font-size: 16px; height: 16px; width: 16px;} }
    
    .config-rows { display: flex; flex-direction: column; gap: 10px; }
    .cr { display: flex; align-items: center; justify-content: space-between; gap: 8px; font-size: 13px; border-bottom: 1px dashed var(--slate-200); padding-bottom: 6px;}
    .cr:last-child { border-bottom: none; padding-bottom: 0;}
    .cr-label { color: var(--slate-600); font-weight: 500;}
    .cr-val { font-weight: 700; color: var(--slate-900); }
    .badge-blue { color: var(--brand-600); } .badge-red { color: var(--danger-600); } .badge-green { color: var(--success-600); } .badge-orange { color: var(--warn-600); }

    /* Priors */
    .prior-node { margin-bottom: 16px; }
    .prior-node:last-child { margin-bottom: 0;}
    .pn-name { font-size: 11px; font-weight: 700; color: var(--slate-500); text-transform: uppercase; letter-spacing: .05em; margin-bottom: 6px; display: block; }
    .pn-bars { display: flex; flex-direction: column; gap: 6px; }
    .pn-bar-row { display: flex; align-items: center; gap: 8px; font-size: 12px; }
    .pn-state { width: 85px; color: var(--slate-700); font-weight: 600;}
    .pn-bar-wrap { flex: 1; height: 6px; background: var(--slate-200); border-radius: 3px; overflow: hidden; }
    .pn-bar { height: 100%; background: var(--slate-400); border-radius: 3px; &.green { background: var(--success-500); } &.red { background: var(--danger-500); } }
    .pn-pct { width: 35px; text-align: right; font-weight: 700; color: var(--slate-900); }

    /* Señales CPT */
    .signal-badge { padding: 4px 10px; border-radius: var(--r-pill); font-size: 11px; font-weight: 700; letter-spacing: 0.03em;
      &.buy { background: var(--success-100); color: var(--success-700); }
      &.sell { background: rgba(124, 58, 237, .15); color: #7C3AED; }
      &.hold { background: var(--warn-100); color: var(--warn-700); }
    }

    /* Tabla CPT */
    .cpt-filters { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }
    .cpt-table-wrap { overflow-x: auto; border: 1px solid var(--border); border-radius: var(--r-sm);}
    .cpt-table { width: 100%; border-collapse: collapse; font-size: 13px; text-align: left; }
    .cpt-table th { background: var(--slate-50); padding: 12px 14px; font-size: 11px; color: var(--slate-500); font-weight: 700; text-transform: uppercase; letter-spacing: .05em; border-bottom: 1px solid var(--border);}
    .cpt-table td { padding: 10px 14px; border-bottom: 1px solid var(--slate-100); color: var(--slate-800); }
    .cpt-table tr:hover { background: var(--slate-50); }
    
    .prob-bar-cell { display: flex; align-items: center; gap: 8px; min-width: 130px;}
    .pb-fill { height: 6px; background: var(--warn-400); border-radius: 3px; flex: 1; &.high { background: linear-gradient(to right, var(--success-500), var(--success-600)); } &.mid { background: var(--warn-400); } &.low { background: linear-gradient(to right, #7C3AED, #5b21b6); } }
    .prob-pct-text { font-weight: 700; min-width: 40px; text-align: right;}
    .txt-green { color: var(--success-700); } .txt-yellow { color: var(--warn-700); } .txt-purple { color: #7C3AED; }
    .muted { color: var(--slate-400); font-weight: 600;}

    /* Chips Evidencia */
    .ev-chip { display: inline-flex; align-items: center; gap: 3px; padding: 3px 8px; border-radius: var(--r-pill); font-size: 11px; font-weight: 600; text-transform: capitalize;}
    .bullish, .oversold, .uptrend { background: var(--success-100); color: var(--success-700); }
    .bearish, .overbought, .downtrend { background: var(--danger-100); color: var(--danger-700); }
    .neutral { background: var(--slate-100); color: var(--slate-600); }
    .vol-low { background: var(--brand-100); color: var(--brand-700); }
    .vol-high { background: var(--warn-100); color: var(--warn-700); }
  `],
})
export class AuditComponent implements OnInit {
  private traceSvc  = inject(TraceService);
  private reportSvc = inject(ReportService);

  loading       = true;
  trace: BayesianTrace | null = null;
  model: ModelConfig | null   = null;
  availableDates: ReportDateEntry[] = [];
  selectedDate  = '';

  cptRows:            any[] = [];
  cptFilterSentiment  = '';
  cptFilterRsi        = '';
  cptFilterTrend      = '';

  limitations: string[] = [];
  priorNodes: { name: string; states: { key: string; value: number }[] }[] = [];

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
        this.availableDates = dates;
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
    // Eliminado el operador de encadenamiento opcional si sabemos que viene.
    this.limitations = t.model_config.known_limitations || [];

    if (t.model_config.priors) {
      this.priorNodes = Object.entries(t.model_config.priors).map(([name, vals]) => ({
        name,
        states: Object.entries(vals)
          .filter(([k]) => k !== 'rationale')
          .map(([key, value]) => ({ key, value: value as number })),
      }));
    }

    if (t.model_config.cpt_market_direction.values_P_up) {
      this.cptRows = this.traceSvc.parseCptMatrix(t.model_config.cpt_market_direction.values_P_up);
    }
  }

  // Traducción estricta para la UI
  translateState(val: string): string {
    const dict: Record<string, string> = {
      bullish: 'Alcista', bearish: 'Bajista', neutral: 'Neutral',
      oversold: 'Sobreventa', overbought: 'Sobrecompra',
      uptrend: 'Alcista', downtrend: 'Bajista',
      low: 'Baja', high: 'Alta'
    };
    return dict[val] || val;
  }

  translateNode(val: string): string {
    const dict: Record<string, string> = {
      Sentiment: 'Sentimiento FinBERT',
      RSI: 'Fuerza (RSI)',
      Trend: 'Tendencia General',
      Volatility: 'Volatilidad'
    };
    return dict[val] || val;
  }

  isPositiveState(state: string): boolean {
    return ['bullish', 'oversold', 'uptrend'].includes(state);
  }

  isNegativeState(state: string): boolean {
    return ['bearish', 'overbought', 'downtrend'].includes(state);
  }
}