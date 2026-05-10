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
  status: string;
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

      <header class="page-head">
        <div class="page-head-text">
          <div class="page-eyebrow">
            <mat-icon>account_tree</mat-icon>
            <span>Arquitectura Serverless (AWS)</span>
          </div>
          <h1 class="page-title">Estado de la Infraestructura</h1>
          <p class="page-sub">
            Monitorización en tiempo real del pipeline de datos y modelos orquestado mediante AWS Step Functions.
          </p>
        </div>
        <button class="btn btn-ghost" (click)="refresh()">
          <mat-icon>refresh</mat-icon>
          <span>Refrescar datos</span>
        </button>
      </header>

      <mat-accordion class="glossary-accordion">
        <mat-expansion-panel class="glossary-panel">
          <mat-expansion-panel-header>
            <mat-panel-title>
              <mat-icon>lightbulb</mat-icon>
              <span>Arquitectura del Sistema: ¿Cómo se calculan los datos cada día? (Glosario)</span>
            </mat-panel-title>
          </mat-expansion-panel-header>
          
          <div class="glossary-content">
            <p class="g-intro">
              Cada día a las 22:00 UTC (tras el cierre del mercado americano), <strong>Amazon EventBridge</strong> despierta a nuestra máquina de estados (<strong>Step Functions</strong>), la cual ejecuta secuencialmente 5 microservicios (<strong>AWS Lambdas</strong>):
            </p>

            <div class="flow-container">
              <div class="f-step">
                <div class="bn-node bn-l1">
                  <div class="lambda-badge">λ1</div>
                  <div><div class="bn-label">Ingesta de Datos</div><small>yFinance + Finnhub</small></div>
                </div>
                <div class="f-text">
                  Se conecta a las APIs financieras para descargar los precios de cierre (OHLCV) de los ETFs y las últimas noticias publicadas. Los datos crudos se guardan en el <strong>Data Lake (Amazon S3)</strong>.
                </div>
              </div>
              <div class="f-arrow">→</div>

              <div class="f-step">
                <div class="bn-node bn-l2">
                  <div class="lambda-badge">λ2</div>
                  <div><div class="bn-label">Sentimiento</div><small>FinBERT (HuggingFace)</small></div>
                </div>
                <div class="f-text">
                  Carga el modelo de Inteligencia Artificial <strong>FinBERT</strong> para leer cada titular financiero descargado y clasificarlo matemáticamente en Positivo, Negativo o Neutral.
                </div>
              </div>
              <div class="f-arrow">→</div>

              <div class="f-step">
                <div class="bn-node bn-l3">
                  <div class="lambda-badge">λ3</div>
                  <div><div class="bn-label">Indicadores</div><small>Pandas TA</small></div>
                </div>
                <div class="f-text">
                  Procesa los precios crudos para calcular métricas técnicas complejas: Fuerza Relativa (RSI), Medias Móviles (Tendencia) y Bandas de Bollinger (Volatilidad).
                </div>
              </div>
              <div class="f-arrow">→</div>

              <div class="f-step">
                <div class="bn-node bn-l4">
                  <div class="lambda-badge">λ4</div>
                  <div><div class="bn-label">Red Bayesiana</div><small>pgmpy</small></div>
                </div>
                <div class="f-text">
                  Cruza los indicadores (λ3) con el sentimiento de las noticias (λ2) para emitir la <strong>Confianza Alcista</strong> y la decisión (Comprar/Mantener/Cash).
                </div>
              </div>
              <div class="f-arrow">→</div>

              <div class="f-step">
                <div class="bn-node bn-l5">
                  <div class="lambda-badge">λ5</div>
                  <div><div class="bn-label">Reporte Final</div><small>Consolidación</small></div>
                </div>
                <div class="f-text">
                  Calcula las métricas financieras (Sharpe, Drawdown, etc.) y genera el archivo <code>report.json</code> que está leyendo este Dashboard.
                </div>
              </div>
            </div>

            <div class="glossary-grid" style="margin-top: 16px; border-top: 1px solid var(--border); padding-top: 16px;">
              <div class="g-col">
                <strong>Batch (Ejecución):</strong> Un ciclo completo de las 5 Lambdas. Si alguna falla por problemas de API, el batch marca error para que el ingeniero lo revise.
              </div>
              <div class="g-col">
                <strong>Cobertura:</strong> Activos con señal generada / Total esperados. Puede ser menor a 100% de forma natural si un día no hay noticias suficientes para procesar un ETF.
              </div>
            </div>

          </div>
        </mat-expansion-panel>
      </mat-accordion>

      @if (loading) {
        <div class="loader">
          <mat-spinner diameter="40"></mat-spinner>
          <p>Cargando historial de ejecuciones desde AWS…</p>
        </div>
      } @else {

        <div class="status-row">
          <div class="status-pill ok">
            <span class="led"></span>
            <div>
              <div class="pill-val">{{ completedCount }}</div>
              <div class="pill-lbl">Ejecuciones Exitosas</div>
            </div>
          </div>
          <div class="status-pill err">
            <span class="led err-led"></span>
            <div>
              <div class="pill-val">{{ failedCount }}</div>
              <div class="pill-lbl">Ejecuciones Fallidas</div>
            </div>
          </div>
          <div class="status-pill info">
            <span class="led info-led"></span>
            <div>
              <div class="pill-val">{{ avgCoverage | number:'1.0-0' }}%</div>
              <div class="pill-lbl">Cobertura Media de Activos</div>
            </div>
          </div>
          <div class="status-pill neutral">
            <mat-icon>history</mat-icon>
            <div>
              <div class="pill-val">{{ batches.length }}</div>
              <div class="pill-lbl">Días Analizados</div>
            </div>
          </div>
        </div>

        <section class="charts-overview">
          
          <div class="chart-box span-4">
            <div class="card-head">
              <div class="card-title">
                <mat-icon>donut_large</mat-icon>
                <span>Estado de las Ejecuciones</span>
              </div>
            </div>
            <div class="donut-container">
              <div class="donut-chart-wrapper">
                <ngx-charts-pie-chart
                  [results]="statusPieChart"
                  [legend]="false"
                  [labels]="false"
                  [doughnut]="true"
                  [arcWidth]="0.35"
                  [customColors]="customStatusColors"
                  [view]="[220, 220]">
                </ngx-charts-pie-chart>
                <div class="donut-center">
                  <div class="dc-num">{{ batches.length }}</div>
                  <div class="dc-lbl">Totales</div>
                </div>
              </div>
              
              <div class="custom-legend">
                <div class="cl-item">
                  <span class="cl-color" style="background-color: #22C55E;"></span>
                  <span class="cl-label">EXITOSAS</span>
                  <span class="cl-value">{{ completedCount }}</span>
                </div>
                <div class="cl-item">
                  <span class="cl-color" style="background-color: #EF4444;"></span>
                  <span class="cl-label">FALLIDAS</span>
                  <span class="cl-value">{{ failedCount }}</span>
                </div>
              </div>
            </div>
          </div>

          <div class="chart-box span-8">
            <div class="card-head">
              <div class="card-title">
                <mat-icon>show_chart</mat-icon>
                <span>Cobertura Diaria de Activos</span>
              </div>
              <span class="card-sub">% de ETFs con Señal Generada</span>
            </div>
            <div class="chart-container-tall">
              <ngx-charts-bar-vertical
                [results]="coverageChart"
                [xAxis]="true" [yAxis]="true" [showGridLines]="true"
                [scheme]="coverageScheme"
                yAxisLabel="Cobertura (%)" [showYAxisLabel]="true"
                [showDataLabel]="true" [yScaleMax]="100" [barPadding]="16" [roundEdges]="true">
              </ngx-charts-bar-vertical>
            </div>
          </div>

        </section>

        <section class="charts-overview">
          
          <div class="chart-box span-6">
            <div class="card-head">
              <div class="card-title">
                <mat-icon>article</mat-icon>
                <span>Volumen de Noticias Procesadas</span>
              </div>
              <span class="card-sub">Titulares analizados por FinBERT</span>
            </div>
            <div class="chart-container-medium">
              <ngx-charts-bar-vertical
                [results]="headlinesChart"
                [xAxis]="true" [yAxis]="true" [showGridLines]="true"
                [scheme]="headlinesScheme"
                [showDataLabel]="true" [barPadding]="16" [roundEdges]="true">
              </ngx-charts-bar-vertical>
            </div>
          </div>

          <div class="chart-box span-6">
            <div class="card-head">
              <div class="card-title">
                <mat-icon>online_prediction</mat-icon>
                <span>Señales Emitidas</span>
              </div>
              <span class="card-sub">Decisiones calculadas por la Red Bayesiana</span>
            </div>
            <div class="chart-container-medium">
              <ngx-charts-bar-vertical
                [results]="signalsChart"
                [xAxis]="true" [yAxis]="true" [showGridLines]="true"
                [scheme]="signalsScheme"
                [showDataLabel]="true" [barPadding]="16" [roundEdges]="true">
              </ngx-charts-bar-vertical>
            </div>
          </div>

        </section>

        <section class="card timeline-card">
          <div class="card-head">
            <div class="card-title">
              <mat-icon>manage_search</mat-icon>
              <span>Logs de Ejecución Diaria</span>
            </div>
            <span class="card-sub">Despliega para ver el rendimiento de cada Lambda (Últimos 14 días)</span>
          </div>

          <div class="timeline">
            @for (batch of batches; track batch.date) {
              <mat-expansion-panel class="batch-panel">

                <mat-expansion-panel-header [collapsedHeight]="'76px'" [expandedHeight]="'76px'">
                  <div class="batch-row">
                    <span class="batch-status {{ getStatusClass(batch.status) }}">
                      <mat-icon>{{ statusIcon(batch.status) }}</mat-icon>
                      {{ translateStatus(batch.status) }}
                    </span>
                    <span class="batch-date">{{ batch.date }}</span>

                    <div class="batch-meta">
                      <span class="m-item" matTooltip="Señales generadas vs Total ETFs">
                        <mat-icon>storage</mat-icon>
                        {{ batch.tickers_with_signals }}/{{ batch.tickers_expected }} Activos
                      </span>
                      <span class="m-item" matTooltip="Noticias procesadas por NLP">
                        <mat-icon>article</mat-icon>
                        {{ batch.headlines_scored }} Noticias
                      </span>
                    </div>

                    <div class="cov-bar" [matTooltip]="'Cobertura: ' + (batch.coverage_ratio*100|number:'1.0-0') + '%'">
                      <div class="cov-fill"
                           [style.width.%]="batch.coverage_ratio*100"
                           [class.full]="batch.coverage_ratio >= 0.95"
                           [class.partial]="batch.coverage_ratio < 0.95 && batch.coverage_ratio >= 0.5"
                           [class.poor]="batch.coverage_ratio < 0.5"></div>
                    </div>
                    <span class="cov-pct">{{ (batch.coverage_ratio*100)|number:'1.0-0' }}%</span>
                  </div>
                </mat-expansion-panel-header>

                <div class="stages">
                  <h4>Métricas detalladas por Microservicio</h4>
                  <div class="stages-grid">

                    @for (stage of getStages(batch.stage_kpis); track stage.key) {
                      <div class="stage">
                        <div class="stage-head">
                          <mat-icon>{{ stageIcon(stage.key) }}</mat-icon>
                          <span class="stage-title">{{ stageLabel(stage.key) }}</span>
                        </div>
                        <div class="stage-body">
                          @for (kv of objectEntries(stage.value); track kv[0]) {
                            <div class="stage-metric">
                              <span class="sm-key">{{ translateKey(kv[0]) }}</span>
                              <span class="sm-val">{{ kv[1] }}</span>
                            </div>
                          }
                        </div>
                      </div>
                    }

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
    .page { max-width: var(--content-max); margin: 0 auto; padding-bottom: 40px;}
    .page-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 24px; flex-wrap: wrap; margin-bottom: 22px; }
    .page-eyebrow { display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px; background: rgba(59, 130, 246, .12); color: var(--brand-600); border-radius: var(--r-pill); font-size: 11px; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; margin-bottom: 10px; mat-icon { font-size: 14px; height: 14px; width: 14px; } }
    .page-title { font-size: 26px; font-weight: 700; color: var(--slate-900); letter-spacing: -.02em; }
    .page-sub { color: var(--slate-500); font-size: 13px; margin-top: 6px; max-width: 760px; }

    .btn { display: inline-flex; align-items: center; gap: 6px; padding: 8px 14px; border-radius: var(--r-sm); border: 1px solid var(--border); font-family: var(--font-sans); font-size: 13px; font-weight: 600; cursor: pointer; transition: all .15s; background: var(--bg-elevated); color: var(--slate-700); mat-icon { font-size: 18px; height: 18px; width: 18px; } }
    .btn:hover { border-color: var(--brand-300); color: var(--brand-600); background: var(--slate-50); }

    .loader { display: flex; flex-direction: column; align-items: center; gap: 14px; padding: 80px 16px; color: var(--slate-500); }

    /* Glosario Integrado con Flujo */
    .glossary-accordion { display: block; margin-bottom: 24px; }
    .glossary-panel { background: rgba(59, 130, 246, 0.03) !important; border: 1px solid rgba(59, 130, 246, 0.2) !important; border-radius: 8px !important; box-shadow: none !important; }
    .glossary-panel mat-panel-title { color: var(--brand-600); font-size: 13px; font-weight: 600; display: flex; align-items: center; gap: 8px; }
    .glossary-panel mat-icon { font-size: 18px; height: 18px; width: 18px; color: var(--brand-600); }
    
    .glossary-content { padding-top: 10px; }
    .g-intro { margin-bottom: 16px; color: var(--slate-600); font-size: 13px; }
    .glossary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; font-size: 12.5px; color: var(--slate-700); line-height: 1.5; }
    .g-col strong { color: var(--slate-900); display: block; margin-bottom: 4px; }

    /* Layout columnar del flujo de Arquitectura */
    .flow-container { display: flex; align-items: flex-start; gap: 12px; flex-wrap: nowrap; overflow-x: auto; padding-top: 14px; padding-bottom: 14px; }
    .f-step { flex: 1; min-width: 160px; display: flex; flex-direction: column; gap: 8px; }
    .f-arrow { margin-top: 15px; font-size: 24px; font-weight: bold; color: var(--slate-400); }
    
    .f-text { font-size: 11px; color: var(--slate-600); line-height: 1.4; padding: 10px; background: #fff; border-radius: 6px; border: 1px solid var(--border); }
    .f-text strong { color: var(--slate-900); }
    
    .bn-node { position: relative; display: flex; align-items: center; gap: 10px; padding: 10px 14px; border-radius: var(--r-sm); background: var(--bg-elevated); border: 1px solid var(--border); box-shadow: var(--shadow-sm); }
    .lambda-badge { position: absolute; top: -8px; left: -8px; background: var(--brand-600); color: #fff; font-size: 10px; font-weight: 700; padding: 2px 6px; border-radius: 4px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);}
    .bn-label { font-size: 12px; font-weight: 700; color: var(--slate-900); }
    .bn-node small { font-size: 10px; color: var(--slate-500); }

    /* Status pills */
    .status-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin-bottom: 24px; }
    .status-pill { display: flex; align-items: center; gap: 12px; padding: 14px 18px; background: var(--bg-elevated); border: 1px solid var(--border); border-radius: var(--r-md); box-shadow: var(--shadow-sm); mat-icon { font-size: 20px; height: 20px; width: 20px; } }
    .status-pill .pill-val { font-size: 22px; font-weight: 700; color: var(--slate-900); font-variant-numeric: tabular-nums; }
    .status-pill .pill-lbl { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; color: var(--slate-500); }
    
    .led { width: 10px; height: 10px; border-radius: 50%; box-shadow: 0 0 0 3px rgba(34, 197, 94, .12); background: var(--success-500); animation: pulse-dot 2s ease-in-out infinite; }
    .err-led  { background: var(--danger-500);  box-shadow: 0 0 0 3px rgba(239,68,68,.12); }
    .info-led { background: var(--brand-500);   box-shadow: 0 0 0 3px rgba(59,130,246,.15); }
    @keyframes pulse-dot { 0%,100% { transform: scale(1); } 50%     { transform: scale(1.25); } }
    .status-pill.ok      { border-left: 4px solid var(--success-500); }
    .status-pill.err     { border-left: 4px solid var(--danger-500); }
    .status-pill.info    { border-left: 4px solid var(--brand-500); }
    .status-pill.neutral { border-left: 4px solid var(--slate-400); mat-icon { color: var(--slate-400); } }

    /* Gráficos Rejilla */
    .charts-overview { display: grid; grid-template-columns: repeat(12, 1fr); gap: 16px; margin-bottom: 24px; }
    .span-4 { grid-column: span 4; }
    .span-6 { grid-column: span 6; }
    .span-8 { grid-column: span 8; }
    @media (max-width: 1100px) { .span-4, .span-6, .span-8 { grid-column: span 12; } }

    .chart-box { background: var(--bg-elevated); border: 1px solid var(--border); border-radius: var(--r-md); padding: 18px; box-shadow: var(--shadow-sm); display: flex; flex-direction: column; }
    
    .card-head { display: flex; flex-direction: column; justify-content: flex-start; margin-bottom: 14px; }
    .card-title { display: flex; align-items: center; gap: 8px; font-size: 14px; font-weight: 600; color: var(--slate-900); mat-icon { font-size: 18px; height: 18px; width: 18px; color: var(--brand-600); } }
    .card-sub { font-size: 12px; color: var(--slate-500); margin-left: 26px;}

    /* Contenedores Gráficos */
    .chart-container-tall { width: 100%; height: 260px; display: flex; align-items: center; justify-content: center; }
    .chart-container-medium { width: 100%; height: 220px; display: flex; align-items: center; justify-content: center; }
    
    /* Donut Inteligente para Estado de Ejecuciones */
    .donut-container { display: flex; align-items: center; justify-content: center; gap: 20px; height: 260px;}
    .donut-chart-wrapper { position: relative; width: 220px; height: 220px; }
    .donut-center { position: absolute; top: 0; left: 0; width: 100%; height: 100%; display: flex; flex-direction: column; align-items: center; justify-content: center; pointer-events: none; }
    .dc-num { font-size: 32px; font-weight: 700; color: var(--slate-900); line-height: 1; }
    .dc-lbl { font-size: 12px; color: var(--slate-500); font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase;}

    .custom-legend { display: flex; flex-direction: column; gap: 12px; min-width: 100px; }
    .cl-item { display: flex; align-items: center; gap: 8px; font-size: 13px; }
    .cl-color { width: 12px; height: 12px; border-radius: 3px; display: inline-block; }
    .cl-label { flex: 1; font-weight: 600; color: var(--slate-700); }
    .cl-value { font-weight: 700; color: var(--slate-900); font-size: 15px; }

    /* Timeline */
    .timeline-card { padding: 18px; background: var(--bg-elevated); border: 1px solid var(--border); border-radius: var(--r-md); box-shadow: var(--shadow-sm);}
    .timeline { display: flex; flex-direction: column; gap: 10px; }
    .batch-panel { border: 1px solid var(--border) !important; border-radius: var(--r-md) !important; box-shadow: none !important; transition: border-color .15s, box-shadow .15s; }
    .batch-panel:hover { border-color: var(--brand-300) !important; box-shadow: var(--shadow-sm) !important; }
    
    .batch-row { display: flex; align-items: center; gap: 16px; flex: 1; flex-wrap: wrap; }
    .batch-status { display: inline-flex; align-items: center; gap: 5px; padding: 4px 12px; border-radius: var(--r-pill); font-size: 11px; font-weight: 700; letter-spacing: .03em; mat-icon { font-size: 14px; height: 14px; width: 14px; } }
    .batch-status.completed { background: var(--success-100); color: var(--success-700); }
    .batch-status.failed    { background: var(--danger-100);  color: var(--danger-700); }
    
    .batch-date { font-weight: 700; font-size: 14px; color: var(--slate-900); min-width: 100px; }
    .batch-meta { display: flex; gap: 16px; flex: 1;}
    .m-item { display: inline-flex; align-items: center; gap: 6px; font-size: 13px; font-weight: 600; color: var(--slate-600); mat-icon { font-size: 16px; height: 16px; width: 16px; color: var(--slate-400); } }
    
    .cov-bar { flex: 1; min-width: 100px; max-width: 180px; height: 6px; background: var(--slate-100); border-radius: var(--r-pill); overflow: hidden; }
    .cov-fill { height: 100%; border-radius: var(--r-pill); transition: width .5s; &.full { background: linear-gradient(to right, var(--success-500), var(--success-600)); } &.partial { background: linear-gradient(to right, var(--warn-500), var(--warn-600)); } &.poor { background: linear-gradient(to right, var(--danger-500), var(--danger-600)); } }
    .cov-pct { font-size: 13px; font-weight: 700; color: var(--slate-700); font-variant-numeric: tabular-nums; width: 40px; text-align: right;}

    /* Stage cards (Logs internos) */
    .stages { padding: 8px 0 16px; }
    .stages h4 { font-size: 13px; font-weight: 600; color: var(--slate-500); text-transform: uppercase; letter-spacing: 0.05em; margin: 0 0 12px; border-bottom: 1px solid var(--border); padding-bottom: 8px;}
    .stages-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 12px; }
    
    .stage { background: #fff; border-radius: var(--r-sm); padding: 14px; border: 1px solid var(--border); box-shadow: var(--shadow-xs);}
    .stage-head { display: flex; align-items: center; gap: 8px; margin-bottom: 12px; font-size: 13px; font-weight: 700; color: var(--brand-700); mat-icon { font-size: 18px; height: 18px; width: 18px; color: var(--brand-600); } }
    
    .stage-body { display: flex; flex-direction: column; gap: 6px; }
    .stage-metric { display: flex; justify-content: space-between; align-items: center; font-size: 12px; padding-bottom: 4px; border-bottom: 1px dashed var(--slate-100);}
    .stage-metric:last-child { border-bottom: none; padding-bottom: 0;}
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
  
  // Gráficos
  statusPieChart: ChartDataPoint[] = [];
  coverageChart: ChartDataPoint[] = [];
  headlinesChart: ChartDataPoint[] = [];
  signalsChart: ChartDataPoint[] = [];

  // Paletas de color
  coverageScheme: any = { domain: ['#06B6D4'] }; // Cyan
  headlinesScheme: any = { domain: ['#8B5CF6'] }; // Violeta
  signalsScheme: any = { domain: ['#3B82F6'] }; // Azul

  customStatusColors = (name: string) => {
    if (name === 'Éxito') return '#22C55E';
    return '#EF4444'; // Error
  };

  // KPI Logic
  get completedCount() { return this.batches.filter(b => b.status === 'COMPLETED' || b.status === 'STARTED').length; }
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
        const toLoad = dates.slice(0, 14); // Cargamos los últimos 14 días
        return forkJoin(toLoad.map(d => this.reportSvc.loadReport(d.date)));
      })
    ).subscribe({
      next: (reports: any[]) => {
        this.batches = reports.map((r: DailyReport) => {
          
          // --- MAGIA: Inyectamos los KPIs de la Lambda 5 artificialmente ---
          const stage_kpis = { ...r.pipeline_health.stage_kpis };
          stage_kpis['report'] = {
            tickers_reported: r.summary?.total_tickers || r.pipeline_health.tickers_expected,
            total_closed_trades: r.summary?.total_closed_trades || 0
          };

          return {
            date: r.report_date,
            status: r.pipeline_health.batch_status,
            tickers_expected: r.pipeline_health.tickers_expected,
            tickers_with_signals: r.pipeline_health.tickers_with_signals,
            headlines_scored: r.pipeline_health.headlines_scored,
            coverage_ratio: r.pipeline_health.coverage_ratio,
            stage_kpis: stage_kpis, // Ahora incluye la Lambda 5
          };
        });

        this.latestHealth = reports[0]?.pipeline_health ?? null;
        this.buildCharts();
        this.loading = false;
      },
      error: () => { this.loading = false; },
    });
  }

  private buildCharts() {
    // 1. Gráfico de Estados (Tarta)
    const counts: Record<string, number> = { COMPLETED: 0, FAILED: 0 };
    for (const b of this.batches) {
      if (b.status === 'COMPLETED' || b.status === 'STARTED') counts['COMPLETED']++;
      else counts['FAILED']++;
    }
    this.statusPieChart = [
      { name: 'Éxito', value: counts['COMPLETED'] },
      { name: 'Error', value: counts['FAILED'] }
    ].filter(i => i.value > 0);

    // 2. Gráfico de Cobertura (Barras) -> Invertimos para cronología correcta
    const sortedBatches = [...this.batches].reverse();

    this.coverageChart = sortedBatches.map(b => ({
      name: b.date.slice(5),
      value: +(b.coverage_ratio * 100).toFixed(1),
    }));

    // 3. Gráfico de Volumen de Noticias
    this.headlinesChart = sortedBatches.map(b => ({
      name: b.date.slice(5),
      value: b.headlines_scored,
    }));

    // 4. Gráfico de Señales Emitidas
    this.signalsChart = sortedBatches.map(b => ({
      name: b.date.slice(5),
      value: b.tickers_with_signals,
    }));
  }

  // Utilidades
  getStatusClass(s: string) {
    return (s === 'COMPLETED' || s === 'STARTED') ? 'completed' : 'failed';
  }

  statusIcon(s: string) {
    return (s === 'COMPLETED' || s === 'STARTED') ? 'check_circle' : 'error';
  }
  
  translateStatus(s: string) {
    return (s === 'COMPLETED' || s === 'STARTED') ? 'COMPLETADO' : 'ERROR';
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
      ingestion: 'λ1 Ingesta API', sentiment: 'λ2 Modelo FinBERT',
      indicators: 'λ3 Índices Técnicos', bayesian: 'λ4 Inferencia Bayesiana', report: 'λ5 Reporte Fin.',
    };
    return m[key] ?? key;
  }
  
  translateKey(key: string): string {
    const dict: Record<string, string> = {
      'headlines_total': 'Noticias Totales',
      'ohlcv_rows_total': 'Filas de Precios',
      'tickers_expected': 'ETFs Esperados',
      'tickers_with_news': 'ETFs con Noticias',
      'tickers_with_ohlcv': 'ETFs con Precios',
      'tickers_in_ohlcv': 'ETFs Procesados',
      'tickers_with_indicators': 'Cálculos de RSI/SMA',
      'tickers_in_news': 'ETFs Analizados (NLP)',
      'headlines_skipped': 'Noticias Descartadas',
      'headlines_processed': 'Noticias Clasificadas',
      'trace_s3_key': 'Archivo Trace S3',
      'model_version': 'Versión del Modelo',
      'tickers_skipped': 'ETFs Omitidos',
      'signals_generated': 'Decisiones Generadas',
      'tickers_with_sentiment': 'Cruces NLP Exitósos',
      'tickers_reported': 'Activos Reportados',      // Nueva Key
      'total_closed_trades': 'Operaciones Cerradas'  // Nueva Key
    };
    return dict[key] || key.replace(/_/g, ' ');
  }

  // Fuerza el orden estricto: λ1, λ2, λ3, λ4, λ5
  getStages(kpis: Record<string, any>) {
    if (!kpis) return [];
    const orderedKeys = ['ingestion', 'sentiment', 'indicators', 'bayesian', 'report'];
    const result = [];
    
    for (const key of orderedKeys) {
      if (kpis[key]) {
        result.push({ key, value: kpis[key] });
      }
    }
    
    // Por seguridad, si aparece una lambda no registrada
    for (const key of Object.keys(kpis)) {
      if (!orderedKeys.includes(key)) {
        result.push({ key, value: kpis[key] });
      }
    }
    
    return result;
  }

  objectEntries(obj: Record<string, any>): [string, any][] {
    return Object.entries(obj);
  }
}