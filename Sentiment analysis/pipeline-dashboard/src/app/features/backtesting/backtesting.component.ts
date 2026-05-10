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
import { NgxChartsModule } from '@swimlane/ngx-charts';
import { switchMap } from 'rxjs';
import { ReportService } from '../../core/services/report.service';
import { DailyReport, TickerView, ReportDateEntry } from '../../core/models/report.model';
import { ChartDataPoint, ChartSeries } from '../../core/models/pipeline.model';

@Component({
  selector: 'app-backtesting',
  standalone: true,
  imports: [
    CommonModule, FormsModule, MatTableModule, MatSortModule,
    MatButtonModule, MatIconModule, MatProgressSpinnerModule,
    MatTooltipModule, MatExpansionModule, NgxChartsModule,
  ],
  template: `
    <div class="page">

      <header class="page-head">
        <div class="page-head-text">
          <div class="page-eyebrow">
            <mat-icon>insights</mat-icon>
            <span>Laboratorio Financiero</span>
          </div>
          <h1 class="page-title">Rendimiento y Riesgo (Backtesting)</h1>
          <p class="page-sub">
            Simulación de inversión sobre los últimos <strong>{{ summary.total_tickers ? 365 : 0 }} días</strong> usando un capital inicial virtual de <strong>$10,000</strong> por activo.
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
        </div>
      </header>

      <mat-accordion class="glossary-accordion">
        <mat-expansion-panel class="glossary-panel">
          <mat-expansion-panel-header>
            <mat-panel-title>
              <mat-icon>lightbulb</mat-icon>
              <span>¿Cómo interpretar estas métricas financieras? (Glosario)</span>
            </mat-panel-title>
          </mat-expansion-panel-header>
          
          <div class="glossary-content">
            <div class="g-col">
              <strong>Simulación (Backtesting):</strong> Evaluamos cómo habría rendido nuestra IA en el último año si hubiera operado con dinero real, usando una estrategia conservadora: compra cuando hay señales positivas y vende para quedarse en efectivo (liquidez) ante el peligro.
            </div>
            <div class="g-col">
              <strong>Mejora vs Mercado (Alpha):</strong> Es la "ventaja competitiva" del algoritmo. Compara el beneficio de la IA frente a la estrategia pasiva de comprar el ETF y no hacer nada (Buy & Hold). Si es positivo, la IA gana al mercado.
            </div>
            <div class="g-col">
              <strong>Calidad del Riesgo (Sharpe Ratio):</strong> Relaciona el beneficio obtenido con el riesgo (volatilidad) asumido. Un valor superior a 1.0 indica un comportamiento excelente.
            </div>
            <div class="g-col">
              <strong>Factor de Beneficio (Profit Factor):</strong> Indica cuánto dinero se gana por cada dólar que se pierde. Un valor superior a 1.0 significa que la estrategia es rentable (ej: 2.0 significa que ganas el doble de lo que pierdes).
            </div>
            <div class="g-col">
              <strong>Caída Máxima (Max Drawdown):</strong> El mayor porcentaje de dinero que la cartera llegó a perder desde su pico más alto. Mantenerse cerca del 0% es el objetivo principal para proteger el capital.
            </div>
          </div>
        </mat-expansion-panel>
      </mat-accordion>

      @if (loading) {
        <div class="loader">
          <mat-spinner diameter="40"></mat-spinner>
          <p>Calculando métricas de rendimiento…</p>
        </div>
      } @else {

        <section class="kpi-grid">
          <article class="kpi"
                   [class.kpi-pos]="summary.avg_cumulative_return>0"
                   [class.kpi-neg]="summary.avg_cumulative_return<0">
            <div class="kpi-head">
              <span class="kpi-label">Beneficio Medio (Estrategia IA)</span>
              <span class="kpi-icon"><mat-icon>account_balance_wallet</mat-icon></span>
            </div>
            <div class="kpi-value">
              {{ summary.avg_cumulative_return>0?'+':'' }}{{ (summary.avg_cumulative_return*100)|number:'1.2-2' }}<span class="unit">%</span>
            </div>
            <div class="kpi-foot">
              <span class="kpi-sub">Media de {{ summary.total_tickers }} activos</span>
            </div>
          </article>

          <article class="kpi" [class.kpi-pos]="summary.avg_sharpe_ratio>1">
            <div class="kpi-head">
              <span class="kpi-label">Ratio de Sharpe (Riesgo)</span>
              <span class="kpi-icon"><mat-icon>balance</mat-icon></span>
            </div>
            <div class="kpi-value">{{ summary.avg_sharpe_ratio|number:'1.2-2' }}</div>
            <div class="kpi-foot">
              <span class="quality" [class.good]="summary.avg_sharpe_ratio>1">{{ qualityLabel(summary.avg_sharpe_ratio) }}</span>
              <span class="kpi-sub">Anualizado (Tasa libre 2%)</span>
            </div>
          </article>

          <article class="kpi kpi-neg">
            <div class="kpi-head">
              <span class="kpi-label">Caída Máxima (Drawdown)</span>
              <span class="kpi-icon"><mat-icon>water_drop</mat-icon></span>
            </div>
            <div class="kpi-value">{{ (summary.avg_max_drawdown*100)|number:'1.2-2' }}<span class="unit">%</span></div>
            <div class="kpi-foot">
              <span class="kpi-sub">Peor escenario del sistema</span>
            </div>
          </article>

          <article class="kpi">
            <div class="kpi-head">
              <span class="kpi-label">Operaciones Completadas</span>
              <span class="kpi-icon"><mat-icon>swap_horiz</mat-icon></span>
            </div>
            <div class="kpi-value">{{ summary.total_closed_trades }}</div>
            <div class="kpi-foot"><span class="kpi-sub">Ciclos de Compra/Venta totales</span></div>
          </article>

          <article class="kpi" [class.kpi-pos]="winnersCount >= summary.total_tickers / 2">
            <div class="kpi-head">
              <span class="kpi-label">ETFs en Positivo</span>
              <span class="kpi-icon"><mat-icon>emoji_events</mat-icon></span>
            </div>
            <div class="kpi-value">
              {{ winnersCount }}<span class="unit">/{{ summary.total_tickers }}</span>
            </div>
            <div class="kpi-foot"><span class="kpi-sub">Generaron ganancias este año</span></div>
          </article>
        </section>

        <section class="card chart-card">
          <div class="card-head">
            <div class="card-title">
              <mat-icon>bar_chart</mat-icon>
              <span>Rentabilidad Acumulada: Inteligencia Artificial vs Mercado</span>
            </div>
            <div class="legend">
              <span class="leg-dot" style="background:var(--brand-600)"></span>
              <span>Estrategia IA</span>
              <span class="leg-dot" style="background:var(--slate-400); margin-left:14px"></span>
              <span>Mercado (Buy &amp; Hold)</span>
            </div>
          </div>
          <div class="chart-host">
            <ngx-charts-bar-vertical-2d
              [results]="returnChart"
              [xAxis]="true" [yAxis]="true" [showGridLines]="false"
              [scheme]="compareScheme"
              yAxisLabel="Rentabilidad Anual (%)" [showYAxisLabel]="true"
              [showDataLabel]="true" [groupPadding]="4" [barPadding]="2" [roundEdges]="false">
            </ngx-charts-bar-vertical-2d>
          </div>
        </section>

        <section class="card chart-card">
          <div class="card-head">
            <div class="card-title">
              <mat-icon>add_circle_outline</mat-icon>
              <span>Mejora vs Mercado (Alpha)</span>
            </div>
            <span class="card-sub">Si es Verde, la IA ha superado al mercado. Si es Rojo, la IA ha rendido menos.</span>
          </div>
          <div class="chart-host">
            <ngx-charts-bar-vertical
              [results]="alphaChart"
              [xAxis]="true" [yAxis]="true" [showGridLines]="true"
              [customColors]="customColorsAlpha"
              yAxisLabel="Exceso de Retorno (%)" [showYAxisLabel]="true"
              [showDataLabel]="true" [roundEdges]="true">
            </ngx-charts-bar-vertical>
          </div>
        </section>

        <section class="row-2">
          <div class="card chart-card">
            <div class="card-head">
              <div class="card-title">
                <mat-icon>speed</mat-icon>
                <span>Ratio de Sharpe Anualizado</span>
              </div>
              <span class="card-sub">Calidad del riesgo por ETF</span>
            </div>
            <div class="chart-host-small">
              <ngx-charts-bar-horizontal
                [results]="sharpeChart"
                [xAxis]="true" [yAxis]="true" [showGridLines]="true"
                [customColors]="customColorsSharpe"
                xAxisLabel="Valor del Ratio" [showXAxisLabel]="true"
                [showDataLabel]="true" [roundEdges]="true">
              </ngx-charts-bar-horizontal>
            </div>
            <div class="hint hint-info">
              <mat-icon>info</mat-icon>
              <span>Sharpe &gt; 1 = Comportamiento ideal</span>
            </div>
          </div>

          <div class="card chart-card">
            <div class="card-head">
              <div class="card-title">
                <mat-icon>arrow_downward</mat-icon>
                <span>Caída Máxima (Max Drawdown)</span>
              </div>
              <span class="card-sub">Riesgo por ETF</span>
            </div>
            <div class="chart-host-small">
              <ngx-charts-bar-horizontal
                [results]="drawdownChart"
                [xAxis]="true" [yAxis]="true" [showGridLines]="true"
                [scheme]="drawdownScheme"
                xAxisLabel="Caída sufrida (%)" [showXAxisLabel]="true"
                [showDataLabel]="true" [roundEdges]="true">
              </ngx-charts-bar-horizontal>
            </div>
            <div class="hint hint-warn">
              <mat-icon>warning_amber</mat-icon>
              <span>Una caída mayor al 15% indica una mala protección de capital</span>
            </div>
          </div>
        </section>

        <section class="card table-card">
          <div class="card-head">
            <div class="card-title">
              <mat-icon>table_chart</mat-icon>
              <span>Detalle Financiero por Activo</span>
            </div>
            <span class="card-sub">Desglose de métricas de los últimos 365 días</span>
          </div>
          
          <table mat-table [dataSource]="tableSource" matSort class="aurora-table">

            <ng-container matColumnDef="ticker">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>Activo</th>
              <td mat-cell *matCellDef="let r">
                <span class="ticker-name">{{ r.ticker }}</span>
              </td>
            </ng-container>

            <ng-container matColumnDef="signal">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>Decisión Actual</th>
              <td mat-cell *matCellDef="let r">
                <span class="signal-pill {{ r.signal.toLowerCase() }}">
                  {{ r.signal === 'BUY' ? 'COMPRAR' : r.signal === 'SELL' ? 'CASH' : 'MANTENER' }}
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="return">
              <th mat-header-cell *matHeaderCellDef mat-sort-header="cumulative_return">Rentabilidad IA</th>
              <td mat-cell *matCellDef="let r">
                <span class="num" [class.pos]="r.cumulative_return>0" [class.neg]="r.cumulative_return<0">
                  {{ r.cumulative_return>0?'+':'' }}{{ (r.cumulative_return*100)|number:'1.2-2' }}%
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="bh">
              <th mat-header-cell *matHeaderCellDef mat-sort-header="buy_hold_return">Mercado (B&H)</th>
              <td mat-cell *matCellDef="let r">
                <span class="num">{{ r.buy_hold_return>0?'+':'' }}{{ (r.buy_hold_return*100)|number:'1.2-2' }}%</span>
              </td>
            </ng-container>

            <ng-container matColumnDef="alpha">
              <th mat-header-cell *matHeaderCellDef mat-sort-header="alpha_vs_benchmark">Mejora vs Mercado</th>
              <td mat-cell *matCellDef="let r">
                <span class="num bold" [class.pos]="r.alpha_vs_benchmark>0" [class.neg]="r.alpha_vs_benchmark<0">
                  {{ r.alpha_vs_benchmark>0?'+':'' }}{{ (r.alpha_vs_benchmark*100)|number:'1.2-2' }}%
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="sharpe">
              <th mat-header-cell *matHeaderCellDef mat-sort-header="sharpe_ratio">Ratio Sharpe</th>
              <td mat-cell *matCellDef="let r">
                <span class="num" [class.pos]="r.sharpe_ratio>1" [class.neg]="r.sharpe_ratio<0">{{ r.sharpe_ratio|number:'1.2-2' }}</span>
              </td>
            </ng-container>

            <ng-container matColumnDef="drawdown">
              <th mat-header-cell *matHeaderCellDef mat-sort-header="max_drawdown">Caída Máx.</th>
              <td mat-cell *matCellDef="let r">
                <span class="num neg">{{ (r.max_drawdown*100)|number:'1.2-2' }}%</span>
              </td>
            </ng-container>

            <ng-container matColumnDef="equity">
              <th mat-header-cell *matHeaderCellDef mat-sort-header="final_equity">Capital Final</th>
              <td mat-cell *matCellDef="let r" matTooltip="Empezando con $10,000">
                <span class="num" [class.pos]="r.final_equity>10000" [class.neg]="r.final_equity<10000">
                  $&nbsp;{{ r.final_equity|number:'1.0-0' }}
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="trades">
              <th mat-header-cell *matHeaderCellDef mat-sort-header="trades_closed">Ciclos</th>
              <td mat-cell *matCellDef="let r">
                <span class="num">{{ r.trades_closed }}</span>
              </td>
            </ng-container>

            <ng-container matColumnDef="winrate">
              <th mat-header-cell *matHeaderCellDef mat-sort-header="win_rate">Tasa Acierto</th>
              <td mat-cell *matCellDef="let r">
                <div class="winrate-cell">
                  <div class="wr-bar"><div class="wr-fill" [style.width.%]="r.win_rate*100" [class.high]="r.win_rate>=0.5"></div></div>
                  <span class="num" [class.pos]="r.win_rate>=0.5">{{ (r.win_rate*100)|number:'1.0-0' }}%</span>
                </div>
              </td>
            </ng-container>

            <ng-container matColumnDef="pf">
              <th mat-header-cell *matHeaderCellDef mat-sort-header="profit_factor" matTooltip="Beneficios Brutos / Pérdidas Brutas (>1 es ganancia)">Factor Beneficio</th>
              <td mat-cell *matCellDef="let r">
                <span class="num" [class.pos]="r.profit_factor>1" [class.neg]="r.profit_factor<1">{{ r.profit_factor|number:'1.2-2' }}</span>
              </td>
            </ng-container>

            <tr mat-header-row *matHeaderRowDef="tableCols; sticky: true"></tr>
            <tr mat-row *matRowDef="let r; columns: tableCols;"></tr>
            
            <tr *matNoDataRow>
              <td [attr.colspan]="tableCols.length" class="no-data">
                <mat-icon>info</mat-icon>
                <p>Calculando datos detallados...</p>
              </td>
            </tr>
          </table>
        </section>

      }
    </div>
  `,
  styles: [`
    .page { max-width: var(--content-max); margin: 0 auto; padding-bottom: 40px;}
    .page-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 24px; flex-wrap: wrap; margin-bottom: 22px; }
    .page-eyebrow { display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px; background: rgba(20, 184, 166, .12); color: var(--accent-teal); border-radius: var(--r-pill); font-size: 11px; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; margin-bottom: 10px; mat-icon { font-size: 14px; height: 14px; width: 14px; } }
    .page-title { font-size: 26px; font-weight: 700; color: var(--slate-900); letter-spacing: -.02em; }
    .page-sub { color: var(--slate-500); font-size: 13px; margin-top: 6px; max-width: 760px; }
    
    /* Filtros Personalizados nativos */
    .page-actions { display: flex; gap: 16px; flex-wrap: wrap; align-items: center; }
    .filter-group { display: flex; flex-direction: column; gap: 4px; }
    .filter-group label { font-size: 11px; font-weight: 600; color: var(--slate-500); text-transform: uppercase; letter-spacing: 0.05em; }
    .aurora-select { appearance: none; background-color: var(--bg-elevated); border: 1px solid var(--border); border-radius: var(--r-sm); padding: 10px 32px 10px 14px; font-family: var(--font-sans); font-size: 14px; font-weight: 600; color: var(--slate-700); cursor: pointer; min-width: 180px; background-image: url('data:image/svg+xml;charset=US-ASCII,<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="%2364748B" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"></polyline></svg>'); background-repeat: no-repeat; background-position: right 8px center; background-size: 16px; transition: all 0.2s ease; }
    .aurora-select:hover { border-color: var(--brand-400); }
    .aurora-select:focus { outline: none; border-color: var(--brand-600); box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1); }

    /* Glosario */
    .glossary-accordion { display: block; margin-bottom: 24px; }
    .glossary-panel { background: rgba(20, 184, 166, 0.05) !important; border: 1px solid rgba(20, 184, 166, 0.2) !important; border-radius: 8px !important; box-shadow: none !important; }
    .glossary-panel mat-panel-title { color: var(--accent-teal); font-size: 13px; font-weight: 600; display: flex; align-items: center; gap: 8px; }
    .glossary-panel mat-icon { font-size: 18px; height: 18px; width: 18px; color: var(--accent-teal); }
    .glossary-content { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; padding-top: 10px; font-size: 12.5px; color: var(--slate-700); line-height: 1.5; }
    .g-col strong { color: var(--slate-900); display: block; margin-bottom: 4px; }

    .loader { display: flex; flex-direction: column; align-items: center; gap: 14px; padding: 80px 16px; color: var(--slate-500); }

    /* KPI grid */
    .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 14px; margin-bottom: 24px; }
    .kpi { background: var(--bg-elevated); border: 1px solid var(--border); border-radius: var(--r-md); padding: 18px; box-shadow: var(--shadow-sm); transition: transform .15s; }
    .kpi:hover { transform: translateY(-2px); box-shadow: var(--shadow-md); border-color: var(--accent-teal); cursor: help; }
    .kpi-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; }
    .kpi-label { font-size: 11px; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; color: var(--slate-500); }
    .kpi-icon { width: 30px; height: 30px; border-radius: 8px; background: var(--slate-50); color: var(--slate-500); display: inline-flex; align-items: center; justify-content: center; mat-icon { font-size: 16px; height: 16px; width: 16px; } }
    .kpi-pos .kpi-icon { background: var(--success-50); color: var(--success-600); }
    .kpi-neg .kpi-icon { background: var(--danger-50);  color: var(--danger-600); }

    .kpi-value { font-size: 28px; font-weight: 700; color: var(--slate-900); letter-spacing: -.02em; line-height: 1.1; font-variant-numeric: tabular-nums; .unit { font-size: 14px; font-weight: 500; color: var(--slate-400); margin-left: 2px; } }
    .kpi-pos .kpi-value { color: var(--success-700); }
    .kpi-neg .kpi-value { color: var(--danger-700); }
    .kpi-foot { margin-top: 10px; display: flex; align-items: center; justify-content: space-between; gap: 8px; }
    .kpi-sub { font-size: 11px; color: var(--slate-400); }
    .quality { font-size: 11px; font-weight: 700; padding: 2px 8px; background: var(--slate-100); color: var(--slate-500); border-radius: var(--r-pill); }
    .quality.good { background: var(--success-100); color: var(--success-700); }

    /* Cards / charts */
    .card { background: var(--bg-elevated); border: 1px solid var(--border); border-radius: var(--r-md); box-shadow: var(--shadow-sm); padding: 18px; margin-bottom: 24px; }
    .card-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 14px; flex-wrap: wrap; }
    .card-title { display: flex; align-items: center; gap: 8px; font-size: 14px; font-weight: 600; color: var(--slate-900); mat-icon { font-size: 18px; height: 18px; width: 18px; color: var(--accent-teal); } }
    .card-sub { font-size: 12px; color: var(--slate-500); }
    .legend { display: inline-flex; align-items: center; gap: 6px; font-size: 12px; font-weight:600; color: var(--slate-600); }
    .leg-dot { width: 10px; height: 10px; border-radius: 3px; display: inline-block; margin-right: 4px; }
    
    /* Gráficos elásticos */
    .chart-host { width: 100%; height: 260px; overflow: hidden; }
    .chart-host-small { width: 100%; height: 220px; overflow: hidden; }
    .chart-card { padding: 18px 16px 12px; }

    .row-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; margin-bottom: 24px; }
    .row-2 .card { margin-bottom: 0; }
    @media (max-width: 1100px) { .row-2 { grid-template-columns: 1fr; } }

    .hint { display: flex; align-items: center; gap: 8px; margin-top: 10px; padding: 8px 12px; border-radius: var(--r-sm); font-size: 12px; font-weight:600; mat-icon { font-size: 16px; height: 16px; width: 16px; } }
    .hint-info { background: var(--brand-100); color: var(--brand-700); mat-icon { color: var(--brand-600); } }
    .hint-warn { background: var(--warn-100);  color: var(--warn-700);  mat-icon { color: var(--warn-600); } }

    /* Table */
    .table-card { padding: 18px 0 6px; }
    .table-card .card-head { padding: 0 18px; }
    .aurora-table { width: 100%; }
    .ticker-name { font-size: 15px; font-weight: 700; color: var(--slate-900); letter-spacing: -.01em; }

    .signal-pill {
      display: inline-flex; align-items: center; gap: 4px;
      padding: 4px 12px; border-radius: var(--r-pill);
      font-size: 11px; font-weight: 700; letter-spacing: .03em;
    }
    .signal-pill.buy  { background: var(--success-100); color: var(--success-700); }
    .signal-pill.sell { background: rgba(124, 58, 237, .15); color: #7C3AED; }
    .signal-pill.hold { background: var(--warn-100);    color: var(--warn-700); }
    
    .num { font-variant-numeric: tabular-nums; font-weight: 600; color: var(--slate-700); font-size: 13px; &.bold { font-weight: 700; } &.pos  { color: var(--success-700); } &.neg  { color: var(--danger-700); } }

    .winrate-cell { display: flex; align-items: center; gap: 8px; min-width: 110px; }
    .wr-bar { flex: 1; height: 6px; background: var(--slate-100); border-radius: var(--r-pill); overflow: hidden; }
    .wr-fill { height: 100%; width: 0; background: var(--slate-400); border-radius: var(--r-pill); transition: width .5s; &.high { background: linear-gradient(to right, var(--success-500), var(--success-600)); } }
    
    .no-data { text-align: center; padding: 40px; color: var(--slate-400); mat-icon { font-size: 36px; height: 36px; width: 36px; opacity: .5; } p { margin-top: 6px; } }
  `],
})
export class BacktestingComponent implements OnInit, AfterViewInit {
  private reportSvc = inject(ReportService);

  @ViewChild(MatSort) sort!: MatSort;

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

  compareScheme: any  = { domain: ['#2563EB', '#94A3B8'] }; // Azul corporativo (IA) vs Gris neutro (B&H)
  drawdownScheme: any = { domain: ['#EF4444'] }; // Rojo
  
  // Coloreado dinámico para Alpha (Verde si gana al mercado, Rojo si pierde)
  customColorsAlpha = (name: string) => {
    const item = this.alphaChart.find(d => d.name === name);
    return (item && item.value >= 0) ? '#22C55E' : '#EF4444'; 
  };

  // Coloreado dinámico para Sharpe (Verde/Azul si es positivo, Rojo si es negativo)
  customColorsSharpe = (name: string) => {
    const item = this.sharpeChart.find(d => d.name === name);
    return (item && item.value >= 0) ? '#06B6D4' : '#EF4444'; 
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

  // Activa la ordenación de las columnas de la tabla
  ngAfterViewInit() {
    this.tableSource.sort = this.sort;
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
    
    // Reconecta la ordenación
    if (this.sort) {
      this.tableSource.sort = this.sort;
    }

    // MAPEO FORZADO PARA EL GRÁFICO AGRUPADO: Eje X = ETF, Eje Y = IA vs Mercado
    this.returnChart = this.tickerViews.map(t => ({
      name: t.ticker,
      series: [
        { name: 'Estrategia IA', value: t.cumulative_return * 100 },
        { name: 'Mercado (Buy & Hold)', value: t.buy_hold_return * 100 }
      ]
    }));

    this.sharpeChart   = this.reportSvc.sharpeChart(this.tickerViews);
    this.drawdownChart = this.reportSvc.drawdownChart(this.tickerViews);
    this.alphaChart    = this.reportSvc.alphaChart(this.tickerViews);
  }

  qualityLabel(s: number) {
    if (s >= 2) return 'Excelente';
    if (s >= 1) return 'Bueno';
    if (s >= 0) return 'Aceptable';
    return 'Deficiente';
  }
}