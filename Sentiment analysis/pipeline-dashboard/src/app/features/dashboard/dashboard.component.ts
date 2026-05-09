import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatSelectModule } from '@angular/material/select';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatExpansionModule } from '@angular/material/expansion';
import { NgxChartsModule } from '@swimlane/ngx-charts';
import { switchMap } from 'rxjs';
import { ReportService } from '../../core/services/report.service';
import { DailyReport, TickerView, ReportDateEntry } from '../../core/models/report.model';
import { ChartDataPoint } from '../../core/models/pipeline.model';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [
    CommonModule, FormsModule, MatIconModule, MatButtonModule,
    MatProgressSpinnerModule, MatSelectModule, MatTooltipModule,
    MatExpansionModule, NgxChartsModule,
  ],
  template: `
    <div class="page">

      <header class="page-head">
        <div class="page-head-text">
          <div class="page-eyebrow">
            <mat-icon>space_dashboard</mat-icon>
            <span>Visión General de la Cartera</span>
          </div>
          <h1 class="page-title">Resumen Operativo</h1>
          <p class="page-sub">Sistema de trading algorítmico · Red Bayesiana + FinBERT · Evaluando los últimos {{ report?.data_period_days || 365 }} días</p>
        </div>
        <div class="page-actions">
          <mat-form-field appearance="outline" class="date-input" subscriptSizing="dynamic">
            <mat-label>Fecha del Informe</mat-label>
            <mat-select [(ngModel)]="selectedDate" (ngModelChange)="onDateChange($event)">
              @for (d of availableDates; track d.date) {
                <mat-option [value]="d.date">{{ d.date }}</mat-option>
              }
            </mat-select>
          </mat-form-field>
          <button class="btn btn-ghost" (click)="refresh()" matTooltip="Recargar datos">
            <mat-icon>refresh</mat-icon>
            <span>Actualizar</span>
          </button>
        </div>
      </header>

      <mat-accordion class="glossary-accordion">
        <mat-expansion-panel class="glossary-panel">
          <mat-expansion-panel-header>
            <mat-panel-title>
              <mat-icon>lightbulb</mat-icon>
              <span>¿Cómo interpretar estas métricas? (Glosario)</span>
            </mat-panel-title>
          </mat-expansion-panel-header>
          <div class="glossary-content">
            <div class="g-col">
              <strong>Estrategia IA vs Mercado:</strong> Compara el rendimiento de nuestra IA frente a la estrategia pasiva "Comprar y Mantener" (Buy & Hold). La IA busca preservar el capital estando en liquidez (cash) ante el peligro.
            </div>
            <div class="g-col">
              <strong>Ratio de Sharpe:</strong> Mide la calidad de la inversión. Relaciona la rentabilidad obtenida con el riesgo asumido. Un valor superior a 1.0 indica un comportamiento excelente ajustado al riesgo.
            </div>
            <div class="g-col">
              <strong>Caída Máxima:</strong> El mayor porcentaje de dinero que la cartera llegó a perder desde su pico más alto. Un valor cercano a 0% demuestra que el sistema es extremadamente seguro.
            </div>
            <div class="g-col">
              <strong>Probabilidad Alcista P(↑):</strong> Confianza matemática de la IA sobre si el ETF subirá. Si es ≥ 65% compramos; si es ≤ 35% pasamos a liquidez (Cash); y en el rango medio mantenemos.
            </div>
          </div>
        </mat-expansion-panel>
      </mat-accordion>

      @if (loading) {
        <div class="loader">
          <mat-spinner diameter="40"></mat-spinner>
          <p>Extrayendo datos del Data Lake de AWS…</p>
        </div>
      } @else if (report) {

        <section class="kpi-grid">

          <article class="kpi" [class.kpi-pos]="report.summary.avg_cumulative_return > 0" [class.kpi-neg]="report.summary.avg_cumulative_return < 0">
            <div class="kpi-head">
              <span class="kpi-label">Beneficio Estrategia IA</span>
              <span class="kpi-icon"><mat-icon>smart_toy</mat-icon></span>
            </div>
            <div class="kpi-value">
              {{ report.summary.avg_cumulative_return > 0 ? '+' : '' }}{{ (report.summary.avg_cumulative_return * 100) | number:'1.2-2' }}<span class="unit">%</span>
            </div>
            <div class="kpi-foot">
              <span class="delta" [class.up]="report.summary.avg_cumulative_return > 0" [class.down]="report.summary.avg_cumulative_return < 0">
                <mat-icon>{{ report.summary.avg_cumulative_return >= 0 ? 'arrow_upward' : 'arrow_downward' }}</mat-icon>
                Rentabilidad Neta
              </span>
            </div>
          </article>

          <article class="kpi" [class.kpi-pos]="avgBenchmark > 0" [class.kpi-neg]="avgBenchmark < 0">
            <div class="kpi-head">
              <span class="kpi-label">Rentabilidad Mercado (B&H)</span>
              <span class="kpi-icon"><mat-icon>public</mat-icon></span>
            </div>
            <div class="kpi-value">
              {{ avgBenchmark > 0 ? '+' : '' }}{{ (avgBenchmark * 100) | number:'1.2-2' }}<span class="unit">%</span>
            </div>
            <div class="kpi-foot">
              <span class="quality">Comprar y Mantener</span>
            </div>
          </article>

          <article class="kpi" [class.kpi-pos]="report.summary.avg_sharpe_ratio > 1">
            <div class="kpi-head">
              <span class="kpi-label">Ratio de Sharpe (Riesgo)</span>
              <span class="kpi-icon"><mat-icon>balance</mat-icon></span>
            </div>
            <div class="kpi-value">{{ report.summary.avg_sharpe_ratio | number:'1.2-2' }}</div>
            <div class="kpi-foot">
              <span class="quality" [class.good]="report.summary.avg_sharpe_ratio > 1" [class.bad]="report.summary.avg_sharpe_ratio < 0">
                {{ qualityLabel(report.summary.avg_sharpe_ratio) }}
              </span>
            </div>
          </article>

          <article class="kpi kpi-neg">
            <div class="kpi-head">
              <span class="kpi-label">Caída Máxima</span>
              <span class="kpi-icon"><mat-icon>water_drop</mat-icon></span>
            </div>
            <div class="kpi-value">{{ (report.summary.avg_max_drawdown * 100) | number:'1.2-2' }}<span class="unit">%</span></div>
            <div class="kpi-foot">
              <span class="delta down"><mat-icon>security</mat-icon> Protección de capital</span>
            </div>
          </article>

        </section>

        <section class="row">
          
          <div class="card chart-card span-4">
            <div class="card-head">
              <div class="card-title"><mat-icon>donut_large</mat-icon> <span>Decisiones de IA (Hoy)</span></div>
            </div>
            
            <div class="donut-container">
              <div class="donut-chart-wrapper">
                <ngx-charts-pie-chart
                  [results]="signalPieChart"
                  [legend]="false"
                  [labels]="false"
                  [doughnut]="true"
                  [arcWidth]="0.35"
                  [customColors]="customSignalColors"
                  [view]="[220, 220]">
                </ngx-charts-pie-chart>
                <div class="donut-center">
                  <div class="dc-num">{{ tickerViews.length }}</div>
                  <div class="dc-lbl">ETFs</div>
                </div>
              </div>
              
              <div class="custom-legend">
                <div class="cl-item">
                  <span class="cl-color" style="background-color: #22C55E;"></span>
                  <span class="cl-label">COMPRAR</span>
                  <span class="cl-value">{{ buyCount }}</span>
                </div>
                <div class="cl-item">
                  <span class="cl-color" style="background-color: #7C3AED;"></span>
                  <span class="cl-label">CASH</span>
                  <span class="cl-value">{{ sellCount }}</span>
                </div>
                <div class="cl-item">
                  <span class="cl-color" style="background-color: #F59E0B;"></span>
                  <span class="cl-label">MANTENER</span>
                  <span class="cl-value">{{ holdCount }}</span>
                </div>
              </div>
            </div>
          </div>

          <div class="card chart-card span-8">
            <div class="card-head">
              <div class="card-title"><mat-icon>track_changes</mat-icon> <span>Tasa de Acierto (Win Rate) Histórica por ETF</span></div>
              <span class="card-sub">Efectividad de las operaciones cerradas en el último año</span>
            </div>
            <div class="chart-host">
              <ngx-charts-bar-vertical
                [results]="winRateChart"
                [xAxis]="true" [yAxis]="true" [showGridLines]="true"
                [customColors]="customWinRateColors"
                yAxisLabel="Acierto (%)" [showYAxisLabel]="true"
                [showDataLabel]="true" [yScaleMax]="100" [barPadding]="4" [roundEdges]="true">
              </ngx-charts-bar-vertical>
            </div>
          </div>

        </section>

        <section class="row">
          <div class="card chart-card span-12">
            <div class="card-head">
              <div class="card-title"><mat-icon>bar_chart</mat-icon> <span>Probabilidad Alcista P(↑) por ETF</span></div>
              <span class="card-sub">Confianza matemática calculada por la Red Bayesiana</span>
            </div>
            <div class="chart-host">
              <ngx-charts-bar-vertical
                [results]="probUpChart"
                [xAxis]="true" [yAxis]="true" [showGridLines]="true"
                [customColors]="customProbColors"
                yAxisLabel="Probabilidad (%)" [showYAxisLabel]="true"
                [showDataLabel]="true" [yScaleMax]="100" [barPadding]="4" [roundEdges]="true">
              </ngx-charts-bar-vertical>
            </div>
            <div class="thresholds">
              <span class="th-buy">COMPRAR ≥ 65%</span>
              <span class="th-hold">MANTENER 35–65%</span>
              <span class="th-sell">CASH ≤ 35%</span>
            </div>
          </div>
        </section>

      } @else {
        <div class="empty">
          <mat-icon>inbox</mat-icon>
          <p>No hay informes disponibles todavía.</p>
        </div>
      }
    </div>
  `,
  styles: [`
    .page { max-width: var(--content-max); margin: 0 auto; padding-bottom: 30px; }
    .page-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 24px; flex-wrap: wrap; margin-bottom: 22px; }
    .page-eyebrow { display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px; background: rgba(6, 182, 212, .12); color: var(--accent-cyan); border-radius: var(--r-pill); font-size: 11px; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; margin-bottom: 10px; }
    .page-eyebrow mat-icon { font-size: 14px; height: 14px; width: 14px; }
    .page-title { font-size: 26px; font-weight: 700; color: var(--slate-900); letter-spacing: -.02em; }
    .page-sub { color: var(--slate-500); font-size: 13px; margin-top: 6px; }

    .btn { display: inline-flex; align-items: center; gap: 6px; padding: 8px 14px; border-radius: var(--r-sm); border: 1px solid var(--border); font-size: 13px; font-weight: 600; cursor: pointer; background: var(--bg-elevated); color: var(--slate-700); }
    .btn:hover { border-color: var(--brand-300); color: var(--brand-600); background: var(--slate-50); }

    /* Glosario */
    .glossary-accordion { display: block; margin-bottom: 24px; }
    .glossary-panel { background: rgba(59, 130, 246, 0.05) !important; border: 1px solid rgba(59, 130, 246, 0.2) !important; border-radius: 8px !important; box-shadow: none !important; }
    .glossary-panel mat-panel-title { color: var(--brand-700); font-size: 13px; font-weight: 600; display: flex; align-items: center; gap: 8px; }
    .glossary-panel mat-icon { font-size: 18px; height: 18px; width: 18px; color: var(--brand-500); }
    .glossary-content { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; padding-top: 10px; font-size: 12.5px; color: var(--slate-700); line-height: 1.5; }
    .g-col strong { color: var(--slate-900); display: block; margin-bottom: 4px; }

    /* KPIs */
    .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-bottom: 22px; }
    .kpi { background: var(--bg-elevated); border: 1px solid var(--border); border-radius: var(--r-md); padding: 18px; box-shadow: var(--shadow-sm); transition: transform .15s; }
    .kpi:hover { transform: translateY(-2px); box-shadow: var(--shadow-md); border-color: var(--brand-300); cursor: help; }
    .kpi-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; }
    .kpi-label { font-size: 11px; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; color: var(--slate-500); }
    .kpi-icon { width: 30px; height: 30px; border-radius: 8px; background: var(--slate-50); display: inline-flex; align-items: center; justify-content: center; color: var(--slate-500); }
    .kpi-icon mat-icon { font-size: 16px; height: 16px; width: 16px; }
    .kpi-pos .kpi-icon { background: var(--success-50); color: var(--success-600); }
    .kpi-neg .kpi-icon { background: var(--danger-50);  color: var(--danger-600); }
    
    .kpi-value { font-size: 28px; font-weight: 700; color: var(--slate-900); font-variant-numeric: tabular-nums; line-height: 1.1; }
    .kpi-value .unit { font-size: 14px; font-weight: 500; color: var(--slate-400); margin-left: 2px; }
    .kpi-pos .kpi-value { color: var(--success-700); }
    .kpi-neg .kpi-value { color: var(--danger-700); }

    .kpi-foot { margin-top: 10px; display: flex; align-items: center; justify-content: space-between; }
    .kpi-sub { font-size: 11px; color: var(--slate-400); }

    .delta { display: inline-flex; align-items: center; gap: 3px; padding: 2px 8px; border-radius: var(--r-pill); font-size: 11px; font-weight: 700; background: var(--slate-100); color: var(--slate-500); }
    .delta mat-icon { font-size: 14px; height: 14px; width: 14px; }
    .delta.up   { background: var(--success-100); color: var(--success-700); }
    .delta.down { background: var(--danger-100);  color: var(--danger-700); }

    .quality { font-size: 11px; font-weight: 700; padding: 2px 8px; background: var(--slate-100); color: var(--slate-500); border-radius: var(--r-pill); }
    .quality.good { background: var(--success-100); color: var(--success-700); }
    .quality.bad  { background: var(--danger-100); color: var(--danger-700); }

    /* Layout Gráficos */
    .row { display: grid; grid-template-columns: repeat(12, 1fr); gap: 16px; margin-bottom: 18px; }
    .span-4 { grid-column: span 4; }
    .span-8 { grid-column: span 8; }
    .span-12 { grid-column: span 12; }
    @media (max-width: 1100px) { .span-4, .span-8, .span-12 { grid-column: span 12; } }

    .card { background: var(--bg-elevated); border: 1px solid var(--border); border-radius: var(--r-md); box-shadow: var(--shadow-sm); padding: 18px; }
    .card-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px; }
    .card-title { display: flex; align-items: center; gap: 8px; font-size: 14px; font-weight: 600; color: var(--slate-900); }
    .card-title mat-icon { font-size: 18px; height: 18px; width: 18px; color: var(--brand-600); }
    .card-sub { font-size: 11px; color: var(--slate-500); }
    .chart-card { padding: 18px 16px 12px; }
    
    /* El truco responsive para los gráficos de barras */
    .chart-host { width: 100%; height: 240px; overflow: hidden; }

    /* Donut y Leyenda Personalizada */
    .donut-container { display: flex; align-items: center; justify-content: center; gap: 20px; }
    .donut-chart-wrapper { position: relative; width: 220px; height: 220px; }
    .donut-center { position: absolute; top: 0; left: 0; width: 100%; height: 100%; display: flex; flex-direction: column; align-items: center; justify-content: center; pointer-events: none; }
    .dc-num { font-size: 32px; font-weight: 700; color: var(--slate-900); line-height: 1; }
    .dc-lbl { font-size: 12px; color: var(--slate-500); font-weight: 600; letter-spacing: 0.05em; }

    .custom-legend { display: flex; flex-direction: column; gap: 12px; min-width: 130px; }
    .cl-title { font-size: 12px; font-weight: 600; color: var(--slate-500); margin: 0 0 4px; text-transform: uppercase; letter-spacing: 0.05em; }
    .cl-item { display: flex; align-items: center; gap: 8px; font-size: 13px; }
    .cl-color { width: 12px; height: 12px; border-radius: 3px; display: inline-block; }
    .cl-label { flex: 1; font-weight: 600; color: var(--slate-700); }
    .cl-value { font-weight: 700; color: var(--slate-900); font-size: 15px; }

    /* Probabilidades */
    .thresholds { display: flex; justify-content: center; gap: 8px; padding-top: 10px; }
    .thresholds span { font-size: 11px; padding: 4px 12px; border-radius: var(--r-pill); font-weight: 700; letter-spacing: 0.03em; }
    .th-buy  { background: var(--success-100); color: var(--success-700); }
    .th-hold { background: var(--warn-100); color: var(--warn-700); }
    .th-sell { background: rgba(124, 58, 237, .15); color: #7C3AED; }

    .loader { display: flex; flex-direction: column; align-items: center; gap: 14px; padding: 80px 16px; color: var(--slate-500); }
    .empty { display: flex; flex-direction: column; align-items: center; gap: 8px; padding: 80px 16px; color: var(--slate-400); }
    .empty mat-icon { font-size: 48px; height: 48px; width: 48px; opacity: .5; }
  `]
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
  winRateChart: ChartDataPoint[] = [];
  
  avgBenchmark: number = 0; 

  // Función para forzar los colores de la Tarta
  customSignalColors = (name: string) => {
    if (name === 'BUY') return '#22C55E';
    if (name === 'SELL') return '#7C3AED';
    return '#F59E0B'; // HOLD
  };

  // Función dinámica para colorear las barras de probabilidad según el valor
  customProbColors = (name: string) => {
    const item = this.probUpChart.find(d => d.name === name);
    if (!item) return '#3B82F6';
    if (item.value >= 65) return '#22C55E'; // BUY (Verde)
    if (item.value <= 35) return '#7C3AED'; // SELL/CASH (Violeta)
    return '#F59E0B'; // HOLD (Amarillo)
  };

  // Función dinámica para colorear la tasa de acierto (Azul corporativo si > 50%, rojo si no)
  customWinRateColors = (name: string) => {
    const item = this.winRateChart.find(d => d.name === name);
    if (!item) return '#3B82F6';
    return item.value >= 50 ? '#06B6D4' : '#EF4444'; 
  };

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
    this.tickerViews = this.reportSvc.buildTickerViews(report).sort((a, b) => b.prob_up - a.prob_up);
    
    // Calcular Media de Mercado
    const sumBH = this.tickerViews.reduce((acc, curr) => acc + curr.buy_hold_return, 0);
    this.avgBenchmark = this.tickerViews.length > 0 ? (sumBH / this.tickerViews.length) : 0;

    // Tarta de Decisiones
    this.signalPieChart = [
      { name: 'BUY', value: this.buyCount },
      { name: 'SELL', value: this.sellCount },
      { name: 'HOLD', value: this.holdCount }
    ].filter(item => item.value > 0);

    // Gráfico de Probabilidades
    this.probUpChart = this.reportSvc.probUpChart(this.tickerViews);

    // Nuevo Gráfico: Tasa de Acierto (Win Rate)
    this.winRateChart = this.tickerViews.map(t => ({
      name: t.ticker,
      value: t.win_rate * 100
    })).sort((a, b) => b.value - a.value); // Ordenado de mejor a peor
  }

  qualityLabel(s: number) {
    if (s >= 2)  return 'Excelente';
    if (s >= 1)  return 'Bueno';
    if (s >= 0)  return 'Aceptable';
    return 'Deficiente';
  }
}