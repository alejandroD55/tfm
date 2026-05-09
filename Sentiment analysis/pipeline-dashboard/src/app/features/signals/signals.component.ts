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
import { switchMap, catchError, of } from 'rxjs';
import { ReportService } from '../../core/services/report.service';
import { TraceService } from '../../core/services/trace.service';
import {
  TickerView, ReportDateEntry, DailyReport,
  SentimentState, RsiState, TrendState, VolatilityState,
} from '../../core/models/report.model';
import { TickerTrace } from '../../core/models/trace.model';
import { ChartDataPoint } from '../../core/models/pipeline.model';

@Component({
  selector: 'app-signals',
  standalone: true,
  imports: [
    CommonModule, FormsModule,
    MatTableModule, MatSortModule,
    MatButtonModule, MatIconModule,
    MatProgressSpinnerModule, MatTooltipModule, MatExpansionModule,
    NgxChartsModule
  ],
  template: `
    <div class="page">

      <header class="page-head">
        <div class="page-head-text">
          <div class="page-eyebrow">
            <mat-icon>psychology</mat-icon>
            <span>Inteligencia Artificial Bayesiana</span>
          </div>
          <h1 class="page-title">Señales & Razonamiento</h1>
          <p class="page-sub">
            Análisis profundo de los 4 nodos de evidencia y el modelo NLP FinBERT.
          </p>
        </div>
        
        <div class="page-actions">
          <div class="filter-group">
            <label>Fecha</label>
            <select class="aurora-select" [(ngModel)]="selectedDate" (change)="onDateChange(selectedDate)">
              @for (d of availableDates; track d.date) {
                <option [value]="d.date">{{ d.date }}</option>
              }
            </select>
          </div>
          <div class="filter-group">
            <label>Filtrar Señal</label>
            <select class="aurora-select" [(ngModel)]="filterSignal" (change)="applyFilter()">
              <option value="">Todas</option>
              <option value="BUY">COMPRAR</option>
              <option value="SELL">CASH</option>
              <option value="HOLD">MANTENER</option>
            </select>
          </div>
        </div>
      </header>

      <mat-accordion class="glossary-accordion">
        <mat-expansion-panel class="glossary-panel" [expanded]="true">
          <mat-expansion-panel-header>
            <mat-panel-title>
              <mat-icon>auto_graph</mat-icon>
              <span>Arquitectura del Razonamiento (Flujo de Evidencia)</span>
            </mat-panel-title>
          </mat-expansion-panel-header>
          
          <div class="glossary-infographic">
            <div class="info-block">
              <div class="bn-node bn-sentiment">
                <mat-icon>sentiment_satisfied</mat-icon>
                <div class="bn-label">Sentimiento</div>
              </div>
              <div class="info-text">
                <strong>FinBERT (NLP)</strong>
                Análisis de titulares.
                <small>Estados: Positivo, Negativo o Neutral.</small>
              </div>
            </div>

            <span class="info-plus">+</span>

            <div class="info-block">
              <div class="bn-node bn-rsi">
                <mat-icon>show_chart</mat-icon>
                <div class="bn-label">Fuerza RSI</div>
              </div>
              <div class="info-text">
                <strong>Índice de Fuerza</strong>
                Velocidad del precio.
                <small>Sobrecompra (>70), Neutral o Sobreventa (<30).</small>
              </div>
            </div>

            <span class="info-plus">+</span>

            <div class="info-block">
              <div class="bn-node bn-trend">
                <mat-icon>trending_up</mat-icon>
                <div class="bn-label">Tendencia</div>
              </div>
              <div class="info-text">
                <strong>Medias Móviles</strong>
                SMA 20 vs SMA 50.
                <small>Alcista (Corta > Larga) o Bajista.</small>
              </div>
            </div>

            <span class="info-plus">+</span>

            <div class="info-block">
              <div class="bn-node bn-vol">
                <mat-icon>swap_vert</mat-icon>
                <div class="bn-label">Volatilidad</div>
              </div>
              <div class="info-text">
                <strong>Bandas Bollinger</strong>
                Dispersión del precio.
                <small>Alta (>5% ancho) o Baja.</small>
              </div>
            </div>

            <span class="info-arrow">→</span>

            <div class="info-block target-block">
              <div class="bn-node bn-target">
                <mat-icon>query_stats</mat-icon>
                <div class="bn-label">Confianza Alcista</div>
              </div>
              <div class="info-text">
                <strong>Probabilidad P(↑)</strong>
                Si ≥ 65% ➔ <span class="green-t">COMPRAR</span><br>
                35-65% ➔ <span class="yellow-t">MANTENER</span><br>
                Si ≤ 35% ➔ <span class="purple-t">CASH</span>
              </div>
            </div>
          </div>
        </mat-expansion-panel>
      </mat-accordion>

      @if (loading) {
        <div class="loader"><mat-spinner diameter="40"></mat-spinner></div>
      } @else {

        <section class="charts-layout">
          <div class="nodes-grid-2x2">
            <div class="mini-chart">
              <h5>Sentimiento Macro</h5>
              <ngx-charts-pie-chart [results]="sentimentChart" [doughnut]="true" [arcWidth]="0.3" [customColors]="customSentimentColors" [legend]="true" [legendTitle]="'Leyenda'" [view]="[300, 150]"></ngx-charts-pie-chart>
            </div>
            <div class="mini-chart">
              <h5>Estado de Fuerza (RSI)</h5>
              <ngx-charts-pie-chart [results]="rsiChart" [doughnut]="true" [arcWidth]="0.3" [customColors]="customRsiColors" [legend]="true" [legendTitle]="'Leyenda'" [view]="[300, 150]"></ngx-charts-pie-chart>
            </div>
            <div class="mini-chart">
              <h5>Tendencia Global</h5>
              <ngx-charts-pie-chart [results]="trendChart" [doughnut]="true" [arcWidth]="0.3" [customColors]="customTrendColors" [legend]="true" [legendTitle]="'Leyenda'" [view]="[300, 150]"></ngx-charts-pie-chart>
            </div>
            <div class="mini-chart">
              <h5>Volatilidad</h5>
              <ngx-charts-pie-chart [results]="volatilityChart" [doughnut]="true" [arcWidth]="0.3" [customColors]="customVolColors" [legend]="true" [legendTitle]="'Leyenda'" [view]="[300, 150]"></ngx-charts-pie-chart>
            </div>
          </div>

          <div class="main-decision-card">
            <h4>Decisión Final del Portfolio</h4>
            <ngx-charts-pie-chart [results]="signalChart" [doughnut]="true" [arcWidth]="0.35" [customColors]="customSignalColors" [legend]="true" [legendTitle]="'Leyenda'" [view]="[400, 320]"></ngx-charts-pie-chart>
          </div>
        </section>

        <div class="card table-card">
          <table mat-table [dataSource]="dataSource" multiTemplateDataRows matSort class="aurora-table">

            <ng-container matColumnDef="signal">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>Decisión</th>
              <td mat-cell *matCellDef="let row">
                <span class="signal-pill {{ row.signal.toLowerCase() }}">
                  <mat-icon>{{ signalIcon(row.signal) }}</mat-icon>
                  {{ translateSignal(row.signal) }}
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="ticker">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>Activo</th>
              <td mat-cell *matCellDef="let row"><span class="ticker-name">{{ row.ticker }}</span></td>
            </ng-container>

            <ng-container matColumnDef="prob_up">
              <th mat-header-cell *matHeaderCellDef mat-sort-header>Confianza Alcista</th>
              <td mat-cell *matCellDef="let row">
                <div class="prob-cell">
                  <div class="prob-gauge">
                    <div class="gauge-fill"
                         [style.width.%]="row.prob_up*100"
                         [class.high]="row.prob_up >= 0.65"
                         [class.mid]="row.prob_up > 0.35 && row.prob_up < 0.65"
                         [class.low]="row.prob_up <= 0.35"></div>
                  </div>
                  <span class="prob-pct" [class.green]="row.prob_up>=0.65" [class.purple]="row.prob_up<=0.35" [class.yellow]="row.prob_up>0.35 && row.prob_up<0.65">
                    {{ (row.prob_up*100)|number:'1.1-1' }}%
                  </span>
                </div>
              </td>
            </ng-container>

            <ng-container matColumnDef="evidence">
              <th mat-header-cell *matHeaderCellDef>Estado de los Nodos</th>
              <td mat-cell *matCellDef="let row">
                <div class="ev-row">
                  <span class="ev-chip {{ sentimentClass(row.evidence.sentiment) }}">
                    {{ translateState(row.evidence.sentiment) }}
                  </span>
                  <span class="ev-chip {{ rsiClass(row.evidence.rsi) }}">
                    {{ translateState(row.evidence.rsi) }}
                  </span>
                  <span class="ev-chip {{ trendClass(row.evidence.trend) }}">
                    {{ translateState(row.evidence.trend) }}
                  </span>
                  <span class="ev-chip {{ volClass(row.evidence.volatility) }}">
                    {{ translateState(row.evidence.volatility) }}
                  </span>
                </div>
              </td>
            </ng-container>

            <ng-container matColumnDef="trades">
              <th mat-header-cell *matHeaderCellDef mat-sort-header="trades_closed">Operaciones (Año)</th>
              <td mat-cell *matCellDef="let row"><span class="num">{{ row.trades_closed }}</span></td>
            </ng-container>

            <ng-container matColumnDef="winrate">
              <th mat-header-cell *matHeaderCellDef mat-sort-header="win_rate">Tasa de Acierto</th>
              <td mat-cell *matCellDef="let row">
                <span class="ts-chip wr" [class.good]="row.win_rate>=0.5">
                  <mat-icon>track_changes</mat-icon>{{ (row.win_rate*100)|number:'1.0-0' }}%
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="return">
              <th mat-header-cell *matHeaderCellDef mat-sort-header="cumulative_return">Rentabilidad</th>
              <td mat-cell *matCellDef="let row">
                <span class="ret-val" [class.pos]="row.cumulative_return>0" [class.neg]="row.cumulative_return<0">
                  {{ row.cumulative_return>0?'+':'' }}{{ (row.cumulative_return*100)|number:'1.2-2' }}%
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="alpha">
              <th mat-header-cell *matHeaderCellDef mat-sort-header="alpha_vs_benchmark">Mejora vs Mercado</th>
              <td mat-cell *matCellDef="let row">
                <span class="ret-val" [class.pos]="row.alpha_vs_benchmark>0" [class.neg]="row.alpha_vs_benchmark<0">
                  {{ row.alpha_vs_benchmark>0?'+':'' }}{{ (row.alpha_vs_benchmark*100)|number:'1.2-2' }}%
                </span>
              </td>
            </ng-container>

            <ng-container matColumnDef="expand">
              <th mat-header-cell *matHeaderCellDef></th>
              <td mat-cell *matCellDef="let row">
                <button mat-icon-button (click)="toggleRow(row.ticker); $event.stopPropagation()">
                  <mat-icon>{{ expandedRows.has(row.ticker) ? 'expand_less' : 'expand_more' }}</mat-icon>
                </button>
              </td>
            </ng-container>

            <ng-container matColumnDef="expandedDetail">
              <td mat-cell *matCellDef="let row" [attr.colspan]="displayedColumns.length" class="detail-cell">
                <div class="detail-row" [class.expanded]="expandedRows.has(row.ticker)">
                  <div class="exp-panel">
                    <div class="exp-grid">
                      <div class="exp-card">
                        <h4><mat-icon>account_tree</mat-icon> Justificación para {{ row.ticker }}</h4>
                        <div class="explainer">
                          <mat-icon>info</mat-icon>
                          <span>
                            <strong>Razonamiento:</strong> Probabilidad alcista del {{ (row.prob_up*100)|number:'1.1-1' }}% basada en el cruce de evidencias técnicas y fundamentales.
                          </span>
                        </div>
                      </div>
                      <div class="exp-card">
                        <h4><mat-icon>article</mat-icon> Titulares hoy (FinBERT)</h4>
                        @if (isTraceLoading(row.ticker)) { <mat-spinner diameter="20"></mat-spinner> }
                        @else {
                          <div class="headlines-list">
                            @for (h of getTickerTrace(row.ticker)?.sentiment_detail?.headlines_sample?.slice(0, 3); track $index) {
                              <div class="hl-row">
                                <span class="ev-chip ev-{{ h.sentiment }} small">{{ translateState(h.sentiment) }}</span>
                                <span class="hl-text">{{ h.headline }}</span>
                              </div>
                            }
                          </div>
                        }
                      </div>
                    </div>
                  </div>
                </div>
              </td>
            </ng-container>

            <tr mat-header-row *matHeaderRowDef="displayedColumns; sticky: true"></tr>
            <tr mat-row *matRowDef="let row; columns: displayedColumns;" class="data-row" (click)="toggleRow(row.ticker)"></tr>
            <tr mat-row *matRowDef="let row; columns: ['expandedDetail']" class="detail-cell-wrapper"></tr>
          </table>
        </div>
      }
    </div>
  `,
  styles: [`
    .page { max-width: var(--content-max); margin: 0 auto; padding-bottom: 40px;}
    .page-head { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 22px; }
    .page-eyebrow { display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px; background: rgba(124, 58, 237, .1); color: var(--accent-violet); border-radius: 99px; font-size: 11px; font-weight: 600; text-transform: uppercase; }
    .page-title { font-size: 26px; font-weight: 700; color: var(--slate-900); }
    .page-sub { color: var(--slate-500); font-size: 13px; }
    
    .page-actions { display: flex; gap: 16px; align-items: center; }
    .filter-group { display: flex; flex-direction: column; gap: 4px; }
    .filter-group label { font-size: 10px; font-weight: 700; color: var(--slate-500); text-transform: uppercase; }
    .aurora-select { appearance: none; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 8px 30px 8px 12px; font-size: 13px; font-weight: 600; color: var(--slate-700); cursor: pointer; background-image: url('data:image/svg+xml;charset=US-ASCII,<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="%2364748B" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"></polyline></svg>'); background-repeat: no-repeat; background-position: right 8px center; background-size: 14px; }

    /* Glosario Infográfico */
    .glossary-panel { background: #fff !important; border: 1px solid var(--border) !important; border-radius: 12px !important; }
    .glossary-infographic { display: flex; align-items: flex-start; justify-content: space-between; gap: 10px; padding: 20px 10px; flex-wrap: wrap; }
    .info-block { flex: 1; min-width: 140px; display: flex; flex-direction: column; align-items: center; text-align: center; }
    .info-text { margin-top: 12px; font-size: 12px; color: var(--slate-600); line-height: 1.4; }
    .info-text strong { display: block; color: var(--slate-900); margin-bottom: 2px; }
    .info-text small { display: block; margin-top: 4px; color: var(--slate-400); font-style: italic; }
    .info-plus, .info-arrow { font-size: 24px; font-weight: 700; color: var(--slate-300); padding-top: 25px; }
    .info-arrow { color: var(--brand-600); }
    .green-t { color: #16a34a; font-weight: 700; }
    .yellow-t { color: #ca8a04; font-weight: 700; }
    .purple-t { color: #7c3aed; font-weight: 700; }

    .bn-node { display: flex; flex-direction: column; align-items: center; gap: 4px; padding: 12px; border-radius: 12px; background: var(--slate-50); border: 1px solid var(--border); width: 100%; }
    .bn-label { font-size: 11px; font-weight: 700; text-transform: uppercase; }
    .bn-sentiment mat-icon { color: var(--brand-600); }
    .bn-rsi mat-icon { color: var(--accent-violet); }
    .bn-trend mat-icon { color: #16a34a; }
    .bn-vol mat-icon { color: #ea580c; }
    .bn-target { background: linear-gradient(135deg, #1e3a8a, #2563eb); color: #fff; border: none; }

    .bn-thresholds { display: flex; gap: 8px; justify-content: center; width: 100%; margin-top: 20px; border-top: 1px solid var(--slate-100); padding-top: 15px; }
    .th { font-size: 10px; font-weight: 700; padding: 4px 12px; border-radius: 99px; }
    .th-buy { background: #dcfce7; color: #166534; }
    .th-hold { background: #fef9c3; color: #854d0e; }

    /* Estructura Gráficos */
    .charts-layout { display: grid; grid-template-columns: 1fr 420px; gap: 20px; margin-bottom: 24px; }
    .nodes-grid-2x2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .mini-chart, .main-decision-card { background: #fff; border: 1px solid var(--border); border-radius: 12px; padding: 16px; display: flex; flex-direction: column; align-items: center; }
    .mini-chart h5 { font-size: 11px; font-weight: 700; color: var(--slate-500); text-transform: uppercase; margin-bottom: 10px; }
    .main-decision-card h4 { font-size: 14px; font-weight: 700; color: var(--brand-700); margin-bottom: 20px; }

    /* Tabla */
    .aurora-table { width: 100%; border: none; }
    .data-row { cursor: pointer; }
    .data-row:hover { background: var(--slate-50); }
    .detail-cell-wrapper { height: 0 !important; border: none !important;}
    .detail-row { display: none; overflow: hidden; }
    .detail-row.expanded { display: block; }
    .exp-panel { padding: 20px; background: var(--slate-50); border-top: 1px solid var(--border); }
    .exp-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
    .exp-card { background: #fff; padding: 16px; border-radius: 8px; border: 1px solid var(--border); }
    .exp-card h4 { font-size: 13px; font-weight: 700; display: flex; align-items: center; gap: 6px; margin-bottom: 12px; }

    .signal-pill { display: inline-flex; align-items: center; gap: 4px; padding: 4px 12px; border-radius: 99px; font-size: 11px; font-weight: 700; }
    .signal-pill.buy { background: #dcfce7; color: #166534; }
    .signal-pill.sell { background: rgba(124, 58, 237, .15); color: #7C3AED; }
    .signal-pill.hold { background: #fef9c3; color: #854d0e; }

    .prob-gauge { flex: 1; height: 6px; background: var(--slate-100); border-radius: 99px; overflow: hidden; }
    .gauge-fill { height: 100%; transition: width .5s; }
    .gauge-fill.high { background: #22c55e; }
    .gauge-fill.mid { background: #eab308; }
    .gauge-fill.low { background: #7c3aed; }
    .prob-pct { font-size: 12px; font-weight: 700; min-width: 45px; text-align: right; }
    .green { color: #16a34a; } .purple { color: #7c3aed; } .yellow { color: #ca8a04; }

    .ev-chip { padding: 2px 8px; border-radius: 4px; font-size: 10px; font-weight: 700; text-transform: uppercase; background: var(--slate-100); color: var(--slate-600); }
    .ev-bullish, .ev-uptrend, .ev-oversold { background: #dcfce7; color: #166534; }
    .ev-bearish, .ev-downtrend, .ev-overbought { background: #fee2e2; color: #991b1b; }
    .ev-low-vol { background: #dbeafe; color: #1e40af; }
    .ev-high-vol { background: #ffedd5; color: #9a3412; }

    .ts-chip.wr { display: inline-flex; align-items: center; gap: 4px; font-size: 12px; font-weight: 700; color: #ef4444; }
    .ts-chip.wr.good { color: #22c55e; }
    .num { font-weight: 700; color: var(--slate-700); }
    .ret-val { font-weight: 700; }
    .pos { color: #16a34a; } .neg { color: #dc2626; }
    .hl-row { display: flex; gap: 8px; margin-bottom: 8px; font-size: 11px; }
    .hl-text { font-style: italic; color: var(--slate-600); }
  `]
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
  dataSource = new MatTableDataSource<TickerView>();
  displayedColumns = ['signal', 'ticker', 'prob_up', 'evidence', 'trades', 'winrate', 'return', 'alpha', 'expand'];

  signalChart: ChartDataPoint[] = [];
  sentimentChart: ChartDataPoint[] = [];
  rsiChart: ChartDataPoint[] = [];
  trendChart: ChartDataPoint[] = [];
  volatilityChart: ChartDataPoint[] = [];

  buyCount = 0; sellCount = 0; holdCount = 0; avgProbUp = 0;
  tickerTraceCache = new Map<string, TickerTrace | null>();
  tickerTraceLoading = new Set<string>();

  customSignalColors = (n: string) => n === 'COMPRAR' ? '#22C55E' : n === 'CASH' ? '#7C3AED' : '#F59E0B';
  customSentimentColors = (n: string) => n === 'BULLISH' ? '#22C55E' : n === 'BEARISH' ? '#EF4444' : '#94A3B8';
  customRsiColors = (n: string) => n === 'OVERSOLD' ? '#22C55E' : n === 'OVERBOUGHT' ? '#EF4444' : '#94A3B8';
  customTrendColors = (n: string) => n === 'UPTREND' ? '#22C55E' : '#EF4444';
  customVolColors = (n: string) => n === 'LOW' ? '#3B82F6' : '#F59E0B';

  ngOnInit() {
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        this.availableDates = dates;
        if (!dates.length) { this.loading = false; return of([]); }
        this.selectedDate = dates[0].date;
        return this.reportSvc.loadReport(this.selectedDate);
      })
    ).subscribe({
      next: (r: any) => { if (r) this.processReport(r); this.loading = false; },
      error: () => { this.loading = false; },
    });
  }

  ngAfterViewInit() { this.dataSource.sort = this.sort; }

  onDateChange(date: string) {
    this.loading = true; this.expandedRows.clear();
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

    this.buyCount = views.filter(v => v.signal === 'BUY').length;
    this.sellCount = views.filter(v => v.signal === 'SELL').length;
    this.holdCount = views.filter(v => v.signal === 'HOLD').length;
    this.avgProbUp = views.length ? (views.reduce((s, v) => s + v.prob_up, 0) / views.length) * 100 : 0;

    const sent = { BULLISH: 0, BEARISH: 0, NEUTRAL: 0 };
    const rsi = { OVERSOLD: 0, OVERBOUGHT: 0, NEUTRAL: 0 };
    const trend = { UPTREND: 0, DOWNTREND: 0 };
    const vol = { HIGH: 0, LOW: 0 };

    views.forEach(v => {
      const e = v.evidence;
      if (e.sentiment === 'bullish') sent.BULLISH++; else if (e.sentiment === 'bearish') sent.BEARISH++; else sent.NEUTRAL++;
      if (e.rsi === 'oversold') rsi.OVERSOLD++; else if (e.rsi === 'overbought') rsi.OVERBOUGHT++; else rsi.NEUTRAL++;
      if (e.trend === 'uptrend') trend.UPTREND++; else trend.DOWNTREND++;
      if (e.volatility === 'high') vol.HIGH++; else vol.LOW++;
    });

    this.signalChart = [{ name: 'COMPRAR', value: this.buyCount }, { name: 'CASH', value: this.sellCount }, { name: 'MANTENER', value: this.holdCount }].filter(i => i.value > 0);
    this.sentimentChart = Object.entries(sent).map(([name, value]) => ({ name, value })).filter(i => i.value > 0);
    this.rsiChart = Object.entries(rsi).map(([name, value]) => ({ name, value })).filter(i => i.value > 0);
    this.trendChart = Object.entries(trend).map(([name, value]) => ({ name, value })).filter(i => i.value > 0);
    this.volatilityChart = Object.entries(vol).map(([name, value]) => ({ name, value })).filter(i => i.value > 0);
  }

  applyFilter() { this.dataSource.filter = this.filterSignal; }
  toggleRow(ticker: string) {
    if (this.expandedRows.has(ticker)) this.expandedRows.delete(ticker);
    else { this.expandedRows.add(ticker); this.loadTickerTrace(ticker); }
  }

  loadTickerTrace(ticker: string) {
    if (this.tickerTraceCache.has(ticker) || this.tickerTraceLoading.has(ticker)) return;
    this.tickerTraceLoading.add(ticker);
    this.traceSvc.getTickerTrace(this.selectedDate, ticker).pipe(catchError(() => of(null))).subscribe(resp => {
      this.tickerTraceLoading.delete(ticker);
      this.tickerTraceCache.set(ticker, resp?.trace ?? null);
    });
  }

  getTickerTrace(ticker: string) { return this.tickerTraceCache.get(ticker) ?? null; }
  isTraceLoading(ticker: string) { return this.tickerTraceLoading.has(ticker); }
  getSentimentDist(ticker: string) {
    const t = this.getTickerTrace(ticker);
    if (!t?.sentiment_detail?.distribution) return [];
    return Object.entries(t.sentiment_detail.distribution).map(([key, v]: [string, any]) => ({ key, count: v.count, pct: v.pct }));
  }

  translateSignal(s: string) { return s === 'BUY' ? 'COMPRAR' : s === 'SELL' ? 'CASH' : 'MANTENER'; }
  translateState(s: string) {
    const m: any = { bullish: 'POSITIVO', bearish: 'NEGATIVO', neutral: 'NEUTRAL', overbought: 'SOBRECOMPRA', oversold: 'SOBREVENTA', uptrend: 'ALCISTA', downtrend: 'BAJISTA', high: 'ALTA', low: 'BAJA' };
    return m[s] || s;
  }

  signalIcon(s: string) { return ({ BUY: 'arrow_upward', SELL: 'arrow_downward', HOLD: 'remove' } as any)[s] || 'remove'; }
  sentimentIcon(s: any) { return ({ bullish: 'sentiment_very_satisfied', bearish: 'sentiment_very_dissatisfied', neutral: 'sentiment_neutral' } as any)[s]; }
  sentimentClass(v: any)  { return `ev-${v}`; }
  rsiClass(v: any) { return v === 'oversold' ? 'ev-oversold' : v === 'overbought' ? 'ev-overbought' : 'ev-neutral-rsi'; }
  trendClass(v: any) { return `ev-${v}`; }
  volClass(v: any) { return v === 'low' ? 'ev-low-vol' : 'ev-high-vol'; }
}