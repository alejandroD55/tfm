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
  templateUrl: './signals.component.html',
  styleUrl: './signals.component.scss',
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

  // 1. SOLUCIÓN: Declaramos la variable que el HTML está buscando para los Smart Donuts
  tickerViews: TickerView[] = [];

  // Nombres Limpios y en Castellano para la Tabla
  displayedColumns = ['ticker', 'signal', 'prob_up', 'evidence', 'trades', 'winrate', 'return', 'alpha', 'expand'];
  dataSource = new MatTableDataSource<TickerView>();

  // Gráficos de Resumen
  signalChart: ChartDataPoint[] = [];
  sentimentChart: ChartDataPoint[] = [];
  rsiChart: ChartDataPoint[] = [];
  trendChart: ChartDataPoint[] = [];
  volatilityChart: ChartDataPoint[] = [];

  buyCount  = 0;
  sellCount = 0;
  holdCount = 0;
  avgProbUp = 0;

  tickerTraceCache = new Map<string, TickerTrace | null>();
  tickerTraceLoading = new Set<string>();
  hasTraceForDate = false;

  // Funciones de Coloreado Dinámico (Para NGX-Charts)
  customSignalColors = (name: string) => {
    if (name === 'COMPRAR') return '#22C55E';
    if (name === 'CASH') return '#7C3AED';
    return '#F59E0B'; // MANTENER
  };
  customSentimentColors = (name: string) => {
    if (name === 'ALCISTA') return '#22C55E';
    if (name === 'BAJISTA') return '#EF4444';
    return '#94A3B8'; // NEUTRAL
  };
  customRsiColors = (name: string) => {
    if (name === 'SOBREVENTA') return '#22C55E';
    if (name === 'SOBRECOMPRA') return '#EF4444';
    return '#94A3B8'; // NEUTRAL
  };
  customTrendColors = (name: string) => name === 'ALCISTA' ? '#22C55E' : '#EF4444';
  customVolColors = (name: string) => name === 'BAJA' ? '#3B82F6' : '#F59E0B';

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
    
    // 2. SOLUCIÓN: Guardamos los datos en la variable para que el HTML pueda contar la longitud
    this.tickerViews = views; 
    this.dataSource.data = views;
    
    if (this.sort) {
      this.dataSource.sort = this.sort;
    }

    this.dataSource.filterPredicate = (row, filter) => !filter || row.signal === filter;

    this.buyCount  = views.filter(v => v.signal === 'BUY').length;
    this.sellCount = views.filter(v => v.signal === 'SELL').length;
    this.holdCount = views.filter(v => v.signal === 'HOLD').length;
    this.avgProbUp = views.length ? (views.reduce((s, v) => s + v.prob_up, 0) / views.length) * 100 : 0;

    // Procesar datos para gráficos de Nodos (Con nombres en castellano)
    const sent = { ALCISTA: 0, BAJISTA: 0, NEUTRAL: 0 };
    const rsi = { SOBREVENTA: 0, SOBRECOMPRA: 0, NEUTRAL: 0 };
    const trend = { ALCISTA: 0, BAJISTA: 0 };
    const vol = { ALTA: 0, BAJA: 0 };

    views.forEach(v => {
      const e = v.evidence;
      if (e.sentiment === 'bullish') sent.ALCISTA++;
      else if (e.sentiment === 'bearish') sent.BAJISTA++;
      else sent.NEUTRAL++;

      if (e.rsi === 'oversold') rsi.SOBREVENTA++;
      else if (e.rsi === 'overbought') rsi.SOBRECOMPRA++;
      else rsi.NEUTRAL++;

      if (e.trend === 'uptrend') trend.ALCISTA++;
      else trend.BAJISTA++;

      if (e.volatility === 'high') vol.ALTA++;
      else vol.BAJA++;
    });

    this.signalChart = [
      { name: 'COMPRAR', value: this.buyCount },
      { name: 'CASH', value: this.sellCount },
      { name: 'MANTENER', value: this.holdCount }
    ].filter(i => i.value > 0);

    this.sentimentChart = Object.entries(sent).map(([name, value]) => ({ name, value })).filter(i => i.value > 0);
    this.rsiChart = Object.entries(rsi).map(([name, value]) => ({ name, value })).filter(i => i.value > 0);
    this.trendChart = Object.entries(trend).map(([name, value]) => ({ name, value })).filter(i => i.value > 0);
    this.volatilityChart = Object.entries(vol).map(([name, value]) => ({ name, value })).filter(i => i.value > 0);
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

  // Utilidad de Traducción
  translateState(state: string): string {
    const dict: Record<string, string> = {
      bullish: 'Alcista', bearish: 'Bajista', neutral: 'Neutral',
      oversold: 'Sobreventa', overbought: 'Sobrecompra',
      uptrend: 'Alcista', downtrend: 'Bajista',
      low: 'Baja', high: 'Alta'
    };
    return dict[state] || state;
  }

  getProbClass(prob: number): string {
    if (prob >= 0.65) return 'high';
    if (prob <= 0.35) return 'low';
    return 'mid'; // Mantener (Amarillo)
  }

  getTextClass(prob: number): string {
    if (prob >= 0.65) return 'green';
    if (prob <= 0.35) return 'purple';
    return 'yellow'; 
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
