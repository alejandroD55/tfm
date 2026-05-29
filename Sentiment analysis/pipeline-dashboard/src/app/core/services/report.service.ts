import { Injectable, signal } from '@angular/core';
import { Observable, map, switchMap, of, tap } from 'rxjs';
import { ApiService } from './api.service';
import { PipelineContextService } from './pipeline-context.service';
import {
  DailyReport, TickerView, ReportDateEntry,
} from '../models/report.model';
import { ChartDataPoint, ChartSeries } from '../models/pipeline.model';

@Injectable({ providedIn: 'root' })
export class ReportService {

  private reportCache = new Map<string, DailyReport>();
  readonly selectedDate = signal<string>('');

  constructor(
    private api: ApiService,
    private pipelineCtx: PipelineContextService,
  ) {}

  clearCache(): void {
    this.reportCache.clear();
  }

  // ─── Lista fechas disponibles (acotada al pipeline activo) ─────────
  listAvailableDates(): Observable<ReportDateEntry[]> {
    const { start, end } = this.pipelineCtx.dateFilter();
    return this.api.listReports(start, end).pipe(
      map(resp => resp.dates.map(d => ({
        date:         d.date,
        s3Key:        d.s3Key ?? '',
        lastModified: d.lastModified ? new Date(d.lastModified) : new Date(),
        has_trace:    d.has_trace ?? false,
      } as ReportDateEntry & { has_trace: boolean })))
    );
  }

  // ─── Carga un report por fecha ────────────────────────────────────
  loadReport(date: string): Observable<DailyReport> {
    if (this.reportCache.has(date)) {
      return of(this.reportCache.get(date)!);
    }
    return this.api.getReport<DailyReport>(date).pipe(
      tap(report => this.reportCache.set(date, report))
    );
  }

  // ─── Carga el report más reciente ─────────────────────────────────
  loadLatestReport(): Observable<DailyReport> {
    return this.listAvailableDates().pipe(
      switchMap(dates => {
        if (dates.length === 0) return of(this.emptyReport());
        const latest = dates[0];
        this.selectedDate.set(latest.date);
        return this.loadReport(latest.date);
      })
    );
  }

  // ─── Construye TickerView[] para todos los tickers ────────────────
  buildTickerViews(report: DailyReport): TickerView[] {
    const tickers = Object.keys(report.backtesting_metrics);
    return tickers.map(ticker => {
      const bm    = report.backtesting_metrics[ticker];
      const diag  = report.signal_diagnostics[ticker];
      const bench = report.benchmark_comparison[ticker];
      const expl  = report.top_signal_explanations.find(e => e.ticker === ticker);

      return {
        ticker,
        signal:    expl?.signal ?? 'HOLD',
        prob_up:   expl?.prob_up ?? 0.5,
        prob_down: expl?.prob_down ?? 0.5,
        evidence:  expl?.evidence ?? {
          sentiment: 'neutral', rsi: 'neutral',
          trend: 'uptrend', volatility: 'low',
        },
        cumulative_return:  bm?.cumulative_return ?? 0,
        sharpe_ratio:       bm?.sharpe_ratio ?? 0,
        max_drawdown:       bm?.max_drawdown ?? 0,
        final_equity:       bm?.final_equity ?? 10000,
        win_rate:           diag?.win_rate ?? 0,
        trades_closed:      diag?.trades_closed ?? 0,
        profit_factor:      diag?.profit_factor ?? 0,
        signals_count:      diag?.signals ?? { BUY: 0, SELL: 0, HOLD: 0 },
        alpha_vs_benchmark: bench?.alpha_vs_benchmark ?? 0,
        buy_hold_return:    bench?.buy_hold_cumulative_return ?? 0,
      } as TickerView;
    });
  }

  // ─── Charts ───────────────────────────────────────────────────────

  signalDistributionChart(views: TickerView[]): ChartDataPoint[] {
    const counts = { BUY: 0, SELL: 0, HOLD: 0 };
    for (const v of views) counts[v.signal as keyof typeof counts]++;
    return Object.entries(counts).filter(([, v]) => v > 0).map(([name, value]) => ({ name, value }));
  }

  returnComparisonChart(views: TickerView[]): ChartSeries[] {
    return [
      { name: 'Estrategia', series: views.map(v => ({ name: v.ticker, value: +(v.cumulative_return * 100).toFixed(2) })) },
      { name: 'Buy & Hold', series: views.map(v => ({ name: v.ticker, value: +(v.buy_hold_return * 100).toFixed(2) })) },
    ];
  }

  sharpeChart(views: TickerView[]): ChartDataPoint[] {
    return views.map(v => ({ name: v.ticker, value: +v.sharpe_ratio.toFixed(3) }));
  }

  drawdownChart(views: TickerView[]): ChartDataPoint[] {
    return views.map(v => ({ name: v.ticker, value: +(Math.abs(v.max_drawdown) * 100).toFixed(2) }));
  }

  alphaChart(views: TickerView[]): ChartDataPoint[] {
    return views.map(v => ({ name: v.ticker, value: +(v.alpha_vs_benchmark * 100).toFixed(2) }));
  }

  probUpChart(views: TickerView[]): ChartDataPoint[] {
    return [...views]
      .sort((a, b) => b.prob_up - a.prob_up)
      .map(v => ({ name: v.ticker, value: +(v.prob_up * 100).toFixed(1) }));
  }

  private emptyReport(): DailyReport {
    return {
      report_date: '', data_period_days: 90,
      pipeline_health: {
        batch_status: 'UNKNOWN', tickers_expected: 0,
        tickers_with_indicators: 0, tickers_with_signals: 0,
        headlines_scored: 0, coverage_ratio: 0, stage_kpis: {},
      },
      signal_diagnostics: {}, benchmark_comparison: {},
      top_signal_explanations: [], backtesting_metrics: {},
      summary: {
        total_tickers: 0, avg_cumulative_return: 0,
        avg_sharpe_ratio: 0, avg_max_drawdown: 0, total_closed_trades: 0,
      },
    };
  }
}
