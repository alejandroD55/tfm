import { Injectable, signal } from '@angular/core';
import { Observable, map, switchMap, of, tap } from 'rxjs';
import { ApiService } from './api.service';
import { PipelineContextService } from './pipeline-context.service';
import {
  DailyReport, TickerView, ReportDateEntry,
  ExposureRecommendation, ConvictionLabel,
  ExposureBacktestingMetrics, ExposureBacktestingDiagnostics,
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

  /** Tickers del reporte — prioriza métricas de exposición continua. */
  private reportTickers(report: DailyReport): string[] {
    const expKeys = Object.keys(report.exposure_backtesting_metrics ?? {});
    if (expKeys.length) return expKeys.sort();
    const benchKeys = Object.keys(report.benchmark_comparison ?? {});
    if (benchKeys.length) return benchKeys.sort();
    return Object.keys(report.backtesting_metrics ?? {}).sort();
  }

  // ─── Construye TickerView[] para todos los tickers ────────────────
  buildTickerViews(report: DailyReport): TickerView[] {
    const tickers = this.reportTickers(report);
    const recommendationDiagnostics = (report as any).recommendation_diagnostics
      ?? (report as any).signal_diagnostics
      ?? {};
    const recommendationExplanations = (report as any).top_recommendation_explanations
      ?? (report as any).top_signal_explanations
      ?? [];
    return tickers.map(ticker => {
      const bm    = report.backtesting_metrics?.[ticker];
      const diag  = recommendationDiagnostics[ticker];
      const bench = report.benchmark_comparison?.[ticker];
      const expl  = recommendationExplanations.find((e: any) => e.ticker === ticker);

      const prob_up = expl?.prob_up ?? 0.5;

      const exposure_pct = this.calcExposurePct(prob_up);
      const exposure_recommendation = this.calcExposureRec(exposure_pct, prob_up);
      const conviction_label = this.calcConviction(prob_up);

      const expBm:   ExposureBacktestingMetrics    = report.exposure_backtesting_metrics?.[ticker]   ?? {} as any;
      const expDiag: ExposureBacktestingDiagnostics = report.exposure_backtesting_diagnostics?.[ticker] ?? {} as any;

      const avg_exp  = (expDiag.avg_exposure ?? 0) * 100;
      const avg_cash = (expDiag.avg_cash_pct ?? (1 - (expDiag.avg_exposure ?? 0))) * 100;

      return {
        ticker,
        prob_up,
        prob_down: expl?.prob_down ?? 0.5,
        evidence:  expl?.evidence ?? {
          sentiment: 'neutral', rsi: 'neutral',
          trend: 'uptrend', volatility: 'low',
        },
        // Binario (referencia legacy — no usar en UI primaria)
        cumulative_return:  bm?.cumulative_return ?? 0,
        sharpe_ratio:       bm?.sharpe_ratio ?? 0,
        max_drawdown:       bm?.max_drawdown ?? 0,
        final_equity:       bm?.final_equity ?? 10000,
        // Exposición continua (primario)
        exp_cumulative_return: expBm.cumulative_return ?? 0,
        exp_sharpe_ratio:      expBm.sharpe_ratio ?? 0,
        exp_max_drawdown:      expBm.max_drawdown ?? 0,
        exp_final_equity:      expBm.final_equity ?? 10000,
        avg_exposure:          avg_exp,
        avg_cash_pct:          avg_cash,
        regime_distribution:   expDiag.regime_distribution ?? {},
        // Operaciones
        win_rate:           diag?.win_rate ?? 0,
        trades_closed:      diag?.trades_closed ?? 0,
        profit_factor:      diag?.profit_factor ?? 0,
        recommendations_count: diag?.recommendations ?? {
          INCREASE_STRONG: 0,
          INCREASE_MILD: 0,
          MAINTAIN: 0,
          REDUCE_MILD: 0,
          REDUCE_STRONG: 0,
        },
        alpha_vs_benchmark: bench?.alpha_vs_benchmark ?? 0,
        buy_hold_return:    bench?.buy_hold_cumulative_return ?? 0,
        // Exposición actual (último día)
        exposure_pct,
        exposure_recommendation,
        conviction_label,
      } as TickerView;
    });
  }

  /** Calcula el % de exposición objetivo (réplica del backend prob_to_exposure, régimen NEUTRAL) */
  private calcExposurePct(prob_up: number): number {
    const floor = 0.50; const ceiling = 0.85;
    const t = Math.max(0, Math.min(1, (prob_up - 0.30) / (0.75 - 0.30)));
    return Math.round((floor + t * (ceiling - floor)) * 1000) / 10;
  }

  /** Deriva la recomendación de exposición en 5 niveles desde el % */
  private calcExposureRec(pct: number, prob_up: number): ExposureRecommendation {
    if (pct >= 75) return 'INCREASE_STRONG';
    if (pct >= 62) return 'INCREASE_MILD';
    if (pct >= 52 && prob_up >= 0.48) return 'MAINTAIN';
    if (pct >= 50) return 'REDUCE_MILD';
    return 'REDUCE_STRONG';
  }

  /** Convicción aproximada desde distancia a la zona de incertidumbre. */
  private calcConviction(prob_up: number): ConvictionLabel {
    const dist = Math.abs(prob_up - 0.5);
    if (dist >= 0.20) return 'high';
    if (dist >= 0.10) return 'medium';
    return 'low';
  }

  /** Media de capital desplegado (% invertido) en toda la cartera. */
  avgPortfolioExposure(views: TickerView[]): number {
    if (!views.length) return 0;
    return views.reduce((s, v) => s + v.avg_exposure, 0) / views.length;
  }

  // ─── Charts ───────────────────────────────────────────────────────

  signalDistributionChart(views: TickerView[]): ChartDataPoint[] {
    const counts: Record<string, number> = {
      INCREASE_STRONG: 0,
      INCREASE_MILD: 0,
      MAINTAIN: 0,
      REDUCE_MILD: 0,
      REDUCE_STRONG: 0,
    };
    for (const v of views) counts[v.exposure_recommendation] = (counts[v.exposure_recommendation] ?? 0) + 1;
    return Object.entries(counts).filter(([, v]) => v > 0).map(([name, value]) => ({ name, value }));
  }

  /** Comparativa de retorno: Exposición continua (IA) vs Buy & Hold — ordenado por ticker */
  returnComparisonChart(views: TickerView[]): ChartSeries[] {
    const sorted = [...views].sort((a, b) => a.ticker.localeCompare(b.ticker));
    return [
      { name: 'Estrategia (Exposición)', series: sorted.map(v => ({ name: v.ticker, value: +(v.exp_cumulative_return * 100).toFixed(2) })) },
      { name: 'Buy & Hold',              series: sorted.map(v => ({ name: v.ticker, value: +(v.buy_hold_return * 100).toFixed(2) })) },
    ];
  }

  sharpeChart(views: TickerView[]): ChartDataPoint[] {
    return [...views].sort((a, b) => a.ticker.localeCompare(b.ticker))
      .map(v => ({ name: v.ticker, value: +v.exp_sharpe_ratio.toFixed(3) }));
  }

  drawdownChart(views: TickerView[]): ChartDataPoint[] {
    return [...views].sort((a, b) => a.ticker.localeCompare(b.ticker))
      .map(v => ({ name: v.ticker, value: +(Math.abs(v.exp_max_drawdown) * 100).toFixed(2) }));
  }

  alphaChart(views: TickerView[]): ChartDataPoint[] {
    return [...views].sort((a, b) => a.ticker.localeCompare(b.ticker))
      .map(v => ({
        name: v.ticker,
        value: +((v.exp_cumulative_return - v.buy_hold_return) * 100).toFixed(2),
      }));
  }

  avgExposureChart(views: TickerView[]): ChartDataPoint[] {
    return [...views].sort((a, b) => a.ticker.localeCompare(b.ticker))
      .map(v => ({ name: v.ticker, value: +v.avg_exposure.toFixed(1) }));
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
        tickers_with_indicators: 0, tickers_with_recommendations: 0,
        headlines_scored: 0, coverage_ratio: 0, stage_kpis: {},
      },
      recommendation_diagnostics: {}, benchmark_comparison: {},
      top_recommendation_explanations: [], backtesting_metrics: {},
      summary: {
        total_tickers: 0, avg_cumulative_return: 0,
        avg_sharpe_ratio: 0, avg_max_drawdown: 0, total_closed_trades: 0,
      },
    };
  }
}
