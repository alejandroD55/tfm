// ─── Tipos primitivos ─────────────────────────────────────────────────────

export type TradingSignal         = 'BUY' | 'SELL' | 'HOLD';
export type BatchStatus           = 'STARTED' | 'COMPLETED' | 'FAILED' | 'UNKNOWN';
export type SentimentState        = 'bullish' | 'bearish' | 'neutral';
export type RsiState              = 'oversold' | 'neutral' | 'overbought';
export type TrendState            = 'uptrend' | 'downtrend';
export type VolatilityState       = 'low' | 'high';

/** Recomendación de exposición continua (5 niveles, sustituye BUY/HOLD/SELL) */
export type ExposureRecommendation =
  | 'INCREASE_STRONG'
  | 'INCREASE_MILD'
  | 'MAINTAIN'
  | 'REDUCE_MILD'
  | 'REDUCE_STRONG';

export type ConvictionLabel = 'high' | 'medium' | 'low' | 'unknown';

// ─── Pipeline health ──────────────────────────────────────────────────────

export interface StageKpis {
  [stage: string]: Record<string, number | string>;
}

export interface PipelineHealth {
  batch_status: BatchStatus;
  tickers_expected: number;
  tickers_with_indicators: number;
  tickers_with_signals: number;
  headlines_scored: number;
  coverage_ratio: number;
  stage_kpis: StageKpis;
}

// ─── Signal explanations (Bayesian evidence) ─────────────────────────────

export interface BayesianEvidence {
  sentiment: SentimentState;
  rsi: RsiState;
  trend: TrendState;
  volatility: VolatilityState;
}

export interface SignalExplanation {
  ticker: string;
  signal: TradingSignal;
  prob_up: number;
  prob_down: number;
  evidence: BayesianEvidence;
}

// ─── Backtesting ─────────────────────────────────────────────────────────

export interface BacktestingMetrics {
  cumulative_return: number;
  sharpe_ratio:      number;
  max_drawdown:      number;
  final_equity:      number;
}

/**
 * Métricas del backtesting de exposición continua.
 * Fórmula: portfolio_return_t = market_return_t × smoothed_exposure_t
 * La IA no sabe el futuro — usa solo la señal del día calculada con datos pasados.
 */
export interface ExposureBacktestingMetrics {
  cumulative_return: number;   // retorno acumulado con exposición modulada
  sharpe_ratio:      number;
  max_drawdown:      number;
  final_equity:      number;   // capital final partiendo de 10.000 €
}

export interface ExposureBacktestingDiagnostics {
  avg_exposure:       number;                     // exposición media durante el periodo
  min_exposure:       number;
  max_exposure:       number;
  regime_distribution: Record<string, number>;    // días por régimen: BULL/NEUTRAL/HIGH_VOL/BEAR
}

export interface SignalDiagnostics {
  signals: { BUY: number; SELL: number; HOLD: number };
  trades_closed: number;
  win_rate: number;
  avg_trade_return: number;
  profit_factor: number;
  time_in_market_ratio: number;
}

export interface BenchmarkComparison {
  strategy_cumulative_return: number;
  buy_hold_cumulative_return: number;
  alpha_vs_benchmark: number;
}

// ─── Summary ──────────────────────────────────────────────────────────────

export interface ReportSummary {
  total_tickers: number;
  avg_cumulative_return: number;
  avg_sharpe_ratio: number;
  avg_max_drawdown: number;
  total_closed_trades: number;
}

// ─── Full report (maps to s3://tfm-unir-datalake/results/{DATE}/report.json) ──

export interface DailyReport {
  report_date:       string;
  data_period_days:  number;
  pipeline_health:   PipelineHealth;
  signal_diagnostics:    Record<string, SignalDiagnostics>;
  benchmark_comparison:  Record<string, BenchmarkComparison>;
  top_signal_explanations: SignalExplanation[];
  backtesting_metrics:   Record<string, BacktestingMetrics>;
  summary:               ReportSummary;
  // Backtesting de exposición continua (output primario)
  exposure_backtesting_metrics?:     Record<string, ExposureBacktestingMetrics>;
  exposure_backtesting_diagnostics?: Record<string, ExposureBacktestingDiagnostics>;
}

// ─── Enriched ticker view (joined from several sections) ─────────────────

export interface TickerView {
  ticker:            string;
  signal:            TradingSignal;
  prob_up:           number;
  prob_down:         number;
  evidence:          BayesianEvidence;

  // ── Backtesting binario (referencia) ─────────────────────────────────────
  cumulative_return: number;
  sharpe_ratio:      number;
  max_drawdown:      number;
  final_equity:      number;

  // ── Backtesting de exposición continua (PRIMARIO) ─────────────────────────
  // La IA modula la posición día a día con datos solo del pasado.
  // Comienza en 0% y escala gradualmente según señales bayesianas.
  exp_cumulative_return: number;
  exp_sharpe_ratio:      number;
  exp_max_drawdown:      number;
  exp_final_equity:      number;
  avg_exposure:          number;   // % de exposición media durante el periodo
  regime_distribution:   Record<string, number>;

  // ── Diagnóstico de operaciones ────────────────────────────────────────────
  win_rate:          number;
  trades_closed:     number;
  profit_factor:     number;
  signals_count:     { BUY: number; SELL: number; HOLD: number };
  alpha_vs_benchmark:number;
  buy_hold_return:   number;

  // ── Exposición actual (último día) ────────────────────────────────────────
  exposure_pct:           number;
  exposure_recommendation:ExposureRecommendation;
  conviction_label:       ConvictionLabel;
}

// ─── S3 index entry ───────────────────────────────────────────────────────

export interface ReportDateEntry {
  date:         string;   // 'YYYY-MM-DD'
  s3Key:        string;   // 'results/YYYY-MM-DD/report.json'
  lastModified: Date;
  has_trace?:   boolean;  // true si existe bayesian_trace.json para esta fecha
}
