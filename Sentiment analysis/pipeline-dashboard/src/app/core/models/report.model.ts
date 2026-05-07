// ─── Tipos primitivos ─────────────────────────────────────────────────────

export type TradingSignal    = 'BUY' | 'SELL' | 'HOLD';
export type BatchStatus      = 'STARTED' | 'COMPLETED' | 'FAILED' | 'UNKNOWN';
export type SentimentState   = 'bullish' | 'bearish' | 'neutral';
export type RsiState         = 'oversold' | 'neutral' | 'overbought';
export type TrendState       = 'uptrend' | 'downtrend';
export type VolatilityState  = 'low' | 'high';

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
  sharpe_ratio: number;
  max_drawdown: number;
  final_equity: number;
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
  report_date: string;
  data_period_days: number;
  pipeline_health: PipelineHealth;
  signal_diagnostics: Record<string, SignalDiagnostics>;
  benchmark_comparison: Record<string, BenchmarkComparison>;
  top_signal_explanations: SignalExplanation[];
  backtesting_metrics: Record<string, BacktestingMetrics>;
  summary: ReportSummary;
}

// ─── Enriched ticker view (joined from several sections) ─────────────────

export interface TickerView {
  ticker: string;
  signal: TradingSignal;
  prob_up: number;
  prob_down: number;
  evidence: BayesianEvidence;
  cumulative_return: number;
  sharpe_ratio: number;
  max_drawdown: number;
  final_equity: number;
  win_rate: number;
  trades_closed: number;
  profit_factor: number;
  signals_count: { BUY: number; SELL: number; HOLD: number };
  alpha_vs_benchmark: number;
  buy_hold_return: number;
}

// ─── S3 index entry ───────────────────────────────────────────────────────

export interface ReportDateEntry {
  date: string;          // 'YYYY-MM-DD'
  s3Key: string;         // 'results/YYYY-MM-DD/report.json'
  lastModified: Date;
}
