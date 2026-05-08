// ─── Tipos del bayesian_trace.json ────────────────────────────────────────────

export interface DiscretizationRules {
  rsi: {
    oversold_below:   number;
    overbought_above: number;
    neutral_range:    [number, number];
    rationale:        string;
  };
  trend:      { rule: string; rationale: string };
  volatility: { high_if_band_width_ratio_above: number; formula: string; rationale: string };
}

export interface SignalThresholds {
  BUY:  { prob_up_above: number; rationale: string };
  SELL: { prob_up_below: number; rationale: string };
  HOLD: { range: [number, number]; rationale: string };
}

export interface NodePrior {
  [state: string]: number | string;
  rationale: string;
}

export interface CptMarketDirection {
  variable:       string;
  states:         string[];
  evidence_order: string[];
  rationale:      Record<string, string>;
  values_P_down:  number[];
  values_P_up:    number[];
}

export interface ModelConfig {
  version:               string;
  description:           string;
  discretization:        DiscretizationRules;
  signal_thresholds:     SignalThresholds;
  priors:                Record<string, Record<string, number | string>>;
  cpt_market_direction:  CptMarketDirection;
  known_limitations:     string[];
}

// ─── Por ticker ───────────────────────────────────────────────────────────────

export interface RawValues {
  close_price:    number;
  rsi_14:         number;
  sma_20:         number;
  sma_50:         number;
  sma_spread:     number;
  bb_upper:       number | null;
  bb_lower:       number | null;
  bb_width_ratio: number;
}

export interface DiscretizedValues {
  sentiment_raw:    string;
  sentiment_conf:   number;
  sentiment_state:  string;
  rsi_state:        string;
  trend_state:      string;
  volatility_state: string;
}

export interface HeadlineSample {
  headline:   string;
  sentiment:  string;
  confidence: number;
}

export interface SentimentDistribution {
  count: number;
  pct:   number;
}

export interface SentimentDetail {
  total_headlines:    number;
  aggregation_method: string;
  distribution:       Record<string, SentimentDistribution>;
  dominant:           { sentiment: string; confidence: number };
  headlines_sample:   HeadlineSample[];
  limitation:         string;
}

export interface TickerInference {
  prob_up:        number;
  prob_down:      number;
  signal:         string;
  threshold_used: number | number[];
}

export interface TickerTrace {
  raw_values:       RawValues;
  discretization:   DiscretizedValues;
  sentiment_detail: SentimentDetail;
  inference:        TickerInference;
  reasoning:        string;
}

// ─── Ejecución ────────────────────────────────────────────────────────────────

export interface ExecutionMeta {
  started_at:        string;
  finished_at:       string;
  duration_seconds:  number;
  tickers_attempted: number;
  signals_generated: number;
  tickers_skipped:   number;
  skipped_detail:    { ticker: string; reason: string }[];
}

// ─── Trace completo ───────────────────────────────────────────────────────────

export interface BayesianTrace {
  schema_version: string;
  batch_date:     string;
  generated_at:   string;
  execution:      ExecutionMeta;
  model_config:   ModelConfig;
  tickers:        Record<string, TickerTrace>;
  audit_notes:    Record<string, string | string[]>;
}

// ─── Respuesta de /model ──────────────────────────────────────────────────────

export interface ModelConfigResponse {
  source_date:    string;
  schema_version: string;
  model_config:   ModelConfig;
  audit_notes:    Record<string, string | string[]>;
}
