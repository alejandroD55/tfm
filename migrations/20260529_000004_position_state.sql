-- position_state: exposición y régimen por ticker/día (bootstrap local + pipeline)
CREATE TABLE IF NOT EXISTS position_state (
    batch_date              DATE         NOT NULL,
    ticker                  VARCHAR(10)  NOT NULL,
    prob_up                 FLOAT,
    market_regime           VARCHAR(20),
    target_exposure         FLOAT,
    smoothed_exposure       FLOAT,
    exposure_delta          FLOAT,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    confirmed_regime        VARCHAR(20),
    raw_regime              VARCHAR(20),
    regime_candidate        VARCHAR(20),
    regime_candidate_days   INTEGER,
    vt_exposure             FLOAT,
    kelly_exposure          FLOAT,
    vol_5d                  FLOAT,
    vol_20d                 FLOAT,
    vol_ratio               FLOAT,
    vol_percentile_1y       FLOAT,
    sentiment_dispersion    FLOAT,
    vix_regime_label        VARCHAR(25),
    PRIMARY KEY (batch_date, ticker)
);

CREATE INDEX IF NOT EXISTS idx_position_state_ticker_date
    ON position_state (ticker, batch_date DESC);
