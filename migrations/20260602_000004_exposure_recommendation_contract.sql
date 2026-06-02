-- Migracion: contrato nativo de recomendaciones de exposicion
-- Objetivo: eliminar dependencia funcional de BUY/SELL/HOLD

-- trading_signals: nuevo campo primario
ALTER TABLE trading_signals
  ADD COLUMN IF NOT EXISTS exposure_recommendation VARCHAR(24);

UPDATE trading_signals
SET exposure_recommendation = CASE
  WHEN signal = 'BUY' THEN 'INCREASE_MILD'
  WHEN signal = 'SELL' THEN 'REDUCE_MILD'
  ELSE 'MAINTAIN'
END
WHERE exposure_recommendation IS NULL;

ALTER TABLE trading_signals
  ALTER COLUMN exposure_recommendation SET NOT NULL;

ALTER TABLE trading_signals
  DROP CONSTRAINT IF EXISTS trading_signals_exposure_recommendation_check;

ALTER TABLE trading_signals
  ADD CONSTRAINT trading_signals_exposure_recommendation_check
  CHECK (exposure_recommendation IN (
    'INCREASE_STRONG','INCREASE_MILD','MAINTAIN','REDUCE_MILD','REDUCE_STRONG'
  ));

CREATE INDEX IF NOT EXISTS idx_signals_exposure_recommendation
  ON trading_signals(exposure_recommendation);

-- signal_outcomes: almacenamiento orientado a recomendacion
ALTER TABLE signal_outcomes
  ADD COLUMN IF NOT EXISTS exposure_recommendation VARCHAR(24);

UPDATE signal_outcomes
SET exposure_recommendation = CASE
  WHEN signal = 'BUY' THEN 'INCREASE_MILD'
  WHEN signal = 'SELL' THEN 'REDUCE_MILD'
  ELSE 'MAINTAIN'
END
WHERE exposure_recommendation IS NULL;

ALTER TABLE signal_outcomes
  ALTER COLUMN exposure_recommendation SET NOT NULL;

ALTER TABLE signal_outcomes
  DROP CONSTRAINT IF EXISTS signal_outcomes_exposure_recommendation_check;

ALTER TABLE signal_outcomes
  ADD CONSTRAINT signal_outcomes_exposure_recommendation_check
  CHECK (exposure_recommendation IN (
    'INCREASE_STRONG','INCREASE_MILD','MAINTAIN','REDUCE_MILD','REDUCE_STRONG'
  ));

CREATE INDEX IF NOT EXISTS idx_so_exposure_recommendation
  ON signal_outcomes(exposure_recommendation);

-- Compatibilidad temporal: permitir nulos en signal legacy (ya no usado por el motor)
ALTER TABLE trading_signals ALTER COLUMN signal DROP NOT NULL;
ALTER TABLE signal_outcomes ALTER COLUMN signal DROP NOT NULL;
