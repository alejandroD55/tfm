-- =============================================================================
-- Limpieza Aurora PostgreSQL — conservar solo SPY, IWM, XLE, GLD
-- =============================================================================
-- PASO 1: Ejecuta el bloque PREVIEW y revisa filas a eliminar.
-- PASO 2: Ejecuta el bloque DELETE en una transacción (haz backup antes si puedes).
-- =============================================================================

-- ─── PASO 1: PREVIEW ─────────────────────────────────────────────────────────

SELECT 'sentiment_scores' AS tabla, ticker, COUNT(*) AS filas
FROM sentiment_scores
WHERE ticker NOT IN ('SPY', 'IWM', 'XLE', 'GLD')
GROUP BY ticker
UNION ALL
SELECT 'technical_indicators', ticker, COUNT(*)
FROM technical_indicators
WHERE ticker NOT IN ('SPY', 'IWM', 'XLE', 'GLD')
GROUP BY ticker
UNION ALL
SELECT 'trading_signals', ticker, COUNT(*)
FROM trading_signals
WHERE ticker NOT IN ('SPY', 'IWM', 'XLE', 'GLD')
GROUP BY ticker
UNION ALL
SELECT 'signal_explanations', ticker, COUNT(*)
FROM signal_explanations
WHERE ticker NOT IN ('SPY', 'IWM', 'XLE', 'GLD')
GROUP BY ticker
ORDER BY tabla, ticker;

SELECT 'sentiment_scores' AS tabla,
       COUNT(*) FILTER (WHERE ticker IN ('SPY','IWM','XLE','GLD')) AS conservar,
       COUNT(*) FILTER (WHERE ticker NOT IN ('SPY','IWM','XLE','GLD')) AS eliminar
FROM sentiment_scores
UNION ALL
SELECT 'technical_indicators',
       COUNT(*) FILTER (WHERE ticker IN ('SPY','IWM','XLE','GLD')),
       COUNT(*) FILTER (WHERE ticker NOT IN ('SPY','IWM','XLE','GLD'))
FROM technical_indicators
UNION ALL
SELECT 'trading_signals',
       COUNT(*) FILTER (WHERE ticker IN ('SPY','IWM','XLE','GLD')),
       COUNT(*) FILTER (WHERE ticker NOT IN ('SPY','IWM','XLE','GLD'))
FROM trading_signals
UNION ALL
SELECT 'signal_explanations',
       COUNT(*) FILTER (WHERE ticker IN ('SPY','IWM','XLE','GLD')),
       COUNT(*) FILTER (WHERE ticker NOT IN ('SPY','IWM','XLE','GLD'))
FROM signal_explanations;


-- ─── PASO 2: DELETE (copia y ejecuta cuando el preview sea correcto) ─────────

/*
BEGIN;

DELETE FROM sentiment_scores
WHERE ticker NOT IN ('SPY', 'IWM', 'XLE', 'GLD');

DELETE FROM technical_indicators
WHERE ticker NOT IN ('SPY', 'IWM', 'XLE', 'GLD');

DELETE FROM trading_signals
WHERE ticker NOT IN ('SPY', 'IWM', 'XLE', 'GLD');

DELETE FROM signal_explanations
WHERE ticker NOT IN ('SPY', 'IWM', 'XLE', 'GLD');

SELECT setval(pg_get_serial_sequence('sentiment_scores', 'id'),
              COALESCE((SELECT MAX(id) FROM sentiment_scores), 1), true);
SELECT setval(pg_get_serial_sequence('technical_indicators', 'id'),
              COALESCE((SELECT MAX(id) FROM technical_indicators), 1), true);
SELECT setval(pg_get_serial_sequence('trading_signals', 'id'),
              COALESCE((SELECT MAX(id) FROM trading_signals), 1), true);
SELECT setval(pg_get_serial_sequence('signal_explanations', 'id'),
              COALESCE((SELECT MAX(id) FROM signal_explanations), 1), true);

COMMIT;
*/

-- ─── OPCIONAL: auditoría batch_log (requested_tickers con símbolos viejos) ───
/*
DELETE FROM batch_log bl
WHERE EXISTS (
  SELECT 1
  FROM jsonb_array_elements_text(bl.requested_tickers) AS t(val)
  WHERE t.val NOT IN ('SPY', 'IWM', 'XLE', 'GLD')
);
*/
