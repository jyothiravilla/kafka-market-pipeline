CREATE TABLE IF NOT EXISTS candles (
  symbol       TEXT        NOT NULL,
  window_start TIMESTAMPTZ NOT NULL,
  open         NUMERIC,
  high         NUMERIC,
  low          NUMERIC,
  close        NUMERIC,
  volume       NUMERIC,
  trade_count  INT,
  PRIMARY KEY (symbol, window_start)
);
