CREATE TABLE IF NOT EXISTS market_candle_1h (
    symbol          VARCHAR(32)      NOT NULL,
    ts              TIMESTAMPTZ      NOT NULL,
    open            NUMERIC(20, 8)   NOT NULL,
    high            NUMERIC(20, 8)   NOT NULL,
    low             NUMERIC(20, 8)   NOT NULL,
    close           NUMERIC(20, 8)   NOT NULL,
    volume          NUMERIC(28, 8)   NOT NULL DEFAULT 0,
    turnover        NUMERIC(28, 8)   NOT NULL DEFAULT 0,
    PRIMARY KEY (symbol, ts)
);

CREATE TABLE IF NOT EXISTS strategy_config (
    id                  BIGSERIAL       PRIMARY KEY,
    name                VARCHAR(64)     NOT NULL,
    symbol              VARCHAR(32)     NOT NULL,
    fast_ema            INTEGER         NOT NULL DEFAULT 5,
    slow_ema            INTEGER         NOT NULL DEFAULT 8,
    risk_amount         NUMERIC(20, 8)  NOT NULL DEFAULT 100,
    hold_bars           INTEGER         NOT NULL DEFAULT 3,
    fee_bps             NUMERIC(10, 4)  NOT NULL DEFAULT 2.8,
    slippage_bps        NUMERIC(10, 4)  NOT NULL DEFAULT 2,
    max_allocation_pct  NUMERIC(10, 6)  NOT NULL DEFAULT 0.95,
    enabled             BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS backtest_run (
    id              BIGSERIAL       PRIMARY KEY,
    symbol          VARCHAR(32)     NOT NULL,
    start_ts        TIMESTAMPTZ     NOT NULL,
    end_ts          TIMESTAMPTZ     NOT NULL,
    periods         TEXT            NOT NULL,
    status          VARCHAR(24)     NOT NULL DEFAULT 'completed',
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS backtest_period_result (
    id                        BIGSERIAL       PRIMARY KEY,
    run_id                    BIGINT          NOT NULL REFERENCES backtest_run(id),
    period                    VARCHAR(8)      NOT NULL,
    bars                      INTEGER         NOT NULL,
    trade_count               INTEGER         NOT NULL DEFAULT 0,
    win_rate                  NUMERIC(10, 4)  NOT NULL DEFAULT 0,
    net_return_pct            NUMERIC(10, 4)  NOT NULL DEFAULT 0,
    profit_factor             NUMERIC(18, 6)  NOT NULL DEFAULT 0,
    max_drawdown_pct          NUMERIC(10, 4)  NOT NULL DEFAULT 0,
    max_drawdown_amount       NUMERIC(20, 8)  NOT NULL DEFAULT 0,
    intrabar_drawdown_pct     NUMERIC(10, 4)  NOT NULL DEFAULT 0,
    rolling_30d_drawdown_pct  NUMERIC(10, 4)  NOT NULL DEFAULT 0,
    rolling_90d_drawdown_pct  NUMERIC(10, 4)  NOT NULL DEFAULT 0,
    drawdown_duration_days    NUMERIC(12, 4)  NOT NULL DEFAULT 0,
    time_under_water_pct      NUMERIC(10, 4)  NOT NULL DEFAULT 0,
    ulcer_index               NUMERIC(12, 4)  NOT NULL DEFAULT 0,
    cdar_95_pct               NUMERIC(10, 4)  NOT NULL DEFAULT 0,
    return_score              NUMERIC(10, 4)  NOT NULL DEFAULT 0,
    drawdown_score            NUMERIC(10, 4)  NOT NULL DEFAULT 0,
    trade_score               NUMERIC(10, 4)  NOT NULL DEFAULT 0,
    final_score               NUMERIC(10, 4)  NOT NULL DEFAULT 0,
    rank                      INTEGER         NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS backtest_trade (
    id              BIGSERIAL       PRIMARY KEY,
    run_id          BIGINT          NOT NULL REFERENCES backtest_run(id),
    period          VARCHAR(8)      NOT NULL,
    entry_ts        TIMESTAMPTZ     NOT NULL,
    exit_ts         TIMESTAMPTZ     NOT NULL,
    side            VARCHAR(16)     NOT NULL,
    qty             NUMERIC(28, 12) NOT NULL,
    entry_price     NUMERIC(20, 8)  NOT NULL,
    exit_price      NUMERIC(20, 8)  NOT NULL,
    pnl             NUMERIC(20, 8)  NOT NULL,
    pnl_pct         NUMERIC(12, 6)  NOT NULL,
    fees            NUMERIC(20, 8)  NOT NULL DEFAULT 0,
    exit_reason     VARCHAR(32)     NOT NULL
);

CREATE TABLE IF NOT EXISTS equity_curve (
    id              BIGSERIAL       PRIMARY KEY,
    run_id          BIGINT          NOT NULL REFERENCES backtest_run(id),
    period          VARCHAR(8)      NOT NULL,
    ts              TIMESTAMPTZ     NOT NULL,
    equity          NUMERIC(20, 8)  NOT NULL,
    equity_low      NUMERIC(20, 8)  NOT NULL,
    cash            NUMERIC(20, 8)  NOT NULL,
    position_qty    NUMERIC(28, 12) NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS live_order_log (
    id              BIGSERIAL       PRIMARY KEY,
    cl_ord_id       VARCHAR(64)     NOT NULL,
    symbol          VARCHAR(32)     NOT NULL,
    side            VARCHAR(8)      NOT NULL,
    ord_type        VARCHAR(24)     NOT NULL,
    price           NUMERIC(20, 8),
    size            NUMERIC(28, 12) NOT NULL,
    state           VARCHAR(24)     NOT NULL,
    request_ts      TIMESTAMPTZ     NOT NULL,
    ack_ts          TIMESTAMPTZ,
    fill_ts         TIMESTAMPTZ,
    source          VARCHAR(24)     NOT NULL DEFAULT 'live'
);

CREATE INDEX IF NOT EXISTS idx_backtest_period_result_run_id ON backtest_period_result(run_id);
CREATE INDEX IF NOT EXISTS idx_backtest_trade_run_id ON backtest_trade(run_id);
CREATE INDEX IF NOT EXISTS idx_equity_curve_run_period_ts ON equity_curve(run_id, period, ts);
CREATE INDEX IF NOT EXISTS idx_live_order_log_cl_ord_id ON live_order_log(cl_ord_id);
