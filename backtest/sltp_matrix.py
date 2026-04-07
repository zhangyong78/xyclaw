from __future__ import annotations

from dataclasses import replace
import html
from typing import Any

import pandas as pd

from backtest.engine import BacktestEngine
from backtest.scorer import score_period_results
from core.types import BacktestConfig


DEFAULT_SL_MULTIPLIERS = [1.0, 1.5, 2.0]
DEFAULT_TP_MULTIPLIERS = [1.0, 2.0, 3.0]


def format_multiplier(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else str(value)


def format_cell_text(*, net_pnl: float, win_rate: float, trade_count: int) -> str:
    return f"{net_pnl:.4f} | {win_rate:.2f}% | {trade_count}笔"


def run_sltp_matrix(
    *,
    bars: pd.DataFrame,
    base_config: BacktestConfig,
    period: str,
    sl_multipliers: list[float] | None = None,
    tp_multipliers: list[float] | None = None,
    include_chart_payloads: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, dict[str, dict[str, Any]]]:
    if bars.empty:
        empty = pd.DataFrame()
        return (empty, {}) if include_chart_payloads else empty

    sl_values = sl_multipliers or DEFAULT_SL_MULTIPLIERS
    tp_values = tp_multipliers or DEFAULT_TP_MULTIPLIERS
    rows: list[dict] = []
    chart_payloads: dict[str, dict[str, Any]] = {}

    for sl_mult in sl_values:
        for tp_mult in tp_values:
            config = replace(
                base_config,
                stop_loss_atr_multiplier=float(sl_mult),
                take_profit_r_multiple=float(tp_mult),
            )
            result = BacktestEngine(config).run(bars, period=period)
            summary = result["summary"]
            net_pnl = float(summary.get("net_pnl", 0.0))
            win_rate = float(summary.get("win_rate", 0.0))
            trade_count = int(summary.get("trade_count", 0))
            matrix_key = build_matrix_key(float(sl_mult), float(tp_mult))
            combo_label = f"SL x{format_multiplier(float(sl_mult))} / TP = SL x{format_multiplier(float(tp_mult))}"
            rows.append(
                {
                    **summary,
                    "matrix_key": matrix_key,
                    "combo_label": combo_label,
                    "sl_multiplier": float(sl_mult),
                    "tp_multiplier": float(tp_mult),
                    "sl_label": f"SL x{format_multiplier(float(sl_mult))}",
                    "tp_label": f"TP = SL x{format_multiplier(float(tp_mult))}",
                    "net_pnl": net_pnl,
                    "win_rate": win_rate,
                    "trade_count": trade_count,
                    "net_return_pct": float(summary.get("net_return_pct", 0.0)),
                    "max_drawdown_pct": float(summary.get("max_drawdown_pct", 0.0)),
                    "display_text": format_cell_text(net_pnl=net_pnl, win_rate=win_rate, trade_count=trade_count),
                }
            )
            if include_chart_payloads:
                chart_payloads[matrix_key] = {
                    "matrix_key": matrix_key,
                    "sl_multiplier": float(sl_mult),
                    "tp_multiplier": float(tp_mult),
                    "sl_label": f"SL x{format_multiplier(float(sl_mult))}",
                    "tp_label": f"TP = SL x{format_multiplier(float(tp_mult))}",
                    "display_label": combo_label,
                    "summary": dict(summary),
                    "signal_frame": result["signal_frame"].copy(),
                    "trades": result["trades"].copy(),
                    "equity": result["equity"].copy(),
                }

    matrix_df = pd.DataFrame(rows)
    if matrix_df.empty:
        return (matrix_df, chart_payloads) if include_chart_payloads else matrix_df

    best_pnl = float(matrix_df["net_pnl"].max())
    matrix_df["is_best"] = matrix_df["net_pnl"] == best_pnl
    scored_df = score_period_results(matrix_df.to_dict(orient="records"))
    ranking_cols = scored_df[["matrix_key", "return_score", "drawdown_score", "trade_score", "final_score", "rank"]].copy()
    matrix_df = matrix_df.merge(ranking_cols, on="matrix_key", how="left")
    matrix_df = matrix_df.sort_values(["sl_multiplier", "tp_multiplier"]).reset_index(drop=True)
    return (matrix_df, chart_payloads) if include_chart_payloads else matrix_df


def build_matrix_key(sl_multiplier: float, tp_multiplier: float) -> str:
    return f"{format_multiplier(float(sl_multiplier))}|{format_multiplier(float(tp_multiplier))}"


def build_sltp_matrix_section_html(
    *,
    matrix_df: pd.DataFrame,
    symbol: str,
    period: str,
    atr_period: int,
) -> str:
    if matrix_df.empty:
        return ""

    tp_values = sorted(matrix_df["tp_multiplier"].astype(float).unique())
    sl_values = sorted(matrix_df["sl_multiplier"].astype(float).unique())
    matrix_map = {
        (float(row["sl_multiplier"]), float(row["tp_multiplier"])): row
        for _, row in matrix_df.iterrows()
    }

    head_cells = ['<div class="sltp-corner">SL \\/ TP</div>']
    for tp in tp_values:
        head_cells.append(f'<div class="sltp-head">{html.escape(f"TP = SL x{format_multiplier(tp)}")}</div>')

    body_rows: list[str] = []
    for sl in sl_values:
        cells = [f'<div class="sltp-side">{html.escape(f"SL x{format_multiplier(sl)}")}</div>']
        for tp in tp_values:
            row = matrix_map[(sl, tp)]
            cell_class = "sltp-cell is-best" if bool(row["is_best"]) else "sltp-cell"
            cells.append(
                f'<div class="{cell_class}">{html.escape(str(row["display_text"]))}</div>'
            )
        body_rows.append(f'<div class="sltp-row">{"".join(cells)}</div>')

    title = html.escape(f"{symbol} {period} SL / TP 参数矩阵")
    desc = html.escape(f"基于 ATR{atr_period} 的止损距离，对不同止损倍数和止盈倍数进行回测。单元格显示：净收益 | 胜率 | 交易笔数。")

    return f"""
    <section class="chart-card sltp-section">
      <div class="chart-head">
        <h2>{title}</h2>
        <p>{desc}</p>
      </div>
      <div class="sltp-grid">
        <div class="sltp-row sltp-header">{''.join(head_cells)}</div>
        {''.join(body_rows)}
      </div>
    </section>
    """
