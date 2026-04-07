from __future__ import annotations

import html
from pathlib import Path

import pandas as pd


TRADE_CHART_TITLE = "\u56de\u6d4b K \u7ebf\u56fe"
TRADE_CHART_DESC = "\u56fe\u4e2d\u5df2\u7ecf\u6807\u51fa\u4e70\u70b9\u3001\u5356\u70b9\uff0c\u5e76\u53e0\u52a0 EMA21 \u548c EMA55\u3002"
BUY_TEXT = "\u4e70\u5165"
SELL_TEXT = "\u5356\u51fa"
DRAWDOWN_CHART_TITLE = "\u56de\u64a4\u66f2\u7ebf\u56fe"
DRAWDOWN_CHART_DESC = "\u663e\u793a\u8d44\u91d1\u66f2\u7ebf\u7684\u56de\u64a4\u767e\u5206\u6bd4\uff0c\u8d8a\u63a5\u8fd1 0 \u8868\u793a\u56de\u64a4\u8d8a\u6d45\u3002"
NO_TRADE_CHART_DATA = "\u6ca1\u6709\u53ef\u7ed8\u5236\u7684 K \u7ebf\u6570\u636e\u3002"
NO_DRAWDOWN_DATA = "\u6ca1\u6709\u53ef\u7ed8\u5236\u7684\u56de\u64a4\u6570\u636e\u3002"
MARKERS_DRAWN = "\u4e70\u5356\u70b9\u5df2\u6807\u6ce8"


def build_chart_artifacts(
    *,
    symbol: str,
    period: str,
    signal_frame: pd.DataFrame,
    trades_df: pd.DataFrame,
    equity_df: pd.DataFrame,
    output_dir: Path,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)

    trade_svg = _build_trade_svg(symbol=symbol, period=period, signal_frame=signal_frame, trades_df=trades_df)
    drawdown_svg = _build_drawdown_svg(symbol=symbol, period=period, equity_df=equity_df)

    trade_chart_path = output_dir / "trade_chart.svg"
    drawdown_chart_path = output_dir / "drawdown_chart.svg"

    trade_chart_path.write_text(trade_svg, encoding="utf-8")
    drawdown_chart_path.write_text(drawdown_svg, encoding="utf-8")

    chart_section_html = f"""
    <section class="charts">
      <div class="chart-card">
        <div class="chart-head">
          <h2>{TRADE_CHART_TITLE}</h2>
          <p>{TRADE_CHART_DESC}</p>
        </div>
        <div class="legend">
          <span class="legend-item"><i class="legend-buy"></i>{BUY_TEXT}</span>
          <span class="legend-item"><i class="legend-sell"></i>{SELL_TEXT}</span>
          <span class="legend-item"><i class="legend-fast"></i>EMA21</span>
          <span class="legend-item"><i class="legend-slow"></i>EMA55</span>
        </div>
        <div class="svg-shell">{trade_svg}</div>
      </div>
      <div class="chart-card">
        <div class="chart-head">
          <h2>{DRAWDOWN_CHART_TITLE}</h2>
          <p>{DRAWDOWN_CHART_DESC}</p>
        </div>
        <div class="svg-shell">{drawdown_svg}</div>
      </div>
    </section>
    """

    return {
        "trade_chart_path": str(trade_chart_path),
        "drawdown_chart_path": str(drawdown_chart_path),
        "chart_section_html": chart_section_html,
    }


def _build_trade_svg(*, symbol: str, period: str, signal_frame: pd.DataFrame, trades_df: pd.DataFrame) -> str:
    df = signal_frame.sort_index().copy()
    if df.empty:
        return _empty_svg(NO_TRADE_CHART_DATA)

    width = max(1400, min(24000, len(df) * 6))
    height = 520
    margin_left = 70
    margin_right = 30
    margin_top = 40
    margin_bottom = 60
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    bar_step = plot_width / max(len(df), 1)
    body_width = max(2.0, min(10.0, bar_step * 0.72))

    low_min = float(df["low"].min())
    high_max = float(df["high"].max())
    padding = max((high_max - low_min) * 0.06, high_max * 0.005)
    y_min = low_min - padding
    y_max = high_max + padding

    def x_at(index: int) -> float:
        return margin_left + index * bar_step + bar_step / 2

    def y_at(value: float) -> float:
        if y_max == y_min:
            return margin_top + plot_height / 2
        return margin_top + (y_max - value) / (y_max - y_min) * plot_height

    grid_lines: list[str] = []
    for i in range(6):
        y = margin_top + plot_height * i / 5
        price = y_max - (y_max - y_min) * i / 5
        grid_lines.append(
            f'<line x1="{margin_left}" y1="{y:.2f}" x2="{width - margin_right}" y2="{y:.2f}" stroke="#d9e0d8" stroke-width="1"/>'
        )
        grid_lines.append(
            f'<text x="{margin_left - 10}" y="{y + 4:.2f}" text-anchor="end" font-size="12" fill="#5e6a60">{price:.2f}</text>'
        )

    candle_parts: list[str] = []
    for idx, row in enumerate(df.itertuples()):
        x = x_at(idx)
        open_px = float(row.open)
        high_px = float(row.high)
        low_px = float(row.low)
        close_px = float(row.close)
        color = "#1c8a68" if close_px >= open_px else "#bf5d35"
        candle_parts.append(
            f'<line x1="{x:.2f}" y1="{y_at(high_px):.2f}" x2="{x:.2f}" y2="{y_at(low_px):.2f}" stroke="{color}" stroke-width="1.2"/>'
        )
        top = min(y_at(open_px), y_at(close_px))
        bottom = max(y_at(open_px), y_at(close_px))
        body_height = max(1.4, bottom - top)
        candle_parts.append(
            f'<rect x="{x - body_width / 2:.2f}" y="{top:.2f}" width="{body_width:.2f}" height="{body_height:.2f}" fill="{color}" rx="1"/>'
        )

    fast_points = " ".join(f"{x_at(i):.2f},{y_at(float(val)):.2f}" for i, val in enumerate(df["ema_fast"]))
    slow_points = " ".join(f"{x_at(i):.2f},{y_at(float(val)):.2f}" for i, val in enumerate(df["ema_slow"]))

    markers = _build_trade_markers(df, trades_df, x_at, y_at)
    time_labels = _build_time_labels(df.index, x_at, height, margin_bottom)
    title = html.escape(f"{symbol} {period} {TRADE_CHART_TITLE}")

    return f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img" aria-label="{title}">
  <rect x="0" y="0" width="{width}" height="{height}" fill="#fffdfa" rx="24"/>
  <text x="{margin_left}" y="24" font-size="20" font-weight="700" fill="#1f241f">{title}</text>
  <text x="{width - margin_right}" y="24" text-anchor="end" font-size="12" fill="#5e6a60">{MARKERS_DRAWN}</text>
  {''.join(grid_lines)}
  <polyline points="{slow_points}" fill="none" stroke="#d58a2f" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round"/>
  <polyline points="{fast_points}" fill="none" stroke="#1f6fb2" stroke-width="2.0" stroke-linejoin="round" stroke-linecap="round"/>
  {''.join(candle_parts)}
  {markers}
  {time_labels}
</svg>"""


def _build_trade_markers(df: pd.DataFrame, trades_df: pd.DataFrame, x_at, y_at) -> str:
    if trades_df.empty:
        return ""

    parts: list[str] = []
    for idx, trade in enumerate(trades_df.itertuples(), start=1):
        entry_idx = _find_bar_index(df.index, pd.Timestamp(trade.entry_ts))
        exit_idx = _find_bar_index(df.index, pd.Timestamp(trade.exit_ts))

        if entry_idx >= 0:
            x = x_at(entry_idx)
            y = y_at(float(trade.entry_price))
            parts.append(
                f'<polygon points="{x:.2f},{y - 14:.2f} {x - 9:.2f},{y + 4:.2f} {x + 9:.2f},{y + 4:.2f}" fill="#1c8a68" stroke="#ffffff" stroke-width="1.5"/>'
            )
            parts.append(
                f'<text x="{x:.2f}" y="{y - 18:.2f}" text-anchor="middle" font-size="11" font-weight="700" fill="#1c8a68">\u4e70{idx}</text>'
            )

        if exit_idx >= 0:
            x = x_at(exit_idx)
            y = y_at(float(trade.exit_price))
            parts.append(
                f'<polygon points="{x:.2f},{y + 14:.2f} {x - 9:.2f},{y - 4:.2f} {x + 9:.2f},{y - 4:.2f}" fill="#bf5d35" stroke="#ffffff" stroke-width="1.5"/>'
            )
            parts.append(
                f'<text x="{x:.2f}" y="{y + 28:.2f}" text-anchor="middle" font-size="11" font-weight="700" fill="#bf5d35">\u5356{idx}</text>'
            )
    return "".join(parts)


def _find_bar_index(index: pd.Index, ts: pd.Timestamp) -> int:
    located = int(index.get_indexer([ts])[0])
    if located >= 0:
        return located
    nearest = int(index.searchsorted(ts))
    if 0 <= nearest < len(index):
        return nearest
    return -1


def _build_time_labels(index: pd.Index, x_at, height: int, margin_bottom: int) -> str:
    if len(index) == 0:
        return ""

    count = min(7, len(index))
    positions = sorted({round(i * (len(index) - 1) / max(count - 1, 1)) for i in range(count)})
    parts: list[str] = []
    baseline = height - margin_bottom + 24

    for pos in positions:
        ts = pd.Timestamp(index[pos]).strftime("%Y-%m-%d\n%H:%M")
        x = x_at(pos)
        date_part, time_part = ts.splitlines()
        parts.append(
            f'<line x1="{x:.2f}" y1="{height - margin_bottom:.2f}" x2="{x:.2f}" y2="{height - margin_bottom + 6:.2f}" stroke="#a9b3aa" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{x:.2f}" y="{baseline:.2f}" text-anchor="middle" font-size="11" fill="#5e6a60">{html.escape(date_part)}</text>'
        )
        parts.append(
            f'<text x="{x:.2f}" y="{baseline + 14:.2f}" text-anchor="middle" font-size="11" fill="#8a948a">{html.escape(time_part)}</text>'
        )

    return "".join(parts)


def _build_drawdown_svg(*, symbol: str, period: str, equity_df: pd.DataFrame) -> str:
    if equity_df.empty:
        return _empty_svg(NO_DRAWDOWN_DATA)

    equity = equity_df["equity"].astype(float)
    peak = equity.cummax()
    drawdown = (equity / peak - 1.0) * 100

    width = max(1400, min(24000, len(drawdown) * 4))
    height = 280
    margin_left = 70
    margin_right = 30
    margin_top = 36
    margin_bottom = 48
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom

    dd_min = min(float(drawdown.min()), -0.01)
    dd_max = 0.0

    def x_at(index: int) -> float:
        return margin_left + index * plot_width / max(len(drawdown) - 1, 1)

    def y_at(value: float) -> float:
        return margin_top + (dd_max - value) / (dd_max - dd_min) * plot_height

    area_points = [f"{margin_left:.2f},{y_at(0):.2f}"]
    for idx, value in enumerate(drawdown):
        area_points.append(f"{x_at(idx):.2f},{y_at(float(value)):.2f}")
    area_points.append(f"{margin_left + plot_width:.2f},{y_at(0):.2f}")

    line_points = " ".join(f"{x_at(i):.2f},{y_at(float(value)):.2f}" for i, value in enumerate(drawdown))

    grid_lines: list[str] = []
    for i in range(5):
        y = margin_top + plot_height * i / 4
        value = dd_max - (dd_max - dd_min) * i / 4
        grid_lines.append(
            f'<line x1="{margin_left}" y1="{y:.2f}" x2="{width - margin_right}" y2="{y:.2f}" stroke="#e1d8d2" stroke-width="1"/>'
        )
        grid_lines.append(
            f'<text x="{margin_left - 10}" y="{y + 4:.2f}" text-anchor="end" font-size="12" fill="#6d625c">{value:.2f}%</text>'
        )

    title = html.escape(f"{symbol} {period} {DRAWDOWN_CHART_TITLE}")
    area = " ".join(area_points)
    return f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img" aria-label="{title}">
  <rect x="0" y="0" width="{width}" height="{height}" fill="#fffdfa" rx="24"/>
  <text x="{margin_left}" y="24" font-size="18" font-weight="700" fill="#1f241f">{title}</text>
  {''.join(grid_lines)}
  <line x1="{margin_left}" y1="{y_at(0):.2f}" x2="{width - margin_right}" y2="{y_at(0):.2f}" stroke="#a59a93" stroke-width="1.2"/>
  <polygon points="{area}" fill="rgba(191,93,53,0.18)" stroke="none"/>
  <polyline points="{line_points}" fill="none" stroke="#bf5d35" stroke-width="2.4" stroke-linejoin="round" stroke-linecap="round"/>
</svg>"""


def _empty_svg(message: str) -> str:
    escaped = html.escape(message)
    return f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 260" role="img" aria-label="{escaped}">
  <rect x="0" y="0" width="1200" height="260" fill="#fffdfa" rx="24"/>
  <text x="600" y="138" text-anchor="middle" font-size="18" fill="#6d625c">{escaped}</text>
</svg>"""
