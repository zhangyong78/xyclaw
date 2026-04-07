from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from tkinter import Canvas, Label, StringVar, Toplevel, ttk

import pandas as pd

from app.app_icon import apply_window_icon


PANEL_BG = "#10161d"
AXIS_BG = "#151d26"
GRID = "#24313d"
TEXT = "#d8e1ea"
MUTED = "#87a0b3"
UP = "#20b486"
DOWN = "#ef6461"
EMA_FAST = "#55a6ff"
EMA_SLOW = "#ffb24a"
VOLUME_UP = "#1e8f71"
VOLUME_DOWN = "#a84e49"
DRAWDOWN = "#ff8d6a"
DRAWDOWN_FILL = "#3b211e"
CROSSHAIR = "#4e6274"
BUY = "#4ee4a5"
SELL = "#ff8d83"


@dataclass(slots=True)
class BacktestChartBundle:
    symbol: str
    period: str
    fast_ema: int
    slow_ema: int
    signal_frame: pd.DataFrame
    trades_df: pd.DataFrame
    equity_df: pd.DataFrame


@dataclass(slots=True)
class _TradeMark:
    label: str
    kind: str
    bar_index: int
    price: float
    pnl: float


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _format_ts(ts: pd.Timestamp, tzinfo) -> str:
    stamp = pd.Timestamp(ts)
    if stamp.tzinfo is not None and tzinfo is not None:
        try:
            stamp = stamp.tz_convert(tzinfo)
        except Exception:
            pass
    if stamp.tzinfo is not None:
        stamp = stamp.tz_localize(None)
    return stamp.strftime("%Y-%m-%d %H:%M")


def _align_timestamp(index: pd.Index, ts: pd.Timestamp) -> pd.Timestamp:
    stamp = pd.Timestamp(ts)
    index_tz = getattr(index, "tz", None)
    if index_tz is not None and stamp.tzinfo is None:
        return stamp.tz_localize(index_tz)
    if index_tz is None and stamp.tzinfo is not None:
        return stamp.tz_localize(None)
    return stamp


def _find_bar_index(index: pd.Index, ts: pd.Timestamp) -> int:
    stamp = _align_timestamp(index, pd.Timestamp(ts))
    located = int(index.get_indexer([stamp])[0])
    if located >= 0:
        return located
    nearest = int(index.searchsorted(stamp))
    if 0 <= nearest < len(index):
        return nearest
    return -1


def _sort_trades_for_display(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df is None or trades_df.empty:
        return pd.DataFrame()
    trades = trades_df.copy()
    for column in ("entry_ts", "exit_ts"):
        if column in trades.columns:
            trades[column] = pd.to_datetime(trades[column], errors="coerce")
    sort_columns = [column for column in ("entry_ts", "exit_ts") if column in trades.columns]
    if sort_columns:
        trades = trades.sort_values(sort_columns, kind="stable", na_position="last")
    return trades.reset_index(drop=True)


class BacktestChartWindow(Toplevel):
    def __init__(self, master, bundle: BacktestChartBundle, *, title_text: str = "回测总图") -> None:
        super().__init__(master)
        self.title_text = title_text
        self.bundle = BacktestChartBundle(
            symbol=bundle.symbol,
            period=bundle.period,
            fast_ema=bundle.fast_ema,
            slow_ema=bundle.slow_ema,
            signal_frame=bundle.signal_frame.sort_index().copy(),
            trades_df=bundle.trades_df.copy(),
            equity_df=bundle.equity_df.sort_index().copy(),
        )
        self.local_tz = datetime.now().astimezone().tzinfo
        self.bar_step = 18.0
        self._min_bar_step = 6.0
        self._max_bar_step = 72.0
        self._default_visible_bars = 34
        self._draw_job: str | None = None
        self._pending_focus_index: float | None = None
        self._pending_redraw = False
        self._scroll_to_latest = True
        self._view_initialized = False
        self._content_width = 1200
        self._content_height = 860
        self._left_pad = 26
        self._price_top = 18
        self._price_bottom = 420
        self._volume_top = 454
        self._volume_bottom = 590
        self._drawdown_top = 624
        self._drawdown_bottom = 760
        self._last_bar_index = len(self.bundle.signal_frame) - 1
        self._trade_marks: list[_TradeMark] = []
        self._bar_events: dict[int, list[str]] = {}
        self._crosshair_v: int | None = None
        self._crosshair_h: int | None = None
        self._pan_active = False
        self._pan_anchor_x = 0.0
        self._pan_view_start = 0.0
        self._aligned_equity = self._build_aligned_equity()
        self._aligned_peak_equity = self._build_aligned_peak_equity()
        self._aligned_drawdown = self._build_aligned_drawdown()
        self._aligned_drawdown_amount = self._build_aligned_drawdown_amount()
        self._initial_equity = self._resolve_initial_equity()
        self._rendered_index_start = 0
        self._rendered_index_end = 0

        self.title(f"{self.bundle.symbol} {self.bundle.period} {self.title_text}")
        self.geometry("1520x980")
        self.minsize(1120, 760)
        self.configure(bg=PANEL_BG)
        apply_window_icon(self)

        self.info_var = StringVar(value="")
        self._build_layout()
        self._prepare_trade_marks()
        self._update_info(self._last_bar_index)
        self.after(40, self._draw)

    def _build_layout(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        header = ttk.Frame(self, padding=(18, 16))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)

        Label(
            header,
            text=f"{self.bundle.symbol} {self.bundle.period} {self.title_text}",
            font=("Microsoft YaHei UI", 15, "bold"),
            foreground="#1b2530",
            background="#f0f0f0",
        ).grid(row=0, column=0, sticky="w")
        Label(
            header,
            text=(
                f"\u4e0a\u65b9 K \u7ebf + EMA{self.bundle.fast_ema}/EMA{self.bundle.slow_ema}\uff0c"
                "\u4e2d\u95f4\u6210\u4ea4\u91cf\uff0c\u4e0b\u65b9\u56de\u64a4 | \u6eda\u8f6e\u7f29\u653e  \u5de6\u952e\u62d6\u52a8\u5de6\u53f3\u79fb\u52a8  Shift+\u6eda\u8f6e\u6a2a\u5411\u79fb\u52a8"
            ),
            foreground="#596a78",
            background="#f0f0f0",
        ).grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Button(header, text="\u56de\u5230\u6700\u65b0", command=self._show_latest).grid(row=0, column=1, rowspan=2, sticky="e")

        Label(
            self,
            textvariable=self.info_var,
            font=("Consolas", 10),
            foreground="#dfe6ee",
            background=PANEL_BG,
            padx=18,
            pady=8,
        ).grid(row=2, column=0, sticky="ew")

        shell = ttk.Frame(self)
        shell.grid(row=1, column=0, sticky="nsew", padx=14)
        shell.columnconfigure(0, weight=1)
        shell.rowconfigure(0, weight=1)

        self.chart_canvas = Canvas(shell, bg=PANEL_BG, highlightthickness=0, xscrollincrement=1)
        self.chart_canvas.grid(row=0, column=0, sticky="nsew")

        self.axis_canvas = Canvas(shell, width=92, bg=AXIS_BG, highlightthickness=0)
        self.axis_canvas.grid(row=0, column=1, sticky="ns")

        self.scrollbar = ttk.Scrollbar(shell, orient="horizontal", command=self._on_scrollbar)
        self.scrollbar.grid(row=1, column=0, sticky="ew")
        self.chart_canvas.configure(xscrollcommand=self.scrollbar.set)

        self.chart_canvas.bind("<Configure>", self._schedule_draw)
        self.axis_canvas.bind("<Configure>", self._schedule_draw)
        self.chart_canvas.bind("<Motion>", self._on_motion)
        self.chart_canvas.bind("<Leave>", self._on_leave)
        self.chart_canvas.bind("<MouseWheel>", self._on_mousewheel)
        self.chart_canvas.bind("<Shift-MouseWheel>", self._on_shift_mousewheel)
        self.chart_canvas.bind("<Control-MouseWheel>", self._on_control_mousewheel)
        self.chart_canvas.bind("<ButtonPress-1>", self._on_pan_start)
        self.chart_canvas.bind("<B1-Motion>", self._on_pan_drag)
        self.chart_canvas.bind("<ButtonRelease-1>", self._on_pan_end)

    def _build_aligned_equity(self) -> pd.Series:
        index = self.bundle.signal_frame.index
        if len(index) == 0 or self.bundle.equity_df.empty or "equity" not in self.bundle.equity_df.columns:
            return pd.Series(index=index, dtype=float)

        equity = self.bundle.equity_df["equity"].astype(float).sort_index()
        aligned = equity.reindex(index)
        aligned = aligned.ffill().bfill()
        return aligned.astype(float)

    def _build_aligned_drawdown(self) -> pd.Series:
        equity = self._aligned_equity
        if equity.empty:
            return pd.Series(index=equity.index, dtype=float)
        peak = self._build_aligned_peak_equity()
        return ((equity / peak) - 1.0) * 100.0

    def _build_aligned_peak_equity(self) -> pd.Series:
        equity = self._aligned_equity
        if equity.empty:
            return pd.Series(index=equity.index, dtype=float)
        return equity.cummax()

    def _build_aligned_drawdown_amount(self) -> pd.Series:
        equity = self._aligned_equity
        peak = self._aligned_peak_equity
        if equity.empty or peak.empty:
            return pd.Series(index=equity.index, dtype=float)
        return equity - peak

    def _resolve_initial_equity(self) -> float:
        if self._aligned_equity.empty:
            return 0.0
        first = self._aligned_equity.iloc[0]
        return 0.0 if pd.isna(first) else float(first)

    def _prepare_trade_marks(self) -> None:
        self._trade_marks.clear()
        self._bar_events.clear()
        if self.bundle.trades_df.empty:
            return

        trades = _sort_trades_for_display(self.bundle.trades_df)
        for idx, trade in enumerate(trades.itertuples(), start=1):
            entry_idx = _find_bar_index(self.bundle.signal_frame.index, pd.Timestamp(trade.entry_ts))
            exit_idx = _find_bar_index(self.bundle.signal_frame.index, pd.Timestamp(trade.exit_ts))
            pnl = float(trade.pnl)
            side = str(getattr(trade, "side", "long")).lower()
            entry_kind = "sell" if side == "short" else "buy"
            exit_kind = "buy" if side == "short" else "sell"

            if entry_idx >= 0:
                label = f"{'卖' if entry_kind == 'sell' else '买'}{idx}"
                self._trade_marks.append(
                    _TradeMark(
                        label=label,
                        kind=entry_kind,
                        bar_index=entry_idx,
                        price=float(trade.entry_price),
                        pnl=pnl,
                    )
                )
                self._bar_events.setdefault(entry_idx, []).append(label)
            if exit_idx >= 0:
                label = f"{'买' if exit_kind == 'buy' else '卖'}{idx}"
                self._trade_marks.append(
                    _TradeMark(
                        label=label,
                        kind=exit_kind,
                        bar_index=exit_idx,
                        price=float(trade.exit_price),
                        pnl=pnl,
                    )
                )
                self._bar_events.setdefault(exit_idx, []).append(label)

    def _schedule_draw(self, _event=None, *, focus_index: float | None = None, delay_ms: int = 12) -> None:
        if focus_index is not None:
            self._pending_focus_index = focus_index
        if self._draw_job is not None:
            self._pending_redraw = True
            return
        self._pending_redraw = False
        self._draw_job = self.after(max(1, int(delay_ms)), self._draw)

    def _resolve_target_view_fraction(self, *, count: int, canvas_width: int, current_fraction: float) -> tuple[float, float, float]:
        max_offset = max(self._content_width - canvas_width, 0)
        if max_offset <= 0:
            return 0.0, 0.0, 0.0

        focus_index = self._pending_focus_index
        self._pending_focus_index = None
        if focus_index is not None:
            center_x = self._left_pad + _clamp(float(focus_index), 0.0, count - 1) * self.bar_step + self.bar_step / 2
            offset = _clamp(center_x - canvas_width / 2, 0.0, float(max_offset))
            return float(offset / max_offset), float(offset), float(max_offset)

        if self._scroll_to_latest:
            self._scroll_to_latest = False
            return 1.0, float(max_offset), float(max_offset)

        fraction = _clamp(float(current_fraction or 0.0), 0.0, 1.0)
        return fraction, float(fraction * max_offset), float(max_offset)

    def _resolve_visible_index_range(
        self,
        *,
        count: int,
        offset: float,
        canvas_width: int,
        buffer_bars: int | None = None,
    ) -> tuple[int, int]:
        if count <= 0:
            return 0, 0

        visible_bars = max(1, int(canvas_width / max(self.bar_step, 1.0)) + 2)
        buffer = max(180, visible_bars * 3) if buffer_bars is None else int(buffer_bars)
        start = int(max(0, ((offset - self._left_pad) / self.bar_step) - buffer))
        end = int(min(count, ((offset + canvas_width - self._left_pad) / self.bar_step) + buffer + 1))
        if end <= start:
            end = min(count, start + 1)
        return start, end

    def _current_view_offset(self) -> tuple[float, int]:
        canvas_width = max(self.chart_canvas.winfo_width(), 1)
        max_offset = max(self._content_width - canvas_width, 0)
        current_fraction = self.chart_canvas.xview()[0] if self.chart_canvas.xview() else 0.0
        return float(current_fraction * max_offset), canvas_width

    def _view_needs_redraw(self) -> bool:
        df = self.bundle.signal_frame
        if df.empty:
            return False
        offset, canvas_width = self._current_view_offset()
        visible_start, visible_end = self._resolve_visible_index_range(
            count=len(df),
            offset=offset,
            canvas_width=canvas_width,
            buffer_bars=0,
        )
        if self._rendered_index_end <= self._rendered_index_start:
            return True

        visible_bars = max(1, int(canvas_width / max(self.bar_step, 1.0)) + 2)
        prefetch_margin = max(48, visible_bars)
        return (
            visible_start <= self._rendered_index_start + prefetch_margin
            or visible_end >= self._rendered_index_end - prefetch_margin
        )

    def _on_scrollbar(self, *args) -> None:
        self._scroll_to_latest = False
        self.chart_canvas.xview(*args)
        if self._view_needs_redraw():
            self._schedule_draw(delay_ms=8)

    def _draw(self) -> None:
        self._draw_job = None
        df = self.bundle.signal_frame
        current_view_start = self.chart_canvas.xview()[0] if self.chart_canvas.xview() else 0.0
        self.chart_canvas.delete("all")
        self.axis_canvas.delete("all")

        widget_width = max(self.chart_canvas.winfo_width(), 980)
        widget_height = max(self.chart_canvas.winfo_height(), 640)
        self._content_height = widget_height

        if df.empty:
            self.chart_canvas.create_text(
                widget_width / 2,
                widget_height / 2,
                text="\u6ca1\u6709\u53ef\u663e\u793a\u7684\u56de\u6d4b\u56fe\u6570\u636e\u3002",
                fill=TEXT,
                font=("Microsoft YaHei UI", 14, "bold"),
            )
            self.axis_canvas.create_text(46, widget_height / 2, text="-", fill=MUTED)
            return

        if not self._view_initialized:
            plot_width = max(widget_width - self._left_pad - 96, 320)
            target_visible = min(max(24, len(df)), self._default_visible_bars)
            self.bar_step = _clamp(plot_width / max(target_visible, 1), self._min_bar_step, self._max_bar_step)
            self._view_initialized = True

        panel_gap = 34
        bottom_axis = 78
        usable_height = widget_height - self._price_top - bottom_axis
        price_height = max(250, int(usable_height * 0.55))
        volume_height = max(90, int(usable_height * 0.16))
        drawdown_height = usable_height - price_height - volume_height - panel_gap * 2
        if drawdown_height < 120:
            missing = 120 - drawdown_height
            reduce_price = min(price_height - 250, int(missing * 0.7))
            reduce_volume = min(volume_height - 90, missing - reduce_price)
            price_height -= reduce_price
            volume_height -= reduce_volume
            drawdown_height = usable_height - price_height - volume_height - panel_gap * 2

        self._price_bottom = self._price_top + price_height
        self._volume_top = self._price_bottom + panel_gap
        self._volume_bottom = self._volume_top + volume_height
        self._drawdown_top = self._volume_bottom + panel_gap
        self._drawdown_bottom = self._drawdown_top + drawdown_height

        self._content_width = max(widget_width, int(self._left_pad + len(df) * self.bar_step + 30))
        self.chart_canvas.configure(scrollregion=(0, 0, self._content_width, self._content_height))
        target_fraction, target_offset, max_offset = self._resolve_target_view_fraction(
            count=len(df),
            canvas_width=widget_width,
            current_fraction=current_view_start,
        )
        view_start, view_end = self._resolve_visible_index_range(
            count=len(df),
            offset=target_offset,
            canvas_width=widget_width,
        )
        self._rendered_index_start = view_start
        self._rendered_index_end = view_end
        view_df = df.iloc[view_start:view_end].copy()
        if view_df.empty:
            view_start, view_end = 0, min(len(df), 1)
            view_df = df.iloc[view_start:view_end].copy()

        visible_left = max(0.0, target_offset - 8.0)
        visible_right = min(float(self._content_width), target_offset + widget_width + 8.0)

        price_min = float(view_df["low"].min())
        price_max = float(view_df["high"].max())
        if "ema_fast" in view_df.columns:
            ema_fast_series = pd.to_numeric(view_df["ema_fast"], errors="coerce").dropna()
            if not ema_fast_series.empty:
                price_min = min(price_min, float(ema_fast_series.min()))
                price_max = max(price_max, float(ema_fast_series.max()))
        if "ema_slow" in view_df.columns:
            ema_slow_series = pd.to_numeric(view_df["ema_slow"], errors="coerce").dropna()
            if not ema_slow_series.empty:
                price_min = min(price_min, float(ema_slow_series.min()))
                price_max = max(price_max, float(ema_slow_series.max()))
        price_pad = max((price_max - price_min) * 0.06, max(abs(price_max), 1.0) * 0.004)
        price_y_min = price_min - price_pad
        price_y_max = price_max + price_pad
        max_volume = max(float(view_df["volume"].max()), 1.0)

        drawdown = self._aligned_drawdown if len(self._aligned_drawdown) == len(df) else pd.Series(index=df.index, dtype=float)
        if drawdown.empty:
            drawdown = pd.Series([0.0] * len(df), index=df.index, dtype=float)
        drawdown = drawdown.iloc[view_start:view_end]
        drawdown_amount = self._aligned_drawdown_amount if len(self._aligned_drawdown_amount) == len(df) else pd.Series(index=df.index, dtype=float)
        if drawdown_amount.empty:
            drawdown_amount = pd.Series([0.0] * len(df), index=df.index, dtype=float)
        drawdown_amount = drawdown_amount.iloc[view_start:view_end]
        pnl_series = self._aligned_equity - self._initial_equity if len(self._aligned_equity) == len(df) else pd.Series(index=df.index, dtype=float)
        if pnl_series.empty:
            pnl_series = pd.Series([0.0] * len(df), index=df.index, dtype=float)
        pnl_series = pnl_series.iloc[view_start:view_end]
        dd_min = min(float(drawdown.min()), -0.01)
        dd_max = 0.0

        def x_at(index: int) -> float:
            return self._left_pad + index * self.bar_step + self.bar_step / 2

        def y_price(value: float) -> float:
            if price_y_max == price_y_min:
                return (self._price_top + self._price_bottom) / 2
            return self._price_top + (price_y_max - value) / (price_y_max - price_y_min) * (self._price_bottom - self._price_top)

        def y_volume(value: float) -> float:
            return self._volume_bottom - value / max_volume * (self._volume_bottom - self._volume_top)

        def y_drawdown(value: float) -> float:
            return self._drawdown_top + (dd_max - value) / (dd_max - dd_min) * (self._drawdown_bottom - self._drawdown_top)

        self.chart_canvas.create_rectangle(0, 0, self._content_width, self._content_height, fill=PANEL_BG, outline="")
        self.chart_canvas.create_line(visible_left, self._price_bottom + panel_gap / 2, visible_right, self._price_bottom + panel_gap / 2, fill=GRID)
        self.chart_canvas.create_line(visible_left, self._volume_bottom + panel_gap / 2, visible_right, self._volume_bottom + panel_gap / 2, fill=GRID)

        for i in range(6):
            y = self._price_top + (self._price_bottom - self._price_top) * i / 5
            value = price_y_max - (price_y_max - price_y_min) * i / 5
            self.chart_canvas.create_line(visible_left, y, visible_right, y, fill=GRID, dash=(2, 4))
            self.axis_canvas.create_text(46, y, text=f"{value:.2f}", fill=MUTED, font=("Consolas", 10))

        self.axis_canvas.create_text(46, self._volume_top - 16, text="VOL", fill=MUTED, font=("Consolas", 10, "bold"))
        self.axis_canvas.create_text(46, self._volume_top, text=f"{max_volume:,.0f}", fill=MUTED, font=("Consolas", 9))
        self.axis_canvas.create_text(46, self._volume_bottom, text="0", fill=MUTED, font=("Consolas", 9))

        self.axis_canvas.create_text(46, self._drawdown_top - 16, text="DD", fill=MUTED, font=("Consolas", 10, "bold"))
        for i in range(4):
            y = self._drawdown_top + (self._drawdown_bottom - self._drawdown_top) * i / 3
            value = dd_max - (dd_max - dd_min) * i / 3
            self.chart_canvas.create_line(visible_left, y, visible_right, y, fill=GRID, dash=(2, 4))
            self.axis_canvas.create_text(46, y, text=f"{value:.2f}%", fill=MUTED, font=("Consolas", 10))

        zero_y = y_drawdown(0.0)
        self.chart_canvas.create_line(visible_left, zero_y, visible_right, zero_y, fill="#7f8a92", width=1)

        visible_count = max(view_end - view_start, 1)
        label_step = max(1, visible_count // 6)
        label_positions = list(range(view_start, view_end, label_step))
        if label_positions and label_positions[-1] != view_end - 1:
            label_positions.append(view_end - 1)
        elif not label_positions:
            label_positions = [view_start]

        for pos in label_positions:
            x = x_at(pos)
            self.chart_canvas.create_line(x, self._price_top, x, self._drawdown_bottom, fill=GRID, dash=(2, 6))
            ts_text = _format_ts(pd.Timestamp(df.index[pos]), self.local_tz)
            date_part, time_part = ts_text.split(" ")
            self.chart_canvas.create_text(x, self._drawdown_bottom + 18, text=date_part, fill=MUTED, font=("Consolas", 9))
            self.chart_canvas.create_text(x, self._drawdown_bottom + 34, text=time_part, fill=MUTED, font=("Consolas", 9))

        fast_line: list[float] = []
        slow_line: list[float] = []
        body_width = max(3.0, min(12.0, self.bar_step * 0.68))
        for absolute_idx, row in zip(range(view_start, view_end), view_df.itertuples()):
            x = x_at(absolute_idx)
            open_px = float(row.open)
            high_px = float(row.high)
            low_px = float(row.low)
            close_px = float(row.close)
            color = UP if close_px >= open_px else DOWN

            self.chart_canvas.create_line(x, y_price(high_px), x, y_price(low_px), fill=color, width=1)

            top = y_price(max(open_px, close_px))
            bottom = y_price(min(open_px, close_px))
            if abs(bottom - top) < 1.2:
                self.chart_canvas.create_line(x - body_width / 2, top, x + body_width / 2, top, fill=color, width=2)
            else:
                self.chart_canvas.create_rectangle(
                    x - body_width / 2,
                    top,
                    x + body_width / 2,
                    bottom,
                    fill=color,
                    outline=color,
                )

            volume_color = VOLUME_UP if close_px >= open_px else VOLUME_DOWN
            self.chart_canvas.create_rectangle(
                x - body_width / 2,
                y_volume(float(row.volume)),
                x + body_width / 2,
                self._volume_bottom,
                fill=volume_color,
                outline="",
            )

            if pd.notna(row.ema_fast):
                fast_line.extend((x, y_price(float(row.ema_fast))))
            if pd.notna(row.ema_slow):
                slow_line.extend((x, y_price(float(row.ema_slow))))

        if len(slow_line) >= 4:
            self.chart_canvas.create_line(*slow_line, fill=EMA_SLOW, width=2.2)
        if len(fast_line) >= 4:
            self.chart_canvas.create_line(*fast_line, fill=EMA_FAST, width=2.0)

        drawdown_area: list[float] = [x_at(view_start), zero_y]
        drawdown_line: list[float] = []
        for absolute_idx, value in zip(range(view_start, view_end), drawdown):
            x = x_at(absolute_idx)
            y = y_drawdown(float(value))
            drawdown_area.extend((x, y))
            drawdown_line.extend((x, y))
        drawdown_area.extend((x_at(view_end - 1), zero_y))
        if len(drawdown_area) >= 6:
            self.chart_canvas.create_polygon(*drawdown_area, fill=DRAWDOWN_FILL, outline="")
        if len(drawdown_line) >= 4:
            self.chart_canvas.create_line(*drawdown_line, fill=DRAWDOWN, width=2.4)

        latest_pnl = float(pnl_series.iloc[-1]) if len(pnl_series) else 0.0
        latest_drawdown_amount = float(drawdown_amount.iloc[-1]) if len(drawdown_amount) else 0.0
        max_drawdown_amount = float(drawdown_amount.min()) if len(drawdown_amount) else 0.0
        latest_drawdown_pct = float(drawdown.iloc[-1]) if len(drawdown) else 0.0
        max_drawdown_pct = float(drawdown.min()) if len(drawdown) else 0.0
        summary_x = max(visible_right - 16, self._left_pad + 220)
        self.chart_canvas.create_text(
            summary_x,
            self._drawdown_top + 14,
            text=f"累计盈亏 {latest_pnl:+,.2f}",
            anchor="e",
            fill=BUY if latest_pnl >= 0 else SELL,
            font=("Consolas", 10, "bold"),
        )
        self.chart_canvas.create_text(
            summary_x,
            self._drawdown_top + 32,
            text=f"当前回撤 {latest_drawdown_amount:+,.2f} / {latest_drawdown_pct:.2f}%",
            anchor="e",
            fill=MUTED if latest_drawdown_amount == 0 else SELL,
            font=("Consolas", 9),
        )
        self.chart_canvas.create_text(
            summary_x,
            self._drawdown_top + 48,
            text=f"最大回撤 {max_drawdown_amount:+,.2f} / {max_drawdown_pct:.2f}%",
            anchor="e",
            fill=SELL,
            font=("Consolas", 9),
        )

        for trade in self.bundle.trades_df.itertuples():
            entry_idx = _find_bar_index(df.index, pd.Timestamp(trade.entry_ts))
            exit_idx = _find_bar_index(df.index, pd.Timestamp(trade.exit_ts))
            if entry_idx < 0 or exit_idx < 0:
                continue
            if max(entry_idx, exit_idx) < view_start or min(entry_idx, exit_idx) >= view_end:
                continue
            if entry_idx >= 0 and exit_idx >= 0:
                pnl_color = BUY if float(trade.pnl) >= 0 else SELL
                self.chart_canvas.create_line(
                    x_at(entry_idx),
                    y_price(float(trade.entry_price)),
                    x_at(exit_idx),
                    y_price(float(trade.exit_price)),
                    fill=pnl_color,
                    width=1.4,
                    dash=(4, 4),
                )

        for mark in self._trade_marks:
            if mark.bar_index < view_start or mark.bar_index >= view_end:
                continue
            x = x_at(mark.bar_index)
            y = y_price(mark.price)
            if mark.kind == "buy":
                points = (x, y - 12, x - 9, y + 4, x + 9, y + 4)
                fill = BUY
                label_y = y - 20
            else:
                points = (x, y + 12, x - 9, y - 4, x + 9, y - 4)
                fill = SELL
                label_y = y + 22
            self.chart_canvas.create_polygon(*points, fill=fill, outline=PANEL_BG, width=1.4)
            self.chart_canvas.create_text(x, label_y, text=mark.label, fill=fill, font=("Microsoft YaHei UI", 9, "bold"))

        self._crosshair_v = self.chart_canvas.create_line(
            0, self._price_top, 0, self._drawdown_bottom, fill=CROSSHAIR, dash=(4, 4), state="hidden"
        )
        self._crosshair_h = self.chart_canvas.create_line(
            0, 0, self._content_width, 0, fill=CROSSHAIR, dash=(4, 4), state="hidden"
        )

        if max_offset > 0:
            self.chart_canvas.xview_moveto(target_fraction)
        else:
            self.chart_canvas.xview_moveto(0.0)

        if self._pending_redraw:
            self._pending_redraw = False
            self._schedule_draw(delay_ms=8)

    def _update_info(self, index: int) -> None:
        df = self.bundle.signal_frame
        if df.empty:
            self.info_var.set("\u6ca1\u6709\u56de\u6d4b\u56fe\u6570\u636e\u3002")
            return

        idx = int(_clamp(index, 0, len(df) - 1))
        row = df.iloc[idx]
        prev_close = float(df.iloc[idx - 1]["close"]) if idx > 0 else float(row["open"])
        change_pct = 0.0 if prev_close == 0 else (float(row["close"]) / prev_close - 1.0) * 100
        events = " / ".join(self._bar_events.get(idx, [])) or "-"

        equity_value = float(self._aligned_equity.iloc[idx]) if len(self._aligned_equity) > idx and pd.notna(self._aligned_equity.iloc[idx]) else 0.0
        pnl_value = equity_value - self._initial_equity
        drawdown_value = float(self._aligned_drawdown.iloc[idx]) if len(self._aligned_drawdown) > idx and pd.notna(self._aligned_drawdown.iloc[idx]) else 0.0
        drawdown_amount = float(self._aligned_drawdown_amount.iloc[idx]) if len(self._aligned_drawdown_amount) > idx and pd.notna(self._aligned_drawdown_amount.iloc[idx]) else 0.0

        self.info_var.set(
            "  ".join(
                [
                    f"时间 {_format_ts(pd.Timestamp(df.index[idx]), self.local_tz)}",
                    f"开 {float(row['open']):.2f}",
                    f"高 {float(row['high']):.2f}",
                    f"低 {float(row['low']):.2f}",
                    f"收 {float(row['close']):.2f}",
                    f"涨跌 {change_pct:+.2f}%",
                    f"EMA{self.bundle.fast_ema} {float(row['ema_fast']):.2f}",
                    f"EMA{self.bundle.slow_ema} {float(row['ema_slow']):.2f}",
                    f"量 {float(row['volume']):,.2f}",
                    f"权益 {equity_value:,.2f}",
                    f"盈亏 {pnl_value:+,.2f}",
                    f"回撤金额 {drawdown_amount:+,.2f}",
                    f"回撤 {drawdown_value:.2f}%",
                    f"标记 {events}",
                ]
            )
        )

    def _on_motion(self, event) -> None:
        df = self.bundle.signal_frame
        if self._pan_active or df.empty or self._crosshair_v is None or self._crosshair_h is None:
            return

        x_pos = self.chart_canvas.canvasx(event.x)
        raw_index = int((x_pos - self._left_pad) // self.bar_step)
        index = int(_clamp(raw_index, 0, len(df) - 1))
        center_x = self._left_pad + index * self.bar_step + self.bar_step / 2

        self.chart_canvas.itemconfigure(self._crosshair_v, state="normal")
        self.chart_canvas.coords(self._crosshair_v, center_x, self._price_top, center_x, self._drawdown_bottom)

        if self._price_top <= event.y <= self._drawdown_bottom:
            self.chart_canvas.itemconfigure(self._crosshair_h, state="normal")
            self.chart_canvas.coords(self._crosshair_h, 0, event.y, self._content_width, event.y)
        else:
            self.chart_canvas.itemconfigure(self._crosshair_h, state="hidden")

        self._update_info(index)

    def _on_leave(self, _event) -> None:
        if self._crosshair_v is not None:
            self.chart_canvas.itemconfigure(self._crosshair_v, state="hidden")
        if self._crosshair_h is not None:
            self.chart_canvas.itemconfigure(self._crosshair_h, state="hidden")
        self._update_info(self._last_bar_index)

    def _on_shift_mousewheel(self, event) -> str:
        self._scroll_to_latest = False
        viewport_width = max(self.chart_canvas.winfo_width(), 1)
        step_pixels = max(72.0, viewport_width * 0.18)
        delta_pixels = -step_pixels if event.delta > 0 else step_pixels
        self._scroll_horizontal_pixels(delta_pixels)
        return "break"

    def _zoom_horizontal(self, delta: int, anchor_x: float) -> str:
        df = self.bundle.signal_frame
        if df.empty:
            return "break"

        current_step = self.bar_step
        target_step = current_step * (1.24 if delta > 0 else 0.82)
        self.bar_step = _clamp(target_step, self._min_bar_step, self._max_bar_step)
        if abs(self.bar_step - current_step) < 0.05:
            return "break"

        self._scroll_to_latest = False
        focus_index = (anchor_x - self._left_pad) / current_step
        self._schedule_draw(focus_index=focus_index, delay_ms=8)
        return "break"

    def _on_mousewheel(self, event) -> str:
        anchor_x = self.chart_canvas.canvasx(event.x)
        return self._zoom_horizontal(event.delta, anchor_x)

    def _on_control_mousewheel(self, event) -> str:
        anchor_x = self.chart_canvas.canvasx(event.x)
        return self._zoom_horizontal(event.delta, anchor_x)

    def _on_pan_start(self, event) -> str:
        self._pan_active = True
        self._scroll_to_latest = False
        self._pan_anchor_x = float(event.x)
        canvas_width = max(self.chart_canvas.winfo_width(), 1)
        max_offset = max(self._content_width - canvas_width, 0)
        current_fraction = self.chart_canvas.xview()[0] if self.chart_canvas.xview() else 0.0
        self._pan_view_start = current_fraction * max_offset
        self.chart_canvas.configure(cursor="fleur")
        if self._crosshair_v is not None:
            self.chart_canvas.itemconfigure(self._crosshair_v, state="hidden")
        if self._crosshair_h is not None:
            self.chart_canvas.itemconfigure(self._crosshair_h, state="hidden")
        return "break"

    def _on_pan_drag(self, event) -> str:
        if not self._pan_active:
            return "break"

        canvas_width = max(self.chart_canvas.winfo_width(), 1)
        max_offset = max(self._content_width - canvas_width, 0)
        if max_offset <= 0:
            return "break"

        delta = self._pan_anchor_x - float(event.x)
        new_offset = _clamp(self._pan_view_start + delta, 0.0, float(max_offset))
        self.chart_canvas.xview_moveto(new_offset / max_offset)
        if self._view_needs_redraw():
            self._schedule_draw(delay_ms=6)
        return "break"

    def _on_pan_end(self, event) -> str:
        self._pan_active = False
        self.chart_canvas.configure(cursor="")
        if self._view_needs_redraw():
            self._schedule_draw(delay_ms=4)
        if event is not None:
            self._on_motion(event)
        return "break"

    def _show_latest(self) -> None:
        self._scroll_to_latest = True
        self._schedule_draw()

    def _scroll_horizontal_pixels(self, delta_pixels: float) -> None:
        canvas_width = max(self.chart_canvas.winfo_width(), 1)
        max_offset = max(self._content_width - canvas_width, 0)
        if max_offset <= 0:
            return
        current_fraction = self.chart_canvas.xview()[0] if self.chart_canvas.xview() else 0.0
        current_offset = current_fraction * max_offset
        new_offset = _clamp(current_offset + delta_pixels, 0.0, float(max_offset))
        self.chart_canvas.xview_moveto(new_offset / max_offset)
        if self._view_needs_redraw():
            self._schedule_draw(delay_ms=8)


class BacktestTradeChartWindow(BacktestChartWindow):
    pass


class BacktestDrawdownWindow(BacktestChartWindow):
    pass
