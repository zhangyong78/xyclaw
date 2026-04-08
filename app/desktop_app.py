from __future__ import annotations

from datetime import datetime
import json
import os
import queue
import re
import threading
from pathlib import Path
from tkinter import BooleanVar, Canvas, Label, StringVar, Text, Tk, filedialog, messagebox, simpledialog
from tkinter import ttk

import pandas as pd

from app.app_icon import apply_window_icon
from app.chart_windows import BacktestChartBundle, BacktestChartWindow
from app.services import (
    BacktestRequest,
    LiveManualOrderRequest,
    LiveRequest,
    run_backtest,
    run_live_check,
    submit_live_manual_limit_order,
    sync_local_period_cache,
)


APP_TITLE = "OKX \u91cf\u5316\u4ea4\u6613\u684c\u9762\u7a0b\u5e8f"
STRATEGY_OPTION_LABEL = "EMA金叉死叉"
LIVE_STRATEGY_FORM_NONE = "不选"
STRATEGY_TEXT = (
    "\u7b56\u7565\u4e00\uff1a4\u5c0f\u65f6 | 5EMA / 8EMA | \u4ea4\u53c9\u4e70\u5165\uff0c\u6b7b\u53c9\u5356\u51fa"
)
HERO_DESC = "\u684c\u9762\u7248\u652f\u6301\u56de\u6d4b\u3001\u5b9e\u65f6\u4fe1\u53f7\u68c0\u67e5\uff0c\u4ee5\u53ca OKX \u6a21\u62df\u76d8\u548c\u5b9e\u76d8\u4e0b\u5355\u3002\u5f53\u524d\u7b56\u7565\u4e3a\u5747\u7ebf\u4ea4\u53c9\u4e70\u5165\uff0c\u5747\u7ebf\u5f62\u6210\u6b7b\u53c9\u5373\u5356\u51fa\u3002"
CHART_HINT = "\u56de\u6d4b\u5b8c\u6210\u540e\u53ef\u5728\u8f6f\u4ef6\u5185\u76f4\u63a5\u67e5\u770b\u5408\u5e76\u540e\u7684\u56de\u6d4b\u603b\u56fe\uff0c\u9875\u9762\u6700\u4e0b\u65b9\u8fd8\u4f1a\u81ea\u52a8\u751f\u6210K\u7ebf\u56fe\u3002"
PERPETUAL_SYMBOL_BASES = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "OKBUSDT", "DOGEUSDT"]
BACKTEST_SYMBOL_OPTIONS = [f"{symbol}永续" for symbol in PERPETUAL_SYMBOL_BASES]
LIVE_SYMBOL_OPTIONS = [f"{symbol}永续" for symbol in PERPETUAL_SYMBOL_BASES]
ORDER_SYMBOL_OPTIONS = [f"{symbol}永续" for symbol in PERPETUAL_SYMBOL_BASES]
SIGNAL_SIDE_OPTIONS = ["\u53ea\u505a\u591a", "\u53ea\u505a\u7a7a", "\u53cc\u5411"]
LEVERAGE_OPTIONS = [str(value) for value in range(1, 31)]
HISTORY_BAR_LIMITS = {"5m": 10_000, "15m": 10_000, "1H": 10_000, "4H": 10_000}
BACKTEST_CACHE_DIR = Path("desktop_reports") / "cache"
AUTO_4H_CACHE_SYNC_MS = 60_000
LIVE_TEST_ORDER_SYMBOL = "BTC-USDT-SWAP"
LIVE_TEST_ORDER_DISPLAY = "BTCUSDT永续"
LIVE_TEST_ORDER_PRICE = 9_999.0
LIVE_TEST_ORDER_BASE_QTY = 0.001
BACKTEST_TIME_TRANSLATOR = str.maketrans(
    {
        "０": "0",
        "１": "1",
        "２": "2",
        "３": "3",
        "４": "4",
        "５": "5",
        "６": "6",
        "７": "7",
        "８": "8",
        "９": "9",
        "／": "/",
        "－": "-",
        "：": ":",
        "年": " ",
        "月": " ",
        "日": " ",
        "时": " ",
        "分": " ",
        "秒": " ",
    }
)


def zh(text: str) -> str:
    return text


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


class TradingDesktopApp(Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1440x920")
        self.minsize(1240, 820)
        self.configure(bg="#efe6d7")
        apply_window_icon(self)

        self.event_queue: queue.Queue[tuple[str, object]] = queue.Queue()
        self.loop_thread: threading.Thread | None = None
        self.loop_stop = threading.Event()
        self.latest_trade_chart_path: str | None = None
        self.latest_drawdown_chart_path: str | None = None
        self.latest_chart_bundle: BacktestChartBundle | None = None
        self.default_chart_bundle: BacktestChartBundle | None = None
        self.live_chart_bundle: BacktestChartBundle | None = None
        self.backtest_chart_window: BacktestChartWindow | None = None
        self.live_chart_window: BacktestChartWindow | None = None
        self.sltp_matrix_rows: list[dict] = []
        self.sltp_chart_payloads: dict[str, dict] = {}
        self.selected_sltp_key: str | None = None
        self.summary_tree_key_map: dict[str, str] = {}
        self.trade_detail_tree: ttk.Treeview | None = None
        self.live_trade_detail_tree: ttk.Treeview | None = None
        self.live_trade_detail_tree2: ttk.Treeview | None = None
        self.live_strategy_tree: ttk.Treeview | None = None
        self.live_log_text: Text | None = None
        self.live_strategy_records: dict[str, dict[str, str]] = {}
        self.live_strategy_workers: dict[str, dict[str, object]] = {}
        self.live_strategy_snapshots: dict[str, dict[str, object]] = {}
        self.active_live_strategy_id: str | None = None
        self._live_strategy_counter = 0
        self.live_strategy_form_map: dict[str, str] = {}
        self.live_strategy_form_combo: ttk.Combobox | None = None
        self.live_strategy_form_combos: list[ttk.Combobox] = []
        self.live_api_profiles: dict[str, dict[str, str]] = {}
        self.live_api_profile_combo: ttk.Combobox | None = None
        self.live_api_profile_combos: list[ttk.Combobox] = []
        self.live_api_profile_option_map: dict[str, str] = {}
        self.live_test_order_button: ttk.Button | None = None
        self._live_test_order_running = False
        self.page_switch_buttons: dict[str, ttk.Button] = {}
        self.page_canvas: Canvas | None = None
        self.page_frame: ttk.Frame | None = None
        self._page_window_id: int | None = None
        self.backtest_page_frame: ttk.Frame | None = None
        self.live_page_frame: ttk.Frame | None = None
        self.live_clone_frame: ttk.Frame | None = None
        self.bt_drawdown_preview_canvas: Canvas | None = None
        self._bt_drawdown_preview_job: str | None = None
        self.kline_preview_canvas: Canvas | None = None
        self.live_kline_preview_canvas: Canvas | None = None
        self.live_kline_preview_canvas2: Canvas | None = None
        self._kline_preview_job: str | None = None
        self._live_kline_preview_job: str | None = None
        self._kline_preview_visible_bars = 220
        self._kline_preview_start_index = 0
        self._kline_preview_follow_latest = True
        self._kline_preview_pan_active = False
        self._kline_preview_pan_anchor_x = 0.0
        self._kline_preview_pan_origin_start = 0
        self._kline_preview_pending = False
        self._kline_preview_mark_bundle_id: int | None = None
        self._kline_preview_trade_marks: list[tuple[str, str, int, float]] = []
        self._live_kline_preview_visible_bars = 220
        self._live_kline_preview_start_index = 0
        self._live_kline_preview_follow_latest = True
        self._live_kline_preview_pan_active = False
        self._live_kline_preview_pan_anchor_x = 0.0
        self._live_kline_preview_pan_origin_start = 0
        self._live_kline_preview_pending = False
        self._live_kline_preview_mark_bundle_id: int | None = None
        self._live_kline_preview_trade_marks: list[tuple[str, str, int, float]] = []
        self._bt_csv_auto_managed = True
        self._auto_4h_cache_job: str | None = None
        self._auto_4h_cache_running = False

        self._init_style()
        self._init_variables()
        self._bind_backtest_csv_defaults()
        self._load_live_api_profiles()
        self._refresh_live_action_account()
        self._build_layout()
        self.after(150, self._drain_queue)
        self._schedule_auto_4h_cache_sync(delay_ms=1200)

    def _init_style(self) -> None:
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure(".", font=("Microsoft YaHei UI", 10), background="#efe6d7", foreground="#1f241f")
        style.configure("Page.TFrame", background="#efe6d7")
        style.configure("Card.TFrame", background="#faf5ed", relief="flat")
        style.configure("Setup.TLabelframe", background="#faf5ed", bordercolor="#cfc6b9", relief="solid")
        style.configure("Setup.TLabelframe.Label", background="#faf5ed", foreground="#1f241f", font=("Microsoft YaHei UI", 10, "bold"))
        style.configure("SetupLabel.TLabel", background="#faf5ed", foreground="#1f241f")
        style.configure("SetupNote.TLabel", background="#faf5ed", foreground="#67706a")
        style.configure("Hero.TFrame", background="#214f41")
        style.configure("SectionTitle.TLabel", font=("Microsoft YaHei UI", 11, "bold"), background="#faf5ed", foreground="#1f241f")
        style.configure("HeroTitle.TLabel", font=("Microsoft YaHei UI", 20, "bold"), background="#214f41", foreground="#fff6ec")
        style.configure("HeroBody.TLabel", font=("Microsoft YaHei UI", 10), background="#214f41", foreground="#d9ece3")
        style.configure("HeroTab.TButton", font=("Microsoft YaHei UI", 10), padding=(14, 7), background="#2e6657", foreground="#eef6f1", borderwidth=0)
        style.map("HeroTab.TButton", background=[("active", "#387765")], foreground=[("active", "#ffffff")])
        style.configure("HeroTabActive.TButton", font=("Microsoft YaHei UI", 10, "bold"), padding=(14, 7), background="#fff6ec", foreground="#214f41", borderwidth=0)
        style.map("HeroTabActive.TButton", background=[("active", "#fff6ec")], foreground=[("active", "#214f41")])
        style.configure("Muted.TLabel", background="#faf5ed", foreground="#5d675d")
        style.configure("Value.TLabel", font=("Consolas", 11, "bold"), background="#faf5ed", foreground="#214f41")
        style.configure("Treeview", rowheight=28, font=("Microsoft YaHei UI", 10))
        style.configure("Treeview.Heading", font=("Microsoft YaHei UI", 10, "bold"))
        style.configure("Primary.TButton", font=("Microsoft YaHei UI", 10, "bold"))

    def _init_variables(self) -> None:
        self.bt_strategy = StringVar(value=STRATEGY_OPTION_LABEL)
        self.bt_symbol = StringVar(value="BTCUSDT永续")
        self.bt_csv = StringVar(value="")
        self.bt_bars = StringVar(value="10000")
        self.bt_initial_cash = StringVar(value="10000")
        self.bt_risk = StringVar(value="100")
        self.bt_fast = StringVar(value="21")
        self.bt_slow = StringVar(value="55")
        self.bt_start = StringVar(value="")
        self.bt_end = StringVar(value="")
        self.bt_fee = StringVar(value="2.8")
        self.bt_slippage = StringVar(value="2")
        self.bt_max_alloc = StringVar(value="2")
        self.bt_periods = StringVar(value="4小时")
        self.bt_signal_side = StringVar(value="\u53cc\u5411")
        self.bt_stop_atr = StringVar(value="1")
        self.bt_take_atr = StringVar(value="2")

        self.live_strategy = StringVar(value=STRATEGY_OPTION_LABEL)
        self.live_symbol = StringVar(value="BTCUSDT永续")
        self.live_order_symbol = StringVar(value="BTCUSDT永续")
        self.live_clone_mode = StringVar(value=LIVE_STRATEGY_FORM_NONE)
        self.live_period = StringVar(value="4H")
        self.live_signal_side = StringVar(value="\u53cc\u5411")
        self.live_run_mode = StringVar(value="\u4ec5\u4fe1\u53f7\u68c0\u67e5")
        self.live_strategy_form = StringVar(value=LIVE_STRATEGY_FORM_NONE)
        self.live_bars = StringVar(value="500")
        self.live_risk = StringVar(value="50")
        self.live_fast = StringVar(value="21")
        self.live_slow = StringVar(value="55")
        self.live_ema_large = StringVar(value="55")
        self.live_atr_period = StringVar(value="10")
        self.live_stop_atr = StringVar(value="1")
        self.live_take_atr = StringVar(value="2")
        self.live_fixed_size = StringVar(value="1")
        self.live_max_alloc = StringVar(value="2")
        self.live_leverage = StringVar(value="1")
        self.live_quote = StringVar(value="")
        self.live_base = StringVar(value="")
        self.live_poll = StringVar(value="60")
        self.live_timeout = StringVar(value="25")
        self.live_api_profile = StringVar(value="API 1")
        self.live_api_profile_display = StringVar(value="API 1")
        self.live_api_key = StringVar(value="")
        self.live_api_secret = StringVar(value="")
        self.live_api_passphrase = StringVar(value="")
        self.live_simulate = BooleanVar(value=False)
        self.live_execute = BooleanVar(value=False)

        self.bt_status = StringVar(value="\u7b49\u5f85\u56de\u6d4b")
        self.live_status = StringVar(value="\u7b49\u5f85\u4fe1\u53f7\u68c0\u67e5")
        self.main_page_mode = StringVar(value="backtest")
        self.strategy_status = StringVar(value=STRATEGY_TEXT)
        self.sltp_view_mode = StringVar(value="SLTP参数矩阵")
        self.top_period = StringVar(value="\u6682\u65e0")
        self.top_score = StringVar(value="0.00")
        self.metric_combo = StringVar(value="-")
        self.metric_net_pnl = StringVar(value="-")
        self.metric_return = StringVar(value="-")
        self.metric_win_rate = StringVar(value="-")
        self.metric_trades = StringVar(value="-")
        self.metric_max_drawdown = StringVar(value="-")
        self.metric_cdar = StringVar(value="-")
        self.metric_score = StringVar(value="-")
        self.metric_rank = StringVar(value="-")
        self.metric_custom_period = StringVar(value="-")
        self.live_view_slot = StringVar(value="1")
        self.live_action = StringVar(value="\u672a\u68c0\u67e5")
        self.live_action_account = StringVar(value="\u5f53\u524d\u8d26\u6237\uff1aAPI 1 | \u4f59\u989d - | \u4eca\u65e5 - | \u603b -")
        self.live_account_panel_name = StringVar(value="API 1")
        self.live_reason = StringVar(value="-")
        self.live_price = StringVar(value="-")
        self.live_size = StringVar(value="-")
        self.live_account_balance = StringVar(value="-")
        self.live_win_rate = StringVar(value="-")
        self.live_today_pnl = StringVar(value="-")
        self.live_total_pnl = StringVar(value="-")
        self.position_symbol = StringVar(value="-")
        self.position_total_assets = StringVar(value="-")
        self.position_market_value = StringVar(value="-")
        self.position_quote_balance = StringVar(value="-")
        self.position_base_balance = StringVar(value="-")
        self.position_allocation = StringVar(value="-")
        self.position_state = StringVar(value="-")
        self.position_signal_time = StringVar(value="-")
        self.position_checked_at = StringVar(value="-")
        self.position_held_bars = StringVar(value="-")
        self.position_ema_fast = StringVar(value="-")
        self.position_ema_slow = StringVar(value="-")
        self.chart_status = StringVar(value=CHART_HINT)
        self.kline_status = StringVar(value="\u56de\u6d4b\u5b8c\u6210\u540e\uff0cK\u7ebf\u56fe\u4f1a\u5728\u9875\u9762\u6700\u4e0b\u65b9\u81ea\u52a8\u751f\u6210\u3002")
        self.trade_detail_status = StringVar(value="\u56de\u6d4b\u5b8c\u6210\u540e\uff0c\u8fd9\u91cc\u4f1a\u663e\u793a\u5f53\u524d\u7ec4\u5408\u7684\u4ea4\u6613\u660e\u7ec6\uff0c\u5e76\u5305\u542b\u624b\u7eed\u8d39\u3002")
        self.live_kline_status = StringVar(value="\u5b9e\u65f6\u68c0\u67e5\u540e\uff0c\u771f\u5b9e\u4e70\u5356K\u7ebf\u56fe\u4f1a\u5728\u771f\u5b9e\u4ed3\u4f4d\u9875\u9762\u6700\u4e0b\u65b9\u81ea\u52a8\u751f\u6210\u3002")
        self.live_trade_detail_status = StringVar(value="\u5b9e\u65f6\u4ea7\u751f\u4e70\u5356\u8bb0\u5f55\u540e\uff0c\u8fd9\u91cc\u4f1a\u663e\u793a\u771f\u5b9e\u4ed3\u4f4d\u7684\u4ea4\u6613\u660e\u7ec6\uff0c\u5e76\u5305\u542b\u624b\u7eed\u8d39\u3002")

    def _bind_backtest_csv_defaults(self) -> None:
        self.bt_symbol.trace_add("write", self._on_backtest_cache_selector_changed)
        self.bt_periods.trace_add("write", self._on_backtest_cache_selector_changed)
        self._sync_backtest_csv_default()

    def _on_backtest_cache_selector_changed(self, *_args: object) -> None:
        self._sync_backtest_csv_default()

    def _sync_backtest_csv_default(self) -> None:
        if not self._bt_csv_auto_managed:
            return
        default_path = self._default_backtest_cache_csv_path(self.bt_symbol.get(), self.bt_periods.get())
        self.bt_csv.set(str(default_path))

    @classmethod
    def _default_backtest_cache_csv_path(cls, symbol: str, period: str) -> Path:
        normalized_symbol = cls._normalize_swap_symbol(symbol).replace("-", "_")
        normalized_period = cls._normalize_backtest_period(period)
        return BACKTEST_CACHE_DIR / f"{normalized_symbol}_{normalized_period}.csv"

    def _schedule_auto_4h_cache_sync(self, *, delay_ms: int = AUTO_4H_CACHE_SYNC_MS) -> None:
        if self._auto_4h_cache_job is not None:
            try:
                self.after_cancel(self._auto_4h_cache_job)
            except Exception:
                pass
        self._auto_4h_cache_job = self.after(max(1, int(delay_ms)), self._trigger_auto_4h_cache_sync)

    def _trigger_auto_4h_cache_sync(self) -> None:
        self._auto_4h_cache_job = None
        if self._auto_4h_cache_running:
            self._schedule_auto_4h_cache_sync()
            return

        self._auto_4h_cache_running = True
        worker = threading.Thread(target=self._run_auto_4h_cache_sync_worker, daemon=True)
        worker.start()

    def _run_auto_4h_cache_sync_worker(self) -> None:
        notices: list[str] = []
        try:
            for symbol in BACKTEST_SYMBOL_OPTIONS:
                result = sync_local_period_cache(
                    symbol=self._normalize_swap_symbol(symbol),
                    period="4H",
                    output_dir=str(Path("desktop_reports")),
                )
                added_bars = int(result.get("added_bars") or 0)
                if added_bars > 0:
                    notices.append(
                        f"已自动补齐 {symbol} 4H 永续本地缓存：新增 {added_bars} 根，最新时间 {result.get('after_last_ts') or '-'}。"
                    )
        except Exception as exc:
            notices.append(f"自动补齐 4H 本地缓存失败：{exc}")
        finally:
            self.event_queue.put(("auto_4h_cache_sync", notices))

    def _build_layout(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        shell = ttk.Frame(self, style="Page.TFrame")
        shell.grid(row=0, column=0, sticky="nsew")
        shell.columnconfigure(0, weight=1)
        shell.rowconfigure(0, weight=1)

        self.page_canvas = Canvas(shell, bg="#efe6d7", highlightthickness=0, bd=0)
        self.page_canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(shell, orient="vertical", command=self.page_canvas.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.page_canvas.configure(yscrollcommand=scrollbar.set)

        self.page_frame = ttk.Frame(self.page_canvas, style="Page.TFrame")
        self._page_window_id = self.page_canvas.create_window((0, 0), window=self.page_frame, anchor="nw")
        self.page_frame.bind("<Configure>", self._on_page_frame_configure)
        self.page_canvas.bind("<Configure>", self._on_page_canvas_configure)
        self.bind_all("<MouseWheel>", self._on_app_mousewheel, add="+")

        page = self.page_frame
        page.columnconfigure(0, weight=1)

        hero = ttk.Frame(page, style="Hero.TFrame", padding=(22, 20))
        hero.grid(row=0, column=0, sticky="nsew", padx=18, pady=(18, 10))
        hero.columnconfigure(0, weight=1)
        hero.columnconfigure(1, weight=0)
        ttk.Label(hero, text=APP_TITLE, style="HeroTitle.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(hero, textvariable=self.strategy_status, style="HeroBody.TLabel").grid(row=1, column=0, sticky="w", pady=(10, 0))
        ttk.Label(hero, text=HERO_DESC, style="HeroBody.TLabel").grid(row=2, column=0, sticky="w", pady=(6, 0))
        switch_bar = ttk.Frame(hero, style="Hero.TFrame")
        switch_bar.grid(row=0, column=1, rowspan=3, sticky="ne", padx=(20, 0))
        ttk.Label(switch_bar, text="页面选择", style="HeroBody.TLabel").grid(row=0, column=0, columnspan=2, sticky="e")
        backtest_button = ttk.Button(
            switch_bar,
            text="回测系统",
            style="HeroTabActive.TButton",
            command=lambda: self._set_main_page("backtest"),
        )
        backtest_button.grid(row=1, column=0, sticky="e", pady=(8, 0))
        live_button = ttk.Button(
            switch_bar,
            text="真实仓位",
            style="HeroTab.TButton",
            command=lambda: self._set_main_page("live"),
        )
        live_button.grid(row=1, column=1, sticky="e", padx=(8, 0), pady=(8, 0))
        self.page_switch_buttons = {
            "backtest": backtest_button,
            "live": live_button,
        }

        content = ttk.Frame(page, style="Page.TFrame")
        content.grid(row=1, column=0, sticky="nsew", padx=18, pady=(8, 18))
        content.columnconfigure(0, weight=1)
        content.rowconfigure(0, weight=1)

        self.backtest_page_frame = ttk.Frame(content, style="Page.TFrame")
        self.backtest_page_frame.grid(row=0, column=0, sticky="nsew")
        self.backtest_page_frame.columnconfigure(0, weight=1)
        self._build_backtest_page(self.backtest_page_frame)

        self.live_page_frame = ttk.Frame(content, style="Page.TFrame")
        self.live_page_frame.grid(row=0, column=0, sticky="nsew")
        self.live_page_frame.columnconfigure(0, weight=1)
        self._build_live_page(self.live_page_frame)

        self._set_main_page(self.main_page_mode.get(), scroll_to_top=False)

    def _build_backtest_page(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)

        backtest_card = ttk.Frame(parent, style="Card.TFrame", padding=18)
        backtest_card.grid(row=0, column=0, sticky="nsew")
        backtest_card.columnconfigure(0, weight=1)
        self._build_backtest_card(backtest_card)

        lower = ttk.Frame(parent, style="Page.TFrame")
        lower.grid(row=1, column=0, sticky="nsew", pady=(16, 18))
        lower.columnconfigure(0, weight=3)
        lower.columnconfigure(1, weight=2)

        result_card = ttk.Frame(lower, style="Card.TFrame", padding=18)
        result_card.grid(row=0, column=0, sticky="nsew", padx=(0, 9))
        result_card.columnconfigure(0, weight=1)
        result_card.columnconfigure(1, weight=0)
        result_card.rowconfigure(4, weight=1)
        self._build_result_card(result_card)

        log_card = ttk.Frame(lower, style="Card.TFrame", padding=18)
        log_card.grid(row=0, column=1, sticky="nsew", padx=(9, 0))
        log_card.columnconfigure(0, weight=1)
        self._build_log_card(log_card)

        kline_card = ttk.Frame(parent, style="Card.TFrame", padding=18)
        kline_card.grid(row=2, column=0, sticky="nsew", pady=(0, 18))
        kline_card.columnconfigure(0, weight=1)
        self._build_kline_preview_card(kline_card)

        trade_detail_card = ttk.Frame(parent, style="Card.TFrame", padding=18)
        trade_detail_card.grid(row=3, column=0, sticky="nsew")
        trade_detail_card.columnconfigure(0, weight=1)
        self._build_trade_detail_card(trade_detail_card)

    def _build_live_page(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)

        live_card = ttk.Frame(parent, style="Card.TFrame", padding=18)
        live_card.grid(row=0, column=0, sticky="nsew")
        live_card.columnconfigure(0, weight=1)
        self._build_live_card(live_card)

        live_kline_card = ttk.Frame(parent, style="Card.TFrame", padding=18)
        live_kline_card.grid(row=1, column=0, sticky="nsew", pady=(18, 18))
        live_kline_card.columnconfigure(0, weight=1)
        self._build_live_kline_preview_card(live_kline_card)

        live_trade_detail_card = ttk.Frame(parent, style="Card.TFrame", padding=18)
        live_trade_detail_card.grid(row=2, column=0, sticky="nsew")
        live_trade_detail_card.columnconfigure(0, weight=1)
        self._build_live_trade_detail_card(live_trade_detail_card)

    def _on_live_clone_mode_changed(self, _event=None) -> None:
        self._sync_live_clone_visibility()
        self.after_idle(self._on_page_frame_configure)

    def _sync_live_clone_visibility(self) -> None:
        if self.live_clone_frame is None:
            return
        if self.live_clone_mode.get().strip() == LIVE_STRATEGY_FORM_NONE:
            self.live_clone_frame.grid_remove()
        else:
            self.live_clone_frame.grid()
            self._schedule_live_kline_preview()
            self._refresh_live_trade_detail_table(self.live_chart_bundle)

    def _build_live_clone_page(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)

        live_card = ttk.Frame(parent, style="Card.TFrame", padding=18)
        live_card.grid(row=0, column=0, sticky="nsew")
        live_card.columnconfigure(0, weight=1)
        self._build_live_card2(live_card)

        live_kline_card = ttk.Frame(parent, style="Card.TFrame", padding=18)
        live_kline_card.grid(row=1, column=0, sticky="nsew", pady=(18, 18))
        live_kline_card.columnconfigure(0, weight=1)
        self._build_live_kline_preview_card2(live_kline_card)

        live_trade_detail_card = ttk.Frame(parent, style="Card.TFrame", padding=18)
        live_trade_detail_card.grid(row=2, column=0, sticky="nsew")
        live_trade_detail_card.columnconfigure(0, weight=1)
        self._build_live_trade_detail_card2(live_trade_detail_card)

    def _build_live_card2(self, parent: ttk.Frame) -> None:
        parent.rowconfigure(2, weight=1)

        setup_box = ttk.LabelFrame(parent, text="策略启动", style="Setup.TLabelframe", padding=(16, 14))
        setup_box.grid(row=0, column=0, sticky="ew")
        setup_box.columnconfigure(1, weight=1)
        setup_box.columnconfigure(3, weight=1)

        self._add_setup_combo(setup_box, 0, 0, "选择策略", self.live_strategy, [STRATEGY_OPTION_LABEL], state="readonly")
        self._add_setup_combo(setup_box, 0, 2, "信号标的", self.live_symbol, LIVE_SYMBOL_OPTIONS)
        self._add_setup_combo(setup_box, 1, 0, "K线周期", self.live_period, ["15m", "30m", "1H", "4H", "24小时"], state="readonly")
        self._add_setup_combo(setup_box, 1, 2, "信号方向", self.live_signal_side, SIGNAL_SIDE_OPTIONS, state="readonly")
        self._add_setup_combo(setup_box, 2, 0, "运行模式", self.live_run_mode, ["仅信号检查", "模拟并下单", "交易并下单"], state="readonly")
        self._add_setup_entry(setup_box, 2, 2, "轮询秒数", self.live_poll)

        clone_strategy_combo = self._add_setup_combo(
            setup_box,
            3,
            0,
            "选择策略形式",
            self.live_strategy_form,
            [LIVE_STRATEGY_FORM_NONE],
            state="readonly",
        )
        clone_strategy_combo.bind("<<ComboboxSelected>>", self._on_live_strategy_form_select)
        self.live_strategy_form_combos.append(clone_strategy_combo)
        self._add_setup_entry(setup_box, 3, 2, "EMA小周期", self.live_fast)
        self._add_setup_entry(setup_box, 4, 0, "EMA大周期", self.live_ema_large)
        self._add_setup_entry(setup_box, 4, 2, "ATR周期", self.live_atr_period)
        self._add_setup_entry(setup_box, 5, 0, "止损 ATR 倍数", self.live_stop_atr)
        self._add_setup_entry(setup_box, 5, 2, "止盈 ATR 倍数", self.live_take_atr)
        self._add_setup_entry(setup_box, 6, 0, "风险金", self.live_risk)
        self._add_setup_entry(setup_box, 6, 2, "固定数量", self.live_fixed_size, state="readonly")
        self._add_setup_combo(setup_box, 7, 0, "下单标的", self.live_order_symbol, ORDER_SYMBOL_OPTIONS)
        self._add_setup_combo(setup_box, 7, 2, "永续杠杆倍数", self.live_leverage, LEVERAGE_OPTIONS, state="readonly")

        ttk.Label(
            setup_box,
            text="当前复用和上面相同的默认信息、SLTP 形式和 API 配置，方便你在同一页做第二套观察。",
            style="SetupNote.TLabel",
            wraplength=620,
            justify="left",
        ).grid(row=8, column=0, columnspan=4, sticky="w", pady=(10, 0))

        button_bar = ttk.Frame(parent, style="Card.TFrame")
        button_bar.grid(row=1, column=0, sticky="ew", pady=(14, 0))
        ttk.Button(button_bar, text="检查一次", style="Primary.TButton", command=self._start_live_once).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(button_bar, text="开始轮询", command=self._start_live_loop).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(button_bar, text="停止轮询", command=self._stop_live_loop).grid(row=0, column=2)

        advanced_box = ttk.LabelFrame(parent, text="连接与余额", style="Setup.TLabelframe", padding=(16, 12))
        advanced_box.grid(row=2, column=0, sticky="nsew", pady=(14, 0))
        advanced_box.columnconfigure(1, weight=1)
        advanced_box.columnconfigure(3, weight=1)

        api_profile_combo = self._add_setup_combo(
            advanced_box,
            0,
            0,
            "API配置",
            self.live_api_profile_display,
            [],
            state="readonly",
        )
        self.live_api_profile_combos.append(api_profile_combo)
        self._refresh_live_api_profile_combo()
        api_profile_combo.bind("<<ComboboxSelected>>", self._on_live_api_profile_select)
        api_button_bar = ttk.Frame(advanced_box, style="Card.TFrame")
        api_button_bar.grid(row=0, column=2, columnspan=2, sticky="e", pady=6)
        ttk.Button(api_button_bar, text="保存当前API", command=self._save_current_live_api_profile).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(api_button_bar, text="改名称", command=self._rename_current_live_api_profile).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(api_button_bar, text="复制到其他API", command=self._copy_current_live_api_profile_to_other_slots).grid(row=0, column=2, padx=(0, 8))
        ttk.Button(api_button_bar, text="清空当前API", command=self._clear_current_live_api_profile).grid(row=0, column=3)

        self._add_setup_entry(advanced_box, 1, 0, "API Key", self.live_api_key, width=22)
        self._add_setup_entry(advanced_box, 1, 2, "API Secret", self.live_api_secret, show="*", width=22)
        self._add_setup_entry(advanced_box, 2, 0, "Passphrase", self.live_api_passphrase, show="*", width=22)
        self._add_setup_entry(advanced_box, 2, 2, "扫描K线数", self.live_bars)
        self._add_setup_entry(advanced_box, 3, 0, "最大资金占比", self.live_max_alloc)
        ttk.Label(
            advanced_box,
            text="最大资金占比请手动填写。",
            style="SetupNote.TLabel",
            wraplength=420,
            justify="left",
        ).grid(row=3, column=2, columnspan=2, sticky="w", padx=(0, 10), pady=6)

        balance_grid = ttk.Frame(advanced_box, style="Card.TFrame")
        balance_grid.grid(row=4, column=0, columnspan=4, sticky="ew", pady=(10, 0))
        for col in range(4):
            balance_grid.columnconfigure(col, weight=1)
        self._add_value(balance_grid, 0, 0, "当前账户", self.live_account_panel_name)
        self._add_value(balance_grid, 0, 1, "账户余额", self.live_account_balance)
        self._add_value(balance_grid, 0, 2, "今日盈亏", self.live_today_pnl)
        self._add_value(balance_grid, 0, 3, "总盈亏", self.live_total_pnl)

        status_grid = ttk.Frame(parent, style="Card.TFrame")
        status_grid.grid(row=3, column=0, sticky="ew", pady=(14, 0))
        status_grid.columnconfigure(0, weight=1)

        action_cell = ttk.Frame(status_grid, style="Card.TFrame")
        action_cell.grid(row=0, column=0, sticky="ew", padx=6, pady=6)
        action_cell.columnconfigure(0, weight=1)
        ttk.Label(action_cell, text="当前动作", style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(action_cell, text="选择", style="Muted.TLabel").grid(row=0, column=1, sticky="e", padx=(18, 6))
        live_view_combo = ttk.Combobox(
            action_cell,
            textvariable=self.live_view_slot,
            values=["1", "2", "3", "4", "5"],
            state="readonly",
            width=5,
        )
        live_view_combo.grid(row=0, column=2, sticky="e")
        live_view_combo.bind("<<ComboboxSelected>>", self._on_live_view_slot_changed)
        ttk.Label(action_cell, textvariable=self.live_action, style="Value.TLabel").grid(row=1, column=0, sticky="w", pady=(4, 0))
        ttk.Label(
            action_cell,
            textvariable=self.live_action_account,
            style="Muted.TLabel",
            wraplength=560,
            justify="left",
        ).grid(row=2, column=0, sticky="w", pady=(4, 0))

        metric_strip = ttk.Frame(status_grid, style="Card.TFrame")
        metric_strip.grid(row=1, column=0, sticky="ew", padx=6, pady=(8, 6))
        for col, weight in enumerate((2, 2, 1, 1)):
            metric_strip.columnconfigure(col, weight=weight)

        self._add_value(metric_strip, 0, 0, "账户余额", self.live_account_balance)
        self._add_value(metric_strip, 0, 1, "触发原因", self.live_reason)
        self._add_value(metric_strip, 0, 2, "最新收盘", self.live_price)
        self._add_value(metric_strip, 0, 3, "建议数量", self.live_size)
        self._add_value(metric_strip, 1, 0, "今日盈亏", self.live_today_pnl)
        self._add_value(metric_strip, 1, 1, "总盈亏", self.live_total_pnl)
        self._add_value(metric_strip, 1, 2, "胜率", self.live_win_rate)
        ttk.Label(parent, textvariable=self.live_status, style="Muted.TLabel").grid(row=4, column=0, sticky="w", pady=(12, 0))

    def _build_live_kline_preview_card2(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent, style="Card.TFrame")
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="真实仓位图", style="SectionTitle.TLabel").grid(row=0, column=0, sticky="w")
        control_bar = ttk.Frame(header, style="Card.TFrame")
        control_bar.grid(row=0, column=1, sticky="e")
        ttk.Button(control_bar, text="打开总图", width=10, command=self._open_live_chart).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(control_bar, text="放大", width=7, command=lambda: self._zoom_live_kline_preview(120, self._live_kline_preview_anchor_x())).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(control_bar, text="缩小", width=7, command=lambda: self._zoom_live_kline_preview(-120, self._live_kline_preview_anchor_x())).grid(row=0, column=2, padx=(0, 6))
        ttk.Button(control_bar, text="左移", width=7, command=lambda: self._nudge_live_kline_preview(-1)).grid(row=0, column=3, padx=(0, 6))
        ttk.Button(control_bar, text="右移", width=7, command=lambda: self._nudge_live_kline_preview(1)).grid(row=0, column=4, padx=(0, 6))
        ttk.Button(control_bar, text="最新", width=7, command=self._show_latest_live_kline_preview).grid(row=0, column=5)
        ttk.Label(header, textvariable=self.live_kline_status, style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(6, 0))

        self.live_kline_preview_canvas2 = Canvas(parent, height=430, bg="#050505", highlightthickness=0, bd=0)
        self.live_kline_preview_canvas2.grid(row=1, column=0, sticky="ew", pady=(16, 0))
        self.live_kline_preview_canvas2.bind("<Configure>", self._schedule_live_kline_preview)

    def _build_live_trade_detail_card2(self, parent: ttk.Frame) -> None:
        parent.rowconfigure(1, weight=1)

        header = ttk.Frame(parent, style="Card.TFrame")
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="交易明细", style="SectionTitle.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(header, textvariable=self.live_trade_detail_status, style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(6, 0))

        columns = ("trade_no", "side", "quantity", "entry_ts", "entry_price", "atr_value", "exit_ts", "exit_price", "pnl", "fees", "exit_reason")
        self.live_trade_detail_tree2 = ttk.Treeview(parent, columns=columns, show="headings", height=14)
        self.live_trade_detail_tree2.grid(row=1, column=0, sticky="nsew", pady=(14, 0))

        headings = {
            "trade_no": "第几次交易",
            "side": "方向",
            "quantity": "数量",
            "entry_ts": "进场时间",
            "entry_price": "进场价格",
            "atr_value": "ATR值",
            "exit_ts": "出场时间",
            "exit_price": "出场价格",
            "pnl": "盈亏",
            "fees": "手续费",
            "exit_reason": "退出原因",
        }
        widths = {
            "trade_no": 92,
            "side": 70,
            "quantity": 90,
            "entry_ts": 150,
            "entry_price": 120,
            "atr_value": 100,
            "exit_ts": 150,
            "exit_price": 120,
            "pnl": 100,
            "fees": 90,
            "exit_reason": 140,
        }
        for column in columns:
            self.live_trade_detail_tree2.heading(column, text=headings[column])
            self.live_trade_detail_tree2.column(column, width=widths[column], anchor="center")

        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=self.live_trade_detail_tree2.yview)
        self.live_trade_detail_tree2.configure(yscrollcommand=scrollbar.set)
        scrollbar.grid(row=1, column=1, sticky="ns", pady=(14, 0))

    def _set_main_page(self, mode: str, *, scroll_to_top: bool = True) -> None:
        raw_mode = str(mode).strip().lower()
        normalized_mode = "live" if raw_mode in {"live", "真实仓位"} else "backtest"
        self.main_page_mode.set(normalized_mode)

        if self.backtest_page_frame is not None:
            if normalized_mode == "backtest":
                self.backtest_page_frame.grid()
            else:
                self.backtest_page_frame.grid_remove()

        if self.live_page_frame is not None:
            if normalized_mode == "live":
                self.live_page_frame.grid()
            else:
                self.live_page_frame.grid_remove()

        for page_name, button in self.page_switch_buttons.items():
            button.configure(style="HeroTabActive.TButton" if page_name == normalized_mode else "HeroTab.TButton")

        self.after_idle(self._on_page_frame_configure)
        if self.page_canvas is not None and scroll_to_top:
            self.after_idle(lambda: self.page_canvas.yview_moveto(0.0))

    def _scroll_page_to_widget(self, widget) -> None:
        if self.page_canvas is None or self.page_frame is None or widget is None:
            return
        self.update_idletasks()
        bbox = self.page_canvas.bbox("all")
        if not bbox:
            return
        total_height = max(1, bbox[3] - bbox[1])
        viewport_height = max(1, self.page_canvas.winfo_height())
        max_scroll = max(1, total_height - viewport_height)
        target_y = self._widget_y_in_page(widget)
        self.page_canvas.yview_moveto(max(0.0, min(1.0, target_y / max_scroll)))

    def _widget_y_in_page(self, widget) -> int:
        if self.page_frame is None or widget is None:
            return 0
        y = 0
        current = widget
        while current is not None and current != self.page_frame:
            y += current.winfo_y()
            parent_name = current.winfo_parent()
            if not parent_name:
                break
            current = current.nametowidget(parent_name)
        return max(0, y - 8)

    def _build_backtest_card(self, parent: ttk.Frame) -> None:
        parent.rowconfigure(5, weight=1)

        header = ttk.Frame(parent, style="Card.TFrame")
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="\u56de\u6d4b\u63a7\u5236", style="SectionTitle.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Button(header, text="\u5f00\u59cb\u56de\u6d4b", style="Primary.TButton", command=self._start_backtest).grid(row=0, column=1, sticky="e")

        form = ttk.Frame(parent, style="Card.TFrame")
        form.grid(row=1, column=0, sticky="ew", pady=(16, 0))
        for col in range(4):
            form.columnconfigure(col, weight=1)

        self._add_combo(form, 0, 0, "选择策略", self.bt_strategy, [STRATEGY_OPTION_LABEL], state="readonly")
        self._add_combo(form, 0, 1, "\u4ea4\u6613\u5bf9", self.bt_symbol, BACKTEST_SYMBOL_OPTIONS, state="readonly")
        self._add_entry(form, 0, 2, "历史数据根数", self.bt_bars)
        self._add_entry(form, 0, 3, "\u521d\u59cb\u8d44\u91d1", self.bt_initial_cash)

        self._add_entry(form, 1, 0, "\u98ce\u9669\u91d1", self.bt_risk)
        self._add_entry(form, 1, 1, "EMA \u5feb\u7ebf", self.bt_fast)
        self._add_entry(form, 1, 2, "EMA \u6162\u7ebf", self.bt_slow)
        self._add_entry(form, 1, 3, "\u624b\u7eed\u8d39 bps", self.bt_fee)

        self._add_entry(form, 2, 0, "\u6ed1\u70b9 bps", self.bt_slippage)
        self._add_entry(form, 2, 1, "\u6700\u5927\u8d44\u91d1\u5360\u6bd4", self.bt_max_alloc)
        self._add_combo(form, 2, 2, "\u56de\u6d4b\u5468\u671f", self.bt_periods, ["15分钟", "5分钟", "1小时", "4小时", "24小时"], state="readonly")

        self._add_combo(form, 2, 3, "信号方向", self.bt_signal_side, SIGNAL_SIDE_OPTIONS, state="readonly")
        self._add_entry(form, 3, 0, "止损 ATR 倍数", self.bt_stop_atr)
        self._add_entry(form, 3, 1, "止盈 ATR 倍数", self.bt_take_atr)
        self._add_entry(form, 3, 2, "开始时间", self.bt_start)
        self._add_entry(form, 3, 3, "结束时间", self.bt_end)

        file_row = ttk.Frame(parent, style="Card.TFrame")
        file_row.grid(row=2, column=0, sticky="ew", pady=(14, 0))
        file_row.columnconfigure(0, weight=1)
        ttk.Label(file_row, text="本地 CSV", style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Entry(file_row, textvariable=self.bt_csv, width=34).grid(row=1, column=0, sticky="ew", pady=(6, 0))
        ttk.Button(file_row, text="\u9009\u62e9\u6587\u4ef6", command=self._pick_csv).grid(row=1, column=1, padx=(10, 0), pady=(6, 0))
        ttk.Label(
            file_row,
            text="默认优先读取 desktop_reports/cache 里的本地同币种同周期K线，不足会自动去 OKX 补齐；时间格式支持 20250101、2025-01-01、2025 1 1、202501010800、2025-01-01 08:00、2025年1月1日。",
            style="Muted.TLabel",
        ).grid(row=2, column=0, columnspan=2, sticky="w", pady=(8, 0))

        ttk.Label(
            parent,
            text="\u5f53\u524d\u7b56\u7565\uff1a\u5747\u7ebf\u4ea4\u53c9\u4e70\u5165\uff0c\u5747\u7ebf\u5f62\u6210\u6b7b\u53c9\u5c31\u5356\u51fa\u3002\u56de\u6d4b\u652f\u6301 BTC\u3001ETH\u3001SOL\u3001BNB\u3001OKB\u3001DOGE \u7684 USDT \u4ea4\u6613\u5bf9\uff0c5\u5206\u949f / 15\u5206\u949f / 1\u5c0f\u65f6 / 4\u5c0f\u65f6\u5386\u53f2K\u7ebf\u4e0a\u9650\u4e3a 10000 \u6839\u3002",
            style="SetupNote.TLabel",
            wraplength=620,
            justify="left",
        ).grid(row=3, column=0, sticky="w", pady=(12, 0))
        ttk.Label(parent, textvariable=self.bt_status, style="Muted.TLabel").grid(row=4, column=0, sticky="w", pady=(10, 0))

        preview_box = ttk.LabelFrame(parent, text="回撤概览", style="Setup.TLabelframe", padding=(12, 10))
        preview_box.grid(row=5, column=0, sticky="nsew", pady=(14, 0))
        preview_box.columnconfigure(0, weight=1)
        preview_box.rowconfigure(0, weight=1)

        self.bt_drawdown_preview_canvas = Canvas(preview_box, height=220, bg="#10161d", highlightthickness=0, bd=0)
        self.bt_drawdown_preview_canvas.grid(row=0, column=0, sticky="nsew")
        self.bt_drawdown_preview_canvas.bind("<Configure>", self._schedule_bt_drawdown_preview)
        self._schedule_bt_drawdown_preview()

    def _build_live_card(self, parent: ttk.Frame) -> None:
        parent.rowconfigure(3, weight=1)
        parent.columnconfigure(0, weight=7)
        parent.columnconfigure(1, weight=5)

        top_bar = ttk.Frame(parent, style="Card.TFrame")
        top_bar.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 12))
        top_bar.columnconfigure(0, weight=1)
        ttk.Label(
            top_bar,
            text="测试开单会直接向 OKX 实盘提交 BTCUSDT永续限价买单：9999 USDT / 0.001 BTC。",
            style="Muted.TLabel",
        ).grid(row=0, column=0, sticky="w")
        self.live_test_order_button = ttk.Button(top_bar, text="测试开单", command=self._start_live_test_order)
        self.live_test_order_button.grid(row=0, column=1, sticky="e")

        setup_box = ttk.LabelFrame(parent, text="策略启动", style="Setup.TLabelframe", padding=(16, 14))
        setup_box.grid(row=1, column=0, sticky="nsew", padx=(0, 14))
        setup_box.columnconfigure(1, weight=1)
        setup_box.columnconfigure(3, weight=1)

        self._add_setup_combo(setup_box, 0, 0, "选择策略", self.live_strategy, [STRATEGY_OPTION_LABEL], state="readonly")
        self._add_setup_combo(setup_box, 0, 2, "信号标的", self.live_symbol, LIVE_SYMBOL_OPTIONS)
        self._add_setup_combo(setup_box, 1, 0, "K\u7ebf\u5468\u671f", self.live_period, ["15m", "30m", "1H", "4H", "24\u5c0f\u65f6"], state="readonly")
        self._add_setup_combo(setup_box, 1, 2, "\u4fe1\u53f7\u65b9\u5411", self.live_signal_side, SIGNAL_SIDE_OPTIONS, state="readonly")
        self._add_setup_combo(setup_box, 2, 0, "\u8fd0\u884c\u6a21\u5f0f", self.live_run_mode, ["\u4ec5\u4fe1\u53f7\u68c0\u67e5", "\u6a21\u62df\u5e76\u4e0b\u5355", "\u4ea4\u6613\u5e76\u4e0b\u5355"], state="readonly")
        self._add_setup_entry(setup_box, 2, 2, "轮询秒数", self.live_poll)

        self.live_strategy_form_combo = self._add_setup_combo(
            setup_box,
            3,
            0,
            "选择策略形式",
            self.live_strategy_form,
            [LIVE_STRATEGY_FORM_NONE],
            state="readonly",
        )
        self.live_strategy_form_combo.bind("<<ComboboxSelected>>", self._on_live_strategy_form_select)
        self.live_strategy_form_combos.append(self.live_strategy_form_combo)
        self._add_setup_entry(setup_box, 3, 2, "EMA\u5c0f\u5468\u671f", self.live_fast)

        self._add_setup_entry(setup_box, 4, 0, "EMA\u5927\u5468\u671f", self.live_ema_large)
        self._add_setup_entry(setup_box, 4, 2, "ATR 周期", self.live_atr_period)

        self._add_setup_entry(setup_box, 5, 0, "止损 ATR 倍数", self.live_stop_atr)
        self._add_setup_entry(setup_box, 5, 2, "止盈 ATR 倍数", self.live_take_atr)

        self._add_setup_entry(setup_box, 6, 0, "\u98ce\u9669\u91d1", self.live_risk)
        self._add_setup_entry(setup_box, 6, 2, "固定数量", self.live_fixed_size, state="readonly")
        self._add_setup_combo(setup_box, 7, 0, "下单标的", self.live_order_symbol, ORDER_SYMBOL_OPTIONS)
        self._add_setup_combo(setup_box, 7, 2, "永续杠杆倍数", self.live_leverage, LEVERAGE_OPTIONS, state="readonly")

        ttk.Label(
            setup_box,
            text="\u5f53\u524d\u7b56\u7565\u4e3a 5EMA / 8EMA\uff1a\u5747\u7ebf\u4ea4\u53c9\u4e70\u5165\uff0c\u5747\u7ebf\u5f62\u6210\u6b7b\u53c9\u5c31\u5356\u51fa\u3002\u771f\u5b9e\u4ed3\u4f4d\u4f1a\u4f7f\u7528 OKX USDT \u6c38\u7eed\u5408\u7ea6\uff0c\u56de\u6d4b\u5b8c\u6210\u540e\uff0c\u53ef\u4ee5\u5728\u201c\u9009\u62e9\u7b56\u7565\u5f62\u5f0f\u201d\u91cc\u76f4\u63a5\u5957\u7528 9 \u5bab\u683c SL / TP \u7ec4\u5408\uff0c15m / 1H / 4H \u626b\u63cfK\u7ebf\u6570\u4e0a\u9650\u4e3a 10000 \u6839\u3002",
            style="SetupNote.TLabel",
            wraplength=620,
            justify="left",
        ).grid(row=8, column=0, columnspan=4, sticky="w", pady=(10, 2))
        ttk.Label(
            setup_box,
            text="\u63d0\u793a\uff1a\u4f18\u5148\u4f7f\u7528\u201c\u9009\u62e9\u7b56\u7565\u5f62\u5f0f\u201d\u91cc\u5df2\u9009\u4e2d\u7684 SL / TP \u7ec4\u5408\uff1b\u5982\u679c\u9009\u201c\u4e0d\u9009\u201d\uff0c\u5c31\u6539\u7528\u4f60\u624b\u52a8\u586b\u5199\u7684 ATR \u53c2\u6570\u3002",
            style="SetupNote.TLabel",
            wraplength=620,
            justify="left",
        ).grid(row=9, column=0, columnspan=4, sticky="w", pady=(2, 0))
        ttk.Label(
            setup_box,
            text="\u98ce\u63a7\u9650\u5236\uff1a\u6bcf\u5355\u6700\u5927\u4e8f\u635f\u6309\u201c\u98ce\u9669\u91d1\u201d\u5c01\u9876\uff0c\u89e6\u53ca\u6b62\u635f\u4ef7\u5c31\u4f1a\u6267\u884c\u6b62\u635f\uff1b\u5f53\u6b62\u635f ATR \u500d\u6570 = 1 \u65f6\uff0c\u98ce\u9669\u91d1\u5bf9\u5e94 1 \u500d ATR \u98ce\u9669\u3002",
            style="SetupNote.TLabel",
            wraplength=620,
            justify="left",
        ).grid(row=10, column=0, columnspan=4, sticky="w", pady=(2, 0))

        strategy_box = ttk.LabelFrame(parent, text="我的策略", style="Setup.TLabelframe", padding=(12, 12))
        strategy_box.grid(row=1, column=1, sticky="nsew")
        strategy_box.columnconfigure(0, weight=1)
        strategy_box.rowconfigure(1, weight=1)

        ttk.Label(
            strategy_box,
            text="开始轮询后，当前已启动的真实仓位策略会显示在这里。",
            style="Muted.TLabel",
            wraplength=360,
            justify="left",
        ).grid(row=0, column=0, sticky="w")

        strategy_columns = ("strategy", "account", "symbol", "signal_side", "ema_pair", "leverage", "stop_atr", "take_atr", "total_pnl")
        self.live_strategy_tree = ttk.Treeview(strategy_box, columns=strategy_columns, show="headings", height=11, selectmode="browse")
        self.live_strategy_tree.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        self.live_strategy_tree.bind("<<TreeviewSelect>>", self._on_live_strategy_tree_select)
        self.live_strategy_tree.bind("<ButtonRelease-1>", self._on_live_strategy_tree_click, add="+")

        strategy_headings = {
            "strategy": "策略",
            "account": "账户",
            "symbol": "币种",
            "signal_side": "信号方向",
            "ema_pair": "EMA小/大",
            "leverage": "杠杆",
            "stop_atr": "止损ATR",
            "take_atr": "止盈ATR",
            "total_pnl": "总盈亏",
        }
        strategy_widths = {
            "strategy": 70,
            "account": 120,
            "symbol": 110,
            "signal_side": 90,
            "ema_pair": 90,
            "leverage": 70,
            "stop_atr": 80,
            "take_atr": 80,
            "total_pnl": 90,
        }
        for column in strategy_columns:
            self.live_strategy_tree.heading(column, text=strategy_headings[column])
            self.live_strategy_tree.column(column, width=strategy_widths[column], anchor="center", stretch=column in {"account", "strategy"})

        strategy_scrollbar = ttk.Scrollbar(strategy_box, orient="vertical", command=self.live_strategy_tree.yview)
        self.live_strategy_tree.configure(yscrollcommand=strategy_scrollbar.set)
        strategy_scrollbar.grid(row=1, column=1, sticky="ns", pady=(12, 0))

        ttk.Button(strategy_box, text="停止此策略", command=self._stop_selected_live_strategy).grid(row=2, column=0, sticky="e", pady=(12, 0))
        self._refresh_live_strategy_table()

        button_bar = ttk.Frame(parent, style="Card.TFrame")
        button_bar.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(14, 0))
        ttk.Button(button_bar, text="\u68c0\u67e5\u4e00\u6b21", style="Primary.TButton", command=self._start_live_once).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(button_bar, text="\u5f00\u59cb\u8f6e\u8be2", command=self._start_live_loop).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(button_bar, text="\u505c\u6b62\u8f6e\u8be2", command=self._stop_live_loop).grid(row=0, column=2)

        advanced_box = ttk.LabelFrame(parent, text="\u8fde\u63a5\u4e0e\u4f59\u989d", style="Setup.TLabelframe", padding=(16, 12))
        advanced_box.grid(row=3, column=0, columnspan=2, sticky="nsew", pady=(14, 0))
        advanced_box.columnconfigure(1, weight=1)
        advanced_box.columnconfigure(3, weight=1)

        api_profile_combo = self._add_setup_combo(
            advanced_box,
            0,
            0,
            "API配置",
            self.live_api_profile_display,
            [],
            state="readonly",
        )
        self.live_api_profile_combo = api_profile_combo
        self.live_api_profile_combos.append(api_profile_combo)
        self._refresh_live_api_profile_combo()
        api_profile_combo.bind("<<ComboboxSelected>>", self._on_live_api_profile_select)
        api_button_bar = ttk.Frame(advanced_box, style="Card.TFrame")
        api_button_bar.grid(row=0, column=2, columnspan=2, sticky="e", pady=6)
        ttk.Button(api_button_bar, text="保存当前API", command=self._save_current_live_api_profile).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(api_button_bar, text="改名称", command=self._rename_current_live_api_profile).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(api_button_bar, text="复制到其他API", command=self._copy_current_live_api_profile_to_other_slots).grid(row=0, column=2, padx=(0, 8))
        ttk.Button(api_button_bar, text="清空当前API", command=self._clear_current_live_api_profile).grid(row=0, column=3)

        self._add_setup_entry(advanced_box, 1, 0, "API Key", self.live_api_key, width=22)
        self._add_setup_entry(advanced_box, 1, 2, "API Secret", self.live_api_secret, show="*", width=22)
        self._add_setup_entry(advanced_box, 2, 0, "Passphrase", self.live_api_passphrase, show="*", width=22)
        self._add_setup_entry(advanced_box, 2, 2, "扫描K线数", self.live_bars)
        self._add_setup_entry(advanced_box, 3, 0, "\u6700\u5927\u8d44\u91d1\u5360\u6bd4", self.live_max_alloc)
        ttk.Label(
            advanced_box,
            text="最大资金占比请手动填写。",
            style="SetupNote.TLabel",
            wraplength=420,
            justify="left",
        ).grid(row=3, column=2, columnspan=2, sticky="w", padx=(0, 10), pady=6)

        balance_grid = ttk.Frame(advanced_box, style="Card.TFrame")
        balance_grid.grid(row=4, column=0, columnspan=4, sticky="ew", pady=(10, 0))
        for col in range(4):
            balance_grid.columnconfigure(col, weight=1)
        self._add_value(balance_grid, 0, 0, "当前账户", self.live_account_panel_name)
        self._add_value(balance_grid, 0, 1, "账户余额", self.live_account_balance)
        self._add_value(balance_grid, 0, 2, "今日盈亏", self.live_today_pnl)
        self._add_value(balance_grid, 0, 3, "总盈亏", self.live_total_pnl)

        status_row = ttk.Frame(parent, style="Card.TFrame")
        status_row.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(14, 0))
        status_row.columnconfigure(0, weight=5)
        status_row.columnconfigure(1, weight=4)

        status_grid = ttk.Frame(status_row, style="Card.TFrame")
        status_grid.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        status_grid.columnconfigure(0, weight=1)

        action_cell = ttk.Frame(status_grid, style="Card.TFrame")
        action_cell.grid(row=0, column=0, sticky="ew", padx=6, pady=6)
        action_cell.columnconfigure(0, weight=1)
        ttk.Label(action_cell, text="当前动作", style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(action_cell, text="选择", style="Muted.TLabel").grid(row=0, column=1, sticky="e", padx=(18, 6))
        live_view_combo = ttk.Combobox(
            action_cell,
            textvariable=self.live_view_slot,
            values=["1", "2", "3", "4", "5"],
            state="readonly",
            width=5,
        )
        live_view_combo.grid(row=0, column=2, sticky="e")
        live_view_combo.bind("<<ComboboxSelected>>", self._on_live_view_slot_changed)
        ttk.Label(action_cell, textvariable=self.live_action, style="Value.TLabel").grid(row=1, column=0, sticky="w", pady=(4, 0))
        ttk.Label(
            action_cell,
            textvariable=self.live_action_account,
            style="Muted.TLabel",
            wraplength=560,
            justify="left",
        ).grid(row=2, column=0, sticky="w", pady=(4, 0))

        metric_strip = ttk.Frame(status_grid, style="Card.TFrame")
        metric_strip.grid(row=1, column=0, sticky="ew", padx=6, pady=(8, 6))
        for col, weight in enumerate((2, 2, 1, 1)):
            metric_strip.columnconfigure(col, weight=weight)

        self._add_value(metric_strip, 0, 0, "账户余额", self.live_account_balance)
        self._add_value(metric_strip, 0, 1, "触发原因", self.live_reason)
        self._add_value(metric_strip, 0, 2, "\u6700\u65b0\u6536\u76d8", self.live_price)
        self._add_value(metric_strip, 0, 3, "建议数量", self.live_size)
        self._add_value(metric_strip, 1, 0, "今日盈亏", self.live_today_pnl)
        self._add_value(metric_strip, 1, 1, "总盈亏", self.live_total_pnl)
        self._add_value(metric_strip, 1, 2, "胜率", self.live_win_rate)

        live_log_box = ttk.LabelFrame(status_row, text="实盘运行日志", style="Setup.TLabelframe", padding=(12, 10))
        live_log_box.grid(row=0, column=1, sticky="nsew")
        self._build_live_log_card(live_log_box)
        ttk.Label(parent, textvariable=self.live_status, style="Muted.TLabel").grid(row=5, column=0, columnspan=2, sticky="w", pady=(12, 0))

    @staticmethod
    def _format_live_strategy_value(value: float | None) -> str:
        if value is None:
            return "-"
        return f"{float(value):g}"

    @staticmethod
    def _format_live_strategy_pnl(value: float | str | None) -> str:
        if value in (None, ""):
            return "-"
        try:
            return f"{float(value):+,.2f}"
        except (TypeError, ValueError):
            return str(value).strip() or "-"

    @staticmethod
    def _format_live_fixed_size(value: float | None) -> str:
        if value is None:
            return "1"
        formatted = f"{float(value):.6f}".rstrip("0").rstrip(".")
        return formatted or "0"

    @staticmethod
    def _display_live_strategy_symbol(symbol: str) -> str:
        raw = (symbol or "").strip().upper()
        if raw.endswith("-SWAP"):
            raw = raw[:-5]
        normalized = raw.replace("-", "") or "-"
        return f"{normalized}永续" if normalized != "-" else "-"

    def _build_live_strategy_signature(self, request: LiveRequest) -> str:
        signature_payload = {
            "account_tag": request.account_tag,
            "symbol": request.symbol,
            "signal_symbol": request.signal_symbol,
            "period": request.period,
            "signal_side": request.signal_side,
            "leverage": request.leverage,
            "bars": request.bars,
            "risk_amount": request.risk_amount,
            "max_allocation_pct": request.max_allocation_pct,
            "fast_ema": request.fast_ema,
            "slow_ema": request.slow_ema,
            "atr_period": request.atr_period,
            "stop_loss_atr_multiplier": request.stop_loss_atr_multiplier,
            "take_profit_r_multiple": request.take_profit_r_multiple,
            "poll_interval": request.poll_interval,
            "order_timeout": request.order_timeout,
            "simulate": request.simulate,
            "execute": request.execute,
        }
        return json.dumps(signature_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    def _build_live_strategy_record(
        self,
        strategy_id: str,
        request: LiveRequest,
        *,
        account_label: str | None = None,
        total_pnl: float | str | None = None,
    ) -> dict[str, str]:
        numeric_suffix = strategy_id.rsplit("-", 1)[-1]
        strategy_label = f"策略{numeric_suffix}" if numeric_suffix.isdigit() else "策略"
        return {
            "strategy": strategy_label,
            "account": (account_label or self._live_api_profile_option_label(request.account_tag or "API 1")).strip() or "-",
            "symbol": self._display_live_strategy_symbol(request.symbol or request.signal_symbol or "-"),
            "signal_side": self._translate_signal_side(request.signal_side),
            "ema_pair": f"{int(request.fast_ema)}/{int(request.slow_ema)}",
            "leverage": f"{self._normalize_live_leverage(str(request.leverage))}x",
            "stop_atr": self._format_live_strategy_value(request.stop_loss_atr_multiplier),
            "take_atr": self._format_live_strategy_value(request.take_profit_r_multiple),
            "total_pnl": self._format_live_strategy_pnl(total_pnl),
        }

    def _refresh_live_strategy_table(self, *, select_id: str | None = None) -> None:
        tree = self.live_strategy_tree
        if tree is None or not tree.winfo_exists():
            return
        current_select = select_id or (tree.selection()[0] if tree.selection() else self.active_live_strategy_id)
        tree.delete(*tree.get_children())
        for strategy_id, row in sorted(
            self.live_strategy_records.items(),
            key=lambda item: int(self._slot_from_strategy_id(item[0]) or "99"),
        ):
            tree.insert(
                "",
                "end",
                iid=strategy_id,
                values=(
                    row.get("strategy", "-"),
                    row.get("account", "-"),
                    row.get("symbol", "-"),
                    row.get("signal_side", "-"),
                    row.get("ema_pair", "-"),
                    row.get("leverage", "-"),
                    row.get("stop_atr", "-"),
                    row.get("take_atr", "-"),
                    row.get("total_pnl", "-"),
                ),
            )
        if current_select and tree.exists(current_select):
            tree.selection_set(current_select)
            tree.focus(current_select)
            slot = self._slot_from_strategy_id(current_select)
            if slot and self.live_view_slot.get().strip() != slot:
                self.live_view_slot.set(slot)

    def _find_running_live_strategy(self, signature: str) -> str | None:
        for strategy_id, worker in self.live_strategy_workers.items():
            if str(worker.get("signature") or "") == signature:
                return strategy_id
        return None

    def _stop_live_strategy_by_id(self, strategy_id: str, *, remove_row: bool = True) -> dict[str, str]:
        worker = self.live_strategy_workers.pop(strategy_id, None)
        if isinstance(worker, dict):
            stop_event = worker.get("stop_event")
            if isinstance(stop_event, threading.Event):
                stop_event.set()

        row = dict(self.live_strategy_records.get(strategy_id) or {})
        self.live_strategy_snapshots.pop(strategy_id, None)
        if remove_row:
            self.live_strategy_records.pop(strategy_id, None)

        if self.active_live_strategy_id == strategy_id:
            self.active_live_strategy_id = None
        if self.active_live_strategy_id not in self.live_strategy_records:
            self.active_live_strategy_id = next(iter(self.live_strategy_records), None)

        self._refresh_live_strategy_table(select_id=self.active_live_strategy_id)
        self._apply_selected_live_strategy_view()
        return row

    def _stop_selected_live_strategy(self) -> None:
        tree = self.live_strategy_tree
        selected_id = tree.selection()[0] if tree is not None and tree.selection() else self.active_live_strategy_id
        if not selected_id:
            messagebox.showinfo("\u63d0\u793a", "\u7b56\u7565\u8868\u91cc\u8fd8\u6ca1\u6709\u53ef\u505c\u6b62\u7684\u7b56\u7565\u3002")
            return

        row = self._stop_live_strategy_by_id(selected_id)
        strategy_name = row.get("strategy", "该策略")
        remaining = len(self.live_strategy_workers)
        if remaining > 0:
            self.live_status.set(f"{strategy_name} 已停止，剩余 {remaining} 个策略轮询中。")
        else:
            self.live_status.set(f"{strategy_name} 已停止。")
        self._live_log(f"已停止 {strategy_name} 的实时轮询，并从“我的策略”中移除。")

    @staticmethod
    def _default_live_api_profiles() -> dict[str, dict[str, str]]:
        return {
            "API 1": {"api_key": "", "api_secret": "", "api_passphrase": "", "custom_name": "", "detected_name": ""},
            "API 2": {"api_key": "", "api_secret": "", "api_passphrase": "", "custom_name": "", "detected_name": ""},
            "API 3": {"api_key": "", "api_secret": "", "api_passphrase": "", "custom_name": "", "detected_name": ""},
            "API 4": {"api_key": "", "api_secret": "", "api_passphrase": "", "custom_name": "", "detected_name": ""},
        }

    def _live_api_profiles_path(self) -> Path:
        return Path("runtime") / "live_api_profiles.json"

    def _load_live_api_profiles(self) -> None:
        profiles = self._default_live_api_profiles()
        path = self._live_api_profiles_path()
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
            if isinstance(payload, dict):
                for profile_name, values in payload.items():
                    if profile_name in profiles and isinstance(values, dict):
                        profiles[profile_name] = {
                            "api_key": str(values.get("api_key") or ""),
                            "api_secret": str(values.get("api_secret") or ""),
                            "api_passphrase": str(values.get("api_passphrase") or ""),
                            "custom_name": str(values.get("custom_name") or ""),
                            "detected_name": str(values.get("detected_name") or values.get("display_name") or ""),
                        }

        self.live_api_profiles = profiles
        selected = self.live_api_profile.get().strip() or "API 1"
        if selected not in self.live_api_profiles:
            selected = "API 1"
        self.live_api_profile.set(selected)
        self._apply_live_api_profile(selected)
        self._refresh_live_api_profile_combo()

    def _save_live_api_profiles(self) -> None:
        path = self._live_api_profiles_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.live_api_profiles, ensure_ascii=True, indent=2), encoding="utf-8")

    def _apply_live_api_profile(self, profile_name: str) -> None:
        profile = self.live_api_profiles.get(profile_name, {})
        self.live_api_key.set(str(profile.get("api_key") or ""))
        self.live_api_secret.set(str(profile.get("api_secret") or ""))
        self.live_api_passphrase.set(str(profile.get("api_passphrase") or ""))

    def _live_api_profile_option_label(self, profile_name: str) -> str:
        profile = self.live_api_profiles.get(profile_name, {})
        display_name = str(profile.get("custom_name") or profile.get("detected_name") or "").strip()
        if display_name and display_name != profile_name:
            return f"{display_name} / {profile_name}"
        return profile_name

    def _current_live_api_profile_label(self) -> str:
        profile_name = self.live_api_profile.get().strip() or "API 1"
        return self._live_api_profile_option_label(profile_name)

    def _refresh_live_api_profile_combo(self) -> None:
        slots = list(self.live_api_profiles.keys()) or list(self._default_live_api_profiles().keys())
        option_map = {self._live_api_profile_option_label(slot): slot for slot in slots}
        self.live_api_profile_option_map = option_map
        values = list(option_map.keys())
        current_label = self._current_live_api_profile_label()
        self.live_api_profile_display.set(current_label)
        for combo in self.live_api_profile_combos:
            if combo is not None and combo.winfo_exists():
                combo.configure(values=values)

    def _update_detected_live_api_profile_name(self, account_name: str, account_uid: str, profile_name: str | None = None) -> None:
        detected_name = (account_name or "").strip()
        detected_uid = (account_uid or "").strip()
        if not detected_name and detected_uid:
            detected_name = f"UID {detected_uid}"
        if not detected_name:
            return

        profile_name = (profile_name or self.live_api_profile.get().strip() or "API 1").strip()
        profile = self.live_api_profiles.setdefault(
            profile_name,
            {"api_key": "", "api_secret": "", "api_passphrase": "", "custom_name": "", "detected_name": ""},
        )
        if str(profile.get("detected_name") or "").strip() == detected_name:
            return
        profile["detected_name"] = detected_name
        self._save_live_api_profiles()
        self._refresh_live_api_profile_combo()
        self._live_log(f"已识别 {profile_name} 对应账户：{detected_name}")

    def _refresh_live_action_account(self) -> None:
        profile_name = self.live_account_panel_name.get().strip() or self._current_live_api_profile_label()
        self.live_account_panel_name.set(profile_name)
        balance = self.live_account_balance.get().strip() or "-"
        win_rate = self.live_win_rate.get().strip() or "-"
        today_pnl = self.live_today_pnl.get().strip() or "-"
        total_pnl = self.live_total_pnl.get().strip() or "-"
        self.live_action_account.set(
            f"\u5f53\u524d\u8d26\u6237\uff1a{profile_name} | \u4f59\u989d {balance} | \u80dc\u7387 {win_rate} | \u4eca\u65e5 {today_pnl} | \u603b {total_pnl}"
        )

    @staticmethod
    def _format_live_win_rate(trades_df: object) -> str:
        if not isinstance(trades_df, pd.DataFrame) or trades_df.empty or "pnl" not in trades_df.columns:
            return "-"

        frame = trades_df.copy()
        if "status" in frame.columns:
            status_series = frame["status"].astype(str).str.lower().str.strip()
            frame = frame.loc[status_series.ne("open")]
        elif "exit_ts" in frame.columns:
            exit_series = frame["exit_ts"].astype(str).str.strip()
            frame = frame.loc[exit_series.ne("")]

        pnl_series = pd.to_numeric(frame.get("pnl"), errors="coerce")
        pnl_series = pnl_series[pnl_series.notna()]
        if pnl_series.empty:
            return "-"

        total_trades = int(len(pnl_series))
        win_trades = int((pnl_series > 0).sum())
        return f"{(win_trades / total_trades) * 100:.2f}%"

    def _reset_live_account_snapshot(self) -> None:
        self.live_action.set("\u672a\u68c0\u67e5")
        self.live_reason.set("-")
        self.live_price.set("-")
        self.live_size.set("-")
        self.live_account_balance.set("-")
        self.live_win_rate.set("-")
        self.live_today_pnl.set("-")
        self.live_total_pnl.set("-")
        self._refresh_live_action_account()

    @staticmethod
    def _strategy_id_for_slot(slot: str | int) -> str | None:
        try:
            slot_value = int(str(slot).strip())
        except (TypeError, ValueError):
            return None
        if 1 <= slot_value <= 5:
            return f"live-strategy-{slot_value}"
        return None

    @staticmethod
    def _slot_from_strategy_id(strategy_id: str | None) -> str | None:
        raw = (strategy_id or "").strip()
        if not raw:
            return None
        suffix = raw.rsplit("-", 1)[-1]
        if suffix.isdigit() and 1 <= int(suffix) <= 5:
            return suffix
        return None

    def _selected_live_strategy_id(self) -> str | None:
        return self._strategy_id_for_slot(self.live_view_slot.get().strip() or "1")

    def _find_available_live_strategy_id(self) -> str | None:
        for slot in range(1, 6):
            strategy_id = f"live-strategy-{slot}"
            if strategy_id not in self.live_strategy_workers:
                return strategy_id
        return None

    def _on_live_view_slot_changed(self, _event=None) -> None:
        strategy_id = self._selected_live_strategy_id()
        if strategy_id:
            self.active_live_strategy_id = strategy_id if strategy_id in self.live_strategy_records else self.active_live_strategy_id
            self._refresh_live_strategy_table(select_id=strategy_id if strategy_id in self.live_strategy_records else self.active_live_strategy_id)
        self._apply_selected_live_strategy_view(strategy_id=strategy_id)

    def _on_live_strategy_tree_select(self, _event=None) -> None:
        tree = self.live_strategy_tree
        if tree is None or not tree.selection():
            return
        strategy_id = tree.selection()[0]
        slot = self._slot_from_strategy_id(strategy_id)
        if slot and self.live_view_slot.get().strip() != slot:
            self.live_view_slot.set(slot)
        self._apply_selected_live_strategy_view(strategy_id=strategy_id)

    def _on_live_strategy_tree_click(self, _event=None) -> None:
        self.after_idle(self._on_live_strategy_tree_select)

    def _apply_selected_live_strategy_view(self, *, strategy_id: str | None = None) -> None:
        target_id = strategy_id or self._selected_live_strategy_id()
        slot = self._slot_from_strategy_id(target_id) or self.live_view_slot.get().strip() or "1"
        strategy_name = f"策略{slot}"
        row = self.live_strategy_records.get(target_id or "")
        snapshot = self.live_strategy_snapshots.get(target_id or "")

        if not snapshot:
            self.live_chart_bundle = None
            self._reset_live_account_snapshot()
            self.live_fixed_size.set("1")
            self.live_account_panel_name.set(row.get("account", strategy_name) if row else strategy_name)
            self._refresh_live_action_account()
            self._clear_live_trade_detail_table()
            if row:
                self.live_action.set(f"{strategy_name} 轮询中")
                self.live_status.set(f"当前查看 {strategy_name}，这条策略正在等待新的检查结果。")
                self.live_kline_status.set(f"{strategy_name} 当前还没有可显示的真实仓位图数据。")
                self.live_trade_detail_status.set(f"{strategy_name} 当前还没有成交记录。")
            else:
                self.live_action.set(f"{strategy_name} 未启用")
                self.live_status.set(f"当前查看 {strategy_name}，这条策略还没有启动。")
                self.live_kline_status.set(f"{strategy_name} 当前未启动，真实仓位图会在这条策略开始轮询后显示。")
                self.live_trade_detail_status.set(f"{strategy_name} 当前未启动，交易明细会在产生真实记录后显示。")
            self._schedule_live_kline_preview()
            return

        report = snapshot.get("report", {})
        request_payload = snapshot.get("request", {})
        bundle = snapshot.get("bundle")
        account_label = str(snapshot.get("account_label") or (row.get("account", strategy_name) if row else strategy_name))
        win_rate = str(snapshot.get("win_rate") or "-")
        fast_value = snapshot.get("fast_ema")
        slow_value = snapshot.get("slow_ema")
        signal_frame = bundle.signal_frame if isinstance(bundle, BacktestChartBundle) else None
        auto_fixed_size = self._resolve_live_auto_fixed_size(
            request_payload=request_payload,
            signal_frame=signal_frame,
        )
        translated_reason = self._translate_reason(str(report.get("reason") or ""), fast=fast_value, slow=slow_value)
        self.live_action.set(self._translate_action(str(report.get("action") or "")))
        self.live_reason.set(translated_reason)
        latest_close = report.get("latest_close")
        self.live_price.set(f"{float(latest_close):.4f}" if latest_close not in (None, "") else "-")
        self.live_size.set(str(report.get("suggested_size") or "-"))
        self.live_fixed_size.set(self._format_live_fixed_size(auto_fixed_size))
        self.live_account_balance.set(f"{float(report.get('total_assets', 0.0)):,.2f}")
        self.live_win_rate.set(win_rate)
        self.live_today_pnl.set(f"{float(report.get('today_pnl', 0.0)):+,.2f}")
        self.live_total_pnl.set(f"{float(report.get('total_pnl', 0.0)):+,.2f}")
        self.live_account_panel_name.set(account_label)
        self._refresh_live_action_account()
        self.live_status.set(f"当前查看 {strategy_name}：动作 {self._translate_action(str(report.get('action') or '-'))}，原因 {translated_reason}")

        self.live_chart_bundle = bundle if isinstance(bundle, BacktestChartBundle) else None
        if self.live_chart_bundle is not None:
            self.live_kline_status.set(
                f"{strategy_name} | {self.live_chart_bundle.symbol} {self.live_chart_bundle.period} 真实仓位图已更新，买卖点和交易明细都按当前任务单独显示。"
            )
            self._refresh_live_trade_detail_table(self.live_chart_bundle, strategy_name=strategy_name)
            self._schedule_live_kline_preview()
        else:
            self._clear_live_trade_detail_table()
            self.live_kline_status.set(f"{strategy_name} 当前还没有可显示的真实仓位图数据。")
            self.live_trade_detail_status.set(f"{strategy_name} 当前还没有成交记录。")

    def _store_live_strategy_snapshot(
        self,
        strategy_id: str,
        *,
        result: dict,
        bundle: BacktestChartBundle | None,
        account_label: str,
        win_rate: str,
        fast_value: int,
        slow_value: int,
    ) -> None:
        self.live_strategy_snapshots[strategy_id] = {
            "report": dict(result.get("report", {})),
            "request": dict(result.get("request", {})),
            "bundle": bundle,
            "account_label": account_label,
            "win_rate": win_rate,
            "fast_ema": fast_value,
            "slow_ema": slow_value,
        }

    def _resolve_live_auto_fixed_size(
        self,
        *,
        request_payload: dict | None = None,
        signal_frame: pd.DataFrame | None = None,
    ) -> float | None:
        request_payload = request_payload if isinstance(request_payload, dict) else {}
        if signal_frame is None or signal_frame.empty or "atr" not in signal_frame.columns:
            return None

        try:
            atr_value = float(pd.to_numeric(signal_frame["atr"], errors="coerce").dropna().iloc[-1])
        except Exception:
            return None

        stop_mult = self._optional_float(str(request_payload.get("stop_loss_atr_multiplier") or self.live_stop_atr.get() or "").strip() or "0")
        risk_amount = self._optional_float(str(request_payload.get("risk_amount") or self.live_risk.get() or "").strip() or "0")
        if atr_value <= 0.0 or stop_mult is None or stop_mult <= 0.0:
            return None
        if risk_amount is None or risk_amount <= 0.0:
            return None

        scaled_atr = float(risk_amount) / atr_value
        if scaled_atr <= 0.0:
            return None
        return max(scaled_atr * float(stop_mult), 0.0)

    def _capture_current_live_api_profile(self) -> dict[str, str]:
        profile_name = self.live_api_profile.get().strip() or "API 1"
        existing = self.live_api_profiles.get(profile_name, {})
        return {
            "api_key": self.live_api_key.get().strip(),
            "api_secret": self.live_api_secret.get().strip(),
            "api_passphrase": self.live_api_passphrase.get().strip(),
            "custom_name": str(existing.get("custom_name") or ""),
            "detected_name": str(existing.get("detected_name") or ""),
        }

    @staticmethod
    def _live_api_profile_has_credentials(profile: dict[str, str] | None) -> bool:
        if not isinstance(profile, dict):
            return False
        return any(str(profile.get(key) or "").strip() for key in ("api_key", "api_secret", "api_passphrase"))

    def _copy_current_live_api_profile_to_other_slots(self) -> None:
        source_name = self.live_api_profile.get().strip() or "API 1"
        source_profile = self._capture_current_live_api_profile()
        if not self._live_api_profile_has_credentials(source_profile):
            messagebox.showinfo("提示", "当前 API 配置还没有完整凭证，先填好再复制。", parent=self)
            return

        self.live_api_profiles[source_name] = source_profile
        copied_slots: list[str] = []
        skipped_slots: list[str] = []
        for slot_name in self._default_live_api_profiles().keys():
            if slot_name == source_name:
                continue
            existing = self.live_api_profiles.setdefault(
                slot_name,
                {"api_key": "", "api_secret": "", "api_passphrase": "", "custom_name": "", "detected_name": ""},
            )
            if self._live_api_profile_has_credentials(existing):
                skipped_slots.append(slot_name)
                continue
            self.live_api_profiles[slot_name] = {
                "api_key": source_profile["api_key"],
                "api_secret": source_profile["api_secret"],
                "api_passphrase": source_profile["api_passphrase"],
                "custom_name": source_profile["custom_name"],
                "detected_name": source_profile["detected_name"],
            }
            copied_slots.append(slot_name)

        self._save_live_api_profiles()
        self._refresh_live_api_profile_combo()
        if copied_slots:
            self.live_status.set(f"{self._current_live_api_profile_label()} 已复制到：{', '.join(copied_slots)}。")
            self._live_log(f"已将 {source_name} 的 API 配置复制到空槽位：{', '.join(copied_slots)}")
        else:
            self.live_status.set("其他 API 槽位已经有内容，这次没有覆盖。")
            self._live_log(f"{source_name} 复制操作已跳过，其他 API 槽位已有配置。")

    def _on_live_api_profile_select(self, _event=None) -> None:
        selected_label = self.live_api_profile_display.get().strip()
        profile_name = self.live_api_profile_option_map.get(selected_label)
        if not profile_name:
            profile_name = selected_label if selected_label in self.live_api_profiles else "API 1"
        self.live_api_profile.set(profile_name)
        self._apply_live_api_profile(profile_name)
        self._refresh_live_api_profile_combo()
        if self.live_strategy_workers:
            self._apply_selected_live_strategy_view()
            self.live_status.set(f"已切换到 {self._current_live_api_profile_label()}，已运行策略继续轮询中。")
        else:
            self._reset_live_account_snapshot()
            self.live_status.set(f"已切换到 {self._current_live_api_profile_label()}。")

    def _save_current_live_api_profile(self) -> None:
        profile_name = self.live_api_profile.get().strip() or "API 1"
        self.live_api_profiles[profile_name] = self._capture_current_live_api_profile()
        self._save_live_api_profiles()
        self._refresh_live_api_profile_combo()
        self.live_status.set(f"{self._current_live_api_profile_label()} 已保存，可直接切换。")
        self._live_log(f"已保存 {self._current_live_api_profile_label()} 的 API 配置。")

    def _set_live_api_profile_custom_name(self, profile_name: str, custom_name: str) -> None:
        profile = self.live_api_profiles.setdefault(
            profile_name,
            {"api_key": "", "api_secret": "", "api_passphrase": "", "custom_name": "", "detected_name": ""},
        )
        profile["custom_name"] = custom_name.strip()
        self._save_live_api_profiles()
        self._refresh_live_api_profile_combo()
        self._refresh_live_action_account()

    def _rename_current_live_api_profile(self) -> None:
        profile_name = self.live_api_profile.get().strip() or "API 1"
        profile = self.live_api_profiles.get(profile_name, {})
        current_name = str(profile.get("custom_name") or profile.get("detected_name") or "").strip()
        new_name = simpledialog.askstring(
            "修改API名称",
            "请输入当前 API 配置要显示的名称。\n留空后保存则恢复为自动检测名称或默认槽位名。",
            initialvalue=current_name,
            parent=self,
        )
        if new_name is None:
            return
        self._set_live_api_profile_custom_name(profile_name, new_name)
        display_label = self._current_live_api_profile_label()
        if new_name.strip():
            self.live_status.set(f"{display_label} 名称已更新。")
            self._live_log(f"已将 {profile_name} 重命名为：{new_name.strip()}")
        else:
            self.live_status.set(f"{display_label} 已恢复为自动名称。")
            self._live_log(f"已清除 {profile_name} 的手动名称，恢复为自动识别。")

    def _clear_current_live_api_profile(self) -> None:
        profile_name = self.live_api_profile.get().strip() or "API 1"
        self.live_api_profiles[profile_name] = {"api_key": "", "api_secret": "", "api_passphrase": "", "custom_name": "", "detected_name": ""}
        self._save_live_api_profiles()
        self._apply_live_api_profile(profile_name)
        self._reset_live_account_snapshot()
        self._refresh_live_api_profile_combo()
        self.live_status.set(f"{self._current_live_api_profile_label()} 已清空。")
        self._live_log(f"已清空 {self._current_live_api_profile_label()} 的 API 配置。")

    def _build_result_card(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent, style="Card.TFrame")
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="\u56de\u6d4b\u7ed3\u679c", style="SectionTitle.TLabel").grid(row=0, column=0, sticky="w")

        button_bar = ttk.Frame(header, style="Card.TFrame")
        button_bar.grid(row=0, column=1, sticky="e")
        ttk.Button(button_bar, text="\u6253\u5f00\u56de\u6d4b\u603b\u56fe", command=self._open_backtest_chart).grid(row=0, column=0)

        summary_line = ttk.Frame(parent, style="Card.TFrame")
        summary_line.grid(row=1, column=0, sticky="ew", pady=(12, 0))
        summary_line.columnconfigure(0, weight=1)
        summary_line.columnconfigure(1, weight=1)
        self._add_value(summary_line, 0, 0, "\u6700\u4f18\u5468\u671f", self.top_period)
        self._add_value(summary_line, 0, 1, "\u7efc\u5408\u5f97\u5206", self.top_score)

        ttk.Label(parent, textvariable=self.chart_status, style="Muted.TLabel").grid(row=2, column=0, sticky="w", pady=(10, 0))

        self.sltp_matrix_box = ttk.LabelFrame(parent, text="SL \\ TP 参数矩阵", style="Setup.TLabelframe", padding=(12, 10))
        self.sltp_matrix_box.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        self._render_sltp_matrix([])

        columns = (
            "combo_label",
            "trade_count",
            "net_pnl",
            "gross_profit",
            "gross_loss",
            "net_return_pct",
            "max_drawdown_pct",
            "cdar_95_pct",
            "final_score",
            "rank",
        )
        self.summary_tree = ttk.Treeview(parent, columns=columns, show="headings", height=9)
        self.summary_tree.grid(row=4, column=0, sticky="nsew", pady=(16, 0))
        self.summary_tree.bind("<<TreeviewSelect>>", self._on_summary_tree_select)

        headings = {
            "combo_label": "SL / TP \u7ec4\u5408",
            "trade_count": "\u4ea4\u6613",
            "net_pnl": "\u51c0\u6536\u76ca",
            "gross_profit": "\u603b\u76c8\u5229",
            "gross_loss": "\u603b\u4e8f\u635f",
            "net_return_pct": "\u6536\u76ca%",
            "max_drawdown_pct": "\u56de\u64a4%",
            "cdar_95_pct": "CDaR%",
            "final_score": "\u5f97\u5206",
            "rank": "\u6392\u540d",
        }
        widths = {
            "combo_label": 170,
            "trade_count": 60,
            "net_pnl": 90,
            "gross_profit": 90,
            "gross_loss": 90,
            "net_return_pct": 75,
            "max_drawdown_pct": 75,
            "cdar_95_pct": 75,
            "final_score": 80,
            "rank": 55,
        }

        for column in columns:
            self.summary_tree.heading(column, text=headings[column])
            self.summary_tree.column(column, width=widths[column], anchor="center")

        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=self.summary_tree.yview)
        self.summary_tree.configure(yscroll=scrollbar.set)
        scrollbar.grid(row=4, column=1, sticky="ns", pady=(16, 0))

    def _build_log_card(self, parent: ttk.Frame) -> None:
        parent.rowconfigure(2, weight=1)

        header = ttk.Frame(parent, style="Card.TFrame")
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="\u8fd0\u884c\u65e5\u5fd7", style="SectionTitle.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Button(header, text="\u6e05\u7a7a\u65e5\u5fd7", command=self._clear_log).grid(row=0, column=1, sticky="e")

        log_shell = ttk.Frame(parent, style="Card.TFrame")
        log_shell.grid(row=1, column=0, sticky="ew", pady=(14, 0))
        log_shell.columnconfigure(0, weight=1)
        log_shell.rowconfigure(0, weight=1)

        self.log_text = Text(log_shell, wrap="word", height=14, bg="#17211b", fg="#d9e7db", insertbackground="#d9e7db", relief="flat")
        self.log_text.grid(row=0, column=0, sticky="ew")
        log_scrollbar = ttk.Scrollbar(log_shell, orient="vertical", command=self.log_text.yview)
        log_scrollbar.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scrollbar.set)
        self.log_text.insert("end", "回测日志已启动（详细模式）。\n")
        self.log_text.configure(state="disabled")

        metric_box = ttk.LabelFrame(parent, text="\u5f53\u524d\u56de\u6d4b\u6307\u6807", style="Setup.TLabelframe", padding=(12, 10))
        metric_box.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=(14, 0))
        self._build_backtest_metric_card(metric_box)

    def _build_live_log_card(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        header = ttk.Frame(parent, style="Card.TFrame")
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Button(header, text="清空日志", command=self._clear_live_log).grid(row=0, column=1, sticky="e")

        log_shell = ttk.Frame(parent, style="Card.TFrame")
        log_shell.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        log_shell.columnconfigure(0, weight=1)
        log_shell.rowconfigure(0, weight=1)

        self.live_log_text = Text(
            log_shell,
            wrap="word",
            height=12,
            bg="#17211b",
            fg="#d9e7db",
            insertbackground="#d9e7db",
            relief="flat",
        )
        self.live_log_text.grid(row=0, column=0, sticky="nsew")
        log_scrollbar = ttk.Scrollbar(log_shell, orient="vertical", command=self.live_log_text.yview)
        log_scrollbar.grid(row=0, column=1, sticky="ns")
        self.live_log_text.configure(yscrollcommand=log_scrollbar.set)
        self.live_log_text.insert("end", "实盘日志已启动（详细模式）。\n")
        self.live_log_text.configure(state="disabled")

    def _build_backtest_metric_card(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)

        combo_row = ttk.Frame(parent, style="Card.TFrame")
        combo_row.grid(row=0, column=0, sticky="ew")
        combo_row.columnconfigure(0, weight=1)
        ttk.Label(combo_row, text="\u5f53\u524d\u7ec4\u5408", style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(
            combo_row,
            textvariable=self.metric_combo,
            style="Value.TLabel",
            wraplength=360,
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(4, 0))

        grid = ttk.Frame(parent, style="Card.TFrame")
        grid.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        for col in range(3):
            grid.columnconfigure(col, weight=1)

        self._add_value(grid, 0, 0, "\u51c0\u6536\u76ca", self.metric_net_pnl)
        self._add_value(grid, 0, 1, "\u6536\u76ca\u7387", self.metric_return)
        self._add_value(grid, 0, 2, "\u80dc\u7387", self.metric_win_rate)
        self._add_value(grid, 1, 0, "\u4ea4\u6613\u6570", self.metric_trades)
        self._add_value(grid, 1, 1, "\u6700\u5927\u56de\u64a4", self.metric_max_drawdown)
        self._add_value(grid, 1, 2, "CDaR", self.metric_cdar)
        self._add_value(grid, 2, 0, "\u7efc\u5408\u5f97\u5206", self.metric_score)
        self._add_value(grid, 2, 1, "\u5f53\u524d\u6392\u540d", self.metric_rank)
        self._add_value(grid, 2, 2, "\u81ea\u5b9a\u4e49\u65f6\u95f4\u6bb5", self.metric_custom_period)

    def _build_kline_preview_card(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent, style="Card.TFrame")
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="回测K线图", style="SectionTitle.TLabel").grid(row=0, column=0, sticky="w")
        control_bar = ttk.Frame(header, style="Card.TFrame")
        control_bar.grid(row=0, column=1, sticky="e")
        ttk.Button(control_bar, text="放大", width=7, command=lambda: self._zoom_kline_preview(120, self._kline_preview_anchor_x())).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(control_bar, text="缩小", width=7, command=lambda: self._zoom_kline_preview(-120, self._kline_preview_anchor_x())).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(control_bar, text="左移", width=7, command=lambda: self._nudge_kline_preview(-1)).grid(row=0, column=2, padx=(0, 6))
        ttk.Button(control_bar, text="右移", width=7, command=lambda: self._nudge_kline_preview(1)).grid(row=0, column=3, padx=(0, 6))
        ttk.Button(control_bar, text="\u6700\u65b0", width=7, command=self._show_latest_kline_preview).grid(row=0, column=4)
        ttk.Label(header, textvariable=self.kline_status, style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(6, 0))

        self.kline_preview_canvas = Canvas(parent, height=430, bg="#050505", highlightthickness=0, bd=0)
        self.kline_preview_canvas.grid(row=1, column=0, sticky="ew", pady=(16, 0))
        self.kline_preview_canvas.bind("<Configure>", self._schedule_kline_preview)
        self.kline_preview_canvas.bind("<Control-MouseWheel>", self._on_kline_preview_mousewheel)
        self.kline_preview_canvas.bind("<Shift-MouseWheel>", self._on_kline_preview_shift_mousewheel)
        self.kline_preview_canvas.bind("<ButtonPress-1>", self._on_kline_preview_pan_start)
        self.kline_preview_canvas.bind("<B1-Motion>", self._on_kline_preview_pan_drag)
        self.kline_preview_canvas.bind("<ButtonRelease-1>", self._on_kline_preview_pan_end)
        self._schedule_kline_preview()

    def _build_trade_detail_card(self, parent: ttk.Frame) -> None:
        parent.rowconfigure(1, weight=1)

        header = ttk.Frame(parent, style="Card.TFrame")
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="交易明细", style="SectionTitle.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(header, textvariable=self.trade_detail_status, style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(6, 0))

        columns = ("trade_no", "side", "quantity", "entry_ts", "entry_price", "atr_value", "exit_ts", "exit_price", "pnl", "fees", "exit_reason")
        self.trade_detail_tree = ttk.Treeview(parent, columns=columns, show="headings", height=14)
        self.trade_detail_tree.grid(row=1, column=0, sticky="nsew", pady=(14, 0))

        headings = {
            "trade_no": "第几次交易",
            "side": "方向",
            "quantity": "数量",
            "entry_ts": "进场时间",
            "entry_price": "进场价格",
            "atr_value": "ATR值",
            "exit_ts": "出场时间",
            "exit_price": "出场价格",
            "pnl": "盈亏",
            "fees": "手续费",
            "exit_reason": "退出原因",
        }
        widths = {
            "trade_no": 92,
            "side": 70,
            "quantity": 90,
            "entry_ts": 150,
            "entry_price": 120,
            "atr_value": 100,
            "exit_ts": 150,
            "exit_price": 120,
            "pnl": 100,
            "fees": 90,
            "exit_reason": 140,
        }
        for column in columns:
            self.trade_detail_tree.heading(column, text=headings[column])
            self.trade_detail_tree.column(column, width=widths[column], anchor="center")

        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=self.trade_detail_tree.yview)
        self.trade_detail_tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.grid(row=1, column=1, sticky="ns", pady=(14, 0))

    def _build_live_kline_preview_card(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent, style="Card.TFrame")
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="真实仓位图", style="SectionTitle.TLabel").grid(row=0, column=0, sticky="w")
        control_bar = ttk.Frame(header, style="Card.TFrame")
        control_bar.grid(row=0, column=1, sticky="e")
        ttk.Button(control_bar, text="打开总图", width=9, command=self._open_live_chart).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(control_bar, text="放大", width=7, command=lambda: self._zoom_live_kline_preview(120, self._live_kline_preview_anchor_x())).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(control_bar, text="缩小", width=7, command=lambda: self._zoom_live_kline_preview(-120, self._live_kline_preview_anchor_x())).grid(row=0, column=2, padx=(0, 6))
        ttk.Button(control_bar, text="左移", width=7, command=lambda: self._nudge_live_kline_preview(-1)).grid(row=0, column=3, padx=(0, 6))
        ttk.Button(control_bar, text="右移", width=7, command=lambda: self._nudge_live_kline_preview(1)).grid(row=0, column=4, padx=(0, 6))
        ttk.Button(control_bar, text="最新", width=7, command=self._show_latest_live_kline_preview).grid(row=0, column=5)
        ttk.Label(header, textvariable=self.live_kline_status, style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(6, 0))

        self.live_kline_preview_canvas = Canvas(parent, height=430, bg="#050505", highlightthickness=0, bd=0)
        self.live_kline_preview_canvas.grid(row=1, column=0, sticky="ew", pady=(16, 0))
        self.live_kline_preview_canvas.bind("<Configure>", self._schedule_live_kline_preview)
        self.live_kline_preview_canvas.bind("<Control-MouseWheel>", self._on_live_kline_preview_mousewheel)
        self.live_kline_preview_canvas.bind("<Shift-MouseWheel>", self._on_live_kline_preview_shift_mousewheel)
        self.live_kline_preview_canvas.bind("<ButtonPress-1>", self._on_live_kline_preview_pan_start)
        self.live_kline_preview_canvas.bind("<B1-Motion>", self._on_live_kline_preview_pan_drag)
        self.live_kline_preview_canvas.bind("<ButtonRelease-1>", self._on_live_kline_preview_pan_end)
        self._schedule_live_kline_preview()

    def _build_live_trade_detail_card(self, parent: ttk.Frame) -> None:
        parent.rowconfigure(1, weight=1)

        header = ttk.Frame(parent, style="Card.TFrame")
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="交易明细", style="SectionTitle.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(header, textvariable=self.live_trade_detail_status, style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(6, 0))

        columns = ("trade_no", "side", "quantity", "entry_ts", "entry_price", "atr_value", "exit_ts", "exit_price", "pnl", "fees", "exit_reason")
        self.live_trade_detail_tree = ttk.Treeview(parent, columns=columns, show="headings", height=14)
        self.live_trade_detail_tree.grid(row=1, column=0, sticky="nsew", pady=(14, 0))

        headings = {
            "trade_no": "第几次交易",
            "side": "方向",
            "quantity": "数量",
            "entry_ts": "进场时间",
            "entry_price": "进场价格",
            "atr_value": "ATR值",
            "exit_ts": "出场时间",
            "exit_price": "出场价格",
            "pnl": "盈亏",
            "fees": "手续费",
            "exit_reason": "退出原因",
        }
        widths = {
            "trade_no": 92,
            "side": 70,
            "quantity": 90,
            "entry_ts": 150,
            "entry_price": 120,
            "atr_value": 100,
            "exit_ts": 150,
            "exit_price": 120,
            "pnl": 100,
            "fees": 90,
            "exit_reason": 140,
        }
        for column in columns:
            self.live_trade_detail_tree.heading(column, text=headings[column])
            self.live_trade_detail_tree.column(column, width=widths[column], anchor="center")

        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=self.live_trade_detail_tree.yview)
        self.live_trade_detail_tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.grid(row=1, column=1, sticky="ns", pady=(14, 0))

    def _clear_trade_detail_table(self) -> None:
        if self.trade_detail_tree is not None:
            for item in self.trade_detail_tree.get_children():
                self.trade_detail_tree.delete(item)
        self.trade_detail_status.set("回测完成后，这里会显示当前组合的交易明细，并包含手续费。")

    def _clear_live_trade_detail_table(self) -> None:
        for tree in (self.live_trade_detail_tree, self.live_trade_detail_tree2):
            if tree is not None:
                for item in tree.get_children():
                    tree.delete(item)
        self.live_trade_detail_status.set("真实仓位产生买卖记录后，这里会显示真实仓位的交易明细，并包含手续费。")

    def _refresh_trade_detail_table(self, bundle: BacktestChartBundle | None, *, display_label: str | None = None) -> None:
        if self.trade_detail_tree is None:
            return

        for item in self.trade_detail_tree.get_children():
            self.trade_detail_tree.delete(item)

        if bundle is None or bundle.trades_df.empty:
            self.trade_detail_status.set("当前组合暂时没有交易明细。")
            return

        trades = self._sort_trades_for_display(bundle.trades_df)
        signal_frame = bundle.signal_frame.copy() if bundle.signal_frame is not None else pd.DataFrame()
        trade_count = 0
        for idx, trade in enumerate(trades.itertuples(), start=1):
            self.trade_detail_tree.insert(
                "",
                "end",
                iid=f"trade_{idx}",
                values=(
                    idx,
                    self._format_trade_side(getattr(trade, "side", "")),
                    self._format_trade_quantity(getattr(trade, "qty", None)),
                    self._format_trade_timestamp(getattr(trade, "entry_ts", "")),
                    self._format_trade_price(getattr(trade, "entry_price", None)),
                    self._format_trade_atr(self._lookup_trade_atr(signal_frame, getattr(trade, "entry_ts", ""))),
                    self._format_trade_timestamp(getattr(trade, "exit_ts", "")),
                    self._format_trade_price(getattr(trade, "exit_price", None)),
                    self._format_trade_pnl(getattr(trade, "pnl", None)),
                    self._format_trade_fee(getattr(trade, "fees", None)),
                    self._format_trade_reason(str(getattr(trade, "exit_reason", ""))),
                ),
            )
            trade_count += 1

        combo_label = display_label or self._current_matrix_display_label()
        self.trade_detail_status.set(f"{bundle.symbol} {bundle.period} {combo_label} 交易明细，共 {trade_count} 笔，已显示手续费和ATR值。")

    def _refresh_live_trade_detail_table(self, bundle: BacktestChartBundle | None, *, strategy_name: str | None = None) -> None:
        if self.live_trade_detail_tree is None and self.live_trade_detail_tree2 is None:
            return

        prefix = f"{strategy_name} | " if strategy_name else ""

        if bundle is None or bundle.trades_df.empty:
            for tree in (self.live_trade_detail_tree, self.live_trade_detail_tree2):
                if tree is not None:
                    for item in tree.get_children():
                        tree.delete(item)
            self.live_trade_detail_status.set(f"{prefix}当前真实仓位还没有成交记录。")
            return

        signal_frame = bundle.signal_frame.copy() if bundle.signal_frame is not None else pd.DataFrame()
        trade_count = 0
        rows: list[tuple[object, str, str, str, str, str, str, str, str, str, str]] = []
        for idx, trade in enumerate(self._sort_trades_for_display(bundle.trades_df).itertuples(), start=1):
            rows.append(
                (
                    idx,
                    self._format_trade_side(getattr(trade, "side", "")),
                    self._format_trade_quantity(getattr(trade, "qty", getattr(trade, "entry_size", None))),
                    self._format_trade_timestamp(getattr(trade, "entry_ts", "")),
                    self._format_trade_price(getattr(trade, "entry_price", None)),
                    self._format_trade_atr(self._lookup_trade_atr(signal_frame, getattr(trade, "entry_ts", ""))),
                    self._format_trade_timestamp(getattr(trade, "exit_ts", "")),
                    self._format_trade_price(getattr(trade, "exit_price", None)),
                    self._format_trade_pnl(getattr(trade, "pnl", None)),
                    self._format_trade_fee(getattr(trade, "fees", None)),
                    self._format_trade_reason(str(getattr(trade, "exit_reason", ""))),
                )
            )
        trade_count = len(rows)
        for tree in (self.live_trade_detail_tree, self.live_trade_detail_tree2):
            if tree is None:
                continue
            for item in tree.get_children():
                tree.delete(item)
            for idx, values in enumerate(rows, start=1):
                tree.insert("", "end", iid=f"{tree}_{idx}", values=values)

        self.live_trade_detail_status.set(
            f"{prefix}{bundle.symbol} {bundle.period} 真实仓位交易明细，共 {trade_count} 笔，已按真实仓位记录计算并显示ATR值。"
        )

    @staticmethod
    def _format_trade_side(side: str) -> str:
        mapping = {"long": "做多", "short": "做空"}
        return mapping.get(str(side).strip(), str(side).strip() or "-")

    @staticmethod
    def _format_trade_quantity(value) -> str:
        try:
            if value is None or value == "" or pd.isna(value):
                return "-"
            return f"{float(value):,.6f}".rstrip("0").rstrip(".")
        except Exception:
            return "-"

    @staticmethod
    def _lookup_trade_atr(signal_frame: pd.DataFrame, ts_value) -> float | None:
        if signal_frame is None or signal_frame.empty or "atr" not in signal_frame.columns or ts_value in (None, "", "NaT"):
            return None
        try:
            stamp = pd.Timestamp(ts_value)
        except Exception:
            return None

        index_tz = getattr(signal_frame.index, "tz", None)
        if index_tz is not None and stamp.tzinfo is None:
            stamp = stamp.tz_localize(index_tz)
        elif index_tz is None and stamp.tzinfo is not None:
            stamp = stamp.tz_localize(None)

        located = int(signal_frame.index.get_indexer([stamp])[0])
        if located < 0:
            located = int(signal_frame.index.searchsorted(stamp))
        if not (0 <= located < len(signal_frame)):
            return None
        try:
            value = float(signal_frame["atr"].iloc[located])
        except Exception:
            return None
        return None if pd.isna(value) else value

    @staticmethod
    def _format_trade_atr(value) -> str:
        try:
            if value is None or value == "" or pd.isna(value):
                return "-"
            return f"{float(value):,.2f}"
        except Exception:
            return "-"

    @staticmethod
    def _format_trade_reason(reason: str) -> str:
        mapping = {
            "slow_ema_stop": "慢线止损",
            "dead_cross": "死叉卖出",
            "golden_cross_cover": "金叉平空",
            "atr_stop_loss": "ATR止损",
            "atr_take_profit": "ATR止盈",
            "time_exit": "时间退出",
            "end_of_data": "数据结束",
            "position_open": "持仓中",
            "exit_signal": "退出信号",
        }
        raw = str(reason).strip()
        return mapping.get(raw, raw or "-")

    @staticmethod
    def _format_trade_price(value) -> str:
        try:
            if value is None or value == "" or pd.isna(value):
                return "-"
            return f"{float(value):.2f}"
        except Exception:
            return "-"

    @staticmethod
    def _format_trade_pnl(value) -> str:
        try:
            if value is None or value == "" or pd.isna(value):
                return "-"
            return f"{float(value):+,.2f}"
        except Exception:
            return "-"

    @staticmethod
    def _format_trade_fee(value) -> str:
        try:
            if value is None or value == "" or pd.isna(value):
                return "-"
            return f"{float(value):,.2f}"
        except Exception:
            return "-"

    @staticmethod
    def _find_preview_bar_index(index: pd.Index, ts_value) -> int:
        if ts_value in (None, "", "NaT") or len(index) == 0:
            return -1
        try:
            stamp = pd.Timestamp(ts_value)
        except Exception:
            return -1
        try:
            index_tz = getattr(index, "tz", None)
            if index_tz is not None and stamp.tzinfo is None:
                stamp = stamp.tz_localize(index_tz)
            elif index_tz is None and stamp.tzinfo is not None:
                stamp = stamp.tz_localize(None)
            located = int(index.get_indexer([stamp])[0])
            if located >= 0:
                return located
            nearest = int(index.searchsorted(stamp))
            if 0 <= nearest < len(index):
                return nearest
        except Exception:
            return -1
        return -1

    def _build_preview_trade_marks(self, bundle: BacktestChartBundle) -> list[tuple[str, str, int, float]]:
        if bundle is None or bundle.signal_frame.empty or bundle.trades_df.empty:
            return []

        marks: list[tuple[str, str, int, float]] = []
        index = bundle.signal_frame.index
        trades = self._sort_trades_for_display(bundle.trades_df)
        for idx, trade in enumerate(trades.itertuples(), start=1):
            side = str(getattr(trade, "side", "long")).lower()
            entry_kind = "sell" if side == "short" else "buy"
            exit_kind = "buy" if side == "short" else "sell"
            entry_label = f"{'卖' if entry_kind == 'sell' else '买'}{idx}"
            exit_label = f"{'买' if exit_kind == 'buy' else '卖'}{idx}"

            entry_idx = self._find_preview_bar_index(index, getattr(trade, "entry_ts", None))
            entry_price = getattr(trade, "entry_price", None)
            if entry_idx >= 0 and entry_price not in (None, "") and not pd.isna(entry_price):
                marks.append((entry_label, entry_kind, entry_idx, float(entry_price)))

            exit_idx = self._find_preview_bar_index(index, getattr(trade, "exit_ts", None))
            exit_price = getattr(trade, "exit_price", None)
            if exit_idx >= 0 and exit_price not in (None, "") and not pd.isna(exit_price):
                marks.append((exit_label, exit_kind, exit_idx, float(exit_price)))

        return marks

    def _get_kline_preview_trade_marks(self, bundle: BacktestChartBundle | None) -> list[tuple[str, str, int, float]]:
        if bundle is None:
            return []
        bundle_id = id(bundle)
        if self._kline_preview_mark_bundle_id != bundle_id:
            self._kline_preview_trade_marks = self._build_preview_trade_marks(bundle)
            self._kline_preview_mark_bundle_id = bundle_id
        return self._kline_preview_trade_marks

    def _get_live_kline_preview_trade_marks(self, bundle: BacktestChartBundle | None) -> list[tuple[str, str, int, float]]:
        if bundle is None:
            return []
        bundle_id = id(bundle)
        if self._live_kline_preview_mark_bundle_id != bundle_id:
            self._live_kline_preview_trade_marks = self._build_preview_trade_marks(bundle)
            self._live_kline_preview_mark_bundle_id = bundle_id
        return self._live_kline_preview_trade_marks

    @staticmethod
    def _sort_trades_for_display(trades_df: pd.DataFrame | None) -> pd.DataFrame:
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

    @staticmethod
    def _format_trade_timestamp(value) -> str:
        if value in (None, "", "NaT"):
            return "-"
        try:
            stamp = pd.Timestamp(value)
        except Exception:
            return str(value)
        if pd.isna(stamp):
            return "-"

        local_tz = datetime.now().astimezone().tzinfo
        if stamp.tzinfo is not None and local_tz is not None:
            try:
                stamp = stamp.tz_convert(local_tz)
            except Exception:
                pass
        if stamp.tzinfo is not None:
            stamp = stamp.tz_localize(None)
        return stamp.strftime("%Y-%m-%d %H:%M")

    def _on_page_frame_configure(self, _event=None) -> None:
        if self.page_canvas is None:
            return
        self.page_canvas.configure(scrollregion=self.page_canvas.bbox("all"))

    def _on_page_canvas_configure(self, event) -> None:
        if self.page_canvas is None or self._page_window_id is None:
            return
        self.page_canvas.itemconfigure(self._page_window_id, width=event.width)

    def _resolve_event_widget(self, event):
        widget = getattr(event, "widget", None)
        if widget is None:
            return None
        if hasattr(widget, "winfo_toplevel"):
            return widget
        if isinstance(widget, str):
            try:
                return self.nametowidget(widget)
            except Exception:
                return None
        return None

    def _on_app_mousewheel(self, event) -> str | None:
        if self.page_canvas is None:
            return None
        widget = self._resolve_event_widget(event)
        if widget is None:
            return None
        if widget.winfo_toplevel() is not self:
            return None
        if self.kline_preview_canvas is not None and widget == self.kline_preview_canvas:
            if event.state & 0x0001:
                return self._on_kline_preview_shift_mousewheel(event)
            if event.state & 0x0004:
                return self._on_kline_preview_mousewheel(event)
        if event.state & 0x0005:
            return None

        steps = int(-event.delta / 120) if event.delta else 0
        if steps == 0:
            steps = -1 if event.delta > 0 else 1
        self.page_canvas.yview_scroll(steps, "units")
        return "break"

    def _add_entry(self, parent: ttk.Frame, row: int, column: int, label: str, variable: StringVar, *, show: str | None = None, width: int = 16) -> None:
        cell = ttk.Frame(parent, style="Card.TFrame")
        cell.grid(row=row, column=column, sticky="ew", padx=6, pady=6)
        cell.columnconfigure(0, weight=1)
        ttk.Label(cell, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Entry(cell, textvariable=variable, width=width, show=show or "").grid(row=1, column=0, sticky="ew", pady=(6, 0))

    def _add_combo(
        self,
        parent: ttk.Frame,
        row: int,
        column: int,
        label: str,
        variable: StringVar,
        values: list[str],
        *,
        width: int = 16,
        state: str = "readonly",
    ) -> None:
        cell = ttk.Frame(parent, style="Card.TFrame")
        cell.grid(row=row, column=column, sticky="ew", padx=6, pady=6)
        cell.columnconfigure(0, weight=1)
        ttk.Label(cell, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Combobox(cell, textvariable=variable, values=values, width=width, state=state).grid(row=1, column=0, sticky="ew", pady=(6, 0))

    def _add_setup_entry(
        self,
        parent: ttk.Widget,
        row: int,
        column: int,
        label: str,
        variable: StringVar,
        *,
        show: str | None = None,
        width: int = 18,
        state: str = "normal",
    ) -> ttk.Entry:
        ttk.Label(parent, text=label, style="SetupLabel.TLabel").grid(row=row, column=column, sticky="w", padx=(0, 10), pady=6)
        entry = ttk.Entry(parent, textvariable=variable, width=width, show=show or "", state=state)
        entry.grid(row=row, column=column + 1, sticky="ew", pady=6)
        return entry

    def _add_setup_combo(
        self,
        parent: ttk.Widget,
        row: int,
        column: int,
        label: str,
        variable: StringVar,
        values: list[str],
        *,
        width: int = 18,
        state: str = "normal",
    ) -> ttk.Combobox:
        ttk.Label(parent, text=label, style="SetupLabel.TLabel").grid(row=row, column=column, sticky="w", padx=(0, 10), pady=6)
        combo = ttk.Combobox(parent, textvariable=variable, values=values, width=width, state=state)
        combo.grid(row=row, column=column + 1, sticky="ew", pady=6)
        return combo

    def _render_sltp_matrix(self, matrix_rows: list[dict]) -> None:
        self.sltp_matrix_rows = list(matrix_rows)
        for child in self.sltp_matrix_box.winfo_children():
            child.destroy()

        control_bar = ttk.Frame(self.sltp_matrix_box, style="Card.TFrame")
        control_bar.grid(row=0, column=0, sticky="ew", padx=4, pady=(0, 8), columnspan=8)
        control_bar.columnconfigure(5, weight=1)
        ttk.Label(control_bar, text="查看方式", style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        view_combo = ttk.Combobox(
            control_bar,
            textvariable=self.sltp_view_mode,
            values=["SLTP参数矩阵", "赢亏热力图"],
            width=16,
            state="readonly",
        )
        view_combo.grid(row=0, column=1, sticky="w", padx=(8, 18))
        view_combo.bind("<<ComboboxSelected>>", lambda _event: self._render_sltp_matrix(self.sltp_matrix_rows))

        if self.sltp_view_mode.get().strip() == "赢亏热力图":
            ttk.Label(control_bar, text="热力指标", style="Muted.TLabel").grid(row=0, column=2, sticky="w")
            metric_combo = ttk.Combobox(
                control_bar,
                values=["总盈亏"],
                width=12,
                state="readonly",
            )
            metric_combo.set("总盈亏")
            metric_combo.grid(row=0, column=3, sticky="w", padx=(8, 18))

        if not matrix_rows:
            ttk.Label(
                self.sltp_matrix_box,
                text="\u56de\u6d4b\u5b8c\u6210\u540e\u4f1a\u5728\u8fd9\u91cc\u663e\u793a SL / TP \u53c2\u6570\u77e9\u9635\u3002",
                style="SetupNote.TLabel",
            ).grid(row=1, column=0, sticky="w", padx=4)
            return

        rows = [row for row in matrix_rows if isinstance(row, dict)]
        sl_values = sorted({float(row.get("sl_multiplier", 0.0)) for row in rows})
        tp_values = sorted({float(row.get("tp_multiplier", 0.0)) for row in rows})
        max_profit = max((float(row.get("net_pnl", 0.0)) for row in rows if float(row.get("net_pnl", 0.0)) > 0), default=0.0)
        max_loss = max((abs(float(row.get("net_pnl", 0.0))) for row in rows if float(row.get("net_pnl", 0.0)) < 0), default=0.0)
        is_heatmap = self.sltp_view_mode.get().strip() == "赢亏热力图"
        matrix_map = {
            (float(row.get("sl_multiplier", 0.0)), float(row.get("tp_multiplier", 0.0))): row
            for row in rows
        }

        for col in range(len(tp_values) + 1):
            self.sltp_matrix_box.columnconfigure(col, weight=1 if col > 0 else 0)

        Label(
            self.sltp_matrix_box,
            text="SL \\ TP",
            bg="#f3ede3",
            fg="#1f241f",
            relief="solid",
            bd=1,
            padx=12,
            pady=10,
            font=("Microsoft YaHei UI", 10, "bold"),
        ).grid(row=1, column=0, sticky="nsew", padx=4, pady=4)

        for col, tp_mult in enumerate(tp_values, start=1):
            Label(
                self.sltp_matrix_box,
                text=f"TP = SL x{self._format_matrix_multiplier(tp_mult)}",
                bg="#f3ede3",
                fg="#1f241f",
                relief="solid",
                bd=1,
                padx=12,
                pady=10,
                font=("Microsoft YaHei UI", 10, "bold"),
            ).grid(row=1, column=col, sticky="nsew", padx=4, pady=4)

        for row_idx, sl_mult in enumerate(sl_values, start=2):
            Label(
                self.sltp_matrix_box,
                text=f"SL x{self._format_matrix_multiplier(sl_mult)}",
                bg="#f3ede3",
                fg="#1f241f",
                relief="solid",
                bd=1,
                padx=12,
                pady=10,
                font=("Microsoft YaHei UI", 10, "bold"),
            ).grid(row=row_idx, column=0, sticky="nsew", padx=4, pady=4)

            for col_idx, tp_mult in enumerate(tp_values, start=1):
                cell = matrix_map.get((sl_mult, tp_mult), {})
                net_pnl = float(cell.get("net_pnl", 0.0))
                is_best = bool(cell.get("is_best"))
                matrix_key = str(cell.get("matrix_key") or self._build_matrix_key(sl_mult, tp_mult))
                is_selected = matrix_key == self.selected_sltp_key
                if is_heatmap:
                    display = f"{net_pnl:.4f}"
                    bg, fg = self._heatmap_cell_colors(net_pnl, max_profit, max_loss)
                    font = ("Consolas", 12, "bold")
                else:
                    display = str(cell.get("display_text") or f"{net_pnl:.4f} | 0.00% | 0\u7b14")
                    bg = "#cfe6d2" if is_selected else ("#e2f1e5" if is_best else "#f8f4ee")
                    fg = "#1b241d" if net_pnl >= 0 else "#3f241f"
                    font = ("Consolas", 10, "bold" if is_best else "normal")
                label = Label(
                    self.sltp_matrix_box,
                    text=display,
                    bg=bg,
                    fg=fg,
                    relief="solid",
                    bd=2 if is_selected else 1,
                    padx=12,
                    pady=14,
                    font=font,
                    cursor="hand2",
                    justify="center",
                )
                label.grid(row=row_idx, column=col_idx, sticky="nsew", padx=4, pady=4)
                label.bind("<Button-1>", lambda _event, key=matrix_key: self._select_sltp_matrix_cell(key))
                if is_selected:
                    label.configure(highlightbackground="#214f41", highlightcolor="#214f41", highlightthickness=1)

        note_text = (
            "\u70ed\u529b\u56fe\u6309\u603b\u76c8\u4e8f\u7740\u8272\uff1a\u6d45\u7eff\u5230\u6df1\u7eff\u8868\u793a\u76c8\u5229\u7531\u5c11\u5230\u591a\uff0c"
            "\u6d45\u7ea2\u5230\u6df1\u7ea2\u8868\u793a\u4e8f\u635f\u7531\u5c11\u5230\u591a\u3002\u5355\u5143\u683c\u4ecd\u7136\u53ef\u4ee5\u70b9\u51fb\u5207\u6362\u5bf9\u5e94\u56de\u6d4b\u3002"
            if is_heatmap
            else "\u5355\u5143\u683c\u663e\u793a\uff1a\u51c0\u6536\u76ca | \u80dc\u7387 | \u4ea4\u6613\u7b14\u6570\u3002\u6bcf\u4e2a\u7ed3\u679c\u90fd\u53ef\u4ee5\u70b9\u51fb\uff0c\u70b9\u4e2d\u54ea\u4e00\u683c\uff0c\u5c31\u5207\u6362\u5230\u54ea\u4e00\u7ec4\u56de\u64a4\u56fe\u548c K \u7ebf\u56fe\u3002"
        )
        ttk.Label(
            self.sltp_matrix_box,
            text=note_text,
            style="SetupNote.TLabel",
        ).grid(row=len(sl_values) + 2, column=0, columnspan=len(tp_values) + 1, sticky="w", padx=4, pady=(8, 2))

    @staticmethod
    def _format_matrix_multiplier(value: float) -> str:
        return str(int(value)) if float(value).is_integer() else str(value)

    @staticmethod
    def _hex_to_rgb(color: str) -> tuple[int, int, int]:
        raw = color.lstrip("#")
        return tuple(int(raw[index:index + 2], 16) for index in (0, 2, 4))

    @classmethod
    def _blend_hex_color(cls, start: str, end: str, ratio: float) -> str:
        clamped = _clamp(float(ratio), 0.0, 1.0)
        start_rgb = cls._hex_to_rgb(start)
        end_rgb = cls._hex_to_rgb(end)
        mixed = tuple(
            int(round(start_rgb[idx] + (end_rgb[idx] - start_rgb[idx]) * clamped))
            for idx in range(3)
        )
        return f"#{mixed[0]:02x}{mixed[1]:02x}{mixed[2]:02x}"

    @classmethod
    def _heatmap_cell_colors(cls, net_pnl: float, max_profit: float, max_loss: float) -> tuple[str, str]:
        if net_pnl > 0 and max_profit > 0:
            bg = cls._blend_hex_color("#e6f4ea", "#2f7d4a", net_pnl / max_profit)
        elif net_pnl < 0 and max_loss > 0:
            bg = cls._blend_hex_color("#fdeaea", "#cf4c4c", abs(net_pnl) / max_loss)
        else:
            bg = "#f6f0e7"

        red, green, blue = cls._hex_to_rgb(bg)
        luminance = 0.299 * red + 0.587 * green + 0.114 * blue
        fg = "#ffffff" if luminance < 150 else "#1f241f"
        return bg, fg

    def _select_sltp_matrix_cell(self, matrix_key: str, *, rerender: bool = True) -> None:
        payload = self.sltp_chart_payloads.get(matrix_key)
        if not payload:
            return

        bundle = self._build_chart_bundle_from_matrix_payload(payload)
        if bundle is None:
            return

        self.selected_sltp_key = matrix_key
        self._sync_live_strategy_form_selection(matrix_key)
        self._apply_live_strategy_form(matrix_key)
        self.latest_chart_bundle = bundle
        display_label = str(payload.get("display_label") or matrix_key)
        self.chart_status.set(f"\u5f53\u524d\u5df2\u5207\u6362\u5230 {display_label}\uff0c\u70b9\u51fb\u4e0a\u65b9\u6309\u94ae\u53ef\u67e5\u770b\u8fd9\u7ec4\u56de\u6d4b\u603b\u56fe\u3002")
        self.kline_status.set(f"{bundle.symbol} {bundle.period} \u5df2\u5207\u6362\u5230 {display_label}\uff0c\u4e0b\u62c9\u5230\u5e95\u90e8\u53ef\u67e5\u770b\u5bf9\u5e94K\u7ebf\u56fe\uff0c\u666e\u901a\u6eda\u8f6e\u53ef\u7ffb\u9875\uff0cCtrl+\u6eda\u8f6e\u7f29\u653e\uff0cShift+\u6eda\u8f6e\u6a2a\u79fb\uff0c\u5de6\u952e\u62d6\u52a8\u3002")
        self._update_backtest_metric_panel(matrix_key)
        self._sync_summary_tree_selection()
        self._schedule_bt_drawdown_preview()
        self._schedule_kline_preview()
        self._refresh_trade_detail_table(bundle, display_label=display_label)
        if rerender:
            self._render_sltp_matrix(self.sltp_matrix_rows)

        if self.backtest_chart_window is not None and self.backtest_chart_window.winfo_exists():
            self.backtest_chart_window.destroy()
            self.backtest_chart_window = BacktestChartWindow(self, bundle)
            self.backtest_chart_window.focus_force()

    def _build_chart_bundle_from_matrix_payload(self, payload: dict) -> BacktestChartBundle | None:
        signal_frame = payload.get("signal_frame")
        trades_df = payload.get("trades")
        equity_df = payload.get("equity")
        source_bundle = self.default_chart_bundle or self.latest_chart_bundle
        if not isinstance(signal_frame, pd.DataFrame):
            return None
        if not isinstance(trades_df, pd.DataFrame):
            trades_df = pd.DataFrame()
        if not isinstance(equity_df, pd.DataFrame):
            return None

        return BacktestChartBundle(
            symbol=source_bundle.symbol if source_bundle is not None else str(payload.get("symbol") or "-"),
            period=source_bundle.period if source_bundle is not None else str(payload.get("period") or "-"),
            fast_ema=source_bundle.fast_ema if source_bundle is not None else 21,
            slow_ema=source_bundle.slow_ema if source_bundle is not None else 55,
            signal_frame=signal_frame.copy(),
            trades_df=trades_df.copy(),
            equity_df=equity_df.copy(),
        )

    def _current_matrix_display_label(self) -> str:
        if not self.selected_sltp_key:
            return "默认组合"
        payload = self.sltp_chart_payloads.get(self.selected_sltp_key, {})
        return str(payload.get("display_label") or self.selected_sltp_key)

    def _clear_backtest_metric_panel(self) -> None:
        self.metric_combo.set("-")
        self.metric_net_pnl.set("-")
        self.metric_return.set("-")
        self.metric_win_rate.set("-")
        self.metric_trades.set("-")
        self.metric_max_drawdown.set("-")
        self.metric_cdar.set("-")
        self.metric_score.set("-")
        self.metric_rank.set("-")
        self.metric_custom_period.set("-")

    def _current_backtest_time_range_text(self) -> str:
        bundle = self.latest_chart_bundle or self.default_chart_bundle
        if bundle is None or bundle.signal_frame is None or bundle.signal_frame.empty:
            return "-"
        try:
            start_stamp = pd.Timestamp(bundle.signal_frame.index.min())
            end_stamp = pd.Timestamp(bundle.signal_frame.index.max())
        except Exception:
            return "-"

        if pd.isna(start_stamp) or pd.isna(end_stamp):
            return "-"

        if start_stamp.tzinfo is not None:
            start_stamp = start_stamp.tz_convert("Asia/Shanghai").tz_localize(None)
        if end_stamp.tzinfo is not None:
            end_stamp = end_stamp.tz_convert("Asia/Shanghai").tz_localize(None)
        return f"{start_stamp:%Y-%m-%d}  {end_stamp:%Y-%m-%d}"

    def _find_sltp_matrix_row(self, matrix_key: str | None) -> dict | None:
        if not matrix_key:
            return None
        for row in self.sltp_matrix_rows:
            if isinstance(row, dict) and str(row.get("matrix_key")) == matrix_key:
                return row
        return None

    def _update_backtest_metric_panel(self, matrix_key: str | None = None) -> None:
        row = self._find_sltp_matrix_row(matrix_key or self.selected_sltp_key)
        if row is None:
            fallback = next((item for item in self.sltp_matrix_rows if isinstance(item, dict) and bool(item.get("is_best"))), None)
            row = fallback if fallback is not None else (self.sltp_matrix_rows[0] if self.sltp_matrix_rows else None)

        if not isinstance(row, dict):
            self._clear_backtest_metric_panel()
            return

        combo_label = str(
            row.get("combo_label")
            or f"{row.get('sl_label', '-')} / {row.get('tp_label', '-')}"
        )
        self.metric_combo.set(combo_label)
        self.metric_net_pnl.set(f"{float(row.get('net_pnl', 0.0)):+,.2f}")
        self.metric_return.set(f"{float(row.get('net_return_pct', 0.0)):+.2f}%")
        self.metric_win_rate.set(f"{float(row.get('win_rate', 0.0)):.2f}%")
        self.metric_trades.set(str(int(row.get('trade_count', 0))))
        self.metric_max_drawdown.set(f"{float(row.get('max_drawdown_pct', 0.0)):.2f}%")
        self.metric_cdar.set(f"{float(row.get('cdar_95_pct', 0.0)):.2f}%")
        self.metric_score.set(f"{float(row.get('final_score', 0.0)):.2f}")
        self.metric_rank.set(str(row.get("rank") or "-"))
        self.metric_custom_period.set(self._current_backtest_time_range_text())

    def _render_summary_rankings(self, matrix_rows: list[dict], fallback_rows: list[dict] | None = None) -> None:
        self.summary_tree_key_map = {}
        for item in self.summary_tree.get_children():
            self.summary_tree.delete(item)

        ranked_rows = [row for row in matrix_rows if isinstance(row, dict)]
        if ranked_rows:
            ranked_rows = sorted(
                ranked_rows,
                key=lambda row: (
                    int(row.get("rank") or 9999),
                    -float(row.get("final_score", 0.0)),
                    -float(row.get("net_return_pct", 0.0)),
                ),
            )
            for index, row in enumerate(ranked_rows, start=1):
                matrix_key = str(row.get("matrix_key") or f"matrix_{index}")
                combo_label = str(
                    row.get("combo_label")
                    or f"{row.get('sl_label', '-')} / {row.get('tp_label', '-')}"
                )
                self.summary_tree.insert(
                    "",
                    "end",
                    iid=matrix_key,
                    values=(
                        combo_label,
                        row.get("trade_count"),
                        f"{float(row.get('net_pnl', 0.0)):+,.2f}",
                        f"{float(row.get('gross_profit', 0.0)):,.2f}",
                        f"{-float(row.get('gross_loss', 0.0)) if float(row.get('gross_loss', 0.0)) else 0.0:,.2f}",
                        f"{float(row.get('net_return_pct', 0.0)):.2f}",
                        f"{float(row.get('max_drawdown_pct', 0.0)):.2f}",
                        f"{float(row.get('cdar_95_pct', 0.0)):.2f}",
                        f"{float(row.get('final_score', 0.0)):.2f}",
                        row.get("rank"),
                    ),
                )
                self.summary_tree_key_map[matrix_key] = matrix_key
            self._sync_summary_tree_selection()
            return

        for index, row in enumerate(fallback_rows or [], start=1):
            item_id = f"summary_{index}"
            self.summary_tree.insert(
                "",
                "end",
                iid=item_id,
                values=(
                    row.get("period"),
                    row.get("trade_count"),
                    f"{float(row.get('net_pnl', 0.0)):+,.2f}",
                    f"{float(row.get('gross_profit', 0.0)):,.2f}",
                    f"{-float(row.get('gross_loss', 0.0)) if float(row.get('gross_loss', 0.0)) else 0.0:,.2f}",
                    f"{float(row.get('net_return_pct', 0.0)):.2f}",
                    f"{float(row.get('max_drawdown_pct', 0.0)):.2f}",
                    f"{float(row.get('cdar_95_pct', 0.0)):.2f}",
                    f"{float(row.get('final_score', 0.0)):.2f}",
                    row.get("rank"),
                ),
            )

    def _sync_summary_tree_selection(self) -> None:
        if not self.selected_sltp_key:
            self.summary_tree.selection_remove(self.summary_tree.selection())
            return
        item_id = self.summary_tree_key_map.get(self.selected_sltp_key)
        if not item_id:
            return
        self.summary_tree.selection_set(item_id)
        self.summary_tree.focus(item_id)
        self.summary_tree.see(item_id)

    def _on_summary_tree_select(self, _event=None) -> None:
        selection = self.summary_tree.selection()
        if not selection:
            return
        matrix_key = selection[0]
        if matrix_key == self.selected_sltp_key:
            return
        if matrix_key in self.sltp_chart_payloads:
            self._select_sltp_matrix_cell(matrix_key)

    def _build_matrix_key(self, sl_multiplier: float, tp_multiplier: float) -> str:
        return f"{self._format_matrix_multiplier(float(sl_multiplier))}|{self._format_matrix_multiplier(float(tp_multiplier))}"

    def _resolve_default_sltp_key(self, matrix_rows: list[dict]) -> str | None:
        rows = [row for row in matrix_rows if isinstance(row, dict)]
        if not rows:
            return None

        preferred_key = self._preferred_backtest_matrix_key()
        if preferred_key:
            for row in rows:
                matrix_key = str(
                    row.get("matrix_key")
                    or self._build_matrix_key(float(row.get("sl_multiplier", 0.0)), float(row.get("tp_multiplier", 0.0)))
                )
                if matrix_key == preferred_key:
                    return matrix_key

        for row in rows:
            if bool(row.get("is_best")):
                return str(row.get("matrix_key") or self._build_matrix_key(float(row.get("sl_multiplier", 0.0)), float(row.get("tp_multiplier", 0.0))))

        first = rows[0]
        return str(first.get("matrix_key") or self._build_matrix_key(float(first.get("sl_multiplier", 0.0)), float(first.get("tp_multiplier", 0.0))))

    def _preferred_backtest_matrix_key(self) -> str | None:
        try:
            stop_mult = float(self.bt_stop_atr.get().strip() or "1")
            take_mult = float(self.bt_take_atr.get().strip() or "2")
        except ValueError:
            return None
        return self._build_matrix_key(stop_mult, take_mult)

    def _reset_live_strategy_form_options(self) -> None:
        self.live_strategy_form_map = {}
        self.live_strategy_form.set(LIVE_STRATEGY_FORM_NONE)
        for combo in self.live_strategy_form_combos:
            if combo is not None and combo.winfo_exists():
                combo.configure(values=[LIVE_STRATEGY_FORM_NONE], state="readonly")

    def _refresh_live_strategy_form_options(self) -> None:
        rows = [row for row in self.sltp_matrix_rows if isinstance(row, dict)]
        if not rows:
            self._reset_live_strategy_form_options()
            return

        options: list[str] = [LIVE_STRATEGY_FORM_NONE]
        option_map: dict[str, str] = {}
        for row in rows:
            matrix_key = str(
                row.get("matrix_key")
                or self._build_matrix_key(float(row.get("sl_multiplier", 0.0)), float(row.get("tp_multiplier", 0.0)))
            )
            label = str(
                row.get("combo_label")
                or f"{row.get('sl_label', '-')} / {row.get('tp_label', '-')}"
            )
            options.append(label)
            option_map[label] = matrix_key

        self.live_strategy_form_map = option_map
        for combo in self.live_strategy_form_combos:
            if combo is not None and combo.winfo_exists():
                combo.configure(values=options, state="readonly")
        self._sync_live_strategy_form_selection()

    def _sync_live_strategy_form_selection(self, matrix_key: str | None = None) -> None:
        target_key = matrix_key or self.selected_sltp_key
        current_label = self.live_strategy_form.get().strip()
        if matrix_key is None and current_label == LIVE_STRATEGY_FORM_NONE:
            self.live_strategy_form.set(LIVE_STRATEGY_FORM_NONE)
            return
        if not target_key:
            if not self.live_strategy_form_map:
                self.live_strategy_form.set(LIVE_STRATEGY_FORM_NONE)
            return

        for label, key in self.live_strategy_form_map.items():
            if key == target_key:
                self.live_strategy_form.set(label)
                return
        self.live_strategy_form.set(LIVE_STRATEGY_FORM_NONE)

    def _apply_live_strategy_form(self, matrix_key: str | None) -> None:
        row = self._find_sltp_matrix_row(matrix_key)
        if not isinstance(row, dict):
            return

        self.live_stop_atr.set(self._format_matrix_multiplier(float(row.get("sl_multiplier", 0.0))))
        self.live_take_atr.set(self._format_matrix_multiplier(float(row.get("tp_multiplier", 0.0))))

    def _current_live_strategy_form_key(self) -> str | None:
        selected_label = self.live_strategy_form.get().strip()
        if not selected_label or selected_label == LIVE_STRATEGY_FORM_NONE:
            return None
        return self.live_strategy_form_map.get(selected_label)

    def _resolve_live_exit_multipliers(self) -> tuple[float | None, float | None]:
        matrix_key = self._current_live_strategy_form_key()
        row = self._find_sltp_matrix_row(matrix_key)
        if isinstance(row, dict):
            try:
                stop_mult = float(row.get("sl_multiplier", 0.0) or 0.0)
            except (TypeError, ValueError):
                stop_mult = 0.0
            try:
                take_mult = float(row.get("tp_multiplier", 0.0) or 0.0)
            except (TypeError, ValueError):
                take_mult = 0.0
            if stop_mult > 0.0:
                return stop_mult, take_mult if take_mult > 0.0 else None

        manual_stop = self._optional_float(self.live_stop_atr.get())
        manual_take = self._optional_float(self.live_take_atr.get())
        if manual_stop is None or manual_stop <= 0.0:
            manual_stop = 1.0
            if (self.live_stop_atr.get().strip() or "") != "1":
                self.live_stop_atr.set("1")
        if manual_take is not None and manual_take <= 0.0:
            manual_take = None
        return manual_stop, manual_take

    def _on_live_strategy_form_select(self, _event=None) -> None:
        selected_label = self.live_strategy_form.get().strip()
        if selected_label == LIVE_STRATEGY_FORM_NONE:
            return
        matrix_key = self.live_strategy_form_map.get(selected_label)
        if not matrix_key:
            return

        self._apply_live_strategy_form(matrix_key)
        if matrix_key in self.sltp_chart_payloads:
            self._select_sltp_matrix_cell(matrix_key)

    def _add_check(self, parent: ttk.Frame, row: int, column: int, label: str, variable: BooleanVar) -> None:
        cell = ttk.Frame(parent, style="Card.TFrame")
        cell.grid(row=row, column=column, sticky="ew", padx=6, pady=6)
        ttk.Label(cell, text="\u6267\u884c\u9009\u9879", style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(cell, text=label, variable=variable).grid(row=1, column=0, sticky="w", pady=(6, 0))

    def _add_value(self, parent: ttk.Frame, row: int, column: int, label: str, variable: StringVar) -> None:
        cell = ttk.Frame(parent, style="Card.TFrame")
        cell.grid(row=row, column=column, sticky="ew", padx=6, pady=6)
        ttk.Label(cell, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(cell, textvariable=variable, style="Value.TLabel").grid(row=1, column=0, sticky="w", pady=(4, 0))

    def _schedule_bt_drawdown_preview(self, _event=None) -> None:
        if self.bt_drawdown_preview_canvas is None:
            return
        if self._bt_drawdown_preview_job is not None:
            self.after_cancel(self._bt_drawdown_preview_job)
        self._bt_drawdown_preview_job = self.after(30, self._draw_bt_drawdown_preview)

    def _draw_bt_drawdown_preview(self) -> None:
        self._bt_drawdown_preview_job = None
        canvas = self.bt_drawdown_preview_canvas
        bundle = self.latest_chart_bundle
        if canvas is None:
            return

        canvas.delete("all")
        width = max(canvas.winfo_width(), 520)
        height = max(canvas.winfo_height(), 220)
        bg = "#10161d"
        grid = "#24313d"
        line = "#ff8d6a"
        fill = "#3b211e"
        text = "#d8e1ea"
        muted = "#87a0b3"

        canvas.create_rectangle(0, 0, width, height, fill=bg, outline="")
        if bundle is None or bundle.signal_frame.empty or bundle.equity_df.empty or "equity" not in bundle.equity_df.columns:
            canvas.create_text(
                width / 2,
                height / 2 - 8,
                text="\u56de\u6d4b\u5b8c\u6210\u540e\uff0c\u8fd9\u91cc\u4f1a\u663e\u793a\u7f29\u5c0f\u7248\u56de\u64a4\u6982\u89c8\u3002",
                fill=text,
                font=("Microsoft YaHei UI", 12, "bold"),
            )
            canvas.create_text(
                width / 2,
                height / 2 + 16,
                text="\u53f3\u4fa7\u7a7a\u767d\u4f1a\u88ab\u8fd9\u5757\u5c0f\u56fe\u8865\u4e0a\u3002",
                fill=muted,
                font=("Microsoft YaHei UI", 9),
            )
            return

        signal_frame = bundle.signal_frame.sort_index()
        equity = bundle.equity_df.sort_index()["equity"].astype(float).reindex(signal_frame.index).ffill().bfill()
        if equity.empty:
            canvas.create_text(width / 2, height / 2, text="\u6ca1\u6709\u53ef\u663e\u793a\u7684\u56de\u64a4\u6570\u636e\u3002", fill=text, font=("Microsoft YaHei UI", 12, "bold"))
            return

        drawdown = ((equity / equity.cummax()) - 1.0) * 100.0
        drawdown_amount = equity - equity.cummax()
        visible = min(len(drawdown), 180)
        drawdown = drawdown.tail(visible)
        drawdown_amount = drawdown_amount.tail(visible)
        equity = equity.tail(visible)

        margin_left = 52
        margin_right = 18
        margin_top = 28
        margin_bottom = 32
        plot_width = max(width - margin_left - margin_right, 1)
        plot_height = max(height - margin_top - margin_bottom, 1)
        dd_min = min(float(drawdown.min()), -0.01)
        dd_max = 0.0

        def x_at(index: int) -> float:
            return margin_left + index * plot_width / max(len(drawdown) - 1, 1)

        def y_at(value: float) -> float:
            return margin_top + (dd_max - value) / (dd_max - dd_min) * plot_height

        selected_label = self._current_matrix_display_label()
        canvas.create_text(
            margin_left,
            14,
            text=f"{bundle.symbol} {bundle.period} 回撤概览",
            anchor="w",
            fill=text,
            font=("Microsoft YaHei UI", 10, "bold"),
        )
        canvas.create_text(
            width - margin_right,
            14,
            text=selected_label,
            anchor="e",
            fill=muted,
            font=("Microsoft YaHei UI", 8),
        )

        for step in range(4):
            y = margin_top + plot_height * step / 3
            value = dd_max - (dd_max - dd_min) * step / 3
            canvas.create_line(margin_left, y, width - margin_right, y, fill=grid, dash=(2, 4))
            canvas.create_text(margin_left - 10, y, text=f"{value:.2f}%", anchor="e", fill=muted, font=("Consolas", 8))

        zero_y = y_at(0.0)
        canvas.create_line(margin_left, zero_y, width - margin_right, zero_y, fill="#6d7e8c", width=1)

        area_points: list[float] = [margin_left, zero_y]
        line_points: list[float] = []
        for idx, value in enumerate(drawdown):
            x = x_at(idx)
            y = y_at(float(value))
            area_points.extend((x, y))
            line_points.extend((x, y))
        area_points.extend((x_at(len(drawdown) - 1), zero_y))
        canvas.create_polygon(*area_points, fill=fill, outline="")
        if len(line_points) >= 4:
            canvas.create_line(*line_points, fill=line, width=2.2)

        latest_pnl = float(equity.iloc[-1] - equity.iloc[0])
        latest_dd = float(drawdown.iloc[-1])
        max_dd = float(drawdown.min())
        latest_dd_amount = float(drawdown_amount.iloc[-1])
        max_dd_amount = float(drawdown_amount.min())
        info_x = width - margin_right
        canvas.create_text(
            info_x,
            14,
            text=f"盈亏 {latest_pnl:+,.2f}",
            anchor="e",
            fill="#4ee4a5" if latest_pnl >= 0 else "#ff8d83",
            font=("Consolas", 10, "bold"),
        )
        canvas.create_text(
            info_x,
            30,
            text=f"当前 {latest_dd_amount:+,.2f} / {latest_dd:.2f}%",
            anchor="e",
            fill=muted,
            font=("Consolas", 8),
        )
        canvas.create_text(
            info_x,
            44,
            text=f"最大回撤 {max_dd_amount:+,.2f} / {max_dd:.2f}%",
            anchor="e",
            fill="#ff8d6a",
            font=("Consolas", 8),
        )

    def _reset_kline_preview_view(self) -> None:
        self._kline_preview_visible_bars = 220
        self._kline_preview_start_index = 0
        self._kline_preview_follow_latest = True
        self._kline_preview_pan_active = False
        self._kline_preview_pan_anchor_x = 0.0
        self._kline_preview_pan_origin_start = 0
        if self.kline_preview_canvas is not None:
            self.kline_preview_canvas.configure(cursor="")

    def _kline_preview_anchor_x(self) -> float:
        canvas = self.kline_preview_canvas
        if canvas is None:
            return 560.0
        return max(canvas.winfo_width(), 1080) / 2

    def _nudge_kline_preview(self, direction: int) -> None:
        bundle = self.latest_chart_bundle
        if bundle is None or bundle.signal_frame.empty:
            return

        total_bars = len(bundle.signal_frame)
        start, _end, visible = self._resolve_kline_preview_window(total_bars)
        step = max(3, visible // 6)
        max_start = max(total_bars - visible, 0)
        self._kline_preview_follow_latest = False
        self._kline_preview_start_index = int(_clamp(float(start + direction * step), 0.0, float(max_start)))
        self._schedule_kline_preview()

    def _show_latest_kline_preview(self) -> None:
        bundle = self.latest_chart_bundle
        if bundle is not None and not bundle.signal_frame.empty:
            total_bars = len(bundle.signal_frame)
            min_visible = total_bars if total_bars <= 24 else 24
            visible = int(_clamp(float(self._kline_preview_visible_bars), float(min_visible), float(total_bars)))
            self._kline_preview_start_index = max(total_bars - visible, 0)
        self._kline_preview_follow_latest = True
        self._schedule_kline_preview()

    def _resolve_kline_preview_window(self, total_bars: int) -> tuple[int, int, int]:
        if total_bars <= 0:
            return 0, 0, 0

        min_visible = total_bars if total_bars <= 24 else 24
        visible = int(_clamp(float(self._kline_preview_visible_bars), float(min_visible), float(total_bars)))
        max_start = max(total_bars - visible, 0)
        if self._kline_preview_follow_latest:
            start = max_start
        else:
            start = int(_clamp(float(self._kline_preview_start_index), 0.0, float(max_start)))
        end = start + visible
        self._kline_preview_visible_bars = visible
        self._kline_preview_start_index = start
        return start, end, visible

    def _zoom_kline_preview(self, delta: int, anchor_x: float) -> str:
        bundle = self.latest_chart_bundle
        canvas = self.kline_preview_canvas
        if canvas is None or bundle is None or bundle.signal_frame.empty:
            return "break"

        total_bars = len(bundle.signal_frame)
        start, end, visible = self._resolve_kline_preview_window(total_bars)
        min_visible = total_bars if total_bars <= 24 else 24
        scale = 0.84 if delta > 0 else 1.18
        target_visible = int(round(visible * scale))
        target_visible = int(_clamp(float(target_visible), float(min_visible), float(total_bars)))
        if target_visible == visible:
            return "break"

        width = max(canvas.winfo_width(), 1080)
        margin_left = 44
        margin_right = 72
        plot_width = max(width - margin_left - margin_right, 1)
        relative = _clamp((anchor_x - margin_left) / plot_width, 0.0, 1.0)
        anchor_index = start + relative * max(visible - 1, 1)
        max_start = max(total_bars - target_visible, 0)
        new_start = int(round(anchor_index - relative * max(target_visible - 1, 1)))
        new_start = int(_clamp(float(new_start), 0.0, float(max_start)))

        self._kline_preview_visible_bars = target_visible
        self._kline_preview_start_index = new_start
        self._kline_preview_follow_latest = end == total_bars and relative >= 0.92
        if self._kline_preview_follow_latest:
            self._kline_preview_start_index = max_start
        self._schedule_kline_preview()
        return "break"

    def _on_kline_preview_mousewheel(self, event) -> str:
        return self._zoom_kline_preview(event.delta, float(event.x))

    def _on_kline_preview_shift_mousewheel(self, event) -> str:
        bundle = self.latest_chart_bundle
        if bundle is None or bundle.signal_frame.empty:
            return "break"

        total_bars = len(bundle.signal_frame)
        start, _end, visible = self._resolve_kline_preview_window(total_bars)
        step = max(3, visible // 8)
        direction = -step if event.delta > 0 else step
        max_start = max(total_bars - visible, 0)
        new_start = int(_clamp(float(start + direction), 0.0, float(max_start)))
        if new_start == start:
            return "break"
        self._kline_preview_follow_latest = False
        self._kline_preview_start_index = new_start
        self._schedule_kline_preview()
        return "break"

    def _on_kline_preview_pan_start(self, event) -> str:
        bundle = self.latest_chart_bundle
        if bundle is None or bundle.signal_frame.empty or self.kline_preview_canvas is None:
            return "break"

        start, _end, _visible = self._resolve_kline_preview_window(len(bundle.signal_frame))
        self._kline_preview_pan_active = True
        self._kline_preview_pan_anchor_x = float(event.x)
        self._kline_preview_pan_origin_start = start
        self.kline_preview_canvas.configure(cursor="fleur")
        return "break"

    def _on_kline_preview_pan_drag(self, event) -> str:
        bundle = self.latest_chart_bundle
        canvas = self.kline_preview_canvas
        if not self._kline_preview_pan_active or canvas is None or bundle is None or bundle.signal_frame.empty:
            return "break"

        total_bars = len(bundle.signal_frame)
        _start, _end, visible = self._resolve_kline_preview_window(total_bars)
        width = max(canvas.winfo_width(), 1080)
        margin_left = 44
        margin_right = 72
        plot_width = max(width - margin_left - margin_right, 1)
        bars_per_pixel = max(visible - 1, 1) / plot_width
        delta_pixels = float(event.x) - self._kline_preview_pan_anchor_x
        max_start = max(total_bars - visible, 0)
        new_start = self._kline_preview_pan_origin_start - int(round(delta_pixels * bars_per_pixel))
        new_start = int(_clamp(float(new_start), 0.0, float(max_start)))
        if new_start == self._kline_preview_start_index:
            return "break"
        self._kline_preview_follow_latest = False
        self._kline_preview_start_index = new_start
        self._schedule_kline_preview()
        return "break"

    def _on_kline_preview_pan_end(self, _event=None) -> str:
        self._kline_preview_pan_active = False
        if self.kline_preview_canvas is not None:
            self.kline_preview_canvas.configure(cursor="")
        return "break"

    def _schedule_kline_preview(self, _event=None) -> None:
        if self.kline_preview_canvas is None:
            return
        if self._kline_preview_job is not None or self._kline_preview_pending:
            self._kline_preview_pending = True
            return
        self._kline_preview_pending = True
        self._kline_preview_job = self.after(12, self._draw_kline_preview)

    def _draw_kline_preview(self) -> None:
        self._kline_preview_job = None
        self._kline_preview_pending = False
        canvas = self.kline_preview_canvas
        bundle = self.latest_chart_bundle
        if canvas is None:
            return

        canvas.delete("all")
        width = max(canvas.winfo_width(), 1080)
        height = max(canvas.winfo_height(), 430)
        background = "#050505"
        grid = "#232323"
        border = "#111111"
        up_color = "#00c176"
        down_color = "#d84f7b"
        fast_color = "#f7c531"
        slow_color = "#67c97a"
        volume_up = "#0a6f48"
        volume_down = "#6a2140"
        current_line = "#00b36b"

        canvas.create_rectangle(0, 0, width, height, fill=background, outline="")
        if bundle is None or bundle.signal_frame.empty:
            canvas.create_text(
                width / 2,
                height / 2 - 12,
                text="\u56de\u6d4b\u5b8c\u6210\u540e\uff0cK\u7ebf\u56fe\u4f1a\u5728\u8fd9\u91cc\u81ea\u52a8\u751f\u6210\u3002",
                fill="#d8e1ea",
                font=("Microsoft YaHei UI", 14, "bold"),
            )
            canvas.create_text(
                width / 2,
                height / 2 + 18,
                text="\u5f53\u524d\u9875\u9762\u5df2\u7ecf\u652f\u6301\u4e0a\u4e0b\u6eda\u52a8\uff0c\u8dd1\u5b8c\u56de\u6d4b\u540e\u76f4\u63a5\u4e0b\u62c9\u5230\u5e95\u90e8\u5c31\u80fd\u770b\u5230\u3002",
                fill="#87a0b3",
                font=("Microsoft YaHei UI", 10),
            )
            return

        signal_frame = bundle.signal_frame.sort_index().copy()
        view_start, view_end, visible_bars = self._resolve_kline_preview_window(len(signal_frame))
        df = signal_frame.iloc[view_start:view_end].copy()
        if df.empty:
            canvas.create_text(width / 2, height / 2, text="\u6ca1\u6709\u53ef\u663e\u793a\u7684 K \u7ebf\u6570\u636e\u3002", fill="#d8e1ea", font=("Microsoft YaHei UI", 14, "bold"))
            return

        margin_left = 44
        margin_right = 72
        margin_top = 46
        margin_bottom = 54
        price_top = margin_top
        price_bottom = int(height * 0.72)
        volume_top = price_bottom + 18
        volume_bottom = height - margin_bottom
        plot_width = max(width - margin_left - margin_right, 1)
        price_height = max(price_bottom - price_top, 1)
        volume_height = max(volume_bottom - volume_top, 1)
        bar_step = plot_width / max(len(df), 1)
        candle_width = max(3.0, min(12.0, bar_step * 0.64))

        price_min = float(df["low"].min())
        price_max = float(df["high"].max())
        if "ema_fast" in df.columns:
            price_min = min(price_min, float(df["ema_fast"].min()))
            price_max = max(price_max, float(df["ema_fast"].max()))
        if "ema_slow" in df.columns:
            price_min = min(price_min, float(df["ema_slow"].min()))
            price_max = max(price_max, float(df["ema_slow"].max()))
        price_pad = max((price_max - price_min) * 0.08, max(abs(price_max), 1.0) * 0.004)
        price_y_min = price_min - price_pad
        price_y_max = price_max + price_pad
        max_volume = max(float(df["volume"].max()), 1.0)

        def x_at(index: int) -> float:
            return margin_left + index * bar_step + bar_step / 2

        def y_price(value: float) -> float:
            if price_y_max == price_y_min:
                return (price_top + price_bottom) / 2
            return price_top + (price_y_max - value) / (price_y_max - price_y_min) * price_height

        def y_volume(value: float) -> float:
            return volume_bottom - value / max_volume * volume_height

        selected_label = self._current_matrix_display_label()
        canvas.create_text(
            margin_left,
            14,
            text=f"{bundle.symbol} {bundle.period} K线图",
            anchor="w",
            fill="#f5f5f5",
            font=("Microsoft YaHei UI", 14, "bold"),
        )
        canvas.create_text(
            width - margin_right,
            14,
            text=f"{selected_label} | EMA{bundle.fast_ema} / EMA{bundle.slow_ema}  买卖点已标注",
            anchor="e",
            fill="#a9b2bb",
            font=("Microsoft YaHei UI", 10),
        )
        canvas.create_text(
            width - margin_right,
            32,
            text="\u6eda\u8f6e\u7f29\u653e  |  \u5de6\u952e\u62d6\u52a8  |  Shift+\u6eda\u8f6e\u6a2a\u79fb",
            anchor="e",
            fill="#6d7b87",
            font=("Microsoft YaHei UI", 9),
        )

        canvas.create_rectangle(margin_left, price_top, width - margin_right, price_bottom, outline=border, width=1)
        canvas.create_rectangle(margin_left, volume_top, width - margin_right, volume_bottom, outline=border, width=1)

        for step in range(6):
            y = price_top + price_height * step / 5
            value = price_y_max - (price_y_max - price_y_min) * step / 5
            canvas.create_line(margin_left, y, width - margin_right, y, fill=grid, dash=(2, 4))
            canvas.create_text(width - margin_right + 10, y, text=f"{value:.2f}", anchor="w", fill="#8b8b8b", font=("Consolas", 9))

        label_positions = sorted({round(i * (len(df) - 1) / max(min(6, len(df)) - 1, 1)) for i in range(min(6, len(df)))})
        for pos in label_positions:
            x = x_at(pos)
            canvas.create_line(x, price_top, x, volume_bottom, fill=grid, dash=(2, 6))
            stamp = pd.Timestamp(df.index[pos])
            label = stamp.strftime("%m-%d\n%H:%M") if bundle.period.endswith("H") or bundle.period.endswith("m") else stamp.strftime("%m-%d")
            canvas.create_text(x, height - 26, text=label, fill="#8b8b8b", font=("Consolas", 9), justify="center")

        fast_points: list[float] = []
        slow_points: list[float] = []
        for idx, row in enumerate(df.itertuples()):
            x = x_at(idx)
            open_px = float(row.open)
            high_px = float(row.high)
            low_px = float(row.low)
            close_px = float(row.close)
            color = up_color if close_px >= open_px else down_color

            canvas.create_line(x, y_price(high_px), x, y_price(low_px), fill=color, width=1)

            body_top = y_price(max(open_px, close_px))
            body_bottom = y_price(min(open_px, close_px))
            if abs(body_bottom - body_top) < 1.2:
                canvas.create_line(x - candle_width / 2, body_top, x + candle_width / 2, body_top, fill=color, width=2)
            else:
                canvas.create_rectangle(
                    x - candle_width / 2,
                    body_top,
                    x + candle_width / 2,
                    body_bottom,
                    fill=color,
                    outline=color,
                    width=1,
                )

            canvas.create_rectangle(
                x - candle_width / 2,
                y_volume(float(row.volume)),
                x + candle_width / 2,
                volume_bottom,
                fill=volume_up if close_px >= open_px else volume_down,
                outline="",
            )

            if pd.notna(row.ema_fast):
                fast_points.extend((x, y_price(float(row.ema_fast))))
            if pd.notna(row.ema_slow):
                slow_points.extend((x, y_price(float(row.ema_slow))))

        if len(slow_points) >= 4:
            canvas.create_line(*slow_points, fill=slow_color, width=3.2, smooth=True, splinesteps=18)
        if len(fast_points) >= 4:
            canvas.create_line(*fast_points, fill=fast_color, width=3.4, smooth=True, splinesteps=18)

        last_close = float(df["close"].iloc[-1])
        last_close_y = y_price(last_close)
        canvas.create_line(margin_left, last_close_y, width - margin_right, last_close_y, fill=current_line, dash=(2, 4))
        canvas.create_text(
            width - margin_right + 10,
            last_close_y,
            text=f"{last_close:.2f}",
            anchor="w",
            fill="#1fd27f",
            font=("Consolas", 9, "bold"),
        )
        canvas.create_text(
            width - margin_right + 10,
            volume_top,
            text=f"VOL {max_volume:,.0f}",
            anchor="w",
            fill="#6d7b87",
            font=("Consolas", 9),
        )

        buy_count = 0
        sell_count = 0
        for label, kind, absolute_idx, price in self._get_kline_preview_trade_marks(bundle):
            if absolute_idx < view_start or absolute_idx >= view_end:
                continue
            local_idx = absolute_idx - view_start
            x = x_at(local_idx)
            y = y_price(price)
            if kind == "sell":
                sell_count += 1
                canvas.create_polygon(x, y - 14, x - 9, y - 28, x + 9, y - 28, fill="#ff6b93", outline="")
                canvas.create_text(x, y - 40, text=label, fill="#ff6b93", font=("Microsoft YaHei UI", 8, "bold"))
            else:
                buy_count += 1
                canvas.create_polygon(x, y + 14, x - 9, y + 28, x + 9, y + 28, fill="#00d885", outline="")
                canvas.create_text(x, y + 40, text=label, fill="#00d885", font=("Microsoft YaHei UI", 8, "bold"))

        canvas.create_text(
            margin_left,
            volume_top - 8,
            text=f"买点 {buy_count}   卖点 {sell_count}   当前显示 {view_start + 1}-{view_end} / {len(signal_frame)}",
            anchor="w",
            fill="#8b8b8b",
            font=("Microsoft YaHei UI", 9),
        )

    def _reset_live_kline_preview_view(self) -> None:
        self._live_kline_preview_visible_bars = 220
        self._live_kline_preview_start_index = 0
        self._live_kline_preview_follow_latest = True
        self._live_kline_preview_pan_active = False
        self._live_kline_preview_pan_anchor_x = 0.0
        self._live_kline_preview_pan_origin_start = 0
        if self.live_kline_preview_canvas is not None:
            self.live_kline_preview_canvas.configure(cursor="")

    def _live_kline_preview_anchor_x(self) -> float:
        canvas = self.live_kline_preview_canvas
        if canvas is None:
            return 560.0
        return max(canvas.winfo_width(), 1080) / 2

    def _nudge_live_kline_preview(self, direction: int) -> None:
        bundle = self.live_chart_bundle
        if bundle is None or bundle.signal_frame.empty:
            return

        total_bars = len(bundle.signal_frame)
        start, _end, visible = self._resolve_live_kline_preview_window(total_bars)
        step = max(3, visible // 6)
        max_start = max(total_bars - visible, 0)
        self._live_kline_preview_follow_latest = False
        self._live_kline_preview_start_index = int(_clamp(float(start + direction * step), 0.0, float(max_start)))
        self._schedule_live_kline_preview()

    def _show_latest_live_kline_preview(self) -> None:
        bundle = self.live_chart_bundle
        if bundle is not None and not bundle.signal_frame.empty:
            total_bars = len(bundle.signal_frame)
            min_visible = total_bars if total_bars <= 24 else 24
            visible = int(_clamp(float(self._live_kline_preview_visible_bars), float(min_visible), float(total_bars)))
            self._live_kline_preview_start_index = max(total_bars - visible, 0)
        self._live_kline_preview_follow_latest = True
        self._schedule_live_kline_preview()

    def _resolve_live_kline_preview_window(self, total_bars: int) -> tuple[int, int, int]:
        if total_bars <= 0:
            return 0, 0, 0

        min_visible = total_bars if total_bars <= 24 else 24
        visible = int(_clamp(float(self._live_kline_preview_visible_bars), float(min_visible), float(total_bars)))
        max_start = max(total_bars - visible, 0)
        if self._live_kline_preview_follow_latest:
            start = max_start
        else:
            start = int(_clamp(float(self._live_kline_preview_start_index), 0.0, float(max_start)))
        end = start + visible
        self._live_kline_preview_visible_bars = visible
        self._live_kline_preview_start_index = start
        return start, end, visible

    def _zoom_live_kline_preview(self, delta: int, anchor_x: float) -> str:
        bundle = self.live_chart_bundle
        canvas = self.live_kline_preview_canvas
        if canvas is None or bundle is None or bundle.signal_frame.empty:
            return "break"

        total_bars = len(bundle.signal_frame)
        start, end, visible = self._resolve_live_kline_preview_window(total_bars)
        min_visible = total_bars if total_bars <= 24 else 24
        scale = 0.84 if delta > 0 else 1.18
        target_visible = int(round(visible * scale))
        target_visible = int(_clamp(float(target_visible), float(min_visible), float(total_bars)))
        if target_visible == visible:
            return "break"

        width = max(canvas.winfo_width(), 1080)
        margin_left = 44
        margin_right = 72
        plot_width = max(width - margin_left - margin_right, 1)
        relative = _clamp((anchor_x - margin_left) / plot_width, 0.0, 1.0)
        anchor_index = start + relative * max(visible - 1, 1)
        max_start = max(total_bars - target_visible, 0)
        new_start = int(round(anchor_index - relative * max(target_visible - 1, 1)))
        new_start = int(_clamp(float(new_start), 0.0, float(max_start)))

        self._live_kline_preview_visible_bars = target_visible
        self._live_kline_preview_start_index = new_start
        self._live_kline_preview_follow_latest = end == total_bars and relative >= 0.92
        if self._live_kline_preview_follow_latest:
            self._live_kline_preview_start_index = max_start
        self._schedule_live_kline_preview()
        return "break"

    def _on_live_kline_preview_mousewheel(self, event) -> str:
        return self._zoom_live_kline_preview(event.delta, float(event.x))

    def _on_live_kline_preview_shift_mousewheel(self, event) -> str:
        bundle = self.live_chart_bundle
        if bundle is None or bundle.signal_frame.empty:
            return "break"

        total_bars = len(bundle.signal_frame)
        start, _end, visible = self._resolve_live_kline_preview_window(total_bars)
        step = max(3, visible // 8)
        direction = -step if event.delta > 0 else step
        max_start = max(total_bars - visible, 0)
        new_start = int(_clamp(float(start + direction), 0.0, float(max_start)))
        if new_start == start:
            return "break"
        self._live_kline_preview_follow_latest = False
        self._live_kline_preview_start_index = new_start
        self._schedule_live_kline_preview()
        return "break"

    def _on_live_kline_preview_pan_start(self, event) -> str:
        bundle = self.live_chart_bundle
        if bundle is None or bundle.signal_frame.empty or self.live_kline_preview_canvas is None:
            return "break"

        start, _end, _visible = self._resolve_live_kline_preview_window(len(bundle.signal_frame))
        self._live_kline_preview_pan_active = True
        self._live_kline_preview_pan_anchor_x = float(event.x)
        self._live_kline_preview_pan_origin_start = start
        self.live_kline_preview_canvas.configure(cursor="fleur")
        return "break"

    def _on_live_kline_preview_pan_drag(self, event) -> str:
        bundle = self.live_chart_bundle
        canvas = self.live_kline_preview_canvas
        if not self._live_kline_preview_pan_active or canvas is None or bundle is None or bundle.signal_frame.empty:
            return "break"

        total_bars = len(bundle.signal_frame)
        _start, _end, visible = self._resolve_live_kline_preview_window(total_bars)
        width = max(canvas.winfo_width(), 1080)
        margin_left = 44
        margin_right = 72
        plot_width = max(width - margin_left - margin_right, 1)
        bars_per_pixel = max(visible - 1, 1) / plot_width
        delta_pixels = float(event.x) - self._live_kline_preview_pan_anchor_x
        max_start = max(total_bars - visible, 0)
        new_start = self._live_kline_preview_pan_origin_start - int(round(delta_pixels * bars_per_pixel))
        new_start = int(_clamp(float(new_start), 0.0, float(max_start)))
        if new_start == self._live_kline_preview_start_index:
            return "break"
        self._live_kline_preview_follow_latest = False
        self._live_kline_preview_start_index = new_start
        self._schedule_live_kline_preview()
        return "break"

    def _on_live_kline_preview_pan_end(self, _event=None) -> str:
        self._live_kline_preview_pan_active = False
        if self.live_kline_preview_canvas is not None:
            self.live_kline_preview_canvas.configure(cursor="")
        return "break"

    def _schedule_live_kline_preview(self, _event=None) -> None:
        if self.live_kline_preview_canvas is None:
            return
        if self._live_kline_preview_job is not None or self._live_kline_preview_pending:
            self._live_kline_preview_pending = True
            return
        self._live_kline_preview_pending = True
        self._live_kline_preview_job = self.after(12, self._draw_live_kline_preview)

    def _draw_live_kline_preview(self) -> None:
        self._live_kline_preview_job = None
        self._live_kline_preview_pending = False
        canvas = self.live_kline_preview_canvas
        bundle = self.live_chart_bundle
        if canvas is None:
            return

        canvas.delete("all")
        width = max(canvas.winfo_width(), 1080)
        height = max(canvas.winfo_height(), 430)
        background = "#050505"
        grid = "#232323"
        border = "#111111"
        up_color = "#00c176"
        down_color = "#d84f7b"
        fast_color = "#f7c531"
        slow_color = "#67c97a"
        volume_up = "#0a6f48"
        volume_down = "#6a2140"
        current_line = "#00b36b"

        canvas.create_rectangle(0, 0, width, height, fill=background, outline="")
        if bundle is None or bundle.signal_frame.empty:
            self._draw_live_kline_preview_clone_hint(bundle)
            canvas.create_text(
                width / 2,
                height / 2 - 12,
                text="真实仓位检查后，真实仓位图会在这里自动生成。",
                fill="#d8e1ea",
                font=("Microsoft YaHei UI", 14, "bold"),
            )
            canvas.create_text(
                width / 2,
                height / 2 + 18,
                text="真实仓位页面支持上下滚动，检查一次后直接下拉到底部就能看到。",
                fill="#87a0b3",
                font=("Microsoft YaHei UI", 10),
            )
            return

        signal_frame = bundle.signal_frame.sort_index().copy()
        view_start, view_end, _visible_bars = self._resolve_live_kline_preview_window(len(signal_frame))
        df = signal_frame.iloc[view_start:view_end].copy()
        if df.empty:
            canvas.create_text(width / 2, height / 2, text="没有可显示的真实仓位K线数据。", fill="#d8e1ea", font=("Microsoft YaHei UI", 14, "bold"))
            return

        margin_left = 44
        margin_right = 72
        margin_top = 46
        margin_bottom = 54
        price_top = margin_top
        price_bottom = int(height * 0.72)
        volume_top = price_bottom + 18
        volume_bottom = height - margin_bottom
        plot_width = max(width - margin_left - margin_right, 1)
        price_height = max(price_bottom - price_top, 1)
        volume_height = max(volume_bottom - volume_top, 1)
        bar_step = plot_width / max(len(df), 1)
        candle_width = max(3.0, min(12.0, bar_step * 0.64))

        price_min = float(df["low"].min())
        price_max = float(df["high"].max())
        if "ema_fast" in df.columns:
            price_min = min(price_min, float(df["ema_fast"].min()))
            price_max = max(price_max, float(df["ema_fast"].max()))
        if "ema_slow" in df.columns:
            price_min = min(price_min, float(df["ema_slow"].min()))
            price_max = max(price_max, float(df["ema_slow"].max()))
        price_pad = max((price_max - price_min) * 0.08, max(abs(price_max), 1.0) * 0.004)
        price_y_min = price_min - price_pad
        price_y_max = price_max + price_pad
        max_volume = max(float(df["volume"].max()), 1.0)

        def x_at(index: int) -> float:
            return margin_left + index * bar_step + bar_step / 2

        def y_price(value: float) -> float:
            if price_y_max == price_y_min:
                return (price_top + price_bottom) / 2
            return price_top + (price_y_max - value) / (price_y_max - price_y_min) * price_height

        def y_volume(value: float) -> float:
            return volume_bottom - value / max_volume * volume_height

        canvas.create_text(
            margin_left,
            14,
            text=f"{bundle.symbol} {bundle.period} 真实仓位图",
            anchor="w",
            fill="#f5f5f5",
            font=("Microsoft YaHei UI", 14, "bold"),
        )
        canvas.create_text(
            width - margin_right,
            14,
            text=f"真实仓位 | EMA{bundle.fast_ema} / EMA{bundle.slow_ema}  买卖点已标注",
            anchor="e",
            fill="#a9b2bb",
            font=("Microsoft YaHei UI", 10),
        )
        canvas.create_text(
            width - margin_right,
            32,
            text="滚轮缩放  |  左键拖动  |  Shift+滚轮横移",
            anchor="e",
            fill="#6d7b87",
            font=("Microsoft YaHei UI", 9),
        )

        canvas.create_rectangle(margin_left, price_top, width - margin_right, price_bottom, outline=border, width=1)
        canvas.create_rectangle(margin_left, volume_top, width - margin_right, volume_bottom, outline=border, width=1)

        for step in range(6):
            y = price_top + price_height * step / 5
            value = price_y_max - (price_y_max - price_y_min) * step / 5
            canvas.create_line(margin_left, y, width - margin_right, y, fill=grid, dash=(2, 4))
            canvas.create_text(width - margin_right + 10, y, text=f"{value:.2f}", anchor="w", fill="#8b8b8b", font=("Consolas", 9))

        label_positions = sorted({round(i * (len(df) - 1) / max(min(6, len(df)) - 1, 1)) for i in range(min(6, len(df)))})
        for pos in label_positions:
            x = x_at(pos)
            canvas.create_line(x, price_top, x, volume_bottom, fill=grid, dash=(2, 6))
            stamp = pd.Timestamp(df.index[pos])
            label = stamp.strftime("%m-%d\n%H:%M") if bundle.period.endswith("H") or bundle.period.endswith("m") else stamp.strftime("%m-%d")
            canvas.create_text(x, height - 26, text=label, fill="#8b8b8b", font=("Consolas", 9), justify="center")

        fast_points: list[float] = []
        slow_points: list[float] = []
        for idx, row in enumerate(df.itertuples()):
            x = x_at(idx)
            open_px = float(row.open)
            high_px = float(row.high)
            low_px = float(row.low)
            close_px = float(row.close)
            color = up_color if close_px >= open_px else down_color

            canvas.create_line(x, y_price(high_px), x, y_price(low_px), fill=color, width=1)

            body_top = y_price(max(open_px, close_px))
            body_bottom = y_price(min(open_px, close_px))
            if abs(body_bottom - body_top) < 1.2:
                canvas.create_line(x - candle_width / 2, body_top, x + candle_width / 2, body_top, fill=color, width=2)
            else:
                canvas.create_rectangle(
                    x - candle_width / 2,
                    body_top,
                    x + candle_width / 2,
                    body_bottom,
                    fill=color,
                    outline=color,
                    width=1,
                )

            canvas.create_rectangle(
                x - candle_width / 2,
                y_volume(float(row.volume)),
                x + candle_width / 2,
                volume_bottom,
                fill=volume_up if close_px >= open_px else volume_down,
                outline="",
            )

            if pd.notna(row.ema_fast):
                fast_points.extend((x, y_price(float(row.ema_fast))))
            if pd.notna(row.ema_slow):
                slow_points.extend((x, y_price(float(row.ema_slow))))

        if len(slow_points) >= 4:
            canvas.create_line(*slow_points, fill=slow_color, width=3.2, smooth=True, splinesteps=18)
        if len(fast_points) >= 4:
            canvas.create_line(*fast_points, fill=fast_color, width=3.4, smooth=True, splinesteps=18)

        last_close = float(df["close"].iloc[-1])
        last_close_y = y_price(last_close)
        canvas.create_line(margin_left, last_close_y, width - margin_right, last_close_y, fill=current_line, dash=(2, 4))
        canvas.create_text(
            width - margin_right + 10,
            last_close_y,
            text=f"{last_close:.2f}",
            anchor="w",
            fill="#1fd27f",
            font=("Consolas", 9, "bold"),
        )
        canvas.create_text(
            width - margin_right + 10,
            volume_top,
            text=f"VOL {max_volume:,.0f}",
            anchor="w",
            fill="#6d7b87",
            font=("Consolas", 9),
        )

        buy_count = 0
        sell_count = 0
        for label, kind, absolute_idx, price in self._get_live_kline_preview_trade_marks(bundle):
            if absolute_idx < view_start or absolute_idx >= view_end:
                continue
            local_idx = absolute_idx - view_start
            x = x_at(local_idx)
            y = y_price(price)
            if kind == "sell":
                sell_count += 1
                canvas.create_polygon(x, y - 14, x - 9, y - 28, x + 9, y - 28, fill="#ff6b93", outline="")
                canvas.create_text(x, y - 40, text=label, fill="#ff6b93", font=("Microsoft YaHei UI", 8, "bold"))
            else:
                buy_count += 1
                canvas.create_polygon(x, y + 14, x - 9, y + 28, x + 9, y + 28, fill="#00d885", outline="")
                canvas.create_text(x, y + 40, text=label, fill="#00d885", font=("Microsoft YaHei UI", 8, "bold"))

        canvas.create_text(
            margin_left,
            volume_top - 8,
            text=f"真实买点 {buy_count}   真实卖点 {sell_count}   当前显示 {view_start + 1}-{view_end} / {len(signal_frame)}",
            anchor="w",
            fill="#8b8b8b",
            font=("Microsoft YaHei UI", 9),
        )
        self._draw_live_kline_preview_clone_hint(bundle, buy_count=buy_count, sell_count=sell_count, view_start=view_start, view_end=view_end, total_bars=len(signal_frame))

    def _draw_live_kline_preview_clone_hint(
        self,
        bundle: BacktestChartBundle | None,
        *,
        buy_count: int = 0,
        sell_count: int = 0,
        view_start: int = 0,
        view_end: int = 0,
        total_bars: int = 0,
    ) -> None:
        canvas = self.live_kline_preview_canvas2
        if canvas is None:
            return

        width = max(canvas.winfo_width(), 1080)
        height = max(canvas.winfo_height(), 430)
        canvas.delete("all")
        canvas.create_rectangle(0, 0, width, height, fill="#050505", outline="")

        if self.live_clone_mode.get().strip() == LIVE_STRATEGY_FORM_NONE:
            canvas.create_text(
                width / 2,
                height / 2,
                text="真实仓位图当前未启用。",
                fill="#d8e1ea",
                font=("Microsoft YaHei UI", 14, "bold"),
            )
            return

        if bundle is None or bundle.signal_frame.empty:
            canvas.create_text(
                width / 2,
                height / 2 - 12,
                text="真实仓位检查后，真实仓位图会在这里自动生成。",
                fill="#d8e1ea",
                font=("Microsoft YaHei UI", 14, "bold"),
            )
            canvas.create_text(
                width / 2,
                height / 2 + 18,
                text="真实仓位页面支持上下滚动，检查一次后直接下拉到底部就能看到。",
                fill="#87a0b3",
                font=("Microsoft YaHei UI", 10),
            )
            return

        summary = [
            f"{bundle.symbol} {bundle.period} 真实仓位图",
            f"当前与上方真实仓位图共享默认参数、API 配置与结果视图。",
            f"真实买点 {buy_count}   真实卖点 {sell_count}",
            f"当前显示 {view_start + 1}-{view_end} / {total_bars}",
            f"EMA{bundle.fast_ema} / EMA{bundle.slow_ema}   点右上“打开总图”可看完整总图。",
        ]
        y = height / 2 - 52
        for idx, line in enumerate(summary):
            canvas.create_text(
                width / 2,
                y + idx * 26,
                text=line,
                fill="#d8e1ea" if idx == 0 else "#87a0b3",
                font=("Microsoft YaHei UI", 14 if idx == 0 else 10, "bold" if idx == 0 else "normal"),
            )

    def _pick_csv(self) -> None:
        path = filedialog.askopenfilename(
            title="\u9009\u62e9\u672c\u5730 K \u7ebf CSV \u6587\u4ef6",
            filetypes=[("CSV", "*.csv"), ("\u5168\u90e8\u6587\u4ef6", "*.*")],
        )
        if path:
            self._bt_csv_auto_managed = False
            self.bt_csv.set(path)

    @classmethod
    def _history_bar_limit_for_period(cls, period: str) -> int | None:
        normalized = cls._normalize_backtest_period(period)
        return HISTORY_BAR_LIMITS.get(normalized)

    @classmethod
    def _normalize_history_bar_count(cls, value: str, *, period: str, default: int) -> int:
        try:
            parsed = int((value or "").strip() or str(default))
        except ValueError:
            parsed = default

        parsed = max(1, parsed)
        limit = cls._history_bar_limit_for_period(period)
        if limit is not None:
            parsed = min(parsed, limit)
        return parsed

    def _apply_history_bar_limit(self, variable: StringVar, *, period: str, default: int, label: str) -> int:
        normalized = self._normalize_history_bar_count(variable.get(), period=period, default=default)
        previous = variable.get().strip()
        variable.set(str(normalized))
        limit = self._history_bar_limit_for_period(period)
        if limit is not None and previous and previous != str(normalized):
            target_log = self._live_log if "扫描K线数" in label else self._log
            target_log(f"{label}已按 {limit} 根上限处理。")
        return normalized

    @classmethod
    def _resolve_backtest_fetch_bars(cls, period: str, visible_bars: int) -> int:
        normalized = cls._normalize_backtest_period(period)
        if normalized == "4H":
            return visible_bars * 4
        return visible_bars

    @staticmethod
    def _normalize_backtest_time_text(value: str, *, end_bound: bool) -> str | None:
        raw = TradingDesktopApp._sanitize_backtest_time_text(value)
        if not raw:
            return None
        try:
            stamp = TradingDesktopApp._parse_backtest_time_text(raw)
        except Exception as exc:
            raise ValueError(f"时间格式无效：{raw}") from exc

        parts = re.findall(r"\d+", raw)
        joined = "".join(parts)
        is_date_only = bool(
            re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}", raw)
            or re.fullmatch(r"\d{4}/\d{1,2}/\d{1,2}", raw)
            or (len(parts) == 3 and len(parts[0]) == 4)
            or (joined.isdigit() and len(joined) == 8)
        )
        if stamp.tzinfo is None:
            stamp = stamp.tz_localize("Asia/Shanghai")
        else:
            stamp = stamp.tz_convert("Asia/Shanghai")
        if is_date_only and end_bound:
            stamp = stamp + pd.Timedelta(days=1) - pd.Timedelta(milliseconds=1)
        return stamp.tz_convert("UTC").isoformat()

    @staticmethod
    def _parse_backtest_time_text(raw: str) -> pd.Timestamp:
        text = TradingDesktopApp._sanitize_backtest_time_text(raw)
        parts = re.findall(r"\d+", text)
        joined = "".join(parts)

        if joined.isdigit() and len(parts) == 1:
            compact_formats = {
                8: "%Y%m%d",
                10: "%Y%m%d%H",
                12: "%Y%m%d%H%M",
                14: "%Y%m%d%H%M%S",
            }
            fmt = compact_formats.get(len(joined))
            if fmt:
                return pd.Timestamp(datetime.strptime(joined, fmt))

        if len(parts) >= 3 and len(parts[0]) == 4:
            year = int(parts[0])
            month = int(parts[1])
            day = int(parts[2])
            hour = int(parts[3]) if len(parts) >= 4 else 0
            minute = int(parts[4]) if len(parts) >= 5 else 0
            second = int(parts[5]) if len(parts) >= 6 else 0
            return pd.Timestamp(datetime(year, month, day, hour, minute, second))

        return pd.Timestamp(text)

    @staticmethod
    def _sanitize_backtest_time_text(raw: str) -> str:
        text = (raw or "").strip().translate(BACKTEST_TIME_TRANSLATOR)
        return re.sub(r"\s+", " ", text).strip()

    def _resolve_backtest_time_range(self) -> tuple[str | None, str | None]:
        start_text = self._normalize_backtest_time_text(self.bt_start.get(), end_bound=False)
        end_text = self._normalize_backtest_time_text(self.bt_end.get(), end_bound=True)
        if start_text and end_text and pd.Timestamp(start_text) > pd.Timestamp(end_text):
            raise ValueError("自定义时间段无效：开始时间不能晚于结束时间。")
        return start_text, end_text

    def _start_backtest(self) -> None:
        selected_period = self._normalize_backtest_period(self.bt_periods.get())
        self._apply_history_bar_limit(self.bt_bars, period=selected_period, default=2000, label="回测历史K线")
        self.bt_status.set("\u56de\u6d4b\u4e2d\uff0c\u8bf7\u7a0d\u5019...")
        self.chart_status.set("\u6b63\u5728\u751f\u6210\u8f6f\u4ef6\u5185\u7684\u56de\u6d4b\u603b\u56fe\u548c\u9875\u9762\u5e95\u90e8\u7684K\u7ebf\u56fe...")
        self.kline_status.set("\u6b63\u5728\u751f\u6210K\u7ebf\u56fe\uff0c\u5b8c\u6210\u540e\u4f1a\u81ea\u52a8\u663e\u793a\u5728\u9875\u9762\u6700\u4e0b\u65b9\u3002")
        self.sltp_chart_payloads = {}
        self.selected_sltp_key = None
        self.sltp_matrix_rows = []
        self._reset_live_strategy_form_options()
        self._clear_backtest_metric_panel()
        self._clear_trade_detail_table()
        self._reset_kline_preview_view()
        self._schedule_bt_drawdown_preview()
        self._schedule_kline_preview()
        self._log("开始执行回测，正在整理参数并准备载入数据。")
        worker = threading.Thread(target=self._run_backtest_worker, daemon=True)
        worker.start()

    def _run_backtest_worker(self) -> None:
        try:
            selected_period = self._normalize_backtest_period(self.bt_periods.get())
            requested_bars = self._normalize_history_bar_count(self.bt_bars.get(), period=selected_period, default=2000)
            start_text, end_text = self._resolve_backtest_time_range()
            manual_csv = None if self._bt_csv_auto_managed else (self.bt_csv.get().strip() or None)
            request = BacktestRequest(
                symbol=self._normalize_swap_symbol(self.bt_symbol.get()),
                csv=manual_csv,
                history_bars=requested_bars,
                history_bars_1h=self._resolve_backtest_fetch_bars(selected_period, requested_bars),
                source_bar=self._resolve_backtest_source_bar(selected_period),
                start=start_text,
                end=end_text,
                initial_cash=float(self.bt_initial_cash.get() or "10000"),
                risk_amount=float(self.bt_risk.get() or "100"),
                fee_bps=float(self.bt_fee.get() or "2.8"),
                slippage_bps=float(self.bt_slippage.get() or "2"),
                fast_ema=int(self.bt_fast.get() or "21"),
                slow_ema=int(self.bt_slow.get() or "55"),
                hold_bars=0,
                max_allocation_pct=float(self.bt_max_alloc.get() or "0.95"),
                signal_side=self._normalize_signal_side(self.bt_signal_side.get()),
                stop_loss_atr_multiplier=float(self.bt_stop_atr.get() or "1"),
                take_profit_r_multiple=float(self.bt_take_atr.get() or "2"),
                periods=[selected_period],
                output_dir=str(Path("desktop_reports")),
            )
            self.event_queue.put(("backtest_trace", self._build_backtest_request_log_text(request)))
            result = run_backtest(request)
            self.event_queue.put(("backtest_ok", result))
        except Exception as exc:
            self.event_queue.put(("error", f"\u56de\u6d4b\u5931\u8d25\uff1a{exc}"))

    def _start_live_once(self) -> None:
        execute, simulate = self._live_run_mode_flags()
        if execute:
            confirmed = messagebox.askyesno(
                "\u786e\u8ba4\u4e0b\u5355",
                "\u4f60\u5f53\u524d\u9009\u62e9\u7684\u8fd0\u884c\u6a21\u5f0f\u4f1a\u53d1\u5355\u3002"
                + ("\u672c\u6b21\u5c06\u8d70 OKX \u6a21\u62df\u76d8\uff0c\u662f\u5426\u7ee7\u7eed\uff1f" if simulate else "\u672c\u6b21\u53ef\u80fd\u4f1a\u76f4\u63a5\u5411 OKX \u53d1\u771f\u5b9e\u8ba2\u5355\uff0c\u662f\u5426\u7ee7\u7eed\uff1f"),
            )
            if not confirmed:
                return

        if not self._ensure_live_credentials_ready():
            return
        self._apply_history_bar_limit(self.live_bars, period=self.live_period.get(), default=240, label="扫描K线数")
        request = self._collect_live_request()
        self.live_status.set("\u6b63\u5728\u68c0\u67e5\u5b9e\u65f6\u4fe1\u53f7...")
        self._live_log(self._build_live_request_log_text(request, headline="开始执行单次实时信号检查"))
        worker = threading.Thread(target=self._run_live_once_worker, args=(request,), daemon=True)
        worker.start()

    def _start_live_test_order(self) -> None:
        if self._live_test_order_running:
            self.live_status.set("测试开单正在提交中，请稍候。")
            return
        if not self._ensure_live_credentials_ready():
            return

        request = self._build_live_test_order_request()
        confirmed = messagebox.askyesno(
            "确认测试开单",
            "这会直接向 OKX 实盘提交一笔真实永续限价买单。\n"
            f"标的：{LIVE_TEST_ORDER_DISPLAY}\n"
            f"价格：{request.price:,.2f} USDT\n"
            f"数量：{request.base_quantity:g} BTC\n"
            f"杠杆：{request.leverage}x\n\n"
            "继续后会真实挂单，是否确认？",
        )
        if not confirmed:
            return

        self._live_test_order_running = True
        self._set_live_test_order_button_state("disabled")
        self.live_status.set("正在提交测试开单，请稍候...")
        self._live_log(self._build_live_test_order_log_text(request))
        worker = threading.Thread(target=self._run_live_test_order_worker, args=(request,), daemon=True)
        worker.start()

    def _build_live_test_order_request(self) -> LiveManualOrderRequest:
        return LiveManualOrderRequest(
            symbol=LIVE_TEST_ORDER_SYMBOL,
            side="buy",
            price=LIVE_TEST_ORDER_PRICE,
            base_quantity=LIVE_TEST_ORDER_BASE_QTY,
            leverage=self._normalize_live_leverage(self.live_leverage.get()),
            order_timeout=int(self.live_timeout.get() or "25"),
            simulate=False,
            api_key=self.live_api_key.get().strip() or None,
            api_secret=self.live_api_secret.get().strip() or None,
            api_passphrase=self.live_api_passphrase.get().strip() or None,
        )

    def _build_live_test_order_log_text(self, request: LiveManualOrderRequest) -> str:
        return "\n".join(
            [
                "开始提交实盘测试开单",
                f"账户={self._current_live_api_profile_label()} | 标的={LIVE_TEST_ORDER_DISPLAY}",
                f"方向=买入 | 类型=限价委托 | 价格={float(request.price):,.2f} USDT",
                f"数量={float(request.base_quantity):g} BTC | 杠杆={int(request.leverage)}x | 模式=真实挂单",
            ]
        )

    def _run_live_test_order_worker(self, request: LiveManualOrderRequest) -> None:
        try:
            result = submit_live_manual_limit_order(request)
            self.event_queue.put(("live_test_ok", result))
        except Exception as exc:
            self.event_queue.put(("error", f"测试开单失败：{exc}"))
        finally:
            self.event_queue.put(("live_test_done", None))

    def _set_live_test_order_button_state(self, state: str) -> None:
        if self.live_test_order_button is None or not self.live_test_order_button.winfo_exists():
            return
        try:
            self.live_test_order_button.configure(state=state)
        except Exception:
            return

    def _run_live_once_worker(self, request: LiveRequest) -> None:
        attempt = 0
        while True:
            try:
                result = run_live_check(request)
                self.event_queue.put(("live_ok", result))
                return
            except Exception as exc:
                attempt += 1
                if not self._looks_like_live_connection_error(exc):
                    self.event_queue.put(("error", f"\u5b9e\u65f6\u68c0\u67e5\u5931\u8d25\uff1a{exc}"))
                    return
                self.event_queue.put(
                    (
                        "live_retry",
                        {
                            "strategy_id": None,
                            "attempt": attempt,
                            "message": str(exc),
                        },
                    )
                )
                threading.Event().wait(min(8, max(2, int(self.live_poll.get().strip() or "3"))))

    def _start_live_loop(self) -> None:
        execute, simulate = self._live_run_mode_flags()
        if execute:
            confirmed = messagebox.askyesno(
                "\u786e\u8ba4\u5f00\u59cb\u8f6e\u8be2",
                "\u8f6e\u8be2\u6a21\u5f0f\u4f1a\u6301\u7eed\u68c0\u67e5\u4fe1\u53f7\u3002"
                + ("\u5f53\u524d\u4e3a OKX \u6a21\u62df\u76d8\u53d1\u5355\u6a21\u5f0f\uff0c\u6ee1\u8db3\u6761\u4ef6\u65f6\u4f1a\u81ea\u52a8\u4e0b\u6a21\u62df\u5355\uff0c\u662f\u5426\u7ee7\u7eed\uff1f" if simulate else "\u5f53\u524d\u4e3a\u771f\u5b9e\u4e0b\u5355\u6a21\u5f0f\uff0c\u6ee1\u8db3\u6761\u4ef6\u65f6\u4f1a\u81ea\u52a8\u53d1\u771f\u5b9e\u8ba2\u5355\uff0c\u662f\u5426\u7ee7\u7eed\uff1f"),
            )
            if not confirmed:
                return

        if not self._ensure_live_credentials_ready():
            return
        self._apply_history_bar_limit(self.live_bars, period=self.live_period.get(), default=240, label="扫描K线数")
        current_view_id = self._selected_live_strategy_id()
        had_running_before = bool(self.live_strategy_workers)
        request = self._collect_live_request()
        signature = self._build_live_strategy_signature(request)
        existing_id = self._find_running_live_strategy(signature)
        if existing_id:
            self.active_live_strategy_id = existing_id
            existing_slot = self._slot_from_strategy_id(existing_id)
            if existing_slot:
                self.live_view_slot.set(existing_slot)
            self._refresh_live_strategy_table(select_id=existing_id)
            self._apply_selected_live_strategy_view(strategy_id=existing_id)
            existing_name = self.live_strategy_records.get(existing_id, {}).get("strategy", "该策略")
            self.live_status.set(f"{existing_name} 已在轮询中，无需重复开始。")
            self._live_log(f"{existing_name} 已在运行，未重复创建同参数策略。")
            return

        strategy_id = self._find_available_live_strategy_id()
        if strategy_id is None:
            messagebox.showinfo("\u63d0\u793a", "真实仓位当前最多同时运行 5 个策略，请先停止一条再继续开启。")
            return

        self.active_live_strategy_id = strategy_id
        self.live_strategy_records[strategy_id] = self._build_live_strategy_record(strategy_id, request)

        stop_event = threading.Event()
        worker_thread = threading.Thread(
            target=self._run_live_loop_worker,
            args=(strategy_id, request, stop_event),
            daemon=True,
        )
        self.live_strategy_workers[strategy_id] = {
            "thread": worker_thread,
            "stop_event": stop_event,
            "request": request,
            "signature": signature,
        }
        preferred_select_id = strategy_id
        if had_running_before and current_view_id in self.live_strategy_records:
            preferred_select_id = current_view_id
            self.active_live_strategy_id = current_view_id
        self._refresh_live_strategy_table(select_id=preferred_select_id)

        strategy_name = self.live_strategy_records.get(strategy_id, {}).get("strategy", "该策略")
        strategy_slot = self._slot_from_strategy_id(strategy_id)
        if strategy_slot and not had_running_before:
            self.live_view_slot.set(strategy_slot)
        status_message = f"{strategy_name} 轮询中..."
        if had_running_before:
            status_message = f"{strategy_name} 已加入轮询，当前已运行 {len(self.live_strategy_workers)} 个策略。"
        self._live_log(self._build_live_request_log_text(request, headline=f"开始实时轮询：{strategy_name}"))
        if had_running_before and current_view_id in self.live_strategy_records:
            self._apply_selected_live_strategy_view(strategy_id=current_view_id)
        else:
            self._apply_selected_live_strategy_view(strategy_id=strategy_id)
        self.live_status.set(status_message)
        worker_thread.start()

    def _run_live_loop_worker(self, strategy_id: str, request: LiveRequest, stop_event: threading.Event) -> None:
        wait_seconds = max(int(request.poll_interval or 60), 1)
        retry_attempt = 0
        while not stop_event.is_set():
            try:
                result = run_live_check(request)
                retry_attempt = 0
                self.event_queue.put(("live_ok", {"strategy_id": strategy_id, "result": result}))
            except Exception as exc:
                if self._looks_like_live_connection_error(exc):
                    retry_attempt += 1
                    self.event_queue.put(
                        (
                            "live_retry",
                            {
                                "strategy_id": strategy_id,
                                "attempt": retry_attempt,
                                "message": str(exc),
                            },
                        )
                    )
                    if stop_event.wait(min(wait_seconds, 8)):
                        break
                    continue
                self.event_queue.put(
                    (
                        "live_loop_error",
                        {
                            "strategy_id": strategy_id,
                            "message": f"\u8f6e\u8be2\u5931\u8d25\uff1a{exc}",
                        },
                    )
                )
                break

            if stop_event.wait(wait_seconds):
                break

    def _stop_live_loop(self) -> None:
        strategy_ids = list(self.live_strategy_workers.keys())
        if not strategy_ids:
            self.live_status.set("当前没有正在轮询的策略。")
            self._live_log("当前没有可停止的实时轮询策略。")
            return

        strategy_names = [self.live_strategy_records.get(strategy_id, {}).get("strategy", strategy_id) for strategy_id in strategy_ids]
        for strategy_id in strategy_ids:
            worker = self.live_strategy_workers.pop(strategy_id, None)
            if isinstance(worker, dict):
                stop_event = worker.get("stop_event")
                if isinstance(stop_event, threading.Event):
                    stop_event.set()

        self.live_strategy_records.clear()
        self.live_strategy_snapshots.clear()
        self.active_live_strategy_id = None
        self._refresh_live_strategy_table()
        self.live_status.set("全部策略轮询已停止。")
        self._live_log(f"已停止全部实时轮询策略：{', '.join(strategy_names)}")
        self._apply_selected_live_strategy_view()

    def _collect_live_request(self) -> LiveRequest:
        execute, simulate = self._live_run_mode_flags()
        normalized_period = self._normalize_live_period(self.live_period.get())
        stop_mult, take_mult = self._resolve_live_exit_multipliers()
        return LiveRequest(
            symbol=self._normalize_swap_symbol(self.live_order_symbol.get()),
            signal_symbol=self._normalize_swap_symbol(self.live_symbol.get()),
            period=normalized_period,
            account_tag=self.live_api_profile.get().strip() or "default",
            signal_side=self._normalize_signal_side(self.live_signal_side.get()),
            leverage=self._normalize_live_leverage(self.live_leverage.get()),
            bars=self._normalize_history_bar_count(self.live_bars.get(), period=normalized_period, default=240),
            risk_amount=float(self.live_risk.get() or "100"),
            max_allocation_pct=float(self.live_max_alloc.get() or "0.95"),
            fast_ema=int(self.live_fast.get() or "21"),
            slow_ema=int(self.live_ema_large.get() or self.live_slow.get() or "55"),
            atr_period=int(self.live_atr_period.get() or "10"),
            hold_bars=0,
            stop_loss_atr_multiplier=stop_mult,
            take_profit_r_multiple=take_mult,
            poll_interval=int(self.live_poll.get() or "60"),
            order_timeout=int(self.live_timeout.get() or "25"),
            simulate=simulate,
            execute=execute,
            quote_balance=self._optional_float(self.live_quote.get()),
            base_balance=self._optional_float(self.live_base.get()),
            api_key=self.live_api_key.get().strip() or None,
            api_secret=self.live_api_secret.get().strip() or None,
            api_passphrase=self.live_api_passphrase.get().strip() or None,
        )

    def _open_backtest_chart(self) -> None:
        if self.latest_chart_bundle is None:
            messagebox.showinfo("\u63d0\u793a", "\u8fd8\u6ca1\u6709\u53ef\u6253\u5f00\u7684\u56de\u6d4b\u603b\u56fe\u3002")
            return

        if self.backtest_chart_window is not None and self.backtest_chart_window.winfo_exists():
            self.backtest_chart_window.destroy()

        self.backtest_chart_window = BacktestChartWindow(self, self.latest_chart_bundle, title_text="回测总图")
        self.backtest_chart_window.focus_force()

    def _open_live_chart(self) -> None:
        if self.live_chart_bundle is None:
            messagebox.showinfo("\u63d0\u793a", "\u8fd8\u6ca1\u6709\u53ef\u6253\u5f00\u7684真实仓位总图。请先检查一次或开始轮询。")
            return

        if self.live_chart_window is not None and self.live_chart_window.winfo_exists():
            self.live_chart_window.destroy()

        self.live_chart_window = BacktestChartWindow(self, self.live_chart_bundle, title_text="真实仓位总图")
        self.live_chart_window.focus_force()

    def _open_trade_chart(self) -> None:
        self._open_backtest_chart()

    def _open_drawdown_chart(self) -> None:
        self._open_backtest_chart()

    @staticmethod
    def _timestamped_message(message: str) -> str:
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        raw_lines = [str(line).rstrip() for line in str(message).splitlines()]
        lines = [line for line in raw_lines if line.strip()]
        if not lines:
            lines = ["-"]
        return "\n".join([f"[{stamp}] {lines[0]}"] + [f"           {line}" for line in lines[1:]])

    @staticmethod
    def _format_log_range(start: str | None, end: str | None) -> str:
        if start or end:
            return f"{start or '-'} 到 {end or '-'}"
        return "自动范围"

    def _format_live_run_mode_text(self, *, execute: bool, simulate: bool) -> str:
        if execute and simulate:
            return "模拟并下单"
        if execute:
            return "交易并下单"
        return "仅信号检查"

    def _build_backtest_request_log_text(self, request: BacktestRequest) -> str:
        periods_text = "/".join(request.periods or [request.source_bar or "-"])
        source_text = f"手动CSV：{Path(request.csv).name}" if request.csv else "本地缓存优先，不足自动补 OKX"
        return "\n".join(
            [
                "回测任务已创建",
                f"策略={self.bt_strategy.get().strip() or STRATEGY_OPTION_LABEL}",
                f"标的={self.bt_symbol.get().strip()} -> {request.symbol}",
                f"周期={periods_text} | 基准K线={request.source_bar} | 时间段={self._format_log_range(request.start, request.end)}",
                f"历史K线={request.history_bars} | 抓取基准K线={request.history_bars_1h} | 信号方向={self._translate_signal_side(request.signal_side)}",
                f"EMA={request.fast_ema}/{request.slow_ema} | 止损ATR={request.stop_loss_atr_multiplier} | 止盈ATR={request.take_profit_r_multiple}",
                f"初始资金={request.initial_cash:,.2f} | 风险金={request.risk_amount:,.2f} | 最大资金占比={request.max_allocation_pct:g}",
                f"手续费={request.fee_bps:g}bps | 滑点={request.slippage_bps:g}bps | 数据模式={source_text}",
            ]
        )

    def _build_backtest_result_log_text(self, result: dict) -> str:
        best_row = next((row for row in (result.get("sltp_matrix") or []) if isinstance(row, dict) and bool(row.get("is_best"))), None)
        combo_label = "-"
        trades = "-"
        win_rate = "-"
        net_pnl = "-"
        drawdown = "-"
        if isinstance(best_row, dict):
            combo_label = str(best_row.get("combo_label") or "-")
            trades = str(best_row.get("trade_count") or "-")
            win_rate = f"{float(best_row.get('win_rate', 0.0)):.2f}%"
            net_pnl = f"{float(best_row.get('net_pnl', 0.0)):+,.2f}"
            drawdown = f"{float(best_row.get('max_drawdown_pct', 0.0)):.2f}%"

        return "\n".join(
            [
                "回测完成",
                f"标的={result.get('symbol', '-')} | 最优周期={result.get('top_period', '-')} | 综合得分={float(result.get('top_score', 0.0)):.2f}",
                f"当前最佳组合={combo_label} | 交易笔数={trades} | 胜率={win_rate}",
                f"净收益={net_pnl} | 最大回撤={drawdown} | 自定义时间段={self._current_backtest_time_range_text()}",
            ]
        )

    def _build_live_request_log_text(self, request: LiveRequest, *, headline: str) -> str:
        return "\n".join(
            [
                headline,
                f"账户={self._live_api_profile_option_label(request.account_tag or 'API 1')} | 运行模式={self._format_live_run_mode_text(execute=request.execute, simulate=request.simulate)}",
                f"信号标的={self._display_live_strategy_symbol(request.signal_symbol or request.symbol)} | 下单标的={self._display_live_strategy_symbol(request.symbol)}",
                f"周期={request.period} | 信号方向={self._translate_signal_side(request.signal_side)} | 扫描K线数={request.bars}",
                f"EMA={request.fast_ema}/{request.slow_ema} | ATR={request.atr_period} | 止损ATR={request.stop_loss_atr_multiplier} | 止盈ATR={request.take_profit_r_multiple}",
                f"风险金={request.risk_amount:,.2f} | 最大资金占比={request.max_allocation_pct:g} | 杠杆={request.leverage}x | 轮询秒数={request.poll_interval}",
            ]
        )

    def _build_live_result_log_text(
        self,
        *,
        strategy_name: str,
        request_payload: dict,
        report: dict,
        account_label: str,
        translated_reason: str,
        fast_value: int,
        slow_value: int,
        win_rate: str,
        auto_fixed_size: float | None,
    ) -> str:
        execute = bool(request_payload.get("execute"))
        simulate = bool(request_payload.get("simulate"))
        execution = report.get("execution") or {}
        execution_status = str(execution.get("status") or "-") if isinstance(execution, dict) else "-"
        execution_id = str(execution.get("cl_ord_id") or "-") if isinstance(execution, dict) else "-"
        fixed_size_text = self._format_live_fixed_size(auto_fixed_size) if auto_fixed_size is not None else "-"
        symbol_text = self._display_live_strategy_symbol(str(request_payload.get("symbol") or request_payload.get("signal_symbol") or "-"))
        signal_symbol_text = self._display_live_strategy_symbol(str(request_payload.get("signal_symbol") or request_payload.get("symbol") or "-"))
        return "\n".join(
            [
                f"{strategy_name} 检查完成",
                f"账户={account_label} | 运行模式={self._format_live_run_mode_text(execute=execute, simulate=simulate)}",
                f"信号标的={signal_symbol_text} | 下单标的={symbol_text} | 周期={request_payload.get('period', '-')}",
                f"信号方向={self._translate_signal_side(str(request_payload.get('signal_side') or '-'))} | EMA={fast_value}/{slow_value} | ATR={request_payload.get('atr_period', '-')}",
                f"止损ATR={request_payload.get('stop_loss_atr_multiplier', '-')} | 止盈ATR={request_payload.get('take_profit_r_multiple', '-')}",
                f"动作={self._translate_action(str(report.get('action') or '-'))} | 原因={translated_reason} | 信号时间={report.get('signal_ts') or '-'}",
                f"最新收盘={float(report.get('latest_close', 0.0)):.4f} | 建议数量={report.get('suggested_size') or '-'} | 固定数量={fixed_size_text}",
                f"账户余额={float(report.get('total_assets', 0.0)):,.2f} | 今日盈亏={float(report.get('today_pnl', 0.0)):+,.2f} | 总盈亏={float(report.get('total_pnl', 0.0)):+,.2f} | 胜率={win_rate}",
                f"订单状态={execution_status} | 订单号={execution_id}",
            ]
        )

    def _clear_log(self) -> None:
        self._replace_log_text(self.log_text, "\u65e5\u5fd7\u5df2\u6e05\u7a7a\u3002\n")

    def _clear_live_log(self) -> None:
        self._replace_log_text(self.live_log_text, "实盘日志已清空。\n")

    def _drain_queue(self) -> None:
        while True:
            try:
                event, payload = self.event_queue.get_nowait()
            except queue.Empty:
                break

            if event == "backtest_ok":
                self._handle_backtest_result(payload)
            elif event == "backtest_trace":
                self._log(str(payload))
            elif event == "live_ok":
                strategy_id = None
                live_result = payload
                if isinstance(payload, dict) and "result" in payload:
                    strategy_id = str(payload.get("strategy_id") or "").strip() or None
                    live_result = payload.get("result")
                if isinstance(live_result, dict):
                    self._handle_live_result(live_result, strategy_id=strategy_id)
            elif event == "live_trace":
                self._live_log(str(payload))
            elif event == "live_test_ok":
                if isinstance(payload, dict):
                    self._handle_live_test_order_result(payload)
            elif event == "live_test_done":
                self._live_test_order_running = False
                self._set_live_test_order_button_state("normal")
            elif event == "live_loop_error":
                if isinstance(payload, dict):
                    strategy_id = str(payload.get("strategy_id") or "").strip()
                    message = str(payload.get("message") or "轮询失败。")
                else:
                    strategy_id = ""
                    message = str(payload)
                if strategy_id and strategy_id not in self.live_strategy_workers and strategy_id not in self.live_strategy_records:
                    continue
                row = self._stop_live_strategy_by_id(strategy_id) if strategy_id else {}
                strategy_name = row.get("strategy", "该策略")
                detail_message = message.removeprefix("轮询失败：").removeprefix("轮询失败")
                detail_message = detail_message.strip() or "未知错误"
                composed_message = f"{strategy_name} 轮询失败：{detail_message}"
                self.live_status.set(composed_message)
                self._live_log(composed_message)
                messagebox.showerror("\u6267\u884c\u5931\u8d25", composed_message)
            elif event == "live_retry":
                strategy_id = ""
                attempt = 1
                detail_message = "连接失败。"
                if isinstance(payload, dict):
                    strategy_id = str(payload.get("strategy_id") or "").strip()
                    attempt = max(int(payload.get("attempt") or 1), 1)
                    detail_message = str(payload.get("message") or detail_message).strip() or detail_message
                strategy_name = self.live_strategy_records.get(strategy_id, {}).get("strategy", "实时检查") if strategy_id else "实时检查"
                composed_message = f"{strategy_name} 连接 OKX 失败，正在第 {attempt} 次重试：{detail_message}"
                self.live_status.set(composed_message)
                self._live_log(composed_message)
            elif event == "auto_4h_cache_sync":
                notices = payload if isinstance(payload, list) else [str(payload)]
                for notice in notices:
                    if notice:
                        self._log(str(notice))
                self._auto_4h_cache_running = False
                self._schedule_auto_4h_cache_sync()
            elif event == "error":
                message = str(payload)
                if message.startswith("回测失败："):
                    self.bt_status.set(message)
                    self._log(message)
                elif message.startswith("实时检查失败："):
                    self.live_status.set(message)
                    self._live_log(message)
                elif message.startswith("测试开单失败："):
                    self.live_status.set(message)
                    self._live_log(message)
                else:
                    self.bt_status.set(message)
                    self.live_status.set(message)
                    self._log(message)
                    self._live_log(message)
                messagebox.showerror("\u6267\u884c\u5931\u8d25", message)

        self.after(150, self._drain_queue)

    def _handle_backtest_result(self, result: dict) -> None:
        if self.backtest_chart_window is not None and self.backtest_chart_window.winfo_exists():
            self.backtest_chart_window.destroy()
            self.backtest_chart_window = None

        self.latest_trade_chart_path = result.get("trade_chart_path")
        self.latest_drawdown_chart_path = result.get("drawdown_chart_path")
        self.default_chart_bundle = self._build_chart_bundle(result)
        self.latest_chart_bundle = self.default_chart_bundle
        chart_payloads = result.get("sltp_chart_payloads")
        self.sltp_chart_payloads = chart_payloads if isinstance(chart_payloads, dict) else {}

        period_cache_csvs = result.get("period_cache_csvs")
        saved_period_cache_csvs = period_cache_csvs if isinstance(period_cache_csvs, dict) else {}

        top_period = str(result.get("top_period") or "-")
        top_score = float(result.get("top_score", 0.0))
        self.bt_status.set(f"\u56de\u6d4b\u5b8c\u6210\uff1a\u6700\u4f18\u5468\u671f {top_period}\uff0c\u7efc\u5408\u5f97\u5206 {top_score:.2f}")
        self.chart_status.set("\u56fe\u8868\u5df2\u751f\u6210\uff0c\u53ef\u70b9\u51fb\u4e0a\u65b9\u6309\u94ae\u76f4\u63a5\u5728\u8f6f\u4ef6\u91cc\u67e5\u770b\u5408\u5e76\u540e\u7684\u56de\u6d4b\u603b\u56fe\u3002\u70b9\u51fb\u4e0b\u65b9 9 \u4e2a SL / TP \u5355\u5143\u683c\uff0c\u53ef\u4ee5\u5207\u6362\u5bf9\u5e94\u56de\u64a4\u56fe\u3002")
        self.kline_status.set(f"{result.get('symbol', '-')} {top_period} K\u7ebf\u56fe\u5df2\u751f\u6210\uff0c\u70b9\u51fb\u77e9\u9635\u5355\u5143\u683c\u53ef\u5207\u6362\u5bf9\u5e94\u7ec4\u5408\uff0c\u666e\u901a\u6eda\u8f6e\u53ef\u7ffb\u9875\uff0cCtrl+\u6eda\u8f6e\u7f29\u653e\uff0cShift+\u6eda\u8f6e\u6a2a\u79fb\uff0c\u5de6\u952e\u62d6\u52a8\u3002")
        self.top_period.set(top_period)
        self.top_score.set(f"{top_score:.2f}")
        matrix_rows = result.get("sltp_matrix", [])
        self.sltp_matrix_rows = [row for row in matrix_rows if isinstance(row, dict)]
        self.selected_sltp_key = self._resolve_default_sltp_key(matrix_rows)
        self._refresh_live_strategy_form_options()
        if self.selected_sltp_key and self.selected_sltp_key in self.sltp_chart_payloads:
            self._select_sltp_matrix_cell(self.selected_sltp_key, rerender=False)
        else:
            self._update_backtest_metric_panel()
            self._sync_live_strategy_form_selection()
            self._refresh_trade_detail_table(self.latest_chart_bundle)
        self._render_sltp_matrix(matrix_rows)
        self._render_summary_rankings(matrix_rows, result.get("summary", []))
        self._schedule_bt_drawdown_preview()
        self._schedule_kline_preview()

        self._log(self._build_backtest_result_log_text(result))
        self._log(f"K\u7ebf\u56fe\uff1a{self.latest_trade_chart_path}")
        self._log(f"\u56de\u64a4\u56fe\uff1a{self.latest_drawdown_chart_path}")
        period_sources = result.get("period_data_sources")
        if isinstance(period_sources, dict):
            for period, source in period_sources.items():
                self._log(f"{period} \u6570\u636e\u6765\u6e90\uff1a{source}")
        for period, path in saved_period_cache_csvs.items():
            self._log(f"\u5df2\u4fdd\u5b58 {period} K\u7ebf\u5230\u672c\u5730\uff1a{path}")
        if result.get("sltp_matrix"):
            self._log("\u5df2\u751f\u6210 SL / TP \u53c2\u6570\u77e9\u9635\u3002")
        if self.latest_chart_bundle is not None:
            self._log("\u5f53\u524d\u5df2\u652f\u6301\u5728\u8f6f\u4ef6\u5185\u76f4\u63a5\u6253\u5f00\u5408\u5e76\u540e\u7684\u56de\u6d4b\u603b\u56fe\u3002")

    def _handle_live_result(self, result: dict, *, strategy_id: str | None = None) -> None:
        if strategy_id and strategy_id not in self.live_strategy_workers:
            return

        report = result["report"]
        trades_df = result.get("trades")
        request_payload = result.get("request", {})
        strategy_name = self.live_strategy_records.get(strategy_id or "", {}).get("strategy", "实时检查")
        fast_value = int(request_payload.get("fast_ema") or report.get("ema_fast") or self.live_fast.get() or 21)
        slow_value = int(request_payload.get("slow_ema") or report.get("ema_slow") or self.live_ema_large.get() or self.live_slow.get() or 55)
        profile_name = str(request_payload.get("account_tag") or "").strip() or "API 1"
        account_label = str(report.get("account_name") or "").strip() or self._live_api_profile_option_label(profile_name)
        win_rate = self._format_live_win_rate(trades_df)
        bundle = self._build_live_chart_bundle(result)
        signal_frame = bundle.signal_frame if isinstance(bundle, BacktestChartBundle) else None
        auto_fixed_size = self._resolve_live_auto_fixed_size(
            request_payload=request_payload,
            signal_frame=signal_frame,
        )
        translated_reason = self._translate_reason(str(report.get("reason") or ""), fast=fast_value, slow=slow_value)

        if strategy_id and strategy_id in self.live_strategy_records:
            request_obj = self.live_strategy_workers.get(strategy_id, {}).get("request")
            if isinstance(request_obj, LiveRequest):
                self.live_strategy_records[strategy_id] = self._build_live_strategy_record(
                    strategy_id,
                    request_obj,
                    account_label=account_label,
                    total_pnl=report.get("total_pnl"),
                )
                self._refresh_live_strategy_table()
                self._store_live_strategy_snapshot(
                    strategy_id,
                    result=result,
                    bundle=bundle,
                    account_label=account_label,
                    win_rate=win_rate,
                    fast_value=fast_value,
                    slow_value=slow_value,
                )

        self._update_detected_live_api_profile_name(
            str(report.get("account_name") or ""),
            str(report.get("account_uid") or ""),
            profile_name=profile_name,
        )
        credential_mode = str(report.get("credential_mode") or "").strip().lower()
        if credential_mode == "simulated":
            self._live_log("当前这次真实仓位检查已自动按 OKX 模拟盘 API 方式完成。")

        self._live_log(
            self._build_live_result_log_text(
                strategy_name=strategy_name,
                request_payload=request_payload,
                report=report,
                account_label=account_label,
                translated_reason=translated_reason,
                fast_value=fast_value,
                slow_value=slow_value,
                win_rate=win_rate,
                auto_fixed_size=auto_fixed_size,
            )
        )

        if strategy_id is None:
            self.live_status.set(
                f"{strategy_name} 检查完成：动作 {self._translate_action(str(report['action']))}，原因 {translated_reason}"
            )
            self.live_action.set(self._translate_action(report["action"]))
            self.live_reason.set(translated_reason)
            self.live_price.set(f"{float(report['latest_close']):.4f}")
            self.live_size.set(str(report.get("suggested_size") or "-"))
            self.live_fixed_size.set(self._format_live_fixed_size(auto_fixed_size))
            self.live_account_balance.set(f"{float(report.get('total_assets', 0.0)):,.2f}")
            self.live_win_rate.set(win_rate)
            self.live_today_pnl.set(f"{float(report.get('today_pnl', 0.0)):+,.2f}")
            self.live_total_pnl.set(f"{float(report.get('total_pnl', 0.0)):+,.2f}")
            self.live_account_panel_name.set(account_label)
            self._refresh_live_action_account()
            self.live_chart_bundle = bundle
            if self.live_chart_bundle is not None:
                self.live_kline_status.set(
                    f"{self.live_chart_bundle.symbol} {self.live_chart_bundle.period} 真实仓位图已更新，买卖点和交易明细都按真实仓位记录显示。"
                )
                self._refresh_live_trade_detail_table(self.live_chart_bundle)
                self._schedule_live_kline_preview()
            else:
                self._clear_live_trade_detail_table()
                self.live_kline_status.set("当前真实仓位还没有可显示的K线数据。")
        elif strategy_id == self._selected_live_strategy_id():
            self._apply_selected_live_strategy_view(strategy_id=strategy_id)

        execution = report.get("execution")
        if execution:
            self._live_log(f"\u8ba2\u5355\u72b6\u6001\uff1a{execution.get('status')}\uff0c\u8ba2\u5355\u53f7\uff1a{execution.get('cl_ord_id')}")
            if isinstance(execution, dict):
                execution_response = execution.get("response")
                if isinstance(execution_response, dict):
                    leverage_notice = str(execution_response.get("leverage_notice") or "").strip()
                    if leverage_notice:
                        self._live_log(leverage_notice)
                    transport_notice = str(execution_response.get("transport_notice") or "").strip()
                    if transport_notice:
                        self._live_log(transport_notice)
            if execution.get("status") == "dry_run" and str(report.get("action") or "") in {"buy", "sell"}:
                self._live_log(
                    f"{strategy_name} 已达到开单条件，但当前运行模式是“仅信号检查”，所以这次不会向 OKX 发单。"
                    "如果要走 OKX 模拟盘，请切到“模拟并下单”；如果要真实发单，请切到“交易并下单”。"
                )
        elif str(report.get("action") or "") in {"buy", "sell"}:
            self._live_log(f"{strategy_name} 已出现交易信号，但本次没有生成有效订单结果。原因：{translated_reason}")

    def _handle_live_test_order_result(self, result: dict) -> None:
        order = result.get("order", {}) if isinstance(result, dict) else {}
        execution = result.get("execution", {}) if isinstance(result, dict) else {}
        status = str(execution.get("status") or "-")
        order_id = str(execution.get("cl_ord_id") or "-")
        execution_response = execution.get("response") if isinstance(execution, dict) else {}
        leverage_notice = ""
        transport_notice = ""
        if isinstance(execution_response, dict):
            leverage_notice = str(execution_response.get("leverage_notice") or "").strip()
            transport_notice = str(execution_response.get("transport_notice") or "").strip()
        contracts = order.get("contracts")
        contracts_text = self._format_live_strategy_value(float(contracts)) if contracts not in (None, "") else "-"
        message = "\n".join(
            [
                "实盘测试开单已提交",
                f"标的={LIVE_TEST_ORDER_DISPLAY} | 方向=买入 | 类型=限价委托",
                f"价格={float(order.get('price', LIVE_TEST_ORDER_PRICE)):,.2f} USDT | 数量={float(order.get('base_quantity', LIVE_TEST_ORDER_BASE_QTY)):g} BTC",
                f"合约张数={contracts_text} | 订单状态={status} | 订单号={order_id}",
            ]
        )
        if leverage_notice:
            message = f"{message}\n{leverage_notice}"
        if transport_notice:
            message = f"{message}\n{transport_notice}"
        self.live_status.set(f"测试开单已提交：状态 {status}，订单号 {order_id}")
        self._live_log(message)
        messagebox.showinfo("测试开单已提交", message)

    def _translate_action(self, action: str) -> str:
        mapping = {
            "buy": "\u51c6\u5907\u4e70\u5165",
            "sell": "\u51c6\u5907\u5356\u51fa",
            "hold": "\u7ee7\u7eed\u6301\u6709 / \u7b49\u5f85",
            "skip": "\u8df3\u8fc7\u6267\u884c",
        }
        return mapping.get(action, action)

    @staticmethod
    def _translate_signal_side(signal_side: str) -> str:
        mapping = {
            "long_only": "只做多",
            "short_only": "只做空",
            "both": "双向",
        }
        return mapping.get((signal_side or "").strip(), signal_side or "-")

    def _translate_reason(self, reason: str, *, fast: int | str | None = None, slow: int | str | None = None) -> str:
        fast = str(fast or self.live_fast.get().strip() or "21")
        slow = str(slow or self.live_ema_large.get().strip() or self.live_slow.get().strip() or "55")
        mapping = {
            "golden_cross": f"EMA{fast} \u4e0a\u7a7f EMA{slow}",
            "dead_cross": f"EMA{fast} \u4e0b\u7a7f EMA{slow}",
            "slow_ema_stop": f"\u6536\u76d8\u4ef7\u8dcc\u7834 EMA{slow} \u6b62\u635f\u7ebf",
            "position_open_no_exit": "\u6301\u4ed3\u4e2d\uff0c\u672a\u89e6\u53d1\u5e73\u4ed3",
            "no_entry_signal": "\u5f53\u524d\u6ca1\u6709\u65b0\u7684\u5165\u573a\u4fe1\u53f7",
            "size_below_minimum": "\u4e0b\u5355\u6570\u91cf\u4f4e\u4e8e\u4ea4\u6613\u6240\u6700\u5c0f\u5355\u4f4d",
            "time_exit": "\u8fbe\u5230\u65f6\u95f4\u9000\u51fa\u6761\u4ef6",
        }
        return mapping.get(reason, reason)

    @staticmethod
    def _replace_log_text(widget: Text | None, text: str) -> None:
        if widget is None or not widget.winfo_exists():
            return
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.insert("end", text)
        widget.configure(state="disabled")

    @staticmethod
    def _append_log_text(widget: Text | None, message: str) -> None:
        if widget is None or not widget.winfo_exists():
            return
        widget.configure(state="normal")
        widget.insert("1.0", f"{TradingDesktopApp._timestamped_message(message)}\n")
        widget.configure(state="disabled")

    def _log(self, message: str) -> None:
        self._append_log_text(self.log_text, message)

    def _live_log(self, message: str) -> None:
        self._append_log_text(self.live_log_text, message)

    @staticmethod
    def _optional_float(value: str) -> float | None:
        stripped = value.strip()
        if not stripped:
            return None
        return float(stripped)

    def _live_run_mode_flags(self) -> tuple[bool, bool]:
        mode = self.live_run_mode.get().strip()
        if mode == "\u4ea4\u6613\u5e76\u4e0b\u5355":
            return True, False
        if mode == "\u6a21\u62df\u5e76\u4e0b\u5355":
            return True, True
        return False, False

    @staticmethod
    def _looks_like_live_connection_error(exc: Exception) -> bool:
        message = str(exc).lower()
        hints = (
            "read timed out",
            "connect timed out",
            "connection aborted",
            "connection reset",
            "max retries exceeded",
            "failed to establish a new connection",
            "httpsconnectionpool",
            "连接 okx 失败",
            "连接 okx 超时",
            "超时",
            "temporarily unavailable",
            "name or service not known",
        )
        return any(hint in message for hint in hints)

    def _has_live_credentials_ready(self) -> bool:
        form_ready = all(
            [
                self.live_api_key.get().strip(),
                self.live_api_secret.get().strip(),
                self.live_api_passphrase.get().strip(),
            ]
        )
        env_ready = all(
            [
                os.getenv("OKX_API_KEY", "").strip(),
                os.getenv("OKX_API_SECRET", "").strip(),
                os.getenv("OKX_API_PASSPHRASE", "").strip(),
            ]
        )
        return form_ready or env_ready

    def _ensure_live_credentials_ready(self) -> bool:
        if self._has_live_credentials_ready():
            return True
        profile_name = self._current_live_api_profile_label()
        messagebox.showwarning(
            "\u7f3a\u5c11API\u51ed\u8bc1",
            f"\u5f53\u524d\u201c{profile_name}\u201d\u8fd8\u6ca1\u6709\u53ef\u7528\u7684 API \u51ed\u8bc1\u3002\n"
            "\u8bf7\u5728\u201c\u8fde\u63a5\u4e0e\u4f59\u989d\u201d\u91cc\u586b\u5199 API Key / API Secret / Passphrase\uff0c"
            "\u7136\u540e\u70b9\u201c\u4fdd\u5b58\u5f53\u524dAPI\u201d\u518d\u8bd5\uff1b"
            "\u4e5f\u53ef\u4ee5\u6539\u7528\u7cfb\u7edf\u73af\u5883\u53d8\u91cf OKX_API_KEY / OKX_API_SECRET / OKX_API_PASSPHRASE\u3002",
        )
        self.live_status.set("\u7f3a\u5c11 API \u51ed\u8bc1\uff0c\u8bf7\u5148\u586b\u5199\u5f53\u524d API \u914d\u7f6e\u3002")
        return False

    @staticmethod
    def _normalize_symbol(value: str) -> str:
        raw = value.strip().upper()
        raw = raw.replace("永续", "").replace("合约", "").replace(" ", "")
        raw = raw.replace("/", "-").replace("_", "-")
        if not raw:
            return "BTC-USDT"
        if "-" in raw:
            return raw

        for suffix in ("USDT", "USDC", "BTC", "ETH"):
            if raw.endswith(suffix) and len(raw) > len(suffix):
                return f"{raw[:-len(suffix)]}-{suffix}"
        return raw

    @staticmethod
    def _normalize_swap_symbol(value: str) -> str:
        raw = TradingDesktopApp._normalize_symbol(value)
        if raw.endswith("-SWAP"):
            return raw
        if raw.count("-") == 1:
            return f"{raw}-SWAP"
        return raw

    @staticmethod
    def _normalize_live_period(value: str) -> str:
        raw = value.strip()
        if not raw:
            return "4H"
        mapping = {
            "24小时": "24H",
            "24h": "24H",
            "24H": "24H",
        }
        return mapping.get(raw, raw)

    @staticmethod
    def _normalize_backtest_period(value: str) -> str:
        raw = value.strip()
        if not raw:
            return "1H"
        mapping = {
            "5分钟": "5m",
            "5m": "5m",
            "15分钟": "15m",
            "15m": "15m",
            "1小时": "1H",
            "1H": "1H",
            "4小时": "4H",
            "4H": "4H",
            "24小时": "24H",
            "24H": "24H",
        }
        return mapping.get(raw, raw)
    @staticmethod
    def _normalize_signal_side(value: str) -> str:
        mapping = {
            "\u53ea\u505a\u591a": "long_only",
            "\u53ea\u505a\u7a7a": "short_only",
            "\u53cc\u5411": "both",
        }
        return mapping.get(value.strip(), "long_only")

    @staticmethod
    def _normalize_live_leverage(value: str) -> int:
        try:
            raw = int(float(value.strip() or "1"))
        except (TypeError, ValueError):
            raw = 1
        return max(1, min(30, raw))

    @staticmethod
    def _resolve_backtest_source_bar(period: str) -> str:
        normalized = TradingDesktopApp._normalize_backtest_period(period)
        if normalized in {"5m", "15m", "1H"}:
            return normalized
        return "1H"

    @staticmethod
    def _build_chart_bundle(result: dict) -> BacktestChartBundle | None:
        payload = result.get("top_chart")
        if not isinstance(payload, dict):
            return None

        signal_frame = payload.get("signal_frame")
        trades_df = payload.get("trades")
        equity_df = payload.get("equity")
        if not isinstance(signal_frame, pd.DataFrame):
            return None
        if not isinstance(trades_df, pd.DataFrame):
            trades_df = pd.DataFrame()
        if not isinstance(equity_df, pd.DataFrame):
            return None

        return BacktestChartBundle(
            symbol=str(payload.get("symbol") or result.get("symbol") or "-"),
            period=str(payload.get("period") or result.get("top_period") or "-"),
            fast_ema=int(payload.get("fast_ema") or result.get("fast_ema") or 21),
            slow_ema=int(payload.get("slow_ema") or result.get("slow_ema") or 55),
            signal_frame=signal_frame,
            trades_df=trades_df,
            equity_df=equity_df,
        )

    def _build_live_chart_bundle(self, result: dict) -> BacktestChartBundle | None:
        signal_frame = result.get("signal_frame")
        trades_df = result.get("trades")
        report = result.get("report", {})
        request = result.get("request", {})
        if not isinstance(signal_frame, pd.DataFrame):
            return None
        if not isinstance(trades_df, pd.DataFrame):
            trades_df = pd.DataFrame()

        total_assets = float(report.get("total_assets", 0.0) or 0.0)
        if signal_frame.empty:
            equity_df = pd.DataFrame(columns=["equity"])
        else:
            equity_df = pd.DataFrame({"equity": [total_assets] * len(signal_frame)}, index=signal_frame.index)

        return BacktestChartBundle(
            symbol=str(request.get("signal_symbol") or request.get("symbol") or self._normalize_swap_symbol(self.live_symbol.get()) or "-"),
            period=str(request.get("period") or self._normalize_live_period(self.live_period.get()) or "-"),
            fast_ema=int(report.get("ema_fast") or self.live_fast.get() or 21),
            slow_ema=int(report.get("ema_slow") or self.live_ema_large.get() or self.live_slow.get() or 55),
            signal_frame=signal_frame.copy(),
            trades_df=trades_df.copy(),
            equity_df=equity_df,
        )


def main() -> None:
    app = TradingDesktopApp()
    app.mainloop()


