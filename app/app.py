import calendar
import csv
import lzma
import queue
import struct
import threading
from bisect import bisect_left, bisect_right
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

try:
    from zoneinfo import ZoneInfo
    JST = ZoneInfo("Asia/Tokyo")
    NEW_YORK = ZoneInfo("America/New_York")
except Exception:
    JST = timezone(timedelta(hours=9))
    NEW_YORK = None

import tkinter as tk
from tkinter import messagebox, ttk


PAIR = "USDJPY"
BASE_URL = "https://datafeed.dukascopy.com/datafeed"
PIP_SIZE = 0.01


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def to_utc_hour_range(start_jst: date, end_jst: date):
    start_dt = datetime.combine(start_jst, time(0, 0), JST)
    end_dt = datetime.combine(end_jst, time(23, 59, 59), JST)
    start_utc = start_dt.astimezone(timezone.utc)
    end_utc = end_dt.astimezone(timezone.utc)

    current = start_utc.replace(minute=0, second=0, microsecond=0)
    hours = []
    while current <= end_utc:
        hours.append(current)
        current += timedelta(hours=1)
    return hours


def hour_to_url(dt_utc: datetime) -> str:
    year = dt_utc.year
    month_zero = dt_utc.month - 1
    return (
        f"{BASE_URL}/{PAIR}/"
        f"{year}/{month_zero:02d}/{dt_utc.day:02d}/{dt_utc.hour:02d}h_ticks.bi5"
    )


def hour_to_path(dt_utc: datetime) -> Path:
    jst_dt = dt_utc.astimezone(JST)
    year = jst_dt.year
    month = jst_dt.month
    return (
        project_root()
        / "data"
        / "bi5"
        / PAIR
        / f"{year}"
        / f"{month:02d}"
        / f"{jst_dt.day:02d}"
        / f"{jst_dt.hour:02d}h_ticks.bi5"
    )


def day_to_csv_path(jst_day: date) -> Path:
    return (
        project_root()
        / "data"
        / "csv"
        / PAIR
        / f"{jst_day.year}"
        / f"{jst_day.month:02d}"
        / f"{jst_day.day:02d}.csv"
    )


def group_hours_by_jst_day(hours):
    grouped = {}
    for dt_utc in hours:
        jst_day = dt_utc.astimezone(JST).date()
        grouped.setdefault(jst_day, []).append(dt_utc)
    for items in grouped.values():
        items.sort()
    return grouped


def weekend_boundary_hour_jst(dt_utc: datetime) -> int:
    if NEW_YORK is None:
        return 7
    ny = dt_utc.astimezone(NEW_YORK)
    if ny.dst() and ny.dst() != timedelta(0):
        return 6
    return 7


def is_weekend_closed(dt_utc: datetime) -> bool:
    jst = dt_utc.astimezone(JST)
    boundary = weekend_boundary_hour_jst(dt_utc)
    weekday = jst.weekday()
    if weekday == 5 and jst.hour >= boundary:
        return True
    if weekday == 6:
        return True
    if weekday == 0 and jst.hour < boundary:
        return True
    return False


def is_excluded_hour(dt_utc: datetime, exclude_weekends: bool) -> bool:
    if not exclude_weekends:
        return False
    return is_weekend_closed(dt_utc)


def build_csv_for_day(jst_day: date, day_hours, exclude_weekends: bool, log_fn=None):
    def log(message: str):
        if log_fn:
            log_fn(message)

    allowed_hours = [
        dt_utc for dt_utc in day_hours if not is_excluded_hour(dt_utc, exclude_weekends)
    ]
    if not allowed_hours:
        log(f"[CSV] 対象外 {jst_day.isoformat()}")
        return False

    csv_path = day_to_csv_path(jst_day)
    if csv_path.exists() and csv_path.stat().st_size > 0:
        log(f"[CSV] スキップ {csv_path}")
        return True

    available_hours = []
    missing = []
    for dt_utc in allowed_hours:
        src = hour_to_path(dt_utc)
        if src.exists() and src.stat().st_size > 0:
            available_hours.append(dt_utc)
        else:
            missing.append(src)

    if not available_hours:
        log(f"[CSV] スキップ {jst_day.isoformat()} 取得0件")
        return False

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp_jst", "bid", "ask", "bid_volume", "ask_volume"])
            for dt_utc in available_hours:
                src = hour_to_path(dt_utc)
                for row in iter_ticks(src, dt_utc):
                    writer.writerow(row)
        if missing:
            log(
                f"[CSV] 不足あり {jst_day.isoformat()} 不足 {len(missing)}件 作成"
            )
        else:
            log(f"[CSV] 成功 {csv_path}")
        return True
    except Exception as e:
        try:
            if csv_path.exists():
                csv_path.unlink()
        except Exception:
            pass
        log(f"[CSV] エラー {jst_day.isoformat()} {e}")
        return False


def iter_ticks(path: Path, hour_start_utc: datetime):
    raw = path.read_bytes()
    data = lzma.decompress(raw)
    if len(data) % 20 != 0:
        raise ValueError("invalid tick data size")
    if not data:
        return
    t0, ask_i0, bid_i0, _ask_v0, _bid_v0 = struct.unpack_from(">IIIff", data, 0)
    scale = 100000 if max(ask_i0, bid_i0) >= 1_000_000 else 1000
    digits = 5 if scale == 100000 else 3
    for offset in range(0, len(data), 20):
        t_ms, ask_i, bid_i, ask_v, bid_v = struct.unpack_from(">IIIff", data, offset)
        ts_utc = hour_start_utc + timedelta(milliseconds=int(t_ms))
        ts_jst = ts_utc.astimezone(JST)
        bid = bid_i / scale
        ask = ask_i / scale
        yield (
            ts_jst.isoformat(),
            f"{bid:.{digits}f}",
            f"{ask:.{digits}f}",
            f"{bid_v:.6f}",
            f"{ask_v:.6f}",
        )


def load_ticks_from_csv(start_jst: date, end_jst: date):
    start_dt = datetime.combine(start_jst, time(0, 0), JST)
    end_dt = datetime.combine(end_jst, time(23, 59, 59), JST)
    points = []
    missing = []
    day = start_jst
    while day <= end_jst:
        path = day_to_csv_path(day)
        if (not path.exists()) or path.stat().st_size == 0:
            missing.append(path)
        else:
            try:
                with path.open("r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        ts_str = row.get("timestamp_jst")
                        bid_str = row.get("bid")
                        if not ts_str or not bid_str:
                            continue
                        try:
                            ts = datetime.fromisoformat(ts_str)
                        except ValueError:
                            continue
                        if ts < start_dt or ts > end_dt:
                            continue
                        try:
                            bid = float(bid_str)
                        except ValueError:
                            continue
                        points.append((ts, bid))
            except Exception:
                missing.append(path)
        day += timedelta(days=1)
    return points, missing


def downsample_points(points, max_points):
    if len(points) <= max_points:
        return list(enumerate(points))
    step = max(1, len(points) // max_points)
    sampled = []
    for idx in range(0, len(points), step):
        sampled.append((idx, points[idx]))
    if sampled and sampled[-1][0] != len(points) - 1:
        sampled.append((len(points) - 1, points[-1]))
    return sampled


def build_minute_candles(points):
    candles = []
    current_minute = None
    open_p = high_p = low_p = close_p = None

    for ts, price in points:
        minute = ts.replace(second=0, microsecond=0)
        if current_minute is None or minute != current_minute:
            if current_minute is not None:
                candles.append((current_minute, open_p, high_p, low_p, close_p))
            current_minute = minute
            open_p = high_p = low_p = close_p = price
        else:
            if price > high_p:
                high_p = price
            if price < low_p:
                low_p = price
            close_p = price

    if current_minute is not None:
        candles.append((current_minute, open_p, high_p, low_p, close_p))
    return candles


def build_minute_ma(candles, period):
    if period <= 0 or not candles:
        return [], [], []
    times = []
    ma_values = [None] * len(candles)
    closes = [c[4] for c in candles]
    running = 0.0
    for i, candle in enumerate(candles):
        running += closes[i]
        if i >= period:
            running -= closes[i - period]
        if i >= period - 1:
            ma_values[i] = running / period
        times.append(candle[0])
    series = [
        (times[i], ma_values[i])
        for i in range(len(times))
        if ma_values[i] is not None
    ]
    return times, ma_values, series


def find_spike_signal(points, times, start_idx, window, spike, retrace_rate):
    t0, p0 = points[start_idx]
    end_time = t0 + window
    end_idx = bisect_right(times, end_time)
    if end_idx <= start_idx + 1:
        return None

    min_price = p0
    max_price = p0
    min_idx = start_idx
    max_idx = start_idx

    for j in range(start_idx + 1, end_idx):
        price = points[j][1]

        if price < min_price:
            min_price = price
            min_idx = j

        drop = p0 - min_price
        if drop >= spike and j >= min_idx:
            retrace_level = min_price + drop * retrace_rate
            if price >= retrace_level:
                return j, "long"

        if price > max_price:
            max_price = price
            max_idx = j

        rise = max_price - p0
        if rise >= spike and j >= max_idx:
            retrace_level = max_price - rise * retrace_rate
            if price <= retrace_level:
                return j, "short"

    return None


def summarize_trades(trades):
    total = len(trades)
    wins = sum(1 for t in trades if t["pips"] > 0)
    losses = sum(1 for t in trades if t["pips"] < 0)
    draws = total - wins - losses
    total_pips = sum(t["pips"] for t in trades)
    avg_pips = total_pips / total if total else 0.0
    win_rate = wins / total * 100 if total else 0.0
    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "draws": draws,
        "total_pips": total_pips,
        "avg_pips": avg_pips,
        "win_rate": win_rate,
    }


def run_backtest(points, params):
    if not points:
        return {
            "trades": [],
            "summary": summarize_trades([]),
            "equity_curve": [],
            "ma_series": [],
            "ma_enabled": params.get("ma_enabled", False),
            "ma_period": params.get("ma_period", 0),
            "ma_deviation_rate": params.get("ma_deviation_rate", 0.0),
        }

    points_sorted = sorted(points, key=lambda x: x[0])
    times = [ts for ts, _ in points_sorted]
    window = timedelta(milliseconds=params["window_ms"])
    spike = params["spike_pips"] * PIP_SIZE
    retrace_rate = params["retrace_rate"]
    spread = params["spread_pips"] * PIP_SIZE
    stop = params["stop_pips"] * PIP_SIZE
    take = params["take_pips"] * PIP_SIZE
    ma_enabled = params.get("ma_enabled", False)
    ma_period = max(1, int(params.get("ma_period", 0)))
    ma_deviation = params.get("ma_deviation_rate", 0.0)

    candle_times = []
    ma_values = []
    ma_series = []
    if ma_enabled:
        candles = build_minute_candles(points_sorted)
        candle_times, ma_values, ma_series = build_minute_ma(candles, ma_period)

    trades = []
    equity_curve = [(times[0], 0.0)]
    cumulative = 0.0

    i = 0
    n = len(points_sorted)
    while i < n - 1:
        signal = find_spike_signal(
            points_sorted, times, i, window, spike, retrace_rate
        )
        if not signal:
            i += 1
            continue

        entry_idx, side = signal
        entry_time, entry_bid = points_sorted[entry_idx]
        entry_price = entry_bid + spread if side == "long" else entry_bid

        if ma_enabled:
            if not candle_times:
                i = entry_idx + 1
                continue
            candle_idx = bisect_right(candle_times, entry_time) - 1
            if candle_idx < 0 or candle_idx >= len(ma_values):
                i = entry_idx + 1
                continue
            ma_value = ma_values[candle_idx]
            if ma_value is None or ma_value <= 0:
                i = entry_idx + 1
                continue
            if side == "long":
                deviation = (ma_value - entry_price) / ma_value
            else:
                deviation = (entry_price - ma_value) / ma_value
            if deviation < ma_deviation:
                i = entry_idx + 1
                continue

        if side == "long":
            stop_price = entry_price - stop
            take_price = entry_price + take
        else:
            stop_price = entry_price + stop
            take_price = entry_price - take

        exit_idx = None
        exit_price = None
        exit_reason = None
        j = entry_idx + 1
        while j < n:
            _t, bid = points_sorted[j]
            ask = bid + spread
            if side == "long":
                if bid <= stop_price:
                    exit_idx = j
                    exit_price = bid
                    exit_reason = "損切"
                    break
                if bid >= take_price:
                    exit_idx = j
                    exit_price = bid
                    exit_reason = "利確"
                    break
            else:
                if ask >= stop_price:
                    exit_idx = j
                    exit_price = ask
                    exit_reason = "損切"
                    break
                if ask <= take_price:
                    exit_idx = j
                    exit_price = ask
                    exit_reason = "利確"
                    break
            j += 1

        if exit_idx is None:
            exit_idx = n - 1
            _t, last_bid = points_sorted[-1]
            exit_price = last_bid + spread if side == "short" else last_bid
            exit_reason = "終了"

        if side == "long":
            pips = (exit_price - entry_price) / PIP_SIZE
        else:
            pips = (entry_price - exit_price) / PIP_SIZE

        trades.append(
            {
                "side": side,
                "entry_time": entry_time,
                "entry_price": entry_price,
                "exit_time": points_sorted[exit_idx][0],
                "exit_price": exit_price,
                "pips": pips,
                "reason": exit_reason,
            }
        )

        cumulative += pips
        equity_curve.append((points_sorted[exit_idx][0], cumulative))
        i = exit_idx + 1

    if equity_curve and equity_curve[-1][0] != times[-1]:
        equity_curve.append((times[-1], cumulative))

    return {
        "trades": trades,
        "summary": summarize_trades(trades),
        "equity_curve": equity_curve,
        "ma_series": ma_series,
        "ma_enabled": ma_enabled,
        "ma_period": ma_period,
        "ma_deviation_rate": ma_deviation,
    }


class CalendarPopup:
    def __init__(self, parent, initial_date: date, on_select):
        self.parent = parent
        self.on_select = on_select
        self.current_year = initial_date.year
        self.current_month = initial_date.month
        self.top = tk.Toplevel(parent)
        self.top.title("カレンダー")
        self.top.resizable(False, False)
        self._build()
        self._render_days()

    def _build(self):
        header = ttk.Frame(self.top, padding=6)
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(1, weight=1)

        ttk.Button(header, text="<", width=3, command=self._prev_month).grid(
            row=0, column=0, padx=2
        )
        self.month_label = ttk.Label(header, text="")
        self.month_label.grid(row=0, column=1)
        ttk.Button(header, text=">", width=3, command=self._next_month).grid(
            row=0, column=2, padx=2
        )

        days = ttk.Frame(self.top, padding=(6, 0, 6, 6))
        days.grid(row=1, column=0)
        self.days_frame = days

        self.day_labels = []
        for i, name in enumerate(["日", "月", "火", "水", "木", "金", "土"]):
            lbl = ttk.Label(days, text=name, width=4, anchor="center")
            lbl.grid(row=0, column=i, padx=1, pady=(0, 2))
            self.day_labels.append(lbl)

    def _render_days(self):
        for child in list(self.days_frame.children.values()):
            if isinstance(child, ttk.Button):
                child.destroy()

        self.month_label.config(
            text=f"{self.current_year:04d}-{self.current_month:02d}"
        )

        cal = calendar.Calendar(firstweekday=calendar.SUNDAY)
        month_days = cal.monthdayscalendar(self.current_year, self.current_month)

        for r, week in enumerate(month_days, start=1):
            for c, day_num in enumerate(week):
                if day_num == 0:
                    ttk.Label(self.days_frame, text=" ", width=4).grid(
                        row=r, column=c, padx=1, pady=1
                    )
                    continue
                btn = ttk.Button(
                    self.days_frame,
                    text=str(day_num),
                    width=4,
                    command=lambda d=day_num: self._select_day(d),
                )
                btn.grid(row=r, column=c, padx=1, pady=1)

    def _select_day(self, day_num: int):
        chosen = date(self.current_year, self.current_month, day_num)
        self.on_select(chosen)
        self.top.destroy()

    def _prev_month(self):
        if self.current_month == 1:
            self.current_month = 12
            self.current_year -= 1
        else:
            self.current_month -= 1
        self._render_days()

    def _next_month(self):
        if self.current_month == 12:
            self.current_month = 1
            self.current_year += 1
        else:
            self.current_month += 1
        self._render_days()


class Step1App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("ティックデータ取得（STEP1）")
        self.queue = queue.Queue()
        self.worker = None
        self.chart_worker = None
        self.chart_data = None
        self.drag_start_x = None
        self.drag_start_view = None
        self.cancel_event = threading.Event()

        today_jst = datetime.now(JST).date()
        self.start_date = today_jst
        self.end_date = today_jst
        self.view_start_date = today_jst
        self.view_end_date = today_jst

        self.start_var = tk.StringVar(value=self.start_date.isoformat())
        self.end_var = tk.StringVar(value=self.end_date.isoformat())
        self.view_start_var = tk.StringVar(value=self.view_start_date.isoformat())
        self.view_end_var = tk.StringVar(value=self.view_end_date.isoformat())
        self.exclude_weekends_var = tk.BooleanVar(value=True)
        self.status_var = tk.StringVar(value="準備完了")
        self.chart_info_var = tk.StringVar(value="")
        self.x_axis_mode_var = tk.StringVar(value="time")
        self.chart_type_var = tk.StringVar(value="tick")
        self.hide_chart_var = tk.BooleanVar(value=False)
        self.ma_filter_var = tk.BooleanVar(value=True)
        self.ma_period_var = tk.StringVar(value="200")
        self.ma_deviation_var = tk.StringVar(value="0.01")
        self.spike_window_var = tk.StringVar(value="500")
        self.spike_pips_var = tk.StringVar(value="1.0")
        self.retrace_var = tk.StringVar(value="90")
        self.spread_var = tk.StringVar(value="1.0")
        self.stop_pips_var = tk.StringVar(value="5.0")
        self.take_pips_var = tk.StringVar(value="5.0")
        self.backtest_info_var = tk.StringVar(value="バックテスト: 未実行")
        self.pnl_info_var = tk.StringVar(value="損益: 未実行")
        self.pnl_data = None
        self.backtest_ready = False

        self._build_ui()
        self._poll_queue()

    def _build_ui(self):
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=0)

        notebook = ttk.Notebook(self.root)
        notebook.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)

        chart_tab = ttk.Frame(notebook, padding=12)
        download_tab = ttk.Frame(notebook, padding=12)
        pnl_tab = ttk.Frame(notebook, padding=12)
        notebook.add(chart_tab, text="チャート")
        notebook.add(download_tab, text="ダウンロード")
        notebook.add(pnl_tab, text="損益")

        status_bar = ttk.Frame(self.root)
        status_bar.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 8))
        status_bar.columnconfigure(0, weight=1)
        ttk.Label(status_bar, textvariable=self.status_var).grid(row=0, column=0, sticky="w")

        chart_tab.columnconfigure(0, weight=1)
        chart_tab.rowconfigure(6, weight=1)

        ttk.Label(chart_tab, text="表示期間（JST）").grid(row=0, column=0, sticky="w")

        view_row = ttk.Frame(chart_tab)
        view_row.grid(row=1, column=0, sticky="ew")
        view_row.columnconfigure(1, weight=1)

        ttk.Label(view_row, text="開始日（JST）").grid(row=0, column=0, sticky="w")
        view_start_entry = ttk.Entry(
            view_row, textvariable=self.view_start_var, width=12, state="readonly"
        )
        view_start_entry.grid(row=0, column=1, padx=6)
        ttk.Button(view_row, text="選択", command=self._pick_view_start).grid(
            row=0, column=2
        )

        ttk.Label(view_row, text="終了日（JST）").grid(row=1, column=0, sticky="w", pady=(6, 0))
        view_end_entry = ttk.Entry(
            view_row, textvariable=self.view_end_var, width=12, state="readonly"
        )
        view_end_entry.grid(row=1, column=1, padx=6, pady=(6, 0))
        ttk.Button(view_row, text="選択", command=self._pick_view_end).grid(
            row=1, column=2, pady=(6, 0)
        )

        chart_controls = ttk.Frame(chart_tab)
        chart_controls.grid(row=2, column=0, sticky="ew", pady=(8, 6))
        chart_controls.columnconfigure(3, weight=1)

        self.chart_button = ttk.Button(chart_controls, text="表示", command=self._show_chart)
        self.chart_button.grid(row=0, column=0, sticky="w")

        ttk.Label(chart_controls, text="横軸").grid(row=0, column=1, padx=(12, 4), sticky="w")
        self.axis_time_radio = ttk.Radiobutton(
            chart_controls,
            text="時間",
            variable=self.x_axis_mode_var,
            value="time",
            command=self._on_axis_mode_change,
        )
        self.axis_time_radio.grid(row=0, column=2, sticky="w")
        self.axis_tick_radio = ttk.Radiobutton(
            chart_controls,
            text="本数",
            variable=self.x_axis_mode_var,
            value="tick",
            command=self._on_axis_mode_change,
        )
        self.axis_tick_radio.grid(row=0, column=3, sticky="w")

        ttk.Label(chart_controls, text="表示").grid(row=1, column=1, padx=(12, 4), sticky="w")
        self.chart_tick_radio = ttk.Radiobutton(
            chart_controls,
            text="ティック",
            variable=self.chart_type_var,
            value="tick",
            command=self._on_chart_type_change,
        )
        self.chart_tick_radio.grid(row=1, column=2, sticky="w")
        self.chart_candle_radio = ttk.Radiobutton(
            chart_controls,
            text="1分足",
            variable=self.chart_type_var,
            value="candle",
            command=self._on_chart_type_change,
        )
        self.chart_candle_radio.grid(row=1, column=3, sticky="w")

        self.hide_chart_check = ttk.Checkbutton(
            chart_controls,
            text="チャート非表示",
            variable=self.hide_chart_var,
            command=self._on_chart_visibility_change,
        )
        self.hide_chart_check.grid(row=0, column=4, padx=(12, 0), sticky="w")

        settings = ttk.LabelFrame(chart_tab, text="バックテスト条件")
        settings.grid(row=3, column=0, sticky="ew", pady=(8, 6))

        ttk.Label(settings, text="スパイク時間（ミリ秒）").grid(row=0, column=0, sticky="w")
        ttk.Entry(settings, textvariable=self.spike_window_var, width=8).grid(
            row=0, column=1, padx=(4, 12), sticky="w"
        )
        ttk.Label(settings, text="スパイク幅（ピップス）").grid(row=0, column=2, sticky="w")
        ttk.Entry(settings, textvariable=self.spike_pips_var, width=8).grid(
            row=0, column=3, padx=(4, 12), sticky="w"
        )
        ttk.Label(settings, text="最小戻し率（％）").grid(row=0, column=4, sticky="w")
        ttk.Entry(settings, textvariable=self.retrace_var, width=8).grid(
            row=0, column=5, padx=(4, 0), sticky="w"
        )

        ttk.Label(settings, text="スプレッド（ピップス）").grid(row=1, column=0, sticky="w")
        ttk.Entry(settings, textvariable=self.spread_var, width=8).grid(
            row=1, column=1, padx=(4, 12), pady=(6, 0), sticky="w"
        )
        ttk.Label(settings, text="損切幅（ピップス）").grid(row=1, column=2, sticky="w", pady=(6, 0))
        ttk.Entry(settings, textvariable=self.stop_pips_var, width=8).grid(
            row=1, column=3, padx=(4, 12), pady=(6, 0), sticky="w"
        )
        ttk.Label(settings, text="利確幅（ピップス）").grid(row=1, column=4, sticky="w", pady=(6, 0))
        ttk.Entry(settings, textvariable=self.take_pips_var, width=8).grid(
            row=1, column=5, padx=(4, 0), pady=(6, 0), sticky="w"
        )

        self.ma_check = ttk.Checkbutton(
            settings,
            text="移動平均フィルター",
            variable=self.ma_filter_var,
            command=self._on_ma_filter_toggle,
        )
        self.ma_check.grid(row=2, column=0, columnspan=2, sticky="w", pady=(6, 0))
        ttk.Label(settings, text="期間").grid(row=2, column=2, sticky="w", pady=(6, 0))
        self.ma_period_entry = ttk.Entry(
            settings, textvariable=self.ma_period_var, width=8
        )
        self.ma_period_entry.grid(row=2, column=3, padx=(4, 12), pady=(6, 0), sticky="w")
        ttk.Label(settings, text="乖離率（％）").grid(row=2, column=4, sticky="w", pady=(6, 0))
        self.ma_deviation_entry = ttk.Entry(
            settings, textvariable=self.ma_deviation_var, width=8
        )
        self.ma_deviation_entry.grid(row=2, column=5, padx=(4, 0), pady=(6, 0), sticky="w")

        ttk.Label(chart_tab, textvariable=self.chart_info_var).grid(
            row=4, column=0, sticky="w"
        )
        ttk.Label(chart_tab, textvariable=self.backtest_info_var).grid(
            row=5, column=0, sticky="w"
        )

        self.chart_canvas = tk.Canvas(chart_tab, bg="white")
        self.chart_canvas.grid(row=6, column=0, sticky="nsew", pady=(6, 0))
        self.chart_canvas.bind("<Configure>", self._on_canvas_resize)
        self.chart_canvas.bind("<MouseWheel>", self._on_mouse_wheel)
        self.chart_canvas.bind("<Button-4>", self._on_mouse_wheel)
        self.chart_canvas.bind("<Button-5>", self._on_mouse_wheel)
        self.chart_canvas.bind("<ButtonPress-1>", self._on_drag_start)
        self.chart_canvas.bind("<B1-Motion>", self._on_drag_move)

        download_tab.columnconfigure(0, weight=1)
        download_tab.rowconfigure(4, weight=1)

        header = ttk.Frame(download_tab)
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="取得期間（JST）").grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(
            header, text="土日を除外（初期オン）", variable=self.exclude_weekends_var
        ).grid(row=0, column=1, sticky="e")

        row = ttk.Frame(download_tab)
        row.grid(row=1, column=0, sticky="ew")
        row.columnconfigure(1, weight=1)

        ttk.Label(row, text="開始日（JST）").grid(row=0, column=0, sticky="w")
        start_entry = ttk.Entry(row, textvariable=self.start_var, width=12, state="readonly")
        start_entry.grid(row=0, column=1, padx=6)
        ttk.Button(row, text="選択", command=self._pick_start).grid(row=0, column=2)

        ttk.Label(row, text="終了日（JST）").grid(row=1, column=0, sticky="w", pady=(6, 0))
        end_entry = ttk.Entry(row, textvariable=self.end_var, width=12, state="readonly")
        end_entry.grid(row=1, column=1, padx=6, pady=(6, 0))
        ttk.Button(row, text="選択", command=self._pick_end).grid(row=1, column=2, pady=(6, 0))

        self.run_button = ttk.Button(download_tab, text="ダウンロード", command=self._start_download)
        self.run_button.grid(row=2, column=0, sticky="w", pady=(8, 6))
        self.cancel_button = ttk.Button(
            download_tab, text="キャンセル", command=self._cancel_download, state="disabled"
        )
        self.cancel_button.grid(row=2, column=0, sticky="w", padx=(110, 0), pady=(8, 6))

        ttk.Label(download_tab, text="実行ログ").grid(row=3, column=0, sticky="w")

        self.log = tk.Text(download_tab, height=14, width=80)
        self.log.grid(row=4, column=0, sticky="nsew", pady=(6, 0))

        pnl_tab.columnconfigure(0, weight=1)
        pnl_tab.rowconfigure(1, weight=1)

        ttk.Label(pnl_tab, textvariable=self.pnl_info_var).grid(row=0, column=0, sticky="w")
        self.pnl_canvas = tk.Canvas(pnl_tab, bg="white")
        self.pnl_canvas.grid(row=1, column=0, sticky="nsew", pady=(6, 0))
        self.pnl_canvas.bind("<Configure>", self._on_pnl_resize)

        self._on_ma_filter_toggle()

    def _pick_start(self):
        CalendarPopup(self.root, self.start_date, self._set_start)

    def _pick_end(self):
        CalendarPopup(self.root, self.end_date, self._set_end)

    def _set_start(self, picked: date):
        self.start_date = picked
        self.start_var.set(picked.isoformat())

    def _set_end(self, picked: date):
        self.end_date = picked
        self.end_var.set(picked.isoformat())

    def _pick_view_start(self):
        CalendarPopup(self.root, self.view_start_date, self._set_view_start)

    def _pick_view_end(self):
        CalendarPopup(self.root, self.view_end_date, self._set_view_end)

    def _set_view_start(self, picked: date):
        self.view_start_date = picked
        self.view_start_var.set(picked.isoformat())

    def _set_view_end(self, picked: date):
        self.view_end_date = picked
        self.view_end_var.set(picked.isoformat())

    def _parse_number(self, value: str) -> float:
        cleaned = value.strip().replace(",", ".").replace("，", ".")
        filtered = "".join(ch for ch in cleaned if ch.isdigit() or ch in ".-")
        return float(filtered)

    def _get_backtest_params(self):
        try:
            window_ms = self._parse_number(self.spike_window_var.get())
            spike_pips = self._parse_number(self.spike_pips_var.get())
            retrace_pct = self._parse_number(self.retrace_var.get())
            spread_pips = self._parse_number(self.spread_var.get())
            stop_pips = self._parse_number(self.stop_pips_var.get())
            take_pips = self._parse_number(self.take_pips_var.get())
            ma_enabled = self.ma_filter_var.get()
            ma_period = self._parse_number(self.ma_period_var.get())
            ma_deviation_pct = self._parse_number(self.ma_deviation_var.get())
        except ValueError:
            messagebox.showerror("エラー", "数値の入力が正しくありません。")
            return None

        if window_ms <= 0:
            messagebox.showerror("エラー", "スパイク時間は0より大きくしてください。")
            return None
        if spike_pips <= 0:
            messagebox.showerror("エラー", "スパイク幅は0より大きくしてください。")
            return None
        if retrace_pct < 0 or retrace_pct > 100:
            messagebox.showerror("エラー", "最小戻し率は0〜100の範囲にしてください。")
            return None
        if spread_pips < 0:
            messagebox.showerror("エラー", "スプレッドは0以上にしてください。")
            return None
        if stop_pips <= 0:
            messagebox.showerror("エラー", "損切幅は0より大きくしてください。")
            return None
        if take_pips <= 0:
            messagebox.showerror("エラー", "利確幅は0より大きくしてください。")
            return None

        if ma_period < 2:
            messagebox.showerror("エラー", "移動平均の期間は2以上にしてください。")
            return None
        if ma_deviation_pct < 0:
            messagebox.showerror("エラー", "乖離率は0以上にしてください。")
            return None

        return {
            "window_ms": window_ms,
            "spike_pips": spike_pips,
            "retrace_rate": retrace_pct / 100.0,
            "spread_pips": spread_pips,
            "stop_pips": stop_pips,
            "take_pips": take_pips,
            "ma_enabled": ma_enabled,
            "ma_period": int(ma_period),
            "ma_deviation_rate": ma_deviation_pct / 100.0,
        }

    def _start_download(self):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("お知らせ", "ダウンロード中です。")
            return
        if self.end_date < self.start_date:
            messagebox.showerror("エラー", "終了日は開始日より後にしてください。")
            return
        self.run_button.config(state="disabled")
        self.cancel_button.config(state="normal")
        self.cancel_event.clear()
        self.status_var.set("準備中...")
        self.log.delete("1.0", tk.END)
        exclude_weekends = self.exclude_weekends_var.get()
        self.worker = threading.Thread(
            target=self._download_worker,
            args=(self.start_date, self.end_date, exclude_weekends),
            daemon=True,
        )
        self.worker.start()

    def _cancel_download(self):
        if self.worker and self.worker.is_alive():
            self.cancel_event.set()
            self.queue.put(("log", "[INFO] キャンセル要求を受け付けました"))
        else:
            self.cancel_button.config(state="disabled")

    def _show_chart(self):
        if self.chart_worker and self.chart_worker.is_alive():
            messagebox.showinfo("お知らせ", "表示処理中です。")
            return
        if self.view_end_date < self.view_start_date:
            messagebox.showerror("エラー", "終了日は開始日より後にしてください。")
            return
        params = self._get_backtest_params()
        if not params:
            return
        self.chart_button.config(state="disabled")
        self.status_var.set("表示準備中...")
        self.backtest_info_var.set("バックテスト: 計算中...")
        self.pnl_info_var.set("損益: 計算中...")
        self.backtest_ready = False
        self.pnl_data = None
        self._draw_pnl_chart()
        self.chart_worker = threading.Thread(
            target=self._chart_worker,
            args=(self.view_start_date, self.view_end_date, params),
            daemon=True,
        )
        self.chart_worker.start()

    def _chart_worker(self, start: date, end: date, params):
        points, missing = load_ticks_from_csv(start, end)
        if missing:
            self.queue.put(("log", f"[表示] CSV不足 {len(missing)}件"))
        if not points:
            self.queue.put(("chart_error", "表示できるデータがありません。"))
            self.queue.put(("chart_done", None))
            return
        points_sorted = sorted(points, key=lambda x: x[0])
        payload = {
            "start": start,
            "end": end,
            "points": points_sorted,
            "missing_count": len(missing),
        }
        self.queue.put(("chart_data", payload))
        self.queue.put(("status", "バックテスト中..."))
        try:
            backtest = run_backtest(points_sorted, params)
            self.queue.put(("backtest_data", backtest))
        except Exception as e:
            self.queue.put(("backtest_error", str(e)))
        self.queue.put(("status", "表示完了"))
        self.queue.put(("chart_done", None))

    def _download_worker(self, start: date, end: date, exclude_weekends: bool):
        hours = to_utc_hour_range(start, end)
        day_groups = group_hours_by_jst_day(hours)
        total = len(hours)
        self.queue.put(("status", f"ダウンロード中...（{total}件）"))

        index = 0
        for jst_day, day_hours in day_groups.items():
            if self.cancel_event.is_set():
                self.queue.put(("cancelled", None))
                return
            for dt_utc in day_hours:
                if self.cancel_event.is_set():
                    self.queue.put(("cancelled", None))
                    return
                index += 1
                if is_excluded_hour(dt_utc, exclude_weekends):
                    jst_dt = dt_utc.astimezone(JST)
                    self.queue.put(
                        (
                            "log",
                            f"[{index}/{total}] 対象外 {jst_dt.strftime('%Y-%m-%d %H')}時",
                        )
                    )
                    continue
                url = hour_to_url(dt_utc)
                path = hour_to_path(dt_utc)
                path.parent.mkdir(parents=True, exist_ok=True)

                if path.exists():
                    if path.stat().st_size == 0:
                        try:
                            path.unlink()
                        except Exception as e:
                            self.queue.put(
                                ("log", f"[{index}/{total}] 削除失敗 {path} {e}")
                            )
                            continue
                    else:
                        self.queue.put(("log", f"[{index}/{total}] スキップ {path}"))
                        continue

                try:
                    data = b""
                    for _ in range(2):
                        with urlopen(url, timeout=30) as resp:
                            data = resp.read()
                        if data:
                            break
                    if not data:
                        if path.exists():
                            path.unlink()
                        self.queue.put(("log", f"[{index}/{total}] 0バイト {url}"))
                        continue
                    path.write_bytes(data)
                    if path.stat().st_size == 0:
                        path.unlink()
                        self.queue.put(("log", f"[{index}/{total}] 0バイト {url}"))
                        continue
                    self.queue.put(("log", f"[{index}/{total}] 成功 {path}"))
                except HTTPError as e:
                    self.queue.put(("log", f"[{index}/{total}] HTTP {e.code} {url}"))
                except URLError as e:
                    self.queue.put(("log", f"[{index}/{total}] URLエラー {e.reason}"))
                except Exception as e:
                    self.queue.put(("log", f"[{index}/{total}] エラー {e}"))

            if self.cancel_event.is_set():
                self.queue.put(("cancelled", None))
                return
            self.queue.put(("status", f"CSV作成中...（{jst_day.isoformat()}）"))
            self._build_csv_for_day(jst_day, day_hours, exclude_weekends)

        self.queue.put(("status", "完了"))
        self.queue.put(("done", None))

    def _build_csv_for_day(self, jst_day, day_hours, exclude_weekends: bool):
        build_csv_for_day(
            jst_day,
            day_hours,
            exclude_weekends,
            log_fn=lambda msg: self.queue.put(("log", msg)),
        )

    def _poll_queue(self):
        try:
            while True:
                kind, payload = self.queue.get_nowait()
                if kind == "log":
                    self.log.insert(tk.END, payload + "\n")
                    self.log.see(tk.END)
                elif kind == "status":
                    self.status_var.set(payload)
                elif kind == "done":
                    self.run_button.config(state="normal")
                    self.cancel_button.config(state="disabled")
                elif kind == "chart_done":
                    self.chart_button.config(state="normal")
                elif kind == "chart_error":
                    messagebox.showerror("エラー", payload)
                    self.backtest_info_var.set("バックテスト: データなし")
                    self.pnl_info_var.set("損益: データなし")
                    self.backtest_ready = False
                    self.pnl_data = None
                    self._draw_pnl_chart()
                elif kind == "chart_data":
                    self._render_chart(payload)
                elif kind == "backtest_data":
                    self._render_backtest(payload)
                elif kind == "backtest_error":
                    messagebox.showerror("エラー", f"バックテストで問題が起きました: {payload}")
                    self.backtest_info_var.set("バックテスト: エラー")
                    self.pnl_info_var.set("損益: エラー")
                    self.backtest_ready = False
                    self.pnl_data = None
                    self._draw_pnl_chart()
                elif kind == "cancelled":
                    self.status_var.set("キャンセルしました")
                    self.run_button.config(state="normal")
                    self.cancel_button.config(state="disabled")
        except queue.Empty:
            pass
        self.root.after(200, self._poll_queue)

    def _render_chart(self, payload):
        points = payload["points"]
        start = payload["start"]
        end = payload["end"]
        missing_count = payload["missing_count"]

        if not points:
            self.chart_info_var.set("表示できるデータがありません。")
            return

        points = sorted(points, key=lambda x: x[0])
        times = [ts for ts, _ in points]
        self.chart_data = {
            "all_points": points,
            "times": times,
            "view_start": 0,
            "view_end": len(points) - 1,
            "count": len(points),
            "start": start,
            "end": end,
            "missing": missing_count,
            "mode": self.x_axis_mode_var.get(),
            "view_start_time": times[0],
            "view_end_time": times[-1],
            "trades": [],
            "ma_series": [],
            "ma_enabled": False,
        }
        self._draw_chart()

    def _render_backtest(self, payload):
        summary = payload.get("summary", {})
        total = summary.get("total", 0)
        wins = summary.get("wins", 0)
        losses = summary.get("losses", 0)
        draws = summary.get("draws", 0)
        total_pips = summary.get("total_pips", 0.0)
        avg_pips = summary.get("avg_pips", 0.0)
        win_rate = summary.get("win_rate", 0.0)

        if total == 0:
            self.backtest_info_var.set("バックテスト: 取引0件")
            self.pnl_info_var.set("損益: 取引0件")
        else:
            draw_text = f" 引き分け{draws}件" if draws else ""
            self.backtest_info_var.set(
                f"バックテスト: 取引{total}件 勝ち{wins}件 負け{losses}件"
                f"{draw_text} 勝率{win_rate:.1f}% 合計損益{total_pips:.1f}ピップス"
                f" 平均損益{avg_pips:.2f}ピップス"
            )
            draw_text_short = f" / 引き分け{draws}" if draws else ""
            self.pnl_info_var.set(
                f"合計損益: {total_pips:.1f}ピップス 取引: {total}件"
                f"（勝ち{wins} / 負け{losses}{draw_text_short}）"
            )

        self.pnl_data = payload.get("equity_curve") or []
        self.backtest_ready = True
        if self.chart_data is not None:
            self.chart_data["trades"] = payload.get("trades") or []
            self.chart_data["ma_series"] = payload.get("ma_series") or []
            self.chart_data["ma_enabled"] = payload.get("ma_enabled", False)
            self._draw_chart()
        self._draw_pnl_chart()

    def _on_axis_mode_change(self):
        if not self.chart_data:
            return
        if self.chart_type_var.get() == "candle":
            self.x_axis_mode_var.set("time")
            self.chart_data["mode"] = "time"
            self._draw_chart()
            return
        data = self.chart_data
        mode = self.x_axis_mode_var.get()
        data["mode"] = mode
        times = data.get("times", [])
        if not times:
            return
        if mode == "time":
            view_start = data.get("view_start", 0)
            view_end = data.get("view_end", len(times) - 1)
            data["view_start_time"] = times[view_start]
            data["view_end_time"] = times[view_end]
        else:
            start_time = data.get("view_start_time", times[0])
            end_time = data.get("view_end_time", times[-1])
            start_idx = bisect_left(times, start_time)
            end_idx = bisect_right(times, end_time) - 1
            start_idx = max(0, min(start_idx, len(times) - 1))
            end_idx = max(start_idx, min(end_idx, len(times) - 1))
            data["view_start"] = start_idx
            data["view_end"] = end_idx
        self._draw_chart()

    def _on_chart_type_change(self):
        chart_type = self.chart_type_var.get()
        if chart_type == "candle":
            self.x_axis_mode_var.set("time")
            self.axis_tick_radio.config(state="disabled")
        else:
            self.axis_tick_radio.config(state="normal")
        if self.chart_data:
            self.chart_data["mode"] = self.x_axis_mode_var.get()
            self._draw_chart()

    def _on_ma_filter_toggle(self):
        enabled = self.ma_filter_var.get()
        state = "normal" if enabled else "disabled"
        self.ma_period_entry.config(state=state)
        self.ma_deviation_entry.config(state=state)
        if self.chart_data:
            self._draw_chart()

    def _on_chart_visibility_change(self):
        if self.hide_chart_var.get():
            self.chart_info_var.set("チャート: 非表示")
            self.chart_canvas.grid_remove()
        else:
            self.chart_canvas.grid()
            if self.chart_data:
                self._draw_chart()

    def _get_plot_area(self, canvas=None):
        canvas = canvas or self.chart_canvas
        width = max(canvas.winfo_width(), 200)
        height = max(canvas.winfo_height(), 200)
        left = 10
        right = 70
        top = 10
        bottom = 30
        plot_width = max(1, width - left - right)
        plot_height = max(1, height - top - bottom)
        return width, height, left, top, right, bottom, plot_width, plot_height

    def _on_mouse_wheel(self, event):
        if not self.chart_data:
            return
        if hasattr(event, "delta") and event.delta:
            direction = 1 if event.delta > 0 else -1
        elif hasattr(event, "num"):
            direction = 1 if event.num == 4 else -1
        else:
            return

        data = self.chart_data
        mode = data.get("mode", "time")
        width, _height, left, _top, right, _bottom, plot_width, _plot_height = (
            self._get_plot_area()
        )
        if plot_width <= 1:
            return

        if left <= event.x <= width - right:
            ratio = (event.x - left) / plot_width
            ratio = min(max(ratio, 0.0), 1.0)
        else:
            ratio = 0.5

        if mode == "tick":
            total = len(data["all_points"])
            view_start = data["view_start"]
            view_end = data["view_end"]
            visible = view_end - view_start + 1
            if visible <= 2:
                return

            min_visible = 50
            if direction > 0:
                new_visible = max(min_visible, int(visible * 0.8))
            else:
                new_visible = min(total, int(visible * 1.25))

            anchor = view_start + int(ratio * (visible - 1))
            new_start = anchor - int(ratio * (new_visible - 1))
            new_start = max(0, min(new_start, total - new_visible))
            data["view_start"] = new_start
            data["view_end"] = new_start + new_visible - 1
        else:
            start_time = data["view_start_time"]
            end_time = data["view_end_time"]
            span = end_time - start_time
            if span.total_seconds() <= 0:
                return

            if direction > 0:
                new_span = span * 0.8
            else:
                new_span = span * 1.25

            min_span = timedelta(minutes=1)
            if new_span < min_span:
                new_span = min_span

            anchor_time = start_time + span * ratio
            new_start = anchor_time - new_span * ratio
            new_end = new_start + new_span

            min_time = data["times"][0]
            max_time = data["times"][-1]
            if new_start < min_time:
                new_start = min_time
                new_end = new_start + new_span
            if new_end > max_time:
                new_end = max_time
                new_start = new_end - new_span
            if new_start < min_time:
                new_start = min_time

            data["view_start_time"] = new_start
            data["view_end_time"] = new_end

        self._draw_chart()

    def _on_drag_start(self, event):
        if not self.chart_data:
            return
        self.drag_start_x = event.x
        data = self.chart_data
        if data.get("mode", "time") == "tick":
            self.drag_start_view = (data["view_start"], data["view_end"])
        else:
            self.drag_start_view = (data["view_start_time"], data["view_end_time"])

    def _on_drag_move(self, event):
        if not self.chart_data or self.drag_start_x is None or not self.drag_start_view:
            return
        width, _height, left, _top, right, _bottom, plot_width, _plot_height = (
            self._get_plot_area()
        )
        if plot_width <= 1:
            return
        data = self.chart_data
        dx = event.x - self.drag_start_x
        if data.get("mode", "time") == "tick":
            view_start, view_end = self.drag_start_view
            visible = view_end - view_start + 1
            shift = int(-dx / plot_width * visible)
            total = len(data["all_points"])
            new_start = max(0, min(view_start + shift, total - visible))
            data["view_start"] = new_start
            data["view_end"] = new_start + visible - 1
        else:
            view_start, view_end = self.drag_start_view
            span = view_end - view_start
            shift = -dx / plot_width
            new_start = view_start + span * shift
            new_end = view_end + span * shift
            min_time = data["times"][0]
            max_time = data["times"][-1]
            if new_start < min_time:
                new_start = min_time
                new_end = new_start + span
            if new_end > max_time:
                new_end = max_time
                new_start = new_end - span
            if new_start < min_time:
                new_start = min_time
            data["view_start_time"] = new_start
            data["view_end_time"] = new_end
        self._draw_chart()

    def _on_canvas_resize(self, _event):
        if self.chart_data:
            self._draw_chart()

    def _draw_chart(self):
        data = self.chart_data
        if not data:
            return
        if self.hide_chart_var.get():
            self.chart_info_var.set("チャート: 非表示")
            return
        points_all = data["all_points"]
        times = data["times"]
        mode = data.get("mode", "time")
        chart_type = self.chart_type_var.get() if hasattr(self, "chart_type_var") else "tick"
        if chart_type == "candle":
            mode = "time"
            data["mode"] = "time"
        start = data["start"]
        end = data["end"]
        missing_count = data["missing"]

        if mode == "tick":
            view_start = data["view_start"]
            view_end = data["view_end"]
            view_points = points_all[view_start : view_end + 1]
            view_start_time = times[view_start]
            view_end_time = times[view_end]
            view_start_idx = view_start
            view_end_idx = view_end
        else:
            view_start_time = data["view_start_time"]
            view_end_time = data["view_end_time"]
            start_idx = bisect_left(times, view_start_time)
            end_idx = bisect_right(times, view_end_time) - 1
            if end_idx < start_idx:
                view_points = []
            else:
                view_points = points_all[start_idx : end_idx + 1]
            view_start_idx = start_idx
            view_end_idx = end_idx

        if not view_points:
            self.chart_info_var.set("表示範囲にデータがありません。")
            canvas = self.chart_canvas
            canvas.delete("all")
            width, height, left, top, right, bottom, _plot_width, _plot_height = (
                self._get_plot_area()
            )
            canvas.create_rectangle(
                left, top, width - right, height - bottom, outline="#888888"
            )
            canvas.create_text(
                width // 2,
                height // 2,
                text="表示範囲にデータがありません。",
                fill="#666666",
            )
            return

        candles = None
        if chart_type == "candle":
            candles = build_minute_candles(view_points)
            if not candles:
                self.chart_info_var.set("表示範囲にデータがありません。")
                canvas = self.chart_canvas
                canvas.delete("all")
                width, height, left, top, right, bottom, _plot_width, _plot_height = (
                    self._get_plot_area()
                )
                canvas.create_rectangle(
                    left, top, width - right, height - bottom, outline="#888888"
                )
                canvas.create_text(
                    width // 2,
                    height // 2,
                    text="表示範囲にデータがありません。",
                    fill="#666666",
                )
                return

            highs = [h for _t, _o, h, _l, _c in candles]
            lows = [l for _t, _o, _h, l, _c in candles]
            min_p = min(lows)
            max_p = max(highs)
        else:
            prices_full = [p for _, p in view_points]
            min_p = min(prices_full)
            max_p = max(prices_full)

        if min_p == max_p:
            min_p -= 0.01
            max_p += 0.01

        if chart_type == "candle":
            info_text = (
                f"表示期間: {start.isoformat()}〜{end.isoformat()}  "
                f"件数: {data['count']}  "
                f"表示中: {len(candles)}本  "
                f"最小: {min_p:.3f}  最大: {max_p:.3f}"
            )
        else:
            info_text = (
                f"表示期間: {start.isoformat()}〜{end.isoformat()}  "
                f"件数: {data['count']}  "
                f"表示中: {len(view_points)}  "
                f"最小: {min_p:.3f}  最大: {max_p:.3f}"
            )
        info_text += "  横軸: 時間" if mode == "time" else "  横軸: 本数"
        if missing_count:
            info_text += f"  不足CSV: {missing_count}件"
        self.chart_info_var.set(info_text)

        canvas = self.chart_canvas
        canvas.delete("all")
        width, height, left, top, right, bottom, plot_width, plot_height = (
            self._get_plot_area()
        )

        canvas.create_rectangle(
            left, top, width - right, height - bottom, outline="#888888"
        )

        span_seconds = (view_end_time - view_start_time).total_seconds()
        n = len(view_points)

        def price_to_y(price):
            return (
                height
                - bottom
                - (price - min_p) / (max_p - min_p) * plot_height
            )

        if chart_type == "candle":
            if span_seconds <= 0:
                return
            minute_width = 60 / span_seconds * plot_width
            half_width = max(1.0, minute_width * 0.35)
            for ts, open_p, high_p, low_p, close_p in candles:
                if ts < view_start_time or ts > view_end_time:
                    continue
                x = left + (ts - view_start_time).total_seconds() / span_seconds * plot_width
                y_high = price_to_y(high_p)
                y_low = price_to_y(low_p)
                y_open = price_to_y(open_p)
                y_close = price_to_y(close_p)

                if close_p >= open_p:
                    color = "#2ca02c"
                else:
                    color = "#d62728"

                canvas.create_line(x, y_high, x, y_low, fill=color)

                top_y = min(y_open, y_close)
                bottom_y = max(y_open, y_close)
                if abs(bottom_y - top_y) < 1:
                    canvas.create_line(
                        x - half_width,
                        top_y,
                        x + half_width,
                        top_y,
                        fill=color,
                        width=2,
                    )
                else:
                    canvas.create_rectangle(
                        x - half_width,
                        top_y,
                        x + half_width,
                        bottom_y,
                        fill=color,
                        outline=color,
                    )
        else:
            if n < 2:
                return
            sampled = downsample_points(view_points, 5000)
            coords = []
            for idx, (ts, price) in sampled:
                if mode == "time" and span_seconds > 0:
                    x = left + (ts - view_start_time).total_seconds() / span_seconds * plot_width
                else:
                    x = left + idx / (n - 1) * plot_width
                y = price_to_y(price)
                coords.extend([x, y])

            if coords:
                canvas.create_line(coords, fill="#1f77b4", width=1)

        ma_series = data.get("ma_series") or []
        if self.ma_filter_var.get() and ma_series:
            ma_coords = []
            if mode == "time":
                if span_seconds > 0:
                    for ts, ma_value in ma_series:
                        if ts < view_start_time or ts > view_end_time:
                            continue
                        x = (
                            left
                            + (ts - view_start_time).total_seconds()
                            / span_seconds
                            * plot_width
                        )
                        y = price_to_y(ma_value)
                        ma_coords.extend([x, y])
            else:
                for ts, ma_value in ma_series:
                    idx = bisect_left(times, ts)
                    if idx < view_start_idx or idx > view_end_idx:
                        continue
                    if n <= 1:
                        continue
                    x = left + (idx - view_start_idx) / (n - 1) * plot_width
                    y = price_to_y(ma_value)
                    ma_coords.extend([x, y])

            if ma_coords:
                canvas.create_line(ma_coords, fill="#ff7f0e", width=1)

        trades = data.get("trades") or []
        if trades:
            def ensure_time(value):
                if isinstance(value, datetime):
                    return value
                if isinstance(value, str):
                    try:
                        return datetime.fromisoformat(value)
                    except ValueError:
                        return None
                return None

            def time_to_x(ts):
                if mode == "time":
                    if ts < view_start_time or ts > view_end_time:
                        return None
                    if span_seconds <= 0:
                        return None
                    return (
                        left
                        + (ts - view_start_time).total_seconds()
                        / span_seconds
                        * plot_width
                    )
                idx = bisect_left(times, ts)
                if idx < view_start_idx or idx > view_end_idx:
                    return None
                if n <= 1:
                    return None
                return left + (idx - view_start_idx) / (n - 1) * plot_width

            def draw_triangle(x, y, size, direction, color):
                if direction == "up":
                    points = [x, y - size, x - size, y + size, x + size, y + size]
                else:
                    points = [x, y + size, x - size, y - size, x + size, y - size]
                canvas.create_polygon(points, fill=color, outline=color)

            size = 6
            for trade in trades:
                entry_time = ensure_time(trade.get("entry_time"))
                exit_time = ensure_time(trade.get("exit_time"))
                entry_price = trade.get("entry_price")
                exit_price = trade.get("exit_price")
                side = trade.get("side")

                if (
                    entry_time is None
                    or exit_time is None
                    or entry_price is None
                    or exit_price is None
                ):
                    continue

                entry_x = time_to_x(entry_time)
                exit_x = time_to_x(exit_time)
                if entry_x is None or exit_x is None:
                    continue

                entry_y = price_to_y(entry_price)
                exit_y = price_to_y(exit_price)

                if side == "short":
                    color = "#d62728"
                    entry_dir = "down"
                    exit_dir = "up"
                else:
                    color = "#2ca02c"
                    entry_dir = "up"
                    exit_dir = "down"

                canvas.create_line(
                    entry_x,
                    entry_y,
                    exit_x,
                    exit_y,
                    fill=color,
                    dash=(4, 3),
                )
                draw_triangle(entry_x, entry_y, size, entry_dir, color)
                draw_triangle(exit_x, exit_y, size, exit_dir, color)

        ticks = 5
        for i in range(ticks + 1):
            y = top + plot_height * i / ticks
            value = max_p - (max_p - min_p) * i / ticks
            canvas.create_line(width - right, y, width - right + 4, y, fill="#333333")
            canvas.create_text(
                width - right + 6,
                y,
                text=f"{value:.3f}",
                anchor="w",
                fill="#333333",
            )

        time_ticks = 5
        for i in range(time_ticks + 1):
            ratio = i / time_ticks if time_ticks > 0 else 0
            if mode == "time" and span_seconds > 0:
                ts = view_start_time + (view_end_time - view_start_time) * ratio
                x = left + ratio * plot_width
            else:
                idx = int((n - 1) * ratio)
                ts = view_points[idx][0]
                x = left + idx / (n - 1) * plot_width
            label = ts.strftime("%m/%d %H:%M")
            canvas.create_line(x, height - bottom, x, height - bottom + 4, fill="#333333")
            canvas.create_text(
                x,
                height - bottom + 6,
                text=label,
                anchor="n",
                fill="#333333",
            )

    def _on_pnl_resize(self, _event):
        self._draw_pnl_chart()

    def _draw_pnl_chart(self):
        if not hasattr(self, "pnl_canvas"):
            return
        canvas = self.pnl_canvas
        canvas.delete("all")

        if not self.backtest_ready:
            message = self.pnl_info_var.get() or "まだ計算していません。"
            canvas.create_text(
                canvas.winfo_width() // 2,
                canvas.winfo_height() // 2,
                text=message,
                fill="#666666",
            )
            return

        if not self.pnl_data or len(self.pnl_data) < 2:
            canvas.create_text(
                canvas.winfo_width() // 2,
                canvas.winfo_height() // 2,
                text="取引がありません。",
                fill="#666666",
            )
            return

        times = [ts for ts, _v in self.pnl_data]
        values = [v for _ts, v in self.pnl_data]

        min_v = min(values)
        max_v = max(values)
        if min_v == max_v:
            min_v -= 1.0
            max_v += 1.0

        width, height, left, top, right, bottom, plot_width, plot_height = (
            self._get_plot_area(canvas)
        )
        canvas.create_rectangle(
            left, top, width - right, height - bottom, outline="#888888"
        )

        span_seconds = (times[-1] - times[0]).total_seconds()
        coords = []
        for ts, value in self.pnl_data:
            if span_seconds > 0:
                x = left + (ts - times[0]).total_seconds() / span_seconds * plot_width
            else:
                x = left + plot_width / 2
            y = height - bottom - (value - min_v) / (max_v - min_v) * plot_height
            coords.extend([x, y])

        if coords:
            canvas.create_line(coords, fill="#d62728", width=1)

        ticks = 5
        for i in range(ticks + 1):
            y = top + plot_height * i / ticks
            value = max_v - (max_v - min_v) * i / ticks
            canvas.create_line(width - right, y, width - right + 4, y, fill="#333333")
            canvas.create_text(
                width - right + 6,
                y,
                text=f"{value:.1f}",
                anchor="w",
                fill="#333333",
            )

        time_ticks = 5
        for i in range(time_ticks + 1):
            ratio = i / time_ticks if time_ticks > 0 else 0
            if span_seconds > 0:
                ts = times[0] + (times[-1] - times[0]) * ratio
                x = left + ratio * plot_width
            else:
                ts = times[0]
                x = left + plot_width / 2
            label = ts.strftime("%m/%d %H:%M")
            canvas.create_line(x, height - bottom, x, height - bottom + 4, fill="#333333")
            canvas.create_text(
                x,
                height - bottom + 6,
                text=label,
                anchor="n",
                fill="#333333",
            )


def main():
    root = tk.Tk()
    Step1App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
