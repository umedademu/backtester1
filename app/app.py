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


def freeze_value(value):
    if isinstance(value, dict):
        return tuple(sorted((k, freeze_value(v)) for k, v in value.items()))
    if isinstance(value, set):
        return tuple(sorted(freeze_value(v) for v in value))
    if isinstance(value, (list, tuple)):
        return tuple(freeze_value(v) for v in value)
    return value


def is_cancel_requested(check_fn):
    return bool(check_fn and check_fn())


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


def build_timeframe_candles(points, interval_minutes=1):
    if not points:
        return []
    interval_minutes = max(1, int(interval_minutes))
    candles = []
    current_time = None
    open_p = high_p = low_p = close_p = None

    for ts, price in points:
        total_minutes = ts.hour * 60 + ts.minute
        bucket_minutes = (total_minutes // interval_minutes) * interval_minutes
        bucket_hour = bucket_minutes // 60
        bucket_minute = bucket_minutes % 60
        bucket_time = ts.replace(
            hour=bucket_hour, minute=bucket_minute, second=0, microsecond=0
        )
        if current_time is None or bucket_time != current_time:
            if current_time is not None:
                candles.append((current_time, open_p, high_p, low_p, close_p))
            current_time = bucket_time
            open_p = high_p = low_p = close_p = price
        else:
            if price > high_p:
                high_p = price
            if price < low_p:
                low_p = price
            close_p = price

    if current_time is not None:
        candles.append((current_time, open_p, high_p, low_p, close_p))
    return candles


def build_minute_candles(points):
    return build_timeframe_candles(points, 1)


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


def build_minute_close_info(points):
    if not points:
        return [], [], []

    minute_times = []
    minute_close_prices = []
    minute_close_indices = []

    current_minute = None
    close_price = None
    close_idx = None

    for idx, (ts, bid) in enumerate(points):
        minute_start = ts.replace(second=0, microsecond=0)
        if current_minute is None or minute_start != current_minute:
            if current_minute is not None:
                minute_times.append(current_minute)
                minute_close_prices.append(close_price)
                minute_close_indices.append(close_idx)
            current_minute = minute_start
        close_price = bid
        close_idx = idx

    if current_minute is not None:
        minute_times.append(current_minute)
        minute_close_prices.append(close_price)
        minute_close_indices.append(close_idx)

    return minute_times, minute_close_prices, minute_close_indices


def build_range_band_segments(candles, lookback_bars=30):
    if not candles:
        return []

    lookback_bars = max(1, int(lookback_bars))

    segments = []
    active = None

    for idx in range(len(candles)):
        start_idx = max(0, idx - lookback_bars + 1)
        window = candles[start_idx : idx + 1]
        window_high = max(c[2] for c in window)
        window_low = min(c[3] for c in window)

        if active is None:
            active = {
                "high": window_high,
                "low": window_low,
                "start_idx": idx,
            }
        else:
            if (
                abs(window_high - active["high"]) > 1e-9
                or abs(window_low - active["low"]) > 1e-9
            ):
                end_idx = idx - 1
                if end_idx >= active["start_idx"]:
                    segments.append(
                        {
                            "start_time": candles[active["start_idx"]][0],
                            "end_time": candles[end_idx][0],
                            "high": active["high"],
                            "low": active["low"],
                        }
                    )
                active = {
                    "high": window_high,
                    "low": window_low,
                    "start_idx": idx,
                }

    if active is not None:
        end_idx = len(candles) - 1
        if end_idx >= active["start_idx"]:
            segments.append(
                {
                    "start_time": candles[active["start_idx"]][0],
                    "end_time": candles[end_idx][0],
                    "high": active["high"],
                    "low": active["low"],
                }
            )
    return segments


def build_zigzag_points(candles, zigzag_pips=5.0, min_bars=5):
    if not candles:
        return []

    threshold = zigzag_pips * PIP_SIZE
    min_bars = max(1, int(min_bars))

    points = []

    def has_long_wick(candle, kind):
        _ts, open_p, high, low, close = candle
        upper = high - max(open_p, close)
        lower = min(open_p, close) - low
        if kind == "resistance":
            return upper >= threshold
        return lower >= threshold

    candidate_high = candles[0][2]
    candidate_low = candles[0][3]
    candidate_high_idx = 0
    candidate_low_idx = 0

    direction = None
    extreme_price = None
    extreme_idx = None
    last_confirm_idx = None

    for idx in range(1, len(candles)):
        _ts, _o, high, low, _close = candles[idx]

        if direction is None:
            if high > candidate_high:
                candidate_high = high
                candidate_high_idx = idx
            if low < candidate_low:
                candidate_low = low
                candidate_low_idx = idx

            if candidate_high - candidate_low >= threshold:
                if (
                    abs(candidate_high_idx - candidate_low_idx) >= min_bars
                    or has_long_wick(
                        candles[candidate_low_idx]
                        if candidate_high_idx > candidate_low_idx
                        else candles[candidate_high_idx],
                        "support" if candidate_high_idx > candidate_low_idx else "resistance",
                    )
                ):
                    if candidate_high_idx > candidate_low_idx:
                        points.append((candles[idx][0], candidate_low))
                        direction = "up"
                        extreme_price = candidate_high
                        extreme_idx = candidate_high_idx
                    else:
                        points.append((candles[idx][0], candidate_high))
                        direction = "down"
                        extreme_price = candidate_low
                        extreme_idx = candidate_low_idx
                    last_confirm_idx = idx
        elif direction == "up":
            if high > extreme_price:
                extreme_price = high
                extreme_idx = idx
            if extreme_price - low >= threshold:
                if (
                    last_confirm_idx is None
                    or idx - last_confirm_idx >= min_bars
                    or has_long_wick(candles[extreme_idx], "resistance")
                ):
                    points.append((candles[idx][0], extreme_price))
                    direction = "down"
                    extreme_price = low
                    extreme_idx = idx
                    last_confirm_idx = idx
        else:
            if low < extreme_price:
                extreme_price = low
                extreme_idx = idx
            if high - extreme_price >= threshold:
                if (
                    last_confirm_idx is None
                    or idx - last_confirm_idx >= min_bars
                    or has_long_wick(candles[extreme_idx], "support")
                ):
                    points.append((candles[idx][0], extreme_price))
                    direction = "up"
                    extreme_price = high
                    extreme_idx = idx
                    last_confirm_idx = idx

    if extreme_idx is not None:
        last_time = candles[-1][0]
        if not points or points[-1][0] != last_time:
            points.append((last_time, extreme_price))

    return points


def build_zigzag_sr_segments(candles, zigzag_pips=5.0, break_pips=1.0, min_bars=5):
    if not candles:
        return []

    threshold = zigzag_pips * PIP_SIZE
    break_threshold = break_pips * PIP_SIZE
    min_bars = max(1, int(min_bars))

    segments = []
    active = []

    def has_long_wick(candle, kind):
        _ts, open_p, high, low, close = candle
        upper = high - max(open_p, close)
        lower = min(open_p, close) - low
        if kind == "resistance":
            return upper >= threshold
        return lower >= threshold

    def add_segment(level, end_idx):
        segments.append(
            {
                "price": level["price"],
                "kind": level["kind"],
                "start_time": level["start_time"],
                "end_time": candles[end_idx][0],
            }
        )

    def add_level(kind, price, confirm_idx):
        active.append(
            {
                "kind": kind,
                "price": price,
                "start_time": candles[confirm_idx][0],
                "start_index": confirm_idx,
            }
        )

    candidate_high = candles[0][2]
    candidate_low = candles[0][3]
    candidate_high_idx = 0
    candidate_low_idx = 0

    direction = None
    extreme_price = None
    extreme_idx = None
    last_confirm_idx = None

    for idx in range(1, len(candles)):
        _ts, _o, high, low, close = candles[idx]

        if direction is None:
            if high > candidate_high:
                candidate_high = high
                candidate_high_idx = idx
            if low < candidate_low:
                candidate_low = low
                candidate_low_idx = idx

            if candidate_high - candidate_low >= threshold:
                if (
                    abs(candidate_high_idx - candidate_low_idx) >= min_bars
                    or has_long_wick(
                        candles[candidate_low_idx]
                        if candidate_high_idx > candidate_low_idx
                        else candles[candidate_high_idx],
                        "support" if candidate_high_idx > candidate_low_idx else "resistance",
                    )
                ):
                    if candidate_high_idx > candidate_low_idx:
                        add_level("support", candidate_low, idx)
                        direction = "up"
                        extreme_price = candidate_high
                        extreme_idx = candidate_high_idx
                    else:
                        add_level("resistance", candidate_high, idx)
                        direction = "down"
                        extreme_price = candidate_low
                        extreme_idx = candidate_low_idx
                    last_confirm_idx = idx
        elif direction == "up":
            if high > extreme_price:
                extreme_price = high
                extreme_idx = idx
            if extreme_price - low >= threshold:
                if (
                    last_confirm_idx is None
                    or idx - last_confirm_idx >= min_bars
                    or has_long_wick(candles[extreme_idx], "resistance")
                ):
                    add_level("resistance", extreme_price, idx)
                    direction = "down"
                    extreme_price = low
                    extreme_idx = idx
                    last_confirm_idx = idx
        else:
            if low < extreme_price:
                extreme_price = low
                extreme_idx = idx
            if high - extreme_price >= threshold:
                if (
                    last_confirm_idx is None
                    or idx - last_confirm_idx >= min_bars
                    or has_long_wick(candles[extreme_idx], "support")
                ):
                    add_level("support", extreme_price, idx)
                    direction = "up"
                    extreme_price = high
                    extreme_idx = idx
                    last_confirm_idx = idx

        if active:
            for li, level in enumerate(active):
                if idx <= level["start_index"]:
                    continue
                if level["kind"] == "support":
                    if low < level["price"] - break_threshold:
                        add_segment(level, idx)
                        level["price"] = low
                        level["start_time"] = candles[idx][0]
                        level["start_index"] = idx
                else:
                    if high > level["price"] + break_threshold:
                        add_segment(level, idx)
                        level["price"] = high
                        level["start_time"] = candles[idx][0]
                        level["start_index"] = idx

    last_idx = len(candles) - 1
    for level in active:
        segments.append(
            {
                "price": level["price"],
                "kind": level["kind"],
                "start_time": level["start_time"],
                "end_time": candles[last_idx][0],
            }
        )
    return segments


def find_spike_signal(
    points, times, start_idx, window, spike, retrace_rate, should_cancel=None
):
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
        if is_cancel_requested(should_cancel):
            raise InterruptedError("cancelled")
        price = points[j][1]

        if price < min_price:
            min_price = price
            min_idx = j

        drop = p0 - min_price
        if drop >= spike and j >= min_idx:
            retrace_level = min_price + drop * retrace_rate
            if price >= retrace_level:
                return {
                    "entry_idx": j,
                    "side": "long",
                    "extreme_idx": min_idx,
                    "extreme_price": min_price,
                }

        if price > max_price:
            max_price = price
            max_idx = j

        rise = max_price - p0
        if rise >= spike and j >= max_idx:
            retrace_level = max_price - rise * retrace_rate
            if price <= retrace_level:
                return {
                    "entry_idx": j,
                    "side": "short",
                    "extreme_idx": max_idx,
                    "extreme_price": max_price,
                }

    return None


def build_reentry_lines(candles, sr_params, range_params, target_type):
    if not candles:
        return []
    sr_params = sr_params or {}
    range_params = range_params or {}

    lines = []
    if target_type in ("sr", "both"):
        segments = build_zigzag_sr_segments(candles, **sr_params)
        for seg in segments:
            price = seg.get("price")
            kind = seg.get("kind")
            start_time = seg.get("start_time")
            end_time = seg.get("end_time")
            if price is None or kind is None or start_time is None or end_time is None:
                continue
            lines.append(
                {
                    "price": price,
                    "kind": kind,
                    "start_time": start_time,
                    "end_time": end_time,
                    "source": "sr",
                }
            )

    if target_type in ("range", "both"):
        lookback_bars = range_params.get("lookback_bars", 30)
        range_segments = build_range_band_segments(candles, lookback_bars=lookback_bars)
        for seg in range_segments:
            high = seg.get("high")
            low = seg.get("low")
            start_time = seg.get("start_time")
            end_time = seg.get("end_time")
            if (
                high is None
                or low is None
                or start_time is None
                or end_time is None
            ):
                continue
            lines.append(
                {
                    "price": high,
                    "kind": "resistance",
                    "start_time": start_time,
                    "end_time": end_time,
                    "source": "range",
                }
            )
            lines.append(
                {
                    "price": low,
                    "kind": "support",
                    "start_time": start_time,
                    "end_time": end_time,
                    "source": "range",
                }
            )

    if lines:
        unique = {}
        for line in lines:
            key = (
                line["price"],
                line["kind"],
                line["start_time"],
                line["end_time"],
                line.get("source"),
            )
            unique[key] = line
        lines = list(unique.values())

    lines.sort(key=lambda x: x["start_time"])
    return lines


def build_line_bins(lines, max_break, line_start_times=None):
    bin_size = max(max_break, PIP_SIZE)
    bins = {}
    for idx, line in enumerate(lines):
        price = line["price"]
        bin_idx = int(price / bin_size)
        bins.setdefault(bin_idx, []).append(idx)
    if line_start_times is not None:
        for bin_idx, idx_list in bins.items():
            idx_list.sort(key=lambda i: line_start_times[i])
    return bin_size, bins


def find_sr_reentry_signal(
    points,
    start_idx,
    lines,
    max_break,
    tick_limit,
    tick_min,
    line_bins=None,
    bin_size=None,
    bin_state=None,
    line_start_times=None,
    disabled_lines=None,
    end_limits=None,
    start_limits=None,
    min_seconds=0.0,
    max_seconds=60.0,
    midpoint_pct=50.0,
    dominance_pct=50.0,
    should_cancel=None,
):
    if not points or not lines:
        return None
    if line_bins is None or bin_size is None:
        bin_size, line_bins = build_line_bins(lines, max_break)
    if bin_state is None:
        bin_state = {}
    if line_start_times is None:
        line_start_times = [line["start_time"] for line in lines]
    if disabled_lines is None:
        disabled_lines = set()

    midpoint_ratio = midpoint_pct / 100.0
    if midpoint_ratio < 0:
        midpoint_ratio = 0.0
    elif midpoint_ratio > 1:
        midpoint_ratio = 1.0
    dominance_ratio = dominance_pct / 100.0
    if dominance_ratio < 0:
        dominance_ratio = 0.0
    elif dominance_ratio > 1:
        dominance_ratio = 1.0

    n = len(points)
    active_states = {}
    prev_bid = points[start_idx - 1][1] if start_idx > 0 else points[start_idx][1]

    for j in range(start_idx, n):
        if is_cancel_requested(should_cancel):
            raise InterruptedError("cancelled")
        ts, bid = points[j]

        if active_states:
            finished = []
            for idx, state in list(active_states.items()):
                line = lines[idx]
                level = line["price"]
                kind = line["kind"]

                elapsed = (ts - state["last_time"]).total_seconds()
                if elapsed > 0:
                    threshold = level + (state["extreme_price"] - level) * midpoint_ratio
                    last_bid = state["last_bid"]
                    if last_bid > threshold:
                        state["stay_above"] += elapsed
                    elif last_bid < threshold:
                        state["stay_below"] += elapsed

                start_time = state["start_time"]
                tick_count = state["tick_count"]
                duration = (ts - start_time).total_seconds()
                if duration > max_seconds:
                    finished.append(idx)
                    continue
                if kind == "resistance":
                    if bid > level:
                        if bid > level + max_break:
                            finished.append(idx)
                        else:
                            state["tick_count"] = tick_count + 1
                            if bid > state["extreme_price"]:
                                state["extreme_price"] = bid
                            state["last_time"] = ts
                            state["last_bid"] = bid
                    elif bid < level:
                        if duration >= min_seconds:
                            denom = duration if duration > 0 else 0.001
                            avg_per_min = tick_count / denom * 60.0
                            total_stay = state["stay_below"] + state["stay_above"]
                            below_ratio = (
                                state["stay_below"] / total_stay
                                if total_stay > 0
                                else 0.0
                            )
                            if (
                                tick_min <= avg_per_min <= tick_limit
                                and below_ratio >= dominance_ratio
                            ):
                                disabled_lines.add(idx)
                                return {
                                    "entry_idx": j,
                                    "side": "short",
                                    "line_price": level,
                                    "line_kind": kind,
                                    "line_source": line.get("source"),
                                    "tick_count": tick_count,
                                    "stay_above": state["stay_above"],
                                    "stay_below": state["stay_below"],
                                }
                        finished.append(idx)
                    else:
                        state["tick_count"] = tick_count + 1
                        state["last_time"] = ts
                        state["last_bid"] = bid
                else:
                    if bid < level:
                        if bid < level - max_break:
                            finished.append(idx)
                        else:
                            state["tick_count"] = tick_count + 1
                            if bid < state["extreme_price"]:
                                state["extreme_price"] = bid
                            state["last_time"] = ts
                            state["last_bid"] = bid
                    elif bid > level:
                        if duration >= min_seconds:
                            denom = duration if duration > 0 else 0.001
                            avg_per_min = tick_count / denom * 60.0
                            total_stay = state["stay_below"] + state["stay_above"]
                            above_ratio = (
                                state["stay_above"] / total_stay
                                if total_stay > 0
                                else 0.0
                            )
                            if (
                                tick_min <= avg_per_min <= tick_limit
                                and above_ratio >= dominance_ratio
                            ):
                                disabled_lines.add(idx)
                                return {
                                    "entry_idx": j,
                                    "side": "long",
                                    "line_price": level,
                                    "line_kind": kind,
                                    "line_source": line.get("source"),
                                    "tick_count": tick_count,
                                    "stay_above": state["stay_above"],
                                    "stay_below": state["stay_below"],
                                }
                        finished.append(idx)
                    else:
                        state["tick_count"] = tick_count + 1
                        state["last_time"] = ts
                        state["last_bid"] = bid
            for idx in finished:
                active_states.pop(idx, None)
                disabled_lines.add(idx)

        low = min(prev_bid, bid) - max_break
        high = max(prev_bid, bid) + max_break
        low_bin = int(low / bin_size)
        high_bin = int(high / bin_size)
        for bin_idx in range(low_bin, high_bin + 1):
            if bin_idx not in line_bins:
                continue
            entry = bin_state.get(bin_idx)
            if entry is None:
                idx_list = list(line_bins.get(bin_idx, []))
                entry = {"indices": idx_list, "cursor": 0, "active": []}
                bin_state[bin_idx] = entry

            idx_list = entry["indices"]
            cursor = entry["cursor"]
            while cursor < len(idx_list) and line_start_times[idx_list[cursor]] <= ts:
                entry["active"].append(idx_list[cursor])
                cursor += 1
            entry["cursor"] = cursor

            if not entry["active"]:
                continue

            new_active = []
            for idx in entry["active"]:
                if idx in active_states or idx in disabled_lines:
                    continue
                if end_limits is not None and ts > end_limits[idx]:
                    disabled_lines.add(idx)
                    continue
                if start_limits is not None and ts < start_limits[idx]:
                    new_active.append(idx)
                    continue
                line = lines[idx]
                level = line["price"]
                kind = line["kind"]
                if kind == "resistance":
                    if prev_bid <= level and bid > level:
                        if bid > level + max_break:
                            disabled_lines.add(idx)
                            continue
                        active_states[idx] = {
                            "start_time": ts,
                            "tick_count": 1,
                            "extreme_price": bid,
                            "stay_above": 0.0,
                            "stay_below": 0.0,
                            "last_time": ts,
                            "last_bid": bid,
                        }
                        continue
                else:
                    if prev_bid >= level and bid < level:
                        if bid < level - max_break:
                            disabled_lines.add(idx)
                            continue
                        active_states[idx] = {
                            "start_time": ts,
                            "tick_count": 1,
                            "extreme_price": bid,
                            "stay_above": 0.0,
                            "stay_below": 0.0,
                            "last_time": ts,
                            "last_bid": bid,
                        }
                        continue
                new_active.append(idx)
            entry["active"] = new_active

        prev_bid = bid

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


def simulate_exit(
    points,
    entry_idx,
    side,
    entry_price,
    spread,
    stop,
    take,
    time_close_minutes=0.0,
    minute_close_info=None,
    should_cancel=None,
):
    if side == "long":
        stop_price = entry_price - stop
        take_price = entry_price + take
    else:
        stop_price = entry_price + stop
        take_price = entry_price - take

    forced_close_time = None
    if time_close_minutes and time_close_minutes > 0:
        forced_close_time = points[entry_idx][0] + timedelta(minutes=time_close_minutes)

    minute_times = []
    minute_close_prices = []
    minute_close_indices = []
    if minute_close_info:
        minute_times, minute_close_prices, minute_close_indices = minute_close_info

    n = len(points)
    exit_idx = None
    exit_price = None
    exit_reason = None
    j = entry_idx + 1
    while j < n:
        if is_cancel_requested(should_cancel):
            raise InterruptedError("cancelled")
        _t, bid = points[j]
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
        if forced_close_time is not None and _t >= forced_close_time:
            close_bid = bid
            close_idx = j
            if minute_times:
                minute_start = _t.replace(second=0, microsecond=0)
                minute_pos = bisect_left(minute_times, minute_start)
                if (
                    0 <= minute_pos < len(minute_times)
                    and minute_times[minute_pos] == minute_start
                ):
                    close_bid = minute_close_prices[minute_pos]
                    close_idx = minute_close_indices[minute_pos]
            exit_idx = close_idx
            exit_price = close_bid if side == "long" else close_bid + spread
            exit_reason = "時間"
            break
        j += 1

    if exit_idx is None:
        exit_idx = n - 1
        _t, last_bid = points[-1]
        exit_price = last_bid + spread if side == "short" else last_bid
        exit_reason = "終了"

    return exit_idx, exit_price, exit_reason


def run_backtest(points, params, runtime_cache=None, should_cancel=None):
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

    if runtime_cache is None:
        runtime_cache = {}

    if is_cancel_requested(should_cancel):
        raise InterruptedError("cancelled")

    if runtime_cache.get("points_ref") is points:
        points_sorted = runtime_cache.get("points_sorted") or points
        times = runtime_cache.get("times") or [ts for ts, _ in points_sorted]
    else:
        points_sorted = sorted(points, key=lambda x: x[0])
        times = [ts for ts, _ in points_sorted]
        runtime_cache.clear()
        runtime_cache["points_ref"] = points
        runtime_cache["points_sorted"] = points_sorted
        runtime_cache["times"] = times
        runtime_cache["candle_cache"] = {}
        runtime_cache["ma_cache"] = {}
        runtime_cache["line_cache"] = {}
        runtime_cache["line_bin_cache"] = {}

    window = timedelta(milliseconds=params["window_ms"])
    spike = params["spike_pips"] * PIP_SIZE
    retrace_rate = params["retrace_rate"]
    spread = params["spread_pips"] * PIP_SIZE
    stop = params["stop_pips"] * PIP_SIZE
    take = params["take_pips"] * PIP_SIZE
    time_close_minutes = float(params.get("time_close_minutes", 0.0))
    ma_enabled = params.get("ma_enabled", False)
    ma_period = max(1, int(params.get("ma_period", 0)))
    ma_deviation = params.get("ma_deviation_rate", 0.0)
    extreme_enabled = params.get("extreme_enabled", False)
    extreme_hold_ms = params.get("extreme_hold_ms", 0.0)
    extreme_distance = params.get("extreme_distance_pips", 0.0) * PIP_SIZE
    exclude_enabled = params.get("exclude_enabled", False)
    exclude_hours = params.get("exclude_hours", set())

    candle_times = []
    ma_values = []
    ma_series = []
    if ma_enabled:
        ma_cache = runtime_cache.setdefault("ma_cache", {})
        ma_entry = ma_cache.get(ma_period)
        if ma_entry is None:
            candle_cache = runtime_cache.setdefault("candle_cache", {})
            minute_candles = candle_cache.get(1)
            if minute_candles is None:
                minute_candles = build_minute_candles(points_sorted)
                candle_cache[1] = minute_candles
            ma_entry = build_minute_ma(minute_candles, ma_period)
            ma_cache[ma_period] = ma_entry
        candle_times, ma_values, ma_series = ma_entry

    minute_close_info = None
    if time_close_minutes > 0:
        minute_close_info = runtime_cache.get("minute_close_info")
        if minute_close_info is None:
            minute_close_info = build_minute_close_info(points_sorted)
            runtime_cache["minute_close_info"] = minute_close_info

    entry_mode = params.get("entry_mode", "spike")

    trades = []
    equity_curve = [(times[0], 0.0)]
    cumulative = 0.0

    i = 0
    n = len(points_sorted)
    if entry_mode == "sr_reentry":
        sr_break_pips = params.get("sr_break_pips", 5.0)
        sr_tick_limit = int(params.get("sr_tick_limit", 10))
        sr_tick_min = float(params.get("sr_tick_min", 0.0))
        sr_wait_bars = int(params.get("sr_wait_bars", 0))
        sr_min_seconds = float(params.get("sr_min_seconds", 0.0))
        sr_max_seconds = float(params.get("sr_max_seconds", 60.0))
        sr_midpoint_pct = float(params.get("sr_midpoint_pct", 50.0))
        sr_dominance_pct = float(params.get("sr_dominance_pct", 50.0))
        sr_target = params.get("sr_target", "both")
        line_interval = max(1, int(params.get("line_interval", 1)))
        sr_params = params.get("sr_params") or {}
        range_params = params.get("range_params") or {}
        line_key = (
            line_interval,
            sr_target,
            freeze_value(sr_params),
            freeze_value(range_params),
        )
        line_cache = runtime_cache.setdefault("line_cache", {})
        line_entry = line_cache.get(line_key)
        if line_entry is None:
            candle_cache = runtime_cache.setdefault("candle_cache", {})
            line_candles = candle_cache.get(line_interval)
            if line_candles is None:
                line_candles = build_timeframe_candles(points_sorted, line_interval)
                candle_cache[line_interval] = line_candles
            lines = build_reentry_lines(line_candles, sr_params, range_params, sr_target)
            line_start_times = [line["start_time"] for line in lines]
            end_limits_base = [
                line["end_time"] + timedelta(minutes=line_interval) for line in lines
            ]
            line_entry = {
                "lines": lines,
                "line_start_times": line_start_times,
                "end_limits_base": end_limits_base,
                "start_limits_cache": {},
            }
            line_cache[line_key] = line_entry

        lines = line_entry["lines"]
        line_start_times = line_entry["line_start_times"]
        end_limits = line_entry["end_limits_base"]
        max_break = sr_break_pips * PIP_SIZE
        bin_key = (line_key, round(max_break, 10))
        line_bin_cache = runtime_cache.setdefault("line_bin_cache", {})
        bin_entry = line_bin_cache.get(bin_key)
        if bin_entry is None:
            bin_size, line_bins = build_line_bins(lines, max_break, line_start_times)
            bin_entry = {
                "bin_size": bin_size,
                "line_bins": line_bins,
            }
            line_bin_cache[bin_key] = bin_entry
        else:
            bin_size = bin_entry["bin_size"]
            line_bins = bin_entry["line_bins"]

        bin_state = {}
        disabled_lines = set()
        start_limits_cache = line_entry.setdefault("start_limits_cache", {})
        start_limits = start_limits_cache.get(sr_wait_bars)
        if start_limits is None:
            wait_delta = timedelta(minutes=line_interval * sr_wait_bars)
            start_limits = [start_time + wait_delta for start_time in line_start_times]
            start_limits_cache[sr_wait_bars] = start_limits

        while i < n - 1:
            if is_cancel_requested(should_cancel):
                raise InterruptedError("cancelled")
            signal = find_sr_reentry_signal(
                points_sorted,
                i,
                lines,
                max_break,
                sr_tick_limit,
                sr_tick_min,
                line_bins,
                bin_size,
                bin_state,
                line_start_times,
                disabled_lines,
                end_limits,
                start_limits,
                sr_min_seconds,
                sr_max_seconds,
                sr_midpoint_pct,
                sr_dominance_pct,
                should_cancel,
            )
            if not signal:
                break

            entry_idx = signal["entry_idx"]
            side = signal["side"]
            entry_time, entry_bid = points_sorted[entry_idx]
            entry_price = entry_bid + spread if side == "long" else entry_bid

            if exclude_enabled and entry_time.hour in exclude_hours:
                i = entry_idx + 1
                continue

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

            exit_idx, exit_price, exit_reason = simulate_exit(
                points_sorted,
                entry_idx,
                side,
                entry_price,
                spread,
                stop,
                take,
                time_close_minutes,
                minute_close_info,
                should_cancel,
            )

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
                    "line_price": signal.get("line_price"),
                    "line_kind": signal.get("line_kind"),
                    "line_source": signal.get("line_source"),
                    "tick_count": signal.get("tick_count"),
                    "stay_above_sec": signal.get("stay_above"),
                    "stay_below_sec": signal.get("stay_below"),
                }
            )

            cumulative += pips
            equity_curve.append((points_sorted[exit_idx][0], cumulative))
            i = exit_idx + 1
    else:
        while i < n - 1:
            if is_cancel_requested(should_cancel):
                raise InterruptedError("cancelled")
            signal = find_spike_signal(
                points_sorted,
                times,
                i,
                window,
                spike,
                retrace_rate,
                should_cancel,
            )
            if not signal:
                i += 1
                continue

            entry_idx = signal["entry_idx"]
            side = signal["side"]
            extreme_idx = signal["extreme_idx"]
            extreme_price = signal["extreme_price"]
            extreme_time = points_sorted[extreme_idx][0]

            if extreme_enabled and extreme_hold_ms > 0:
                hold_limit = extreme_time + timedelta(milliseconds=extreme_hold_ms)
                hold_idx = bisect_left(times, hold_limit, extreme_idx, n)
                if hold_idx >= n:
                    i = entry_idx + 1
                    continue
                breached = False
                for k in range(extreme_idx + 1, hold_idx + 1):
                    price_k = points_sorted[k][1]
                    if side == "long":
                        if price_k < extreme_price:
                            breached = True
                            break
                    else:
                        if price_k > extreme_price:
                            breached = True
                            break
                if breached:
                    i = entry_idx + 1
                    continue
                if hold_idx > entry_idx:
                    entry_idx = hold_idx

            entry_time, entry_bid = points_sorted[entry_idx]
            entry_price = entry_bid + spread if side == "long" else entry_bid

            if exclude_enabled and entry_time.hour in exclude_hours:
                i = entry_idx + 1
                continue

            if extreme_enabled and extreme_distance > 0:
                if side == "long":
                    distance = entry_price - extreme_price
                else:
                    distance = extreme_price - entry_price
                if distance > extreme_distance:
                    i = entry_idx + 1
                    continue

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

            exit_idx, exit_price, exit_reason = simulate_exit(
                points_sorted,
                entry_idx,
                side,
                entry_price,
                spread,
                stop,
                take,
                time_close_minutes,
                minute_close_info,
                should_cancel,
            )

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
        "entry_mode": entry_mode,
        "sr_target": params.get("sr_target"),
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
        self.chart_cancel_event = threading.Event()

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
        self.candle_interval_var = tk.IntVar(value=1)
        self.entry_mode_var = tk.StringVar(value="sr_reentry")
        self.hide_chart_var = tk.BooleanVar(value=False)
        self.ma_filter_var = tk.BooleanVar(value=True)
        self.ma_period_var = tk.StringVar(value="200")
        self.ma_deviation_var = tk.StringVar(value="0.01")
        self.zigzag_show_var = tk.BooleanVar(value=False)
        self.range_band_show_var = tk.BooleanVar(value=False)
        self.range_band_bars_var = tk.StringVar(value="30")
        self.cursor_info_var = tk.StringVar(value="")
        self.extreme_filter_var = tk.BooleanVar(value=False)
        self.extreme_hold_ms_var = tk.StringVar(value="0")
        self.extreme_distance_pips_var = tk.StringVar(value="0")
        self.backtest_exclude_var = tk.BooleanVar(value=True)
        self.backtest_exclude_hours_vars = [
            tk.BooleanVar(value=5 <= i <= 10) for i in range(24)
        ]
        self.backtest_exclude_label_var = tk.StringVar(value="除外時間: なし")
        self.spike_window_var = tk.StringVar(value="500")
        self.spike_pips_var = tk.StringVar(value="1.0")
        self.retrace_var = tk.StringVar(value="90")
        self.spread_var = tk.StringVar(value="1.0")
        self.stop_pips_var = tk.StringVar(value="5.0")
        self.take_pips_var = tk.StringVar(value="5.0")
        self.time_close_minutes_var = tk.StringVar(value="0")
        self.sr_zigzag_pips_var = tk.StringVar(value="10.0")
        self.sr_break_pips_var = tk.StringVar(value="0.01")
        self.sr_min_bars_var = tk.StringVar(value="10")
        self.sr_reentry_break_pips_var = tk.StringVar(value="5.0")
        self.sr_reentry_tick_limit_var = tk.StringVar(value="100")
        self.sr_reentry_tick_min_var = tk.StringVar(value="0")
        self.sr_reentry_wait_bars_var = tk.StringVar(value="3")
        self.sr_reentry_min_seconds_var = tk.StringVar(value="5")
        self.sr_reentry_max_seconds_var = tk.StringVar(value="60")
        self.sr_reentry_midpoint_var = tk.StringVar(value="50")
        self.sr_reentry_dominance_var = tk.StringVar(value="50")
        self.sr_reentry_target_var = tk.StringVar(value="両方")
        self.backtest_info_var = tk.StringVar(value="バックテスト: 未実行")
        self.pnl_info_var = tk.StringVar(value="損益: 未実行")
        self.pnl_data = None
        self.backtest_ready = False
        self.analysis_cache_key = None
        self.analysis_cache = None

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
        chart_tab.rowconfigure(7, weight=1)

        ttk.Label(chart_tab, text="表示期間（JST）").grid(row=0, column=0, sticky="w")

        view_row = ttk.Frame(chart_tab)
        view_row.grid(row=1, column=0, sticky="ew")
        view_row.columnconfigure(7, weight=1)

        ttk.Label(view_row, text="開始日（JST）").grid(row=0, column=0, sticky="w")
        view_start_entry = ttk.Entry(
            view_row, textvariable=self.view_start_var, width=12, state="readonly"
        )
        view_start_entry.grid(row=0, column=1, padx=6)
        ttk.Button(view_row, text="選択", command=self._pick_view_start).grid(
            row=0, column=2
        )

        ttk.Label(view_row, text="終了日（JST）").grid(row=0, column=3, padx=(12, 0), sticky="w")
        view_end_entry = ttk.Entry(
            view_row, textvariable=self.view_end_var, width=12, state="readonly"
        )
        view_end_entry.grid(row=0, column=4, padx=6)
        ttk.Button(view_row, text="選択", command=self._pick_view_end).grid(
            row=0, column=5
        )

        chart_controls = ttk.Frame(chart_tab)
        chart_controls.grid(row=2, column=0, sticky="ew", pady=(4, 4))
        chart_controls.columnconfigure(4, weight=1)

        self.chart_button = ttk.Button(chart_controls, text="表示", command=self._show_chart)
        self.chart_button.grid(row=0, column=0, sticky="w")
        self.chart_cancel_button = ttk.Button(
            chart_controls,
            text="中止",
            command=self._cancel_chart,
            state="disabled",
        )
        self.chart_cancel_button.grid(row=0, column=1, padx=(6, 0), sticky="w")

        ttk.Label(chart_controls, text="横軸").grid(row=0, column=2, padx=(12, 4), sticky="w")
        self.axis_time_radio = ttk.Radiobutton(
            chart_controls,
            text="時間",
            variable=self.x_axis_mode_var,
            value="time",
            command=self._on_axis_mode_change,
        )
        self.axis_time_radio.grid(row=0, column=3, sticky="w")
        self.axis_tick_radio = ttk.Radiobutton(
            chart_controls,
            text="本数",
            variable=self.x_axis_mode_var,
            value="tick",
            command=self._on_axis_mode_change,
        )
        self.axis_tick_radio.grid(row=0, column=4, sticky="w")

        ttk.Label(chart_controls, text="表示").grid(row=1, column=2, padx=(12, 4), sticky="w")
        self.chart_tick_radio = ttk.Radiobutton(
            chart_controls,
            text="ティック",
            variable=self.chart_type_var,
            value="tick",
            command=self._on_chart_type_change,
        )
        self.chart_tick_radio.grid(row=1, column=3, sticky="w")
        self.chart_candle_radio = ttk.Radiobutton(
            chart_controls,
            text="足",
            variable=self.chart_type_var,
            value="candle",
            command=self._on_chart_type_change,
        )
        self.chart_candle_radio.grid(row=1, column=4, sticky="w")

        self.hide_chart_check = ttk.Checkbutton(
            chart_controls,
            text="チャート非表示",
            variable=self.hide_chart_var,
            command=self._on_chart_visibility_change,
        )
        self.hide_chart_check.grid(row=0, column=5, padx=(12, 0), sticky="w")

        self.zigzag_check = ttk.Checkbutton(
            chart_controls,
            text="ジグザグ表示",
            variable=self.zigzag_show_var,
            command=self._on_zigzag_toggle,
        )
        self.zigzag_check.grid(row=1, column=5, padx=(12, 0), sticky="w")
        self.range_band_check = ttk.Checkbutton(
            chart_controls,
            text="レンジ補助線",
            variable=self.range_band_show_var,
            command=self._on_range_band_toggle,
        )
        self.range_band_check.grid(row=1, column=6, padx=(8, 0), sticky="w")

        ttk.Label(chart_controls, text="足").grid(row=2, column=1, padx=(12, 4), sticky="w")
        self.candle_1_radio = ttk.Radiobutton(
            chart_controls,
            text="1分",
            variable=self.candle_interval_var,
            value=1,
            command=self._on_candle_interval_change,
        )
        self.candle_1_radio.grid(row=2, column=2, sticky="w")
        self.candle_5_radio = ttk.Radiobutton(
            chart_controls,
            text="5分",
            variable=self.candle_interval_var,
            value=5,
            command=self._on_candle_interval_change,
        )
        self.candle_5_radio.grid(row=2, column=3, sticky="w")
        self.candle_15_radio = ttk.Radiobutton(
            chart_controls,
            text="15分",
            variable=self.candle_interval_var,
            value=15,
            command=self._on_candle_interval_change,
        )
        self.candle_15_radio.grid(row=2, column=4, sticky="w")
        self.candle_30_radio = ttk.Radiobutton(
            chart_controls,
            text="30分",
            variable=self.candle_interval_var,
            value=30,
            command=self._on_candle_interval_change,
        )
        self.candle_30_radio.grid(row=2, column=5, sticky="w")
        self.candle_60_radio = ttk.Radiobutton(
            chart_controls,
            text="1時間",
            variable=self.candle_interval_var,
            value=60,
            command=self._on_candle_interval_change,
        )
        self.candle_60_radio.grid(row=2, column=6, sticky="w")
        self.candle_3_radio = ttk.Radiobutton(
            chart_controls,
            text="3分",
            variable=self.candle_interval_var,
            value=3,
            command=self._on_candle_interval_change,
        )
        self.candle_3_radio.grid(row=3, column=2, sticky="w")
        self.candle_120_radio = ttk.Radiobutton(
            chart_controls,
            text="2時間",
            variable=self.candle_interval_var,
            value=120,
            command=self._on_candle_interval_change,
        )
        self.candle_120_radio.grid(row=3, column=3, sticky="w")
        self.candle_240_radio = ttk.Radiobutton(
            chart_controls,
            text="4時間",
            variable=self.candle_interval_var,
            value=240,
            command=self._on_candle_interval_change,
        )
        self.candle_240_radio.grid(row=3, column=4, sticky="w")
        self.candle_1440_radio = ttk.Radiobutton(
            chart_controls,
            text="日足",
            variable=self.candle_interval_var,
            value=1440,
            command=self._on_candle_interval_change,
        )
        self.candle_1440_radio.grid(row=3, column=5, sticky="w")

        param_area = ttk.Frame(chart_tab)
        param_area.grid(row=3, column=0, sticky="ew", pady=(4, 4))
        param_area.columnconfigure(0, weight=1)
        param_area.columnconfigure(1, weight=1)
        param_area.columnconfigure(2, weight=1)

        settings = ttk.LabelFrame(param_area, text="バックテスト条件")
        settings.grid(row=0, column=0, sticky="nsew", padx=(0, 6))

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
        ttk.Label(settings, text="時間経過クローズ（分）").grid(
            row=1, column=6, sticky="w", pady=(6, 0)
        )
        ttk.Entry(settings, textvariable=self.time_close_minutes_var, width=8).grid(
            row=1, column=7, padx=(4, 0), pady=(6, 0), sticky="w"
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

        self.extreme_check = ttk.Checkbutton(
            settings,
            text="天底フィルター",
            variable=self.extreme_filter_var,
            command=self._on_extreme_filter_toggle,
        )
        self.extreme_check.grid(row=3, column=0, sticky="w", pady=(6, 0))
        ttk.Label(settings, text="天底維持ms").grid(row=3, column=1, sticky="w", pady=(6, 0))
        self.extreme_hold_entry = ttk.Entry(
            settings, textvariable=self.extreme_hold_ms_var, width=8
        )
        self.extreme_hold_entry.grid(
            row=3, column=2, padx=(4, 12), pady=(6, 0), sticky="w"
        )
        ttk.Label(settings, text="天底距離pips").grid(row=3, column=3, sticky="w", pady=(6, 0))
        self.extreme_distance_entry = ttk.Entry(
            settings, textvariable=self.extreme_distance_pips_var, width=8
        )
        self.extreme_distance_entry.grid(
            row=3, column=4, padx=(4, 12), pady=(6, 0), sticky="w"
        )

        self.backtest_exclude_check = ttk.Checkbutton(
            settings,
            text="時間帯除外",
            variable=self.backtest_exclude_var,
            command=self._on_backtest_exclude_toggle,
        )
        self.backtest_exclude_check.grid(row=4, column=0, sticky="w", pady=(6, 0))
        self.backtest_exclude_button = ttk.Button(
            settings, text="時間帯設定", command=self._open_backtest_exclude_hours
        )
        self.backtest_exclude_button.grid(
            row=4, column=1, padx=(4, 12), pady=(6, 0), sticky="w"
        )
        ttk.Label(settings, textvariable=self.backtest_exclude_label_var).grid(
            row=4, column=2, columnspan=4, sticky="w", pady=(6, 0)
        )

        ttk.Label(settings, text="戦略").grid(row=5, column=0, sticky="w", pady=(6, 0))
        self.strategy_spike_radio = ttk.Radiobutton(
            settings,
            text="スパイク",
            variable=self.entry_mode_var,
            value="spike",
        )
        self.strategy_spike_radio.grid(row=5, column=1, sticky="w", pady=(6, 0))
        self.strategy_sr_radio = ttk.Radiobutton(
            settings,
            text="水平線戻り",
            variable=self.entry_mode_var,
            value="sr_reentry",
        )
        self.strategy_sr_radio.grid(row=5, column=2, sticky="w", pady=(6, 0))

        sr_settings = ttk.LabelFrame(param_area, text="水平線条件")
        sr_settings.grid(row=0, column=1, sticky="nsew", padx=(0, 6))

        ttk.Label(sr_settings, text="ジグザグ幅（ピップス）").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Entry(sr_settings, textvariable=self.sr_zigzag_pips_var, width=8).grid(
            row=0, column=1, padx=(4, 12), sticky="w"
        )
        ttk.Label(sr_settings, text="ブレイク幅（ピップス）").grid(
            row=0, column=2, sticky="w"
        )
        ttk.Entry(sr_settings, textvariable=self.sr_break_pips_var, width=8).grid(
            row=0, column=3, padx=(4, 0), sticky="w"
        )
        ttk.Label(sr_settings, text="最小本数").grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Entry(sr_settings, textvariable=self.sr_min_bars_var, width=8).grid(
            row=1, column=1, padx=(4, 12), pady=(6, 0), sticky="w"
        )
        ttk.Label(sr_settings, text="レンジ本数").grid(row=2, column=0, sticky="w", pady=(6, 0))
        ttk.Entry(sr_settings, textvariable=self.range_band_bars_var, width=8).grid(
            row=2, column=1, padx=(4, 0), pady=(6, 0), sticky="w"
        )

        sr_reentry_settings = ttk.LabelFrame(param_area, text="水平線戻り条件")
        sr_reentry_settings.grid(row=0, column=2, sticky="nsew")
        ttk.Label(sr_reentry_settings, text="抜け幅（pp）").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Entry(
            sr_reentry_settings,
            textvariable=self.sr_reentry_break_pips_var,
            width=8,
        ).grid(row=0, column=1, padx=(4, 12), sticky="w")
        ttk.Label(sr_reentry_settings, text="平均ティック/分 上限").grid(
            row=0, column=2, sticky="w"
        )
        ttk.Entry(
            sr_reentry_settings,
            textvariable=self.sr_reentry_tick_limit_var,
            width=8,
        ).grid(row=0, column=3, padx=(4, 12), sticky="w")
        ttk.Label(sr_reentry_settings, text="対象線").grid(
            row=0, column=4, sticky="w"
        )
        self.sr_reentry_target_combo = ttk.Combobox(
            sr_reentry_settings,
            textvariable=self.sr_reentry_target_var,
            values=["水平線", "補助線", "両方"],
            width=10,
            state="readonly",
        )
        self.sr_reentry_target_combo.grid(row=0, column=5, sticky="w")
        ttk.Label(sr_reentry_settings, text="待機本数").grid(
            row=1, column=0, sticky="w", pady=(6, 0)
        )
        ttk.Entry(
            sr_reentry_settings,
            textvariable=self.sr_reentry_wait_bars_var,
            width=8,
        ).grid(row=1, column=1, padx=(4, 12), pady=(6, 0), sticky="w")
        ttk.Label(sr_reentry_settings, text="最小秒数").grid(
            row=1, column=2, sticky="w", pady=(6, 0)
        )
        ttk.Entry(
            sr_reentry_settings,
            textvariable=self.sr_reentry_min_seconds_var,
            width=8,
        ).grid(row=1, column=3, padx=(4, 12), pady=(6, 0), sticky="w")
        ttk.Label(sr_reentry_settings, text="対象秒数").grid(
            row=1, column=4, sticky="w", pady=(6, 0)
        )
        ttk.Entry(
            sr_reentry_settings,
            textvariable=self.sr_reentry_max_seconds_var,
            width=8,
        ).grid(row=1, column=5, padx=(4, 0), pady=(6, 0), sticky="w")
        ttk.Label(sr_reentry_settings, text="平均ティック/分 下限").grid(
            row=2, column=0, sticky="w", pady=(6, 0)
        )
        ttk.Entry(
            sr_reentry_settings,
            textvariable=self.sr_reentry_tick_min_var,
            width=8,
        ).grid(row=2, column=1, padx=(4, 0), pady=(6, 0), sticky="w")
        ttk.Label(sr_reentry_settings, text="滞在判定位置（％）").grid(
            row=2, column=2, sticky="w", pady=(6, 0)
        )
        ttk.Entry(
            sr_reentry_settings,
            textvariable=self.sr_reentry_midpoint_var,
            width=8,
        ).grid(row=2, column=3, padx=(4, 12), pady=(6, 0), sticky="w")
        ttk.Label(sr_reentry_settings, text="優勢滞在率（％）").grid(
            row=2, column=4, sticky="w", pady=(6, 0)
        )
        ttk.Entry(
            sr_reentry_settings,
            textvariable=self.sr_reentry_dominance_var,
            width=8,
        ).grid(row=2, column=5, padx=(4, 0), pady=(6, 0), sticky="w")

        ttk.Label(chart_tab, textvariable=self.chart_info_var).grid(
            row=4, column=0, sticky="w"
        )
        ttk.Label(chart_tab, textvariable=self.backtest_info_var).grid(
            row=5, column=0, sticky="w"
        )
        ttk.Label(chart_tab, textvariable=self.cursor_info_var).grid(
            row=6, column=0, sticky="w"
        )

        self.chart_canvas = tk.Canvas(chart_tab, bg="white")
        self.chart_canvas.grid(row=7, column=0, sticky="nsew", pady=(4, 0))
        self.chart_canvas.bind("<Configure>", self._on_canvas_resize)
        self.chart_canvas.bind("<MouseWheel>", self._on_mouse_wheel)
        self.chart_canvas.bind("<Button-4>", self._on_mouse_wheel)
        self.chart_canvas.bind("<Button-5>", self._on_mouse_wheel)
        self.chart_canvas.bind("<ButtonPress-1>", self._on_drag_start)
        self.chart_canvas.bind("<B1-Motion>", self._on_drag_move)
        self.chart_canvas.bind("<Motion>", self._on_mouse_move)
        self.chart_canvas.bind("<Leave>", self._on_mouse_leave)

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
        self._on_extreme_filter_toggle()
        self._on_backtest_exclude_toggle()

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
            time_close_minutes = self._parse_number(self.time_close_minutes_var.get())
            ma_enabled = self.ma_filter_var.get()
            ma_period = self._parse_number(self.ma_period_var.get())
            ma_deviation_pct = self._parse_number(self.ma_deviation_var.get())
            extreme_enabled = self.extreme_filter_var.get()
            if extreme_enabled:
                extreme_hold_ms = self._parse_number(self.extreme_hold_ms_var.get())
                extreme_distance_pips = self._parse_number(
                    self.extreme_distance_pips_var.get()
                )
            else:
                extreme_hold_ms = 0.0
                extreme_distance_pips = 0.0
            exclude_enabled = self.backtest_exclude_var.get()
            exclude_hours = self._get_backtest_exclude_hours() if exclude_enabled else set()
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
        if time_close_minutes < 0:
            messagebox.showerror("エラー", "時間経過クローズは0以上にしてください。")
            return None

        if ma_period < 2:
            messagebox.showerror("エラー", "移動平均の期間は2以上にしてください。")
            return None
        if ma_deviation_pct < 0:
            messagebox.showerror("エラー", "乖離率は0以上にしてください。")
            return None
        if extreme_hold_ms < 0:
            messagebox.showerror("エラー", "天底維持msは0以上にしてください。")
            return None
        if extreme_distance_pips < 0:
            messagebox.showerror("エラー", "天底距離pipsは0以上にしてください。")
            return None

        return {
            "window_ms": window_ms,
            "spike_pips": spike_pips,
            "retrace_rate": retrace_pct / 100.0,
            "spread_pips": spread_pips,
            "stop_pips": stop_pips,
            "take_pips": take_pips,
            "time_close_minutes": time_close_minutes,
            "ma_enabled": ma_enabled,
            "ma_period": int(ma_period),
            "ma_deviation_rate": ma_deviation_pct / 100.0,
            "extreme_enabled": extreme_enabled,
            "extreme_hold_ms": extreme_hold_ms,
            "extreme_distance_pips": extreme_distance_pips,
            "exclude_enabled": exclude_enabled,
            "exclude_hours": exclude_hours,
        }

    def _get_sr_params(self):
        try:
            zigzag_pips = self._parse_number(self.sr_zigzag_pips_var.get())
            break_pips = self._parse_number(self.sr_break_pips_var.get())
            min_bars = int(self._parse_number(self.sr_min_bars_var.get()))
        except ValueError:
            messagebox.showerror("エラー", "水平線の数値入力が正しくありません。")
            return None

        if zigzag_pips <= 0:
            messagebox.showerror("エラー", "ジグザグ幅は0より大きくしてください。")
            return None
        if break_pips < 0:
            messagebox.showerror("エラー", "ブレイク幅は0以上にしてください。")
            return None
        if min_bars < 1:
            messagebox.showerror("エラー", "最小本数は1以上にしてください。")
            return None

        return {
            "zigzag_pips": zigzag_pips,
            "break_pips": break_pips,
            "min_bars": min_bars,
        }

    def _get_range_params(self):
        try:
            lookback_bars = int(self._parse_number(self.range_band_bars_var.get()))
        except ValueError:
            messagebox.showerror("エラー", "レンジ補助線の数値入力が正しくありません。")
            return None

        if lookback_bars < 1:
            messagebox.showerror("エラー", "レンジ本数は1以上にしてください。")
            return None

        return {
            "lookback_bars": lookback_bars,
        }

    def _get_sr_reentry_params(self):
        try:
            break_pips = self._parse_number(self.sr_reentry_break_pips_var.get())
            tick_limit = int(self._parse_number(self.sr_reentry_tick_limit_var.get()))
            tick_min = self._parse_number(self.sr_reentry_tick_min_var.get())
            wait_bars = int(self._parse_number(self.sr_reentry_wait_bars_var.get()))
            min_seconds = self._parse_number(self.sr_reentry_min_seconds_var.get())
            max_seconds = self._parse_number(self.sr_reentry_max_seconds_var.get())
            midpoint_pct = self._parse_number(self.sr_reentry_midpoint_var.get())
            dominance_pct = self._parse_number(self.sr_reentry_dominance_var.get())
        except ValueError:
            messagebox.showerror("エラー", "水平線戻りの数値入力が正しくありません。")
            return None

        if break_pips <= 0:
            messagebox.showerror("エラー", "抜け幅は0より大きくしてください。")
            return None
        if tick_limit < 1:
            messagebox.showerror("エラー", "ティック数上限は1以上にしてください。")
            return None
        if tick_min < 0:
            messagebox.showerror("エラー", "ティック数下限は0以上にしてください。")
            return None
        if tick_limit < tick_min:
            messagebox.showerror("エラー", "ティック数上限は下限以上にしてください。")
            return None
        if wait_bars < 0:
            messagebox.showerror("エラー", "待機本数は0以上にしてください。")
            return None
        if min_seconds < 0:
            messagebox.showerror("エラー", "最小秒数は0以上にしてください。")
            return None
        if max_seconds <= 0:
            messagebox.showerror("エラー", "対象秒数は0より大きくしてください。")
            return None
        if max_seconds < min_seconds:
            messagebox.showerror("エラー", "対象秒数は最小秒数以上にしてください。")
            return None
        if midpoint_pct < 0 or midpoint_pct > 100:
            messagebox.showerror("エラー", "滞在判定位置は0〜100の範囲にしてください。")
            return None
        if dominance_pct < 0 or dominance_pct > 100:
            messagebox.showerror("エラー", "優勢滞在率は0〜100の範囲にしてください。")
            return None

        target_label = self.sr_reentry_target_var.get()
        target_map = {
            "水平線": "sr",
            "補助線": "range",
            "両方": "both",
        }
        target_kind = target_map.get(target_label, "both")

        return {
            "sr_break_pips": break_pips,
            "sr_tick_limit": tick_limit,
            "sr_tick_min": tick_min,
            "sr_wait_bars": wait_bars,
            "sr_min_seconds": min_seconds,
            "sr_max_seconds": max_seconds,
            "sr_midpoint_pct": midpoint_pct,
            "sr_dominance_pct": dominance_pct,
            "sr_target": target_kind,
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

    def _cancel_chart(self):
        if self.chart_worker and self.chart_worker.is_alive():
            self.chart_cancel_event.set()
            self.status_var.set("中止要求を受け付けました...")
        else:
            self.chart_cancel_button.config(state="disabled")

    def _show_chart(self):
        if self.chart_worker and self.chart_worker.is_alive():
            messagebox.showinfo("お知らせ", "表示処理中です。")
            return
        if self.view_end_date < self.view_start_date:
            messagebox.showerror("エラー", "終了日は開始日より後にしてください。")
            return
        params = self._get_backtest_params()
        sr_params = self._get_sr_params()
        range_params = self._get_range_params()
        if not params or not sr_params or not range_params:
            return
        entry_mode = self.entry_mode_var.get()
        sr_reentry_params = {}
        if entry_mode == "sr_reentry":
            sr_reentry_params = self._get_sr_reentry_params()
            if not sr_reentry_params:
                return
        try:
            line_interval = int(self.candle_interval_var.get())
        except Exception:
            line_interval = 1
        params["entry_mode"] = entry_mode
        params["line_interval"] = max(1, line_interval)
        params["sr_params"] = sr_params
        params["range_params"] = range_params
        params.update(sr_reentry_params)
        self.chart_cancel_event.clear()
        self.chart_button.config(state="disabled")
        self.chart_cancel_button.config(state="normal")
        self.status_var.set("表示準備中...")
        self.backtest_info_var.set("バックテスト: 計算中...")
        self.pnl_info_var.set("損益: 計算中...")
        self.backtest_ready = False
        self.pnl_data = None
        self._draw_pnl_chart()
        self.chart_worker = threading.Thread(
            target=self._chart_worker,
            args=(self.view_start_date, self.view_end_date, params, sr_params, range_params),
            daemon=True,
        )
        self.chart_worker.start()

    def _chart_worker(self, start: date, end: date, params, sr_params, range_params):
        cache, cache_hit = self._load_analysis_cache(start, end)
        points_sorted = cache.get("points_sorted") or []
        missing = cache.get("missing") or ()

        if missing and not cache_hit:
            self.queue.put(("log", f"[表示] CSV不足 {len(missing)}件"))
        if not points_sorted:
            self.queue.put(("chart_error", "表示できるデータがありません。"))
            self.queue.put(("chart_done", None))
            return

        chart_signature = (
            len(points_sorted),
            freeze_value(sr_params or {}),
            freeze_value(range_params or {}),
        )
        should_refresh_chart = (
            self.chart_data is None
            or cache.get("chart_signature") != chart_signature
            or not cache_hit
        )
        if should_refresh_chart:
            payload = {
                "start": start,
                "end": end,
                "points": points_sorted,
                "missing_count": len(missing),
                "sr_params": dict(sr_params or {}),
                "range_params": dict(range_params or {}),
            }
            self.queue.put(("chart_data", payload))
            cache["chart_signature"] = chart_signature

        if self.chart_cancel_event.is_set():
            self.queue.put(("chart_cancelled", None))
            self.queue.put(("chart_done", None))
            return

        self.queue.put(("status", "バックテスト中..."))
        try:
            backtest = run_backtest(
                points_sorted,
                params,
                runtime_cache=cache.get("backtest_cache"),
                should_cancel=self.chart_cancel_event.is_set,
            )
            self.queue.put(("backtest_data", backtest))
        except InterruptedError:
            self.queue.put(("chart_cancelled", None))
            self.queue.put(("chart_done", None))
            return
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

    def _clear_analysis_cache(self):
        self.analysis_cache_key = None
        self.analysis_cache = None

    def _load_analysis_cache(self, start: date, end: date):
        key = (start, end)
        if self.analysis_cache_key == key and self.analysis_cache is not None:
            return self.analysis_cache, True

        points, missing = load_ticks_from_csv(start, end)
        points_sorted = sorted(points, key=lambda x: x[0])
        times = [ts for ts, _ in points_sorted]
        backtest_cache = {
            "points_ref": points_sorted,
            "points_sorted": points_sorted,
            "times": times,
            "candle_cache": {},
            "ma_cache": {},
            "line_cache": {},
            "line_bin_cache": {},
        }
        cache = {
            "points_sorted": points_sorted,
            "times": times,
            "missing": tuple(missing),
            "backtest_cache": backtest_cache,
            "chart_signature": None,
        }
        self.analysis_cache_key = key
        self.analysis_cache = cache
        return cache, False

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
                    self._clear_analysis_cache()
                elif kind == "chart_done":
                    self.chart_button.config(state="normal")
                    self.chart_cancel_button.config(state="disabled")
                elif kind == "chart_error":
                    messagebox.showerror("エラー", payload)
                    self.backtest_info_var.set("バックテスト: データなし")
                    self.pnl_info_var.set("損益: データなし")
                    self.backtest_ready = False
                    self.pnl_data = None
                    self._draw_pnl_chart()
                    self.chart_cancel_button.config(state="disabled")
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
                    self.chart_cancel_button.config(state="disabled")
                elif kind == "chart_cancelled":
                    self.status_var.set("表示計算を中止しました")
                    self.backtest_info_var.set("バックテスト: 中止")
                    self.pnl_info_var.set("損益: 中止")
                    self.backtest_ready = False
                    self.pnl_data = None
                    self._draw_pnl_chart()
                elif kind == "cancelled":
                    self.status_var.set("キャンセルしました")
                    self.run_button.config(state="normal")
                    self.cancel_button.config(state="disabled")
                    self._clear_analysis_cache()
        except queue.Empty:
            pass
        self.root.after(200, self._poll_queue)

    def _render_chart(self, payload):
        points = payload["points"]
        start = payload["start"]
        end = payload["end"]
        missing_count = payload["missing_count"]
        sr_params = payload.get("sr_params") or {}
        range_params = payload.get("range_params") or {}

        if not points:
            self.chart_info_var.set("表示できるデータがありません。")
            return

        points = sorted(points, key=lambda x: x[0])
        times = [ts for ts, _ in points]
        view_end_time = times[-1]
        view_start_time = view_end_time - timedelta(hours=6)
        if view_start_time < times[0]:
            view_start_time = times[0]
        view_start_idx = bisect_left(times, view_start_time)
        view_end_idx = len(points) - 1
        self.chart_data = {
            "all_points": points,
            "times": times,
            "view_start": view_start_idx,
            "view_end": view_end_idx,
            "count": len(points),
            "start": start,
            "end": end,
            "missing": missing_count,
            "mode": self.x_axis_mode_var.get(),
            "view_start_time": view_start_time,
            "view_end_time": view_end_time,
            "trades": [],
            "ma_series": [],
            "ma_enabled": False,
            "sr_params": sr_params,
            "range_params": range_params,
            "sr_segments": [],
            "zigzag_points": [],
            "range_segments": [],
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
        entry_mode = payload.get("entry_mode")
        sr_target = payload.get("sr_target")

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

        if entry_mode == "sr_reentry" and sr_target == "both":
            trades = payload.get("trades", [])
            sr_trades = [t for t in trades if t.get("line_source") == "sr"]
            range_trades = [t for t in trades if t.get("line_source") == "range"]
            sr_pips = sum(t.get("pips", 0.0) for t in sr_trades)
            range_pips = sum(t.get("pips", 0.0) for t in range_trades)
            breakdown = (
                f"\n内訳: 水平線 取引{len(sr_trades)}件 合計損益{sr_pips:.1f}ピップス"
                f" / 補助線 取引{len(range_trades)}件 合計損益{range_pips:.1f}ピップス"
            )
            self.pnl_info_var.set(self.pnl_info_var.get() + breakdown)

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

    def _on_candle_interval_change(self):
        if self.chart_type_var.get() != "candle":
            self.chart_type_var.set("candle")
            self._on_chart_type_change()
            return
        if self.chart_data:
            self._draw_chart()

    def _on_ma_filter_toggle(self):
        enabled = self.ma_filter_var.get()
        state = "normal" if enabled else "disabled"
        self.ma_period_entry.config(state=state)
        self.ma_deviation_entry.config(state=state)
        if self.chart_data:
            self._draw_chart()

    def _on_zigzag_toggle(self):
        if self.chart_data:
            self._draw_chart()

    def _on_range_band_toggle(self):
        if self.chart_data:
            self._draw_chart()

    def _on_extreme_filter_toggle(self):
        enabled = self.extreme_filter_var.get()
        state = "normal" if enabled else "disabled"
        self.extreme_hold_entry.config(state=state)
        self.extreme_distance_entry.config(state=state)

    def _get_backtest_exclude_hours(self):
        return {i for i, var in enumerate(self.backtest_exclude_hours_vars) if var.get()}

    def _update_backtest_exclude_label(self):
        hours = sorted(self._get_backtest_exclude_hours())
        if not hours:
            self.backtest_exclude_label_var.set("除外時間: なし")
            return
        label = ",".join(f"{hour:02d}" for hour in hours)
        self.backtest_exclude_label_var.set(f"除外時間: {label}")

    def _open_backtest_exclude_hours(self):
        top = tk.Toplevel(self.root)
        top.title("除外する時間帯（JST）")
        top.resizable(False, False)

        frame = ttk.Frame(top, padding=8)
        frame.grid(row=0, column=0, sticky="nsew")
        ttk.Label(frame, text="除外する時間帯にチェック（JST）").grid(
            row=0, column=0, columnspan=6, sticky="w", pady=(0, 6)
        )

        for hour in range(24):
            r = 1 + hour // 6
            c = hour % 6
            ttk.Checkbutton(
                frame,
                text=f"{hour:02d}時",
                variable=self.backtest_exclude_hours_vars[hour],
            ).grid(row=r, column=c, padx=4, pady=2, sticky="w")

        def close_dialog():
            self._update_backtest_exclude_label()
            top.destroy()

        ttk.Button(frame, text="閉じる", command=close_dialog).grid(
            row=5, column=0, columnspan=6, pady=(6, 0)
        )
        top.protocol("WM_DELETE_WINDOW", close_dialog)

    def _on_backtest_exclude_toggle(self):
        enabled = self.backtest_exclude_var.get()
        state = "normal" if enabled else "disabled"
        self.backtest_exclude_button.config(state=state)
        if enabled:
            self._update_backtest_exclude_label()
        else:
            self.backtest_exclude_label_var.set("除外時間: なし")

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

    def _on_mouse_leave(self, _event):
        self.cursor_info_var.set("")

    def _on_mouse_move(self, event):
        data = self.chart_data
        if not data or self.hide_chart_var.get():
            return
        width, height, left, top, right, bottom, plot_width, _plot_height = (
            self._get_plot_area()
        )
        if plot_width <= 0:
            return
        if (
            event.x < left
            or event.x > width - right
            or event.y < top
            or event.y > height - bottom
        ):
            self.cursor_info_var.set("")
            return

        chart_type = (
            self.chart_type_var.get() if hasattr(self, "chart_type_var") else "tick"
        )
        if chart_type == "candle":
            candles = data.get("view_candles") or []
            candle_times = data.get("view_candle_times") or []
            if not candles or not candle_times:
                return
            span_seconds = (
                data["view_end_time"] - data["view_start_time"]
            ).total_seconds()
            if span_seconds <= 0:
                return
            ratio = (event.x - left) / plot_width
            ratio = min(max(ratio, 0.0), 1.0)
            target_time = data["view_start_time"] + (
                data["view_end_time"] - data["view_start_time"]
            ) * ratio
            idx = bisect_left(candle_times, target_time)
            if idx >= len(candle_times):
                idx = len(candle_times) - 1
            elif idx > 0:
                prev = candle_times[idx - 1]
                if abs((target_time - prev).total_seconds()) < abs(
                    (candle_times[idx] - target_time).total_seconds()
                ):
                    idx -= 1
            ts, open_p, high_p, low_p, close_p = candles[idx]
            self.cursor_info_var.set(
                f"カーソル: {ts.strftime('%m/%d %H:%M')} "
                f"始値{open_p:.3f} 高値{high_p:.3f} "
                f"安値{low_p:.3f} 終値{close_p:.3f}"
            )
            return

        mode = data.get("mode", "time")
        if mode == "time":
            span_seconds = (
                data["view_end_time"] - data["view_start_time"]
            ).total_seconds()
            if span_seconds <= 0:
                return
            ratio = (event.x - left) / plot_width
            ratio = min(max(ratio, 0.0), 1.0)
            target_time = data["view_start_time"] + (
                data["view_end_time"] - data["view_start_time"]
            ) * ratio
            times = data.get("times") or []
            if not times:
                return
            idx = bisect_left(times, target_time)
            if idx >= len(times):
                idx = len(times) - 1
            elif idx > 0:
                prev = times[idx - 1]
                if abs((target_time - prev).total_seconds()) < abs(
                    (times[idx] - target_time).total_seconds()
                ):
                    idx -= 1
        else:
            view_start = data.get("view_start", 0)
            view_end = data.get("view_end", 0)
            visible = max(1, view_end - view_start + 1)
            ratio = (event.x - left) / plot_width
            ratio = min(max(ratio, 0.0), 1.0)
            idx = view_start + int(round(ratio * (visible - 1)))
            idx = max(view_start, min(idx, view_end))

        points = data.get("all_points") or []
        if not points:
            return
        ts, price = points[idx]
        self.cursor_info_var.set(
            f"カーソル: {ts.strftime('%m/%d %H:%M:%S')} 価格{price:.3f}"
        )

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
        candle_interval = 1
        if chart_type == "candle":
            try:
                candle_interval = int(self.candle_interval_var.get())
            except Exception:
                candle_interval = 1
            candle_interval = max(1, candle_interval)
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
            candle_cache = data.get("candle_cache")
            if candle_cache is None:
                candle_cache = {}
                data["candle_cache"] = candle_cache

            cache_entry = candle_cache.get(candle_interval)
            if cache_entry is None or cache_entry.get("source_len") != len(points_all):
                full_candles = build_timeframe_candles(points_all, candle_interval)
                sr_params = {
                    "zigzag_pips": 5.0,
                    "break_pips": 1.0,
                    "min_bars": 5,
                }
                sr_params.update(data.get("sr_params") or {})
                range_params = {"lookback_bars": 30}
                range_params.update(data.get("range_params") or {})

                zigzag_points = build_zigzag_points(
                    full_candles,
                    zigzag_pips=sr_params.get("zigzag_pips", 5.0),
                    min_bars=sr_params.get("min_bars", 5),
                )
                range_segments = build_range_band_segments(
                    full_candles,
                    lookback_bars=range_params.get("lookback_bars", 30),
                )
                sr_segments = build_zigzag_sr_segments(full_candles, **sr_params)

                cache_entry = {
                    "source_len": len(points_all),
                    "candles": full_candles,
                    "zigzag_points": zigzag_points,
                    "range_segments": range_segments,
                    "sr_segments": sr_segments,
                }
                candle_cache[candle_interval] = cache_entry

            full_candles = cache_entry.get("candles") or []
            view_candles = [
                c for c in full_candles if view_start_time <= c[0] <= view_end_time
            ]
            candles = view_candles
            data["view_candles"] = candles
            data["view_candle_times"] = [c[0] for c in candles]
            data["view_candle_interval"] = candle_interval
            data["zigzag_points"] = cache_entry.get("zigzag_points") or []
            data["range_segments"] = cache_entry.get("range_segments") or []
            data["sr_segments"] = cache_entry.get("sr_segments") or []

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
                f"足: {candle_interval}分  "
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
            bar_seconds = candle_interval * 60
            bar_width = bar_seconds / span_seconds * plot_width
            half_width = max(1.0, bar_width * 0.35)
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

        zigzag_points = data.get("zigzag_points") or []
        if self.zigzag_show_var.get() and chart_type == "candle" and zigzag_points:
            zz_coords = []
            if span_seconds > 0:
                for ts, price in zigzag_points:
                    if ts < view_start_time or ts > view_end_time:
                        continue
                    x = (
                        left
                        + (ts - view_start_time).total_seconds()
                        / span_seconds
                        * plot_width
                    )
                    y = price_to_y(price)
                    zz_coords.extend([x, y])
            if zz_coords:
                canvas.create_line(zz_coords, fill="#7f7f7f", width=1)

        range_segments = data.get("range_segments") or []
        if self.range_band_show_var.get() and chart_type == "candle" and range_segments:
            if span_seconds > 0:
                segments_view = []
                for seg in range_segments:
                    start_ts = seg.get("start_time")
                    end_ts = seg.get("end_time")
                    high = seg.get("high")
                    low = seg.get("low")
                    if (
                        start_ts is None
                        or end_ts is None
                        or high is None
                        or low is None
                    ):
                        continue
                    if end_ts < view_start_time or start_ts > view_end_time:
                        continue
                    draw_start = (
                        start_ts if start_ts > view_start_time else view_start_time
                    )
                    draw_end = end_ts if end_ts < view_end_time else view_end_time
                    segments_view.append(
                        {
                            "start_time": draw_start,
                            "end_time": draw_end,
                            "high": high,
                            "low": low,
                        }
                    )

                if segments_view:
                    top_coords = []
                    prev_x_end = None
                    prev_y_high = None
                    for i, seg in enumerate(segments_view):
                        x_start = (
                            left
                            + (seg["start_time"] - view_start_time).total_seconds()
                            / span_seconds
                            * plot_width
                        )
                        x_end = (
                            left
                            + (seg["end_time"] - view_start_time).total_seconds()
                            / span_seconds
                            * plot_width
                        )
                        y_high = price_to_y(seg["high"])
                        if i == 0:
                            top_coords.extend([x_start, y_high])
                        else:
                            if prev_x_end is not None and abs(x_start - prev_x_end) > 1e-6:
                                top_coords.extend([x_start, prev_y_high])
                            if prev_y_high is not None and abs(y_high - prev_y_high) > 1e-6:
                                top_coords.extend([x_start, y_high])
                        top_coords.extend([x_end, y_high])
                        prev_x_end = x_end
                        prev_y_high = y_high

                    bottom_coords = []
                    prev_x_start = None
                    prev_y_low = None
                    for i, seg in enumerate(reversed(segments_view)):
                        x_end = (
                            left
                            + (seg["end_time"] - view_start_time).total_seconds()
                            / span_seconds
                            * plot_width
                        )
                        x_start = (
                            left
                            + (seg["start_time"] - view_start_time).total_seconds()
                            / span_seconds
                            * plot_width
                        )
                        y_low = price_to_y(seg["low"])
                        if i == 0:
                            bottom_coords.extend([x_end, y_low])
                        else:
                            if prev_x_start is not None and abs(x_end - prev_x_start) > 1e-6:
                                bottom_coords.extend([x_end, prev_y_low])
                            if prev_y_low is not None and abs(y_low - prev_y_low) > 1e-6:
                                bottom_coords.extend([x_end, y_low])
                        bottom_coords.extend([x_start, y_low])
                        prev_x_start = x_start
                        prev_y_low = y_low

                    if top_coords and bottom_coords:
                        polygon_coords = top_coords + bottom_coords
                        canvas.create_polygon(
                            polygon_coords,
                            outline="#1f77b4",
                            fill="",
                            width=1,
                            dash=(2, 2),
                        )

        sr_segments = data.get("sr_segments") or []
        if chart_type == "candle" and sr_segments and span_seconds > 0:
            for seg in sr_segments:
                start_ts = seg.get("start_time")
                end_ts = seg.get("end_time")
                price = seg.get("price")
                kind = seg.get("kind")
                if (
                    start_ts is None
                    or end_ts is None
                    or price is None
                    or kind is None
                ):
                    continue
                if end_ts < view_start_time or start_ts > view_end_time:
                    continue
                draw_start = start_ts if start_ts > view_start_time else view_start_time
                draw_end = end_ts if end_ts < view_end_time else view_end_time
                x1 = (
                    left
                    + (draw_start - view_start_time).total_seconds()
                    / span_seconds
                    * plot_width
                )
                x2 = (
                    left
                    + (draw_end - view_start_time).total_seconds()
                    / span_seconds
                    * plot_width
                )
                y = price_to_y(price)
                color = "#2ca02c" if kind == "support" else "#d62728"
                canvas.create_line(x1, y, x2, y, fill=color, width=1, dash=(3, 3))

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
