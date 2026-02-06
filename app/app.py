import calendar
import csv
import hashlib
import json
import lzma
import os
import queue
import re
import struct
import threading
import time as pytime
from collections import deque
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
from tkinter import filedialog, messagebox, ttk


PAIR = "USDJPY"
BASE_URL = "https://datafeed.dukascopy.com/datafeed"
PIP_SIZE = 0.01
RESULTS_DIR_NAME = "results"


def freeze_value(value):
    if isinstance(value, dict):
        return tuple(sorted((k, freeze_value(v)) for k, v in value.items()))
    if isinstance(value, set):
        return tuple(sorted(freeze_value(v) for v in value))
    if isinstance(value, (list, tuple)):
        return tuple(freeze_value(v) for v in value)
    return value


def is_cancel_requested(check_fn):
    if check_fn and check_fn():
        return True
    counter = getattr(is_cancel_requested, "_counter", 0) + 1
    if counter >= 2000:
        pytime.sleep(0)
        counter = 0
    is_cancel_requested._counter = counter
    return False


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


def build_timeframe_candles(points, interval_minutes=1, should_cancel=None):
    if not points:
        return []
    interval_minutes = max(1, int(interval_minutes))
    candles = []
    current_time = None
    open_p = high_p = low_p = close_p = None

    for ts, price in points:
        if should_cancel and is_cancel_requested(should_cancel):
            raise InterruptedError("cancelled")
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


def build_minute_candles(points, should_cancel=None):
    return build_timeframe_candles(points, 1, should_cancel=should_cancel)


def build_minute_ma(candles, period, should_cancel=None):
    if period <= 0 or not candles:
        return [], [], []
    times = []
    ma_values = [None] * len(candles)
    closes = [c[4] for c in candles]
    running = 0.0
    for i, candle in enumerate(candles):
        if should_cancel and is_cancel_requested(should_cancel):
            raise InterruptedError("cancelled")
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


def build_second_closes(points, should_cancel=None):
    if not points:
        return [], []
    start_time = points[0][0].replace(microsecond=0)
    end_time = points[-1][0].replace(microsecond=0)
    times = []
    closes = []
    idx = 0
    last_price = points[0][1]
    current_time = start_time
    n = len(points)
    while current_time <= end_time:
        if should_cancel and is_cancel_requested(should_cancel):
            raise InterruptedError("cancelled")
        next_time = current_time + timedelta(seconds=1)
        while idx < n and points[idx][0] < next_time:
            last_price = points[idx][1]
            idx += 1
        times.append(current_time)
        closes.append(last_price)
        current_time = next_time
    return times, closes


def build_second_ma(points, period, should_cancel=None):
    if period <= 0 or not points:
        return [], [], []
    times, closes = build_second_closes(points, should_cancel=should_cancel)
    if not closes:
        return [], [], []
    ma_values = [None] * len(closes)
    running = 0.0
    for i, close in enumerate(closes):
        if should_cancel and is_cancel_requested(should_cancel):
            raise InterruptedError("cancelled")
        running += close
        if i >= period:
            running -= closes[i - period]
        if i >= period - 1:
            ma_values[i] = running / period
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


def build_range_band_segments(candles, lookback_bars=30, should_cancel=None):
    if not candles:
        return []

    lookback_bars = max(1, int(lookback_bars))

    segments = []
    active = None

    for idx in range(len(candles)):
        if should_cancel and is_cancel_requested(should_cancel):
            raise InterruptedError("cancelled")
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


def build_zigzag_points(candles, zigzag_pips=5.0, min_bars=5, should_cancel=None):
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
        if should_cancel and is_cancel_requested(should_cancel):
            raise InterruptedError("cancelled")
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


def build_zigzag_sr_segments(
    candles, zigzag_pips=5.0, break_pips=1.0, min_bars=5, should_cancel=None
):
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
        if should_cancel and is_cancel_requested(should_cancel):
            raise InterruptedError("cancelled")
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


def find_reverse_signal(
    points,
    times,
    start_idx,
    window,
    move,
    hold_seconds=0.0,
    max_pullback=0.0,
    should_cancel=None,
):
    t0, p0 = points[start_idx]
    end_time = t0 + window
    end_idx = bisect_right(times, end_time)
    if end_idx <= start_idx + 1:
        return None

    monitor_high = p0
    monitor_low = p0

    upper = p0 + move
    lower = p0 - move

    direction = None
    trigger_idx = None
    trigger_time = None
    for j in range(start_idx + 1, end_idx):
        if is_cancel_requested(should_cancel):
            raise InterruptedError("cancelled")
        price = points[j][1]
        if price > monitor_high:
            monitor_high = price
        if price < monitor_low:
            monitor_low = price
        if price >= upper:
            direction = "short"
            trigger_idx = j
            trigger_time = points[j][0]
            break
        if price <= lower:
            direction = "long"
            trigger_idx = j
            trigger_time = points[j][0]
            break

    if direction is None or trigger_idx is None:
        return None

    if hold_seconds <= 0:
        return {
            "entry_idx": trigger_idx,
            "side": direction,
            "entry_reason": "秒逆張り",
            "monitor_high": monitor_high,
            "monitor_low": monitor_low,
        }

    last_extreme_time = trigger_time
    if direction == "short":
        extreme = points[trigger_idx][1]
        for j in range(trigger_idx + 1, len(points)):
            if should_cancel and is_cancel_requested(should_cancel):
                raise InterruptedError("cancelled")
            ts, price = points[j]
            if price > monitor_high:
                monitor_high = price
            if price < monitor_low:
                monitor_low = price
            if price > extreme:
                extreme = price
                last_extreme_time = ts
            if max_pullback > 0 and (extreme - price) >= max_pullback:
                return None
            if (ts - last_extreme_time).total_seconds() >= hold_seconds:
                return {
                    "entry_idx": j,
                    "side": "short",
                    "entry_reason": "秒逆張り",
                    "monitor_high": monitor_high,
                    "monitor_low": monitor_low,
                }
    else:
        extreme = points[trigger_idx][1]
        for j in range(trigger_idx + 1, len(points)):
            if is_cancel_requested(should_cancel):
                raise InterruptedError("cancelled")
            ts, price = points[j]
            if price > monitor_high:
                monitor_high = price
            if price < monitor_low:
                monitor_low = price
            if price < extreme:
                extreme = price
                last_extreme_time = ts
            if max_pullback > 0 and (price - extreme) >= max_pullback:
                return None
            if (ts - last_extreme_time).total_seconds() >= hold_seconds:
                return {
                    "entry_idx": j,
                    "side": "long",
                    "entry_reason": "秒逆張り",
                    "monitor_high": monitor_high,
                    "monitor_low": monitor_low,
                }

    return None


def build_second_extremes(points, should_cancel=None):
    n = len(points)
    lows = [0.0] * n
    highs = [0.0] * n
    i = 0
    while i < n:
        if should_cancel and is_cancel_requested(should_cancel):
            raise InterruptedError("cancelled")
        sec = points[i][0].replace(microsecond=0)
        low = points[i][1]
        high = points[i][1]
        j = i + 1
        while j < n:
            if should_cancel and is_cancel_requested(should_cancel):
                raise InterruptedError("cancelled")
            ts = points[j][0]
            if ts.replace(microsecond=0) != sec:
                break
            price = points[j][1]
            if price < low:
                low = price
            if price > high:
                high = price
            j += 1
        for k in range(i, j):
            lows[k] = low
            highs[k] = high
        i = j
    return lows, highs


def find_momentum_signal(
    points,
    times,
    start_idx,
    window,
    spike,
    boundary_pct,
    tick_min_per_min,
    hold_seconds,
    max_move,
    second_lows,
    second_highs,
    should_cancel=None,
):
    t0, _p0 = points[start_idx]
    end_time = t0 + window
    end_idx = bisect_right(times, end_time)
    if end_idx <= start_idx + 1:
        return None

    base_low = second_lows[start_idx]
    base_high = second_highs[start_idx]
    up_threshold = base_low + spike
    down_threshold = base_high - spike

    direction = None
    monitor_idx = None
    for j in range(start_idx + 1, end_idx):
        if is_cancel_requested(should_cancel):
            raise InterruptedError("cancelled")
        price = points[j][1]
        if price >= up_threshold:
            direction = "long"
            monitor_idx = j
            break
        if price <= down_threshold:
            direction = "short"
            monitor_idx = j
            break

    if direction is None:
        return None

    ratio = boundary_pct / 100.0
    if ratio < 0:
        ratio = 0.0
    elif ratio > 1:
        ratio = 1.0

    monitor_start_time = points[monitor_idx][0]
    tick_count = 0
    last_price = (
        points[monitor_idx - 1][1] if monitor_idx > 0 else points[monitor_idx][1]
    )
    if direction == "long":
        low_base = base_low
        high = points[monitor_idx][1]
    else:
        high_base = base_high
        low = points[monitor_idx][1]

    n = len(points)
    for j in range(monitor_idx, n):
        if is_cancel_requested(should_cancel):
            raise InterruptedError("cancelled")
        ts, price = points[j]
        if price != last_price:
            tick_count += 1
        last_price = price
        if direction == "long":
            if price > high:
                high = price
            if max_move > 0 and (high - low_base) >= max_move:
                return None
            boundary = low_base + (high - low_base) * ratio
            if price < boundary:
                return None
        else:
            if price < low:
                low = price
            if max_move > 0 and (high_base - low) >= max_move:
                return None
            boundary = high_base - (high_base - low) * ratio
            if price > boundary:
                return None

        duration = (ts - monitor_start_time).total_seconds()
        if duration >= hold_seconds:
            denom = duration if duration > 0 else 0.001
            avg_per_min = tick_count / denom * 60.0
            if avg_per_min >= tick_min_per_min:
                return {
                    "entry_idx": j,
                    "side": direction,
                    "entry_reason": "勢い追随",
                }

    return None


def build_reentry_lines(candles, sr_params, range_params, target_type, should_cancel=None):
    if not candles:
        return []
    sr_params = sr_params or {}
    range_params = range_params or {}

    lines = []
    if target_type in ("sr", "both"):
        segments = build_zigzag_sr_segments(candles, **sr_params, should_cancel=should_cancel)
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
        range_segments = build_range_band_segments(
            candles, lookback_bars=lookback_bars, should_cancel=should_cancel
        )
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
    move_ratio_pct=100.0,
    move_speed_ratio_pct=100.0,
    favored_tick_min_move=0.0,
    move_ratio_enabled=True,
    move_speed_ratio_enabled=True,
    ratio_join_mode="and",
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
    move_ratio = move_ratio_pct / 100.0
    if move_ratio < 0:
        move_ratio = 0.0
    move_speed_ratio = move_speed_ratio_pct / 100.0
    if move_speed_ratio < 0:
        move_speed_ratio = 0.0
    if favored_tick_min_move < 0:
        favored_tick_min_move = 0.0

    def passes_move_ratio(favored_avg, opposite_avg):
        if move_ratio <= 0:
            return True
        if favored_avg <= 0:
            return False
        if opposite_avg <= 0:
            return True
        return favored_avg / opposite_avg >= move_ratio

    def passes_move_speed_ratio(favored_sum, favored_time, opposite_sum, opposite_time):
        if move_speed_ratio <= 0:
            return True
        if favored_time <= 0 or opposite_time <= 0:
            return False
        favored_speed = favored_sum / favored_time
        opposite_speed = opposite_sum / opposite_time
        if favored_speed <= 0 or opposite_speed <= 0:
            return False
        return favored_speed / opposite_speed >= move_speed_ratio

    def combine_ratio_filters(move_ok, speed_ok):
        if not move_ratio_enabled and not move_speed_ratio_enabled:
            return True
        if move_ratio_enabled and not move_speed_ratio_enabled:
            return move_ok
        if move_speed_ratio_enabled and not move_ratio_enabled:
            return speed_ok
        if ratio_join_mode == "or":
            return move_ok or speed_ok
        return move_ok and speed_ok

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

                delta = bid - state["last_bid"]
                moved = delta != 0
                if delta > 0:
                    state["up_move_sum"] += delta
                    state["up_move_count"] += 1
                    if delta > state["max_up_tick_move"]:
                        state["max_up_tick_move"] = delta
                    if elapsed > 0:
                        state["up_move_time"] += elapsed
                elif delta < 0:
                    state["down_move_sum"] += -delta
                    state["down_move_count"] += 1
                    if -delta > state["max_down_tick_move"]:
                        state["max_down_tick_move"] = -delta
                    if elapsed > 0:
                        state["down_move_time"] += elapsed

                if kind == "resistance":
                    if bid > level:
                        if bid > level + max_break:
                            finished.append(idx)
                        else:
                            if moved:
                                tick_count += 1
                            state["tick_count"] = tick_count
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
                            up_avg = (
                                state["up_move_sum"] / state["up_move_count"]
                                if state["up_move_count"] > 0
                                else 0.0
                            )
                            down_avg = (
                                state["down_move_sum"] / state["down_move_count"]
                                if state["down_move_count"] > 0
                                else 0.0
                            )
                            move_ok = passes_move_ratio(down_avg, up_avg)
                            speed_ok = passes_move_speed_ratio(
                                state["down_move_sum"],
                                state["down_move_time"],
                                state["up_move_sum"],
                                state["up_move_time"],
                            )
                            down_speed = (
                                state["down_move_sum"] / state["down_move_time"]
                                if state["down_move_time"] > 0
                                else 0.0
                            )
                            up_speed = (
                                state["up_move_sum"] / state["up_move_time"]
                                if state["up_move_time"] > 0
                                else 0.0
                            )
                            if (
                                tick_min <= avg_per_min <= tick_limit
                                and below_ratio >= dominance_ratio
                                and combine_ratio_filters(move_ok, speed_ok)
                                and state["max_down_tick_move"] >= favored_tick_min_move
                            ):
                                disabled_lines.add(idx)
                                return {
                                    "entry_idx": j,
                                    "side": "short",
                                    "entry_reason": "抵抗線戻り",
                                    "line_price": level,
                                    "line_kind": kind,
                                    "line_source": line.get("source"),
                                    "tick_count": tick_count,
                                    "stay_above": state["stay_above"],
                                    "stay_below": state["stay_below"],
                                    "up_avg_move": up_avg,
                                    "down_avg_move": down_avg,
                                    "up_move_speed": up_speed,
                                    "down_move_speed": down_speed,
                                    "max_up_tick_move": state["max_up_tick_move"],
                                    "max_down_tick_move": state["max_down_tick_move"],
                                    "move_ratio_ok": move_ok,
                                    "speed_ratio_ok": speed_ok,
                                }
                        finished.append(idx)
                    else:
                        if moved:
                            tick_count += 1
                        state["tick_count"] = tick_count
                        state["last_time"] = ts
                        state["last_bid"] = bid
                else:
                    if bid < level:
                        if bid < level - max_break:
                            finished.append(idx)
                        else:
                            if moved:
                                tick_count += 1
                            state["tick_count"] = tick_count
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
                            up_avg = (
                                state["up_move_sum"] / state["up_move_count"]
                                if state["up_move_count"] > 0
                                else 0.0
                            )
                            down_avg = (
                                state["down_move_sum"] / state["down_move_count"]
                                if state["down_move_count"] > 0
                                else 0.0
                            )
                            move_ok = passes_move_ratio(up_avg, down_avg)
                            speed_ok = passes_move_speed_ratio(
                                state["up_move_sum"],
                                state["up_move_time"],
                                state["down_move_sum"],
                                state["down_move_time"],
                            )
                            down_speed = (
                                state["down_move_sum"] / state["down_move_time"]
                                if state["down_move_time"] > 0
                                else 0.0
                            )
                            up_speed = (
                                state["up_move_sum"] / state["up_move_time"]
                                if state["up_move_time"] > 0
                                else 0.0
                            )
                            if (
                                tick_min <= avg_per_min <= tick_limit
                                and above_ratio >= dominance_ratio
                                and combine_ratio_filters(move_ok, speed_ok)
                                and state["max_up_tick_move"] >= favored_tick_min_move
                            ):
                                disabled_lines.add(idx)
                                return {
                                    "entry_idx": j,
                                    "side": "long",
                                    "entry_reason": "支持線戻り",
                                    "line_price": level,
                                    "line_kind": kind,
                                    "line_source": line.get("source"),
                                    "tick_count": tick_count,
                                    "stay_above": state["stay_above"],
                                    "stay_below": state["stay_below"],
                                    "up_avg_move": up_avg,
                                    "down_avg_move": down_avg,
                                    "up_move_speed": up_speed,
                                    "down_move_speed": down_speed,
                                    "max_up_tick_move": state["max_up_tick_move"],
                                    "max_down_tick_move": state["max_down_tick_move"],
                                    "move_ratio_ok": move_ok,
                                    "speed_ratio_ok": speed_ok,
                                }
                        finished.append(idx)
                    else:
                        if moved:
                            tick_count += 1
                        state["tick_count"] = tick_count
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
                            "up_move_sum": 0.0,
                            "up_move_count": 0,
                            "up_move_time": 0.0,
                            "down_move_sum": 0.0,
                            "down_move_count": 0,
                            "down_move_time": 0.0,
                            "max_up_tick_move": 0.0,
                            "max_down_tick_move": 0.0,
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
                            "up_move_sum": 0.0,
                            "up_move_count": 0,
                            "up_move_time": 0.0,
                            "down_move_sum": 0.0,
                            "down_move_count": 0,
                            "down_move_time": 0.0,
                            "max_up_tick_move": 0.0,
                            "max_down_tick_move": 0.0,
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


def compute_max_drawdown(equity_curve):
    if not equity_curve:
        return 0.0
    max_peak = None
    max_dd = 0.0
    for _ts, value in equity_curve:
        if max_peak is None or value > max_peak:
            max_peak = value
        drawdown = max_peak - value
        if drawdown > max_dd:
            max_dd = drawdown
    return max_dd


def simulate_exit(
    points,
    entry_idx,
    side,
    entry_price,
    spread,
    stop,
    take,
    fixed_exit_price=True,
    time_close_seconds=0.0,
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
    if time_close_seconds and time_close_seconds > 0:
        forced_close_time = points[entry_idx][0] + timedelta(seconds=time_close_seconds)

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
                exit_price = stop_price if fixed_exit_price else bid
                exit_reason = "損切"
                break
            if bid >= take_price:
                exit_idx = j
                exit_price = take_price if fixed_exit_price else bid
                exit_reason = "利確"
                break
        else:
            if ask >= stop_price:
                exit_idx = j
                exit_price = stop_price if fixed_exit_price else ask
                exit_reason = "損切"
                break
            if ask <= take_price:
                exit_idx = j
                exit_price = take_price if fixed_exit_price else ask
                exit_reason = "利確"
                break
        if forced_close_time is not None and _t >= forced_close_time:
            exit_idx = j
            exit_price = bid if side == "long" else bid + spread
            exit_reason = "時間"
            break
        j += 1

    if exit_idx is None:
        exit_idx = n - 1
        _t, last_bid = points[-1]
        exit_price = last_bid + spread if side == "short" else last_bid
        exit_reason = "終了"

    return exit_idx, exit_price, exit_reason


def simulate_namping_trade(
    points,
    signal_idx,
    side,
    base_entry_price,
    spread,
    stop,
    take,
    fixed_exit_price=True,
    time_close_seconds=0.0,
    minute_close_info=None,
    first_entry_enabled=True,
    steps=None,
    should_cancel=None,
    extra_stop_price=None,
    extra_stop_reason="抜け撤退",
    fast_take_min_pips=0.0,
    fast_take_window_ms=0.0,
    fast_take_pips=0.0,
):
    if not points:
        return None

    n = len(points)
    if signal_idx < 0 or signal_idx >= n:
        return None

    if steps is None:
        steps = []

    enabled_steps = []
    for step in steps:
        if not step.get("enabled", False):
            continue
        enabled_steps.append(
            {
                "pips": float(step.get("pips", 0.0)),
                "lot": float(step.get("lot", 0.0)),
                "label": step.get("label") or "段階",
                "done": False,
                "trigger": 0.0,
            }
        )

    cumulative = 0.0
    for step in enabled_steps:
        cumulative += step["pips"]
        step["trigger"] = cumulative

    if not first_entry_enabled and not enabled_steps:
        return None

    stop_ready = False

    entries = []
    total_lot = 0.0
    avg_price = None
    first_entry_idx = None
    last_entry_time = None

    def add_entry(idx, price, lot, kind):
        nonlocal total_lot, avg_price, first_entry_idx, last_entry_time
        if lot <= 0:
            return
        if avg_price is None:
            avg_price = price
            total_lot = lot
        else:
            new_total = total_lot + lot
            avg_price = (avg_price * total_lot + price * lot) / new_total
            total_lot = new_total
        if first_entry_idx is None:
            first_entry_idx = idx
        last_entry_time = points[idx][0]
        entries.append({"idx": idx, "price": price, "lot": lot, "kind": kind})

    if first_entry_enabled:
        add_entry(signal_idx, base_entry_price, 1.0, "初回")
        if not enabled_steps:
            stop_ready = True

    forced_close_time = None
    if time_close_seconds and time_close_seconds > 0 and last_entry_time is not None:
        forced_close_time = last_entry_time + timedelta(seconds=time_close_seconds)

    fast_take_enabled = (
        fast_take_min_pips > 0 and fast_take_window_ms > 0 and fast_take_pips > 0
    )
    fast_take_window = (
        timedelta(milliseconds=fast_take_window_ms) if fast_take_enabled else None
    )
    fast_take_move = fast_take_pips * PIP_SIZE
    min_price_deque = deque()
    max_price_deque = deque()

    j = signal_idx + 1
    while j < n:
        if is_cancel_requested(should_cancel):
            raise InterruptedError("cancelled")
        ts, bid = points[j]
        ask = bid + spread
        current_entry_price = ask if side == "long" else bid
        if side == "long":
            adverse_move = base_entry_price - current_entry_price
        else:
            adverse_move = current_entry_price - base_entry_price

        for step in enabled_steps:
            if step["done"]:
                continue
            if adverse_move >= step["trigger"]:
                add_entry(j, current_entry_price, step["lot"], step["label"])
                step["done"] = True
                if time_close_seconds and time_close_seconds > 0:
                    forced_close_time = ts + timedelta(seconds=time_close_seconds)
        if enabled_steps and all(step["done"] for step in enabled_steps):
            stop_ready = True

        if total_lot <= 0:
            j += 1
            continue

        if fast_take_enabled and avg_price is not None:
            while min_price_deque and min_price_deque[-1][1] >= bid:
                min_price_deque.pop()
            min_price_deque.append((ts, bid))
            while min_price_deque and min_price_deque[0][0] < ts - fast_take_window:
                min_price_deque.popleft()

            while max_price_deque and max_price_deque[-1][1] <= bid:
                max_price_deque.pop()
            max_price_deque.append((ts, bid))
            while max_price_deque and max_price_deque[0][0] < ts - fast_take_window:
                max_price_deque.popleft()

            if min_price_deque and max_price_deque:
                min_price = min_price_deque[0][1]
                max_price = max_price_deque[0][1]
                if side == "long":
                    if bid - min_price >= fast_take_move:
                        exit_price = bid
                        profit = (exit_price - avg_price) / PIP_SIZE
                        if profit >= fast_take_min_pips:
                            return {
                                "entry_idx": first_entry_idx,
                                "entry_price": entries[0]["price"],
                                "avg_entry_price": avg_price,
                                "lot_total": total_lot,
                                "entries": entries,
                                "exit_idx": j,
                                "exit_price": exit_price,
                                "exit_reason": "急伸利確",
                            }
                else:
                    if max_price - bid >= fast_take_move:
                        exit_price = ask
                        profit = (avg_price - exit_price) / PIP_SIZE
                        if profit >= fast_take_min_pips:
                            return {
                                "entry_idx": first_entry_idx,
                                "entry_price": entries[0]["price"],
                                "avg_entry_price": avg_price,
                                "lot_total": total_lot,
                                "entries": entries,
                                "exit_idx": j,
                                "exit_price": exit_price,
                                "exit_reason": "急伸利確",
                            }

        if extra_stop_price is not None:
            if side == "long":
                if bid <= extra_stop_price:
                    exit_price = extra_stop_price if fixed_exit_price else bid
                    return {
                        "entry_idx": first_entry_idx,
                        "entry_price": entries[0]["price"],
                        "avg_entry_price": avg_price,
                        "lot_total": total_lot,
                        "entries": entries,
                        "exit_idx": j,
                        "exit_price": exit_price,
                        "exit_reason": extra_stop_reason,
                    }
            else:
                if ask >= extra_stop_price:
                    exit_price = extra_stop_price if fixed_exit_price else ask
                    return {
                        "entry_idx": first_entry_idx,
                        "entry_price": entries[0]["price"],
                        "avg_entry_price": avg_price,
                        "lot_total": total_lot,
                        "entries": entries,
                        "exit_idx": j,
                        "exit_price": exit_price,
                        "exit_reason": extra_stop_reason,
                    }

        last_entry_price = entries[-1]["price"]
        if side == "long":
            stop_price = last_entry_price - stop
            take_price = avg_price + take
            if stop > 0 and stop_ready and bid <= stop_price:
                exit_price = stop_price if fixed_exit_price else bid
                return {
                    "entry_idx": first_entry_idx,
                    "entry_price": entries[0]["price"],
                    "avg_entry_price": avg_price,
                    "lot_total": total_lot,
                    "entries": entries,
                    "exit_idx": j,
                    "exit_price": exit_price,
                    "exit_reason": "損切",
                }
            if take > 0 and bid >= take_price:
                exit_price = take_price if fixed_exit_price else bid
                return {
                    "entry_idx": first_entry_idx,
                    "entry_price": entries[0]["price"],
                    "avg_entry_price": avg_price,
                    "lot_total": total_lot,
                    "entries": entries,
                    "exit_idx": j,
                    "exit_price": exit_price,
                    "exit_reason": "利確",
                }
        else:
            stop_price = last_entry_price + stop
            take_price = avg_price - take
            if stop > 0 and stop_ready and ask >= stop_price:
                exit_price = stop_price if fixed_exit_price else ask
                return {
                    "entry_idx": first_entry_idx,
                    "entry_price": entries[0]["price"],
                    "avg_entry_price": avg_price,
                    "lot_total": total_lot,
                    "entries": entries,
                    "exit_idx": j,
                    "exit_price": exit_price,
                    "exit_reason": "損切",
                }
            if take > 0 and ask <= take_price:
                exit_price = take_price if fixed_exit_price else ask
                return {
                    "entry_idx": first_entry_idx,
                    "entry_price": entries[0]["price"],
                    "avg_entry_price": avg_price,
                    "lot_total": total_lot,
                    "entries": entries,
                    "exit_idx": j,
                    "exit_price": exit_price,
                    "exit_reason": "利確",
                }

        if forced_close_time is not None and ts >= forced_close_time:
            exit_price = bid if side == "long" else bid + spread
            return {
                "entry_idx": first_entry_idx,
                "entry_price": entries[0]["price"],
                "avg_entry_price": avg_price,
                "lot_total": total_lot,
                "entries": entries,
                "exit_idx": j,
                "exit_price": exit_price,
                "exit_reason": "時間",
            }

        j += 1

    if total_lot <= 0 or not entries:
        return None

    exit_idx = n - 1
    _t, last_bid = points[-1]
    exit_price = last_bid + spread if side == "short" else last_bid
    return {
        "entry_idx": first_entry_idx,
        "entry_price": entries[0]["price"],
        "avg_entry_price": avg_price,
        "lot_total": total_lot,
        "entries": entries,
        "exit_idx": exit_idx,
        "exit_price": exit_price,
        "exit_reason": "終了",
    }


def run_backtest(points, params, runtime_cache=None, should_cancel=None):
    if not points:
        return {
            "trades": [],
            "summary": summarize_trades([]),
            "equity_curve": [],
            "ma_series": [],
            "ma_enabled": params.get("ma_enabled", False),
            "ma_period": params.get("ma_period", 0),
            "ma_unit": params.get("ma_unit", "min"),
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
    common_stop_pips = float(
        params.get("common_stop_pips", params.get("stop_pips", 0.0))
    )
    common_take_pips = float(
        params.get("common_take_pips", params.get("take_pips", 0.0))
    )
    common_time_close_seconds = float(
        params.get("common_time_close_seconds", params.get("time_close_seconds", 0.0))
    )
    common_fixed_exit_price = bool(
        params.get("common_fixed_exit_price", params.get("fixed_exit_price", True))
    )
    common_fast_take_min = float(params.get("common_fast_take_min", 0.0))
    common_fast_take_window_ms = float(params.get("common_fast_take_window_ms", 0.0))
    common_fast_take_pips = float(params.get("common_fast_take_pips", 0.0))
    common_stop_enabled = bool(params.get("common_stop_enabled", True))
    common_take_enabled = bool(params.get("common_take_enabled", True))
    common_time_enabled = bool(params.get("common_time_enabled", True))
    common_fast_take_enabled = bool(params.get("common_fast_take_enabled", True))
    common_stop_override = bool(params.get("common_stop_override", True))
    common_take_override = bool(params.get("common_take_override", True))
    common_time_override = bool(params.get("common_time_override", True))
    common_fixed_override = bool(params.get("common_fixed_override", True))
    common_namping_override = bool(params.get("common_namping_override", True))
    common_fast_take_override = bool(params.get("common_fast_take_override", True))
    ma_enabled = params.get("ma_enabled", False)
    ma_period = max(1, int(params.get("ma_period", 0)))
    ma_unit = params.get("ma_unit", "min")
    if ma_unit not in ("sec", "min"):
        ma_unit = "min"
    ma_deviation = params.get("ma_deviation_rate", 0.0)
    extreme_enabled = params.get("extreme_enabled", False)
    extreme_hold_ms = params.get("extreme_hold_ms", 0.0)
    extreme_distance = params.get("extreme_distance_pips", 0.0) * PIP_SIZE
    exclude_enabled = params.get("exclude_enabled", False)
    exclude_hours = params.get("exclude_hours", set())
    allow_same_direction = bool(params.get("allow_same_direction", False))
    allow_opposite_direction = bool(params.get("allow_opposite_direction", False))
    allow_overlap = allow_same_direction or allow_opposite_direction
    momentum_window_ms = float(params.get("momentum_window_ms", 1000.0))
    momentum_spike = float(params.get("momentum_spike_pips", 3.0)) * PIP_SIZE
    momentum_boundary_pct = float(params.get("momentum_boundary_pct", 50.0))
    momentum_tick_min_per_min = float(params.get("momentum_tick_min_per_min", 100.0))
    momentum_hold_seconds = float(params.get("momentum_hold_seconds", 2.0))
    momentum_max_move = float(params.get("momentum_max_pips", 0.0)) * PIP_SIZE
    reverse_window_seconds = float(params.get("reverse_window_seconds", 2.0))
    reverse_move = float(params.get("reverse_pips", 3.0)) * PIP_SIZE
    reverse_hold_seconds = float(params.get("reverse_hold_seconds", 2.0))
    reverse_max_pullback = float(params.get("reverse_max_pullback_pips", 5.0)) * PIP_SIZE
    reverse_monitor_stop_enabled = bool(
        params.get("reverse_monitor_stop_enabled", False)
    )
    reverse_monitor_stop_pips = float(
        params.get("reverse_monitor_stop_pips", 0.0)
    ) * PIP_SIZE
    signal_chain_pos_move = float(params.get("signal_chain_pos_pips", 10.0)) * PIP_SIZE
    signal_chain_neg_move = float(params.get("signal_chain_neg_pips", 5.0)) * PIP_SIZE
    signal_chain_count = int(params.get("signal_chain_count", 3))
    signal_chain_monitor_minutes = float(
        params.get("signal_chain_monitor_minutes", 240.0)
    )
    signal_chain_ignore_reverse = bool(
        params.get("signal_chain_ignore_reverse", False)
    )
    signal_chain_ignore_momentum = bool(
        params.get("signal_chain_ignore_momentum", False)
    )
    signal_chain_ignore_sr = bool(params.get("signal_chain_ignore_sr", False))
    signal_chain_ignore_spike = bool(params.get("signal_chain_ignore_spike", False))
    signal_chain_enabled = bool(params.get("signal_chain_enabled", True))
    signal_chain_count_reverse = bool(
        params.get("signal_chain_count_reverse", True)
    )
    signal_chain_count_momentum = bool(
        params.get("signal_chain_count_momentum", True)
    )
    signal_chain_count_sr = bool(params.get("signal_chain_count_sr", True))
    signal_chain_count_spike = bool(params.get("signal_chain_count_spike", True))
    signal_chain_trigger_reverse = bool(
        params.get("signal_chain_trigger_reverse", True)
    )
    signal_chain_trigger_momentum = bool(
        params.get("signal_chain_trigger_momentum", True)
    )
    signal_chain_trigger_sr = bool(params.get("signal_chain_trigger_sr", True))
    signal_chain_trigger_spike = bool(params.get("signal_chain_trigger_spike", True))
    entry_mode = params.get("entry_mode", "spike")
    entry_spike_enabled = bool(
        params.get("entry_spike_enabled", entry_mode in ("spike", "both", "multi"))
    )
    entry_sr_enabled = bool(
        params.get("entry_sr_enabled", entry_mode in ("sr_reentry", "both", "multi"))
    )
    entry_momentum_enabled = bool(
        params.get("entry_momentum_enabled", entry_mode in ("momentum", "multi"))
    )
    entry_reverse_enabled = bool(
        params.get("entry_reverse_enabled", entry_mode in ("reverse", "multi"))
    )

    def build_namping(prefix):
        return {
            "first_enabled": bool(params.get(f"{prefix}namping_first_enabled", True)),
            "steps": [
                {
                    "enabled": bool(
                        params.get(f"{prefix}namping_step1_enabled", True)
                    ),
                    "pips": float(params.get(f"{prefix}namping_step1_pips", 5.0))
                    * PIP_SIZE,
                    "lot": float(params.get(f"{prefix}namping_step1_lot", 2.0)),
                    "label": "段階1",
                },
                {
                    "enabled": bool(
                        params.get(f"{prefix}namping_step2_enabled", True)
                    ),
                    "pips": float(params.get(f"{prefix}namping_step2_pips", 5.0))
                    * PIP_SIZE,
                    "lot": float(params.get(f"{prefix}namping_step2_lot", 4.0)),
                    "label": "段階2",
                },
                {
                    "enabled": bool(
                        params.get(f"{prefix}namping_step3_enabled", False)
                    ),
                    "pips": float(params.get(f"{prefix}namping_step3_pips", 5.0))
                    * PIP_SIZE,
                    "lot": float(params.get(f"{prefix}namping_step3_lot", 8.0)),
                    "label": "段階3",
                },
                {
                    "enabled": bool(
                        params.get(f"{prefix}namping_step4_enabled", False)
                    ),
                    "pips": float(params.get(f"{prefix}namping_step4_pips", 5.0))
                    * PIP_SIZE,
                    "lot": float(params.get(f"{prefix}namping_step4_lot", 16.0)),
                    "label": "段階4",
                },
                {
                    "enabled": bool(
                        params.get(f"{prefix}namping_step5_enabled", False)
                    ),
                    "pips": float(params.get(f"{prefix}namping_step5_pips", 5.0))
                    * PIP_SIZE,
                    "lot": float(params.get(f"{prefix}namping_step5_lot", 32.0)),
                    "label": "段階5",
                },
            ],
        }

    common_namping = build_namping("common_")
    reverse_namping = build_namping("reverse_")
    momentum_namping = build_namping("momentum_")
    sr_namping = build_namping("sr_")
    spike_namping = build_namping("spike_")

    namping_map = {
        "reverse": reverse_namping,
        "momentum": momentum_namping,
        "sr": sr_namping,
        "spike": spike_namping,
    }

    def resolve_trade_params(kind):
        stop_enabled = (
            common_stop_enabled
            if common_stop_override
            else bool(params.get(f"{kind}_stop_enabled", True))
        )
        take_enabled = (
            common_take_enabled
            if common_take_override
            else bool(params.get(f"{kind}_take_enabled", True))
        )
        time_enabled = (
            common_time_enabled
            if common_time_override
            else bool(params.get(f"{kind}_time_enabled", True))
        )
        fast_enabled = (
            common_fast_take_enabled
            if common_fast_take_override
            else bool(params.get(f"{kind}_fast_take_enabled", True))
        )
        stop_pips = (
            common_stop_pips
            if common_stop_override
            else float(params.get(f"{kind}_stop_pips", common_stop_pips))
        )
        take_pips = (
            common_take_pips
            if common_take_override
            else float(params.get(f"{kind}_take_pips", common_take_pips))
        )
        time_close_seconds = (
            common_time_close_seconds
            if common_time_override
            else float(
                params.get(f"{kind}_time_close_seconds", common_time_close_seconds)
            )
        )
        fixed_exit_price = (
            common_fixed_exit_price
            if common_fixed_override
            else bool(
                params.get(f"{kind}_fixed_exit_price", common_fixed_exit_price)
            )
        )
        fast_take_min = (
            common_fast_take_min
            if common_fast_take_override
            else float(params.get(f"{kind}_fast_take_min", common_fast_take_min))
        )
        fast_take_window_ms = (
            common_fast_take_window_ms
            if common_fast_take_override
            else float(
                params.get(f"{kind}_fast_take_window_ms", common_fast_take_window_ms)
            )
        )
        fast_take_pips = (
            common_fast_take_pips
            if common_fast_take_override
            else float(params.get(f"{kind}_fast_take_pips", common_fast_take_pips))
        )
        namping = common_namping if common_namping_override else namping_map[kind]
        return {
            "stop": stop_pips * PIP_SIZE if stop_enabled else 0.0,
            "take": take_pips * PIP_SIZE if take_enabled else 0.0,
            "time_close_seconds": time_close_seconds if time_enabled else 0.0,
            "fixed_exit_price": fixed_exit_price,
            "fast_take_min": fast_take_min if fast_enabled else 0.0,
            "fast_take_window_ms": fast_take_window_ms if fast_enabled else 0.0,
            "fast_take_pips": fast_take_pips if fast_enabled else 0.0,
            "namping_first_enabled": namping["first_enabled"],
            "namping_steps": namping["steps"],
        }

    trade_params_by_kind = {
        "reverse": resolve_trade_params("reverse"),
        "momentum": resolve_trade_params("momentum"),
        "sr": resolve_trade_params("sr"),
        "spike": resolve_trade_params("spike"),
    }

    candle_times = []
    ma_values = []
    ma_series = []
    if ma_enabled:
        ma_cache = runtime_cache.setdefault("ma_cache", {})
        ma_key = (ma_unit, ma_period)
        ma_entry = ma_cache.get(ma_key)
        if ma_entry is None:
            if ma_unit == "sec":
                ma_entry = build_second_ma(points_sorted, ma_period, should_cancel=should_cancel)
            else:
                candle_cache = runtime_cache.setdefault("candle_cache", {})
                minute_candles = candle_cache.get(1)
                if minute_candles is None:
                    minute_candles = build_minute_candles(points_sorted, should_cancel=should_cancel)
                    candle_cache[1] = minute_candles
                ma_entry = build_minute_ma(minute_candles, ma_period, should_cancel=should_cancel)
            ma_cache[ma_key] = ma_entry
        candle_times, ma_values, ma_series = ma_entry
        if ma_series and len(ma_series) > 10000:
            ma_series = [point for _, point in downsample_points(ma_series, 5000)]

    def resolve_ma_value(entry_time):
        if not ma_enabled:
            return None
        if not candle_times:
            return None
        candle_idx = bisect_right(candle_times, entry_time) - 2
        if candle_idx < 0 or candle_idx >= len(ma_values):
            return None
        ma_value = ma_values[candle_idx]
        if ma_value is None or ma_value <= 0:
            return None
        return ma_value

    minute_close_info = None

    momentum_window = None
    second_lows = None
    second_highs = None
    if entry_momentum_enabled:
        momentum_window = timedelta(milliseconds=momentum_window_ms)
        second_extremes = runtime_cache.get("second_extremes")
        if second_extremes is None or second_extremes.get("points_ref") is not points_sorted:
            lows, highs = build_second_extremes(points_sorted, should_cancel=should_cancel)
            second_extremes = {
                "points_ref": points_sorted,
                "lows": lows,
                "highs": highs,
            }
            runtime_cache["second_extremes"] = second_extremes
        second_lows = second_extremes.get("lows")
        second_highs = second_extremes.get("highs")

    trades = []
    active_positions_sr = []
    active_positions_spike = []
    active_positions_momentum = []
    active_positions_reverse = []

    def can_open_position(entry_idx, side, positions):
        if not allow_overlap:
            return True
        positions[:] = [pos for pos in positions if pos["exit_idx"] > entry_idx]
        has_same = any(pos["side"] == side for pos in positions)
        has_opposite = any(pos["side"] != side for pos in positions)
        if has_same and not allow_same_direction:
            return False
        if has_opposite and not allow_opposite_direction:
            return False
        return True

    def register_position(side, exit_idx, positions):
        if allow_overlap:
            positions.append({"side": side, "exit_idx": exit_idx})

    n = len(points_sorted)
    signal_chain_count = max(1, signal_chain_count)
    signal_chain_pos_enabled = signal_chain_pos_move > 0
    signal_chain_neg_enabled = signal_chain_neg_move > 0

    def signal_key(signal):
        return (
            signal.get("source"),
            signal.get("entry_idx"),
            signal.get("side"),
        )

    def signal_passes_filters(signal):
        entry_idx = signal["entry_idx"]
        entry_time, entry_bid = points_sorted[entry_idx]
        side = signal["side"]
        entry_price = entry_bid + spread if side == "long" else entry_bid
        if exclude_enabled and entry_time.hour in exclude_hours:
            return False
        if ma_enabled:
            ma_value = resolve_ma_value(entry_time)
            if ma_value is None:
                return False
            if side == "long":
                deviation = (ma_value - entry_price) / ma_value
            else:
                deviation = (entry_price - ma_value) / ma_value
            if deviation < ma_deviation:
                return False
        return True

    def build_signal_gate(all_signals):
        if not all_signals:
            return set(), {}
        allowed = set()
        chain_map = {}
        pending = {"long": None, "short": None}
        countable_sources = {
            "reverse": signal_chain_count_reverse,
            "momentum": signal_chain_count_momentum,
            "sr": signal_chain_count_sr,
            "spike": signal_chain_count_spike,
        }
        trigger_sources = {
            "reverse": signal_chain_trigger_reverse,
            "momentum": signal_chain_trigger_momentum,
            "sr": signal_chain_trigger_sr,
            "spike": signal_chain_trigger_spike,
        }
        ignore_sources = {
            "reverse": signal_chain_ignore_reverse,
            "momentum": signal_chain_ignore_momentum,
            "sr": signal_chain_ignore_sr,
            "spike": signal_chain_ignore_spike,
        }

        def new_state(side, idx, price, ts, countable, signal):
            expires_at = None
            if signal_chain_monitor_minutes and signal_chain_monitor_minutes > 0:
                expires_at = ts + timedelta(minutes=signal_chain_monitor_minutes)
            counted = []
            if countable and signal is not None:
                counted.append(
                    {
                        "entry_idx": signal.get("entry_idx"),
                        "side": signal.get("side"),
                        "source": signal.get("source"),
                    }
                )
            return {
                "side": side,
                "first_idx": idx,
                "first_price": price,
                "max_price": price,
                "min_price": price,
                "count": 1 if countable else 0,
                "last_idx": idx,
                "expires_at": expires_at,
                "counted": counted,
            }

        def update_state(state, idx):
            if idx <= state["last_idx"]:
                return
            for k in range(state["last_idx"] + 1, idx + 1):
                price = points_sorted[k][1]
                if state["side"] == "long":
                    if price > state["max_price"]:
                        state["max_price"] = price
                else:
                    if price < state["min_price"]:
                        state["min_price"] = price
            state["last_idx"] = idx

        ordered = sorted(
            all_signals,
            key=lambda s: (s.get("entry_idx", -1), s.get("source") or ""),
        )
        for sig in ordered:
            if not signal_passes_filters(sig):
                continue
            idx = sig["entry_idx"]
            ts, price = points_sorted[idx]
            countable = countable_sources.get(sig.get("source"), True)
            triggerable = trigger_sources.get(sig.get("source"), True)

            for side_key in ("long", "short"):
                state = pending.get(side_key)
                if not state:
                    continue
                if state["expires_at"] is not None and ts > state["expires_at"]:
                    pending[side_key] = None
                    continue
                update_state(state, idx)

            side = sig["side"]
            other = "short" if side == "long" else "long"
            if ignore_sources.get(sig.get("source"), False) and pending.get(other):
                pending[other] = None

            state = pending.get(side)
            if state is None:
                state = new_state(side, idx, price, ts, countable, sig)
                pending[side] = state
            else:
                if countable:
                    state["count"] += 1
                    state["counted"].append(
                        {
                            "entry_idx": sig.get("entry_idx"),
                            "side": sig.get("side"),
                            "source": sig.get("source"),
                        }
                    )

            if side == "long":
                favorable = state["max_price"] - state["first_price"]
                adverse = state["first_price"] - price
            else:
                favorable = state["first_price"] - state["min_price"]
                adverse = price - state["first_price"]

            if signal_chain_pos_enabled and favorable >= signal_chain_pos_move:
                pending[side] = new_state(side, idx, price, ts, countable, sig)
                continue
            if countable:
                meets_count = state["count"] >= signal_chain_count
            else:
                meets_count = state["count"] + 1 >= signal_chain_count
            if signal_chain_neg_enabled:
                if adverse >= signal_chain_neg_move and meets_count and triggerable:
                    allowed.add(signal_key(sig))
                    chain_map[signal_key(sig)] = list(state["counted"])
                    pending[side] = None
            else:
                if meets_count and triggerable:
                    allowed.add(signal_key(sig))
                    chain_map[signal_key(sig)] = list(state["counted"])
                    pending[side] = None

        return allowed, chain_map
    gate_signals = []

    if signal_chain_enabled and entry_sr_enabled:
        sr_break_pips = params.get("sr_break_pips", 5.0)
        sr_tick_limit = int(params.get("sr_tick_limit", 10))
        sr_tick_min = float(params.get("sr_tick_min", 0.0))
        sr_wait_bars = int(params.get("sr_wait_bars", 0))
        sr_min_seconds = float(params.get("sr_min_seconds", 0.0))
        sr_max_seconds = float(params.get("sr_max_seconds", 60.0))
        sr_midpoint_pct = float(params.get("sr_midpoint_pct", 50.0))
        sr_dominance_pct = float(params.get("sr_dominance_pct", 50.0))
        sr_move_ratio_pct = float(params.get("sr_move_ratio_pct", 100.0))
        sr_move_speed_ratio_pct = float(params.get("sr_move_speed_ratio_pct", 100.0))
        sr_favored_tick_min_pips = float(params.get("sr_favored_tick_min_pips", 1.0))
        sr_move_ratio_enabled = bool(params.get("sr_move_ratio_enabled", True))
        sr_move_speed_ratio_enabled = bool(params.get("sr_move_speed_ratio_enabled", True))
        sr_ratio_join_mode = params.get("sr_ratio_join_mode", "and")
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
                line_candles = build_timeframe_candles(
                    points_sorted, line_interval, should_cancel=should_cancel
                )
                candle_cache[line_interval] = line_candles
            lines = build_reentry_lines(
                line_candles,
                sr_params,
                range_params,
                sr_target,
                should_cancel=should_cancel,
            )
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
        favored_tick_min_move = sr_favored_tick_min_pips * PIP_SIZE
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

        bin_state_gate = {}
        disabled_lines_gate = set()
        start_limits_cache = line_entry.setdefault("start_limits_cache", {})
        start_limits = start_limits_cache.get(sr_wait_bars)
        if start_limits is None:
            wait_delta = timedelta(minutes=line_interval * sr_wait_bars)
            start_limits = [start_time + wait_delta for start_time in line_start_times]
            start_limits_cache[sr_wait_bars] = start_limits

        i_gate = 0
        while i_gate < n - 1:
            if is_cancel_requested(should_cancel):
                raise InterruptedError("cancelled")
            signal = find_sr_reentry_signal(
                points_sorted,
                i_gate,
                lines,
                max_break,
                sr_tick_limit,
                sr_tick_min,
                line_bins,
                bin_size,
                bin_state_gate,
                line_start_times,
                disabled_lines_gate,
                end_limits,
                start_limits,
                sr_min_seconds,
                sr_max_seconds,
                sr_midpoint_pct,
                sr_dominance_pct,
                sr_move_ratio_pct,
                sr_move_speed_ratio_pct,
                favored_tick_min_move,
                sr_move_ratio_enabled,
                sr_move_speed_ratio_enabled,
                sr_ratio_join_mode,
                should_cancel,
            )
            if not signal:
                break
            gate_signal = dict(signal)
            gate_signal["source"] = "sr"
            gate_signals.append(gate_signal)
            i_gate = signal["entry_idx"] + 1

    if signal_chain_enabled and entry_spike_enabled:
        i_gate = 0
        while i_gate < n - 1:
            if is_cancel_requested(should_cancel):
                raise InterruptedError("cancelled")
            signal = find_spike_signal(
                points_sorted,
                times,
                i_gate,
                window,
                spike,
                retrace_rate,
                should_cancel,
            )
            if not signal:
                i_gate += 1
                continue
            gate_signal = dict(signal)
            gate_signal["source"] = "spike"
            gate_signals.append(gate_signal)
            i_gate = signal["entry_idx"] + 1

    if signal_chain_enabled and entry_momentum_enabled:
        i_gate = 0
        while i_gate < n - 1:
            if is_cancel_requested(should_cancel):
                raise InterruptedError("cancelled")
            signal = find_momentum_signal(
                points_sorted,
                times,
                i_gate,
                momentum_window,
                momentum_spike,
                momentum_boundary_pct,
                momentum_tick_min_per_min,
                momentum_hold_seconds,
                momentum_max_move,
                second_lows,
                second_highs,
                should_cancel,
            )
            if not signal:
                i_gate += 1
                continue
            gate_signal = dict(signal)
            gate_signal["source"] = "momentum"
            gate_signals.append(gate_signal)
            i_gate = signal["entry_idx"] + 1

    if signal_chain_enabled and entry_reverse_enabled:
        reverse_window = timedelta(seconds=reverse_window_seconds)
        i_gate = 0
        while i_gate < n - 1:
            if is_cancel_requested(should_cancel):
                raise InterruptedError("cancelled")
            signal = find_reverse_signal(
                points_sorted,
                times,
                i_gate,
                reverse_window,
                reverse_move,
                reverse_hold_seconds,
                reverse_max_pullback,
                should_cancel,
            )
            if not signal:
                i_gate += 1
                continue
            gate_signal = dict(signal)
            gate_signal["source"] = "reverse"
            gate_signals.append(gate_signal)
            i_gate = signal["entry_idx"] + 1

    if signal_chain_enabled:
        allowed_signals, chain_map = build_signal_gate(gate_signals)
    else:
        allowed_signals, chain_map = set(), {}

    if entry_sr_enabled:
        sr_break_pips = params.get("sr_break_pips", 5.0)
        sr_tick_limit = int(params.get("sr_tick_limit", 10))
        sr_tick_min = float(params.get("sr_tick_min", 0.0))
        sr_wait_bars = int(params.get("sr_wait_bars", 0))
        sr_min_seconds = float(params.get("sr_min_seconds", 0.0))
        sr_max_seconds = float(params.get("sr_max_seconds", 60.0))
        sr_midpoint_pct = float(params.get("sr_midpoint_pct", 50.0))
        sr_dominance_pct = float(params.get("sr_dominance_pct", 50.0))
        sr_move_ratio_pct = float(params.get("sr_move_ratio_pct", 100.0))
        sr_move_speed_ratio_pct = float(params.get("sr_move_speed_ratio_pct", 100.0))
        sr_favored_tick_min_pips = float(params.get("sr_favored_tick_min_pips", 1.0))
        sr_move_ratio_enabled = bool(params.get("sr_move_ratio_enabled", True))
        sr_move_speed_ratio_enabled = bool(params.get("sr_move_speed_ratio_enabled", True))
        sr_ratio_join_mode = params.get("sr_ratio_join_mode", "and")
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
                line_candles = build_timeframe_candles(
                    points_sorted, line_interval, should_cancel=should_cancel
                )
                candle_cache[line_interval] = line_candles
            lines = build_reentry_lines(
                line_candles,
                sr_params,
                range_params,
                sr_target,
                should_cancel=should_cancel,
            )
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
        favored_tick_min_move = sr_favored_tick_min_pips * PIP_SIZE
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

        i = 0
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
                sr_move_ratio_pct,
                sr_move_speed_ratio_pct,
                favored_tick_min_move,
                sr_move_ratio_enabled,
                sr_move_speed_ratio_enabled,
                sr_ratio_join_mode,
                should_cancel,
            )
            if not signal:
                break

            entry_idx = signal["entry_idx"]
            signal["source"] = "sr"
            gate_key = signal_key(signal)
            if signal_chain_enabled and gate_signals and gate_key not in allowed_signals:
                i = entry_idx + 1
                continue
            chain_signals = chain_map.get(gate_key, []) if signal_chain_enabled else []
            side = signal["side"]
            entry_time, entry_bid = points_sorted[entry_idx]
            entry_price = entry_bid + spread if side == "long" else entry_bid

            if exclude_enabled and entry_time.hour in exclude_hours:
                i = entry_idx + 1
                continue

            if ma_enabled:
                ma_value = resolve_ma_value(entry_time)
                if ma_value is None:
                    i = entry_idx + 1
                    continue
                if side == "long":
                    deviation = (ma_value - entry_price) / ma_value
                else:
                    deviation = (entry_price - ma_value) / ma_value
                if deviation < ma_deviation:
                    i = entry_idx + 1
                    continue

            trade_params = trade_params_by_kind["sr"]
            trade_result = simulate_namping_trade(
                points_sorted,
                entry_idx,
                side,
                entry_price,
                spread,
                trade_params["stop"],
                trade_params["take"],
                trade_params["fixed_exit_price"],
                trade_params["time_close_seconds"],
                None,
                trade_params["namping_first_enabled"],
                trade_params["namping_steps"],
                should_cancel,
                fast_take_min_pips=trade_params["fast_take_min"],
                fast_take_window_ms=trade_params["fast_take_window_ms"],
                fast_take_pips=trade_params["fast_take_pips"],
            )
            if not trade_result:
                i = entry_idx + 1
                continue

            entry_idx_actual = trade_result["entry_idx"]
            if entry_idx_actual is None:
                i = entry_idx + 1
                continue
            if not can_open_position(entry_idx_actual, side, active_positions_sr):
                i = entry_idx + 1
                continue

            entry_time = points_sorted[entry_idx_actual][0]
            entry_price_actual = trade_result["entry_price"]
            avg_entry_price = trade_result["avg_entry_price"]
            total_lot = trade_result["lot_total"]
            exit_idx = trade_result["exit_idx"]
            exit_price = trade_result["exit_price"]
            exit_reason = trade_result["exit_reason"]

            if side == "long":
                pips_per_lot = (exit_price - avg_entry_price) / PIP_SIZE
            else:
                pips_per_lot = (avg_entry_price - exit_price) / PIP_SIZE
            pips = pips_per_lot * total_lot

            trades.append(
                {
                    "side": side,
                    "entry_reason": signal.get("entry_reason", "水平線戻り"),
                    "entry_idx": entry_idx_actual,
                    "entry_time": entry_time,
                    "entry_price": entry_price_actual,
                    "avg_entry_price": avg_entry_price,
                    "lot_total": total_lot,
                    "pips_per_lot": pips_per_lot,
                    "namping_entries": trade_result.get("entries") or [],
                    "signal_chain": chain_signals,
                    "exit_idx": exit_idx,
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
                    "up_avg_move": signal.get("up_avg_move"),
                    "down_avg_move": signal.get("down_avg_move"),
                    "up_move_speed": signal.get("up_move_speed"),
                    "down_move_speed": signal.get("down_move_speed"),
                    "move_ratio_ok": signal.get("move_ratio_ok"),
                    "speed_ratio_ok": signal.get("speed_ratio_ok"),
                }
            )
            register_position(side, exit_idx, active_positions_sr)
            i = entry_idx_actual + 1 if allow_overlap else exit_idx + 1
    if entry_spike_enabled:
        i = 0
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
            signal["source"] = "spike"
            gate_key = signal_key(signal)
            if signal_chain_enabled and gate_signals and gate_key not in allowed_signals:
                i = entry_idx + 1
                continue
            chain_signals = chain_map.get(gate_key, []) if signal_chain_enabled else []
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
                ma_value = resolve_ma_value(entry_time)
                if ma_value is None:
                    i = entry_idx + 1
                    continue
                if side == "long":
                    deviation = (ma_value - entry_price) / ma_value
                else:
                    deviation = (entry_price - ma_value) / ma_value
                if deviation < ma_deviation:
                    i = entry_idx + 1
                    continue

            trade_params = trade_params_by_kind["spike"]
            trade_result = simulate_namping_trade(
                points_sorted,
                entry_idx,
                side,
                entry_price,
                spread,
                trade_params["stop"],
                trade_params["take"],
                trade_params["fixed_exit_price"],
                trade_params["time_close_seconds"],
                None,
                trade_params["namping_first_enabled"],
                trade_params["namping_steps"],
                should_cancel,
                fast_take_min_pips=trade_params["fast_take_min"],
                fast_take_window_ms=trade_params["fast_take_window_ms"],
                fast_take_pips=trade_params["fast_take_pips"],
            )
            if not trade_result:
                i = entry_idx + 1
                continue

            entry_idx_actual = trade_result["entry_idx"]
            if entry_idx_actual is None:
                i = entry_idx + 1
                continue
            if not can_open_position(entry_idx_actual, side, active_positions_spike):
                i = entry_idx + 1
                continue

            entry_time = points_sorted[entry_idx_actual][0]
            entry_price_actual = trade_result["entry_price"]
            avg_entry_price = trade_result["avg_entry_price"]
            total_lot = trade_result["lot_total"]
            exit_idx = trade_result["exit_idx"]
            exit_price = trade_result["exit_price"]
            exit_reason = trade_result["exit_reason"]

            if side == "long":
                pips_per_lot = (exit_price - avg_entry_price) / PIP_SIZE
            else:
                pips_per_lot = (avg_entry_price - exit_price) / PIP_SIZE
            pips = pips_per_lot * total_lot

            trades.append(
                {
                    "side": side,
                    "entry_reason": "スパイク戻り",
                    "entry_idx": entry_idx_actual,
                    "entry_time": entry_time,
                    "entry_price": entry_price_actual,
                    "avg_entry_price": avg_entry_price,
                    "lot_total": total_lot,
                    "pips_per_lot": pips_per_lot,
                    "namping_entries": trade_result.get("entries") or [],
                    "signal_chain": chain_signals,
                    "exit_idx": exit_idx,
                    "exit_time": points_sorted[exit_idx][0],
                    "exit_price": exit_price,
                    "pips": pips,
                    "reason": exit_reason,
                }
            )
            register_position(side, exit_idx, active_positions_spike)
            i = entry_idx_actual + 1 if allow_overlap else exit_idx + 1

    if entry_momentum_enabled:
        i = 0
        while i < n - 1:
            if is_cancel_requested(should_cancel):
                raise InterruptedError("cancelled")
            signal = find_momentum_signal(
                points_sorted,
                times,
                i,
                momentum_window,
                momentum_spike,
                momentum_boundary_pct,
                momentum_tick_min_per_min,
                momentum_hold_seconds,
                momentum_max_move,
                second_lows,
                second_highs,
                should_cancel,
            )
            if not signal:
                i += 1
                continue

            entry_idx = signal["entry_idx"]
            signal["source"] = "momentum"
            gate_key = signal_key(signal)
            if signal_chain_enabled and gate_signals and gate_key not in allowed_signals:
                i = entry_idx + 1
                continue
            chain_signals = chain_map.get(gate_key, []) if signal_chain_enabled else []
            side = signal["side"]
            entry_time, entry_bid = points_sorted[entry_idx]
            entry_price = entry_bid + spread if side == "long" else entry_bid

            if exclude_enabled and entry_time.hour in exclude_hours:
                i = entry_idx + 1
                continue

            if ma_enabled:
                ma_value = resolve_ma_value(entry_time)
                if ma_value is None:
                    i = entry_idx + 1
                    continue
                if side == "long":
                    deviation = (ma_value - entry_price) / ma_value
                else:
                    deviation = (entry_price - ma_value) / ma_value
                if deviation < ma_deviation:
                    i = entry_idx + 1
                    continue

            trade_params = trade_params_by_kind["momentum"]
            trade_result = simulate_namping_trade(
                points_sorted,
                entry_idx,
                side,
                entry_price,
                spread,
                trade_params["stop"],
                trade_params["take"],
                trade_params["fixed_exit_price"],
                trade_params["time_close_seconds"],
                None,
                trade_params["namping_first_enabled"],
                trade_params["namping_steps"],
                should_cancel,
                fast_take_min_pips=trade_params["fast_take_min"],
                fast_take_window_ms=trade_params["fast_take_window_ms"],
                fast_take_pips=trade_params["fast_take_pips"],
            )
            if not trade_result:
                i = entry_idx + 1
                continue

            entry_idx_actual = trade_result["entry_idx"]
            if entry_idx_actual is None:
                i = entry_idx + 1
                continue
            if not can_open_position(entry_idx_actual, side, active_positions_momentum):
                i = entry_idx + 1
                continue

            entry_time = points_sorted[entry_idx_actual][0]
            entry_price_actual = trade_result["entry_price"]
            avg_entry_price = trade_result["avg_entry_price"]
            total_lot = trade_result["lot_total"]
            exit_idx = trade_result["exit_idx"]
            exit_price = trade_result["exit_price"]
            exit_reason = trade_result["exit_reason"]

            if side == "long":
                pips_per_lot = (exit_price - avg_entry_price) / PIP_SIZE
            else:
                pips_per_lot = (avg_entry_price - exit_price) / PIP_SIZE
            pips = pips_per_lot * total_lot

            trades.append(
                {
                    "side": side,
                    "entry_reason": signal.get("entry_reason", "勢い追随"),
                    "entry_idx": entry_idx_actual,
                    "entry_time": entry_time,
                    "entry_price": entry_price_actual,
                    "avg_entry_price": avg_entry_price,
                    "lot_total": total_lot,
                    "pips_per_lot": pips_per_lot,
                    "namping_entries": trade_result.get("entries") or [],
                    "signal_chain": chain_signals,
                    "exit_idx": exit_idx,
                    "exit_time": points_sorted[exit_idx][0],
                    "exit_price": exit_price,
                    "pips": pips,
                    "reason": exit_reason,
                }
            )
            register_position(side, exit_idx, active_positions_momentum)
            i = entry_idx_actual + 1 if allow_overlap else exit_idx + 1

    if entry_reverse_enabled:
        reverse_window = timedelta(seconds=reverse_window_seconds)
        i = 0
        while i < n - 1:
            if is_cancel_requested(should_cancel):
                raise InterruptedError("cancelled")
            signal = find_reverse_signal(
                points_sorted,
                times,
                i,
                reverse_window,
                reverse_move,
                reverse_hold_seconds,
                reverse_max_pullback,
                should_cancel,
            )
            if not signal:
                i += 1
                continue

            entry_idx = signal["entry_idx"]
            signal["source"] = "reverse"
            gate_key = signal_key(signal)
            if signal_chain_enabled and gate_signals and gate_key not in allowed_signals:
                i = entry_idx + 1
                continue
            chain_signals = chain_map.get(gate_key, []) if signal_chain_enabled else []
            side = signal["side"]
            entry_time, entry_bid = points_sorted[entry_idx]
            entry_price = entry_bid + spread if side == "long" else entry_bid

            if exclude_enabled and entry_time.hour in exclude_hours:
                i = entry_idx + 1
                continue

            if ma_enabled:
                ma_value = resolve_ma_value(entry_time)
                if ma_value is None:
                    i = entry_idx + 1
                    continue
                if side == "long":
                    deviation = (ma_value - entry_price) / ma_value
                else:
                    deviation = (entry_price - ma_value) / ma_value
                if deviation < ma_deviation:
                    i = entry_idx + 1
                    continue

            trade_params = trade_params_by_kind["reverse"]
            extra_stop_price = None
            if reverse_monitor_stop_enabled:
                monitor_high = signal.get("monitor_high")
                monitor_low = signal.get("monitor_low")
                if monitor_high is not None and monitor_low is not None:
                    if side == "short":
                        extra_stop_price = monitor_high + reverse_monitor_stop_pips
                    else:
                        extra_stop_price = monitor_low - reverse_monitor_stop_pips
            trade_result = simulate_namping_trade(
                points_sorted,
                entry_idx,
                side,
                entry_price,
                spread,
                trade_params["stop"],
                trade_params["take"],
                trade_params["fixed_exit_price"],
                trade_params["time_close_seconds"],
                None,
                trade_params["namping_first_enabled"],
                trade_params["namping_steps"],
                should_cancel,
                extra_stop_price=extra_stop_price,
                fast_take_min_pips=trade_params["fast_take_min"],
                fast_take_window_ms=trade_params["fast_take_window_ms"],
                fast_take_pips=trade_params["fast_take_pips"],
            )
            if not trade_result:
                i = entry_idx + 1
                continue

            entry_idx_actual = trade_result["entry_idx"]
            if entry_idx_actual is None:
                i = entry_idx + 1
                continue
            if not can_open_position(entry_idx_actual, side, active_positions_reverse):
                i = entry_idx + 1
                continue

            entry_time = points_sorted[entry_idx_actual][0]
            entry_price_actual = trade_result["entry_price"]
            avg_entry_price = trade_result["avg_entry_price"]
            total_lot = trade_result["lot_total"]
            exit_idx = trade_result["exit_idx"]
            exit_price = trade_result["exit_price"]
            exit_reason = trade_result["exit_reason"]

            if side == "long":
                pips_per_lot = (exit_price - avg_entry_price) / PIP_SIZE
            else:
                pips_per_lot = (avg_entry_price - exit_price) / PIP_SIZE
            pips = pips_per_lot * total_lot

            trades.append(
                {
                    "side": side,
                    "entry_reason": signal.get("entry_reason", "秒逆張り"),
                    "entry_idx": entry_idx_actual,
                    "entry_time": entry_time,
                    "entry_price": entry_price_actual,
                    "avg_entry_price": avg_entry_price,
                    "lot_total": total_lot,
                    "pips_per_lot": pips_per_lot,
                    "namping_entries": trade_result.get("entries") or [],
                    "signal_chain": chain_signals,
                    "exit_idx": exit_idx,
                    "exit_time": points_sorted[exit_idx][0],
                    "exit_price": exit_price,
                    "pips": pips,
                    "reason": exit_reason,
                }
            )
            register_position(side, exit_idx, active_positions_reverse)
            i = entry_idx_actual + 1 if allow_overlap else exit_idx + 1

    if trades:
        trades.sort(
            key=lambda t: (
                t.get("entry_idx") if t.get("entry_idx") is not None else -1,
                t.get("exit_idx") if t.get("exit_idx") is not None else -1,
            )
        )

    equity_curve = [(times[0], 0.0)]
    cumulative = 0.0
    if trades:
        exit_events = sorted(
            ((t.get("exit_time"), idx, t.get("pips", 0.0)) for idx, t in enumerate(trades)),
            key=lambda item: (item[0], item[1]),
        )
        for exit_time, _idx, pips in exit_events:
            cumulative += pips
            equity_curve.append((exit_time, cumulative))
    if equity_curve and equity_curve[-1][0] != times[-1]:
        equity_curve.append((times[-1], cumulative))

    enabled_count = sum(
        1
        for flag in (
            entry_spike_enabled,
            entry_sr_enabled,
            entry_momentum_enabled,
            entry_reverse_enabled,
        )
        if flag
    )
    if enabled_count >= 2:
        if (
            entry_spike_enabled
            and entry_sr_enabled
            and not entry_momentum_enabled
            and not entry_reverse_enabled
        ):
            entry_mode = "both"
        else:
            entry_mode = "multi"
    elif entry_sr_enabled:
        entry_mode = "sr_reentry"
    elif entry_momentum_enabled:
        entry_mode = "momentum"
    elif entry_reverse_enabled:
        entry_mode = "reverse"
    elif entry_spike_enabled:
        entry_mode = "spike"

    summary = summarize_trades(trades)
    summary["max_dd"] = compute_max_drawdown(equity_curve)
    return {
        "trades": trades,
        "summary": summary,
        "equity_curve": equity_curve,
        "ma_series": ma_series,
        "ma_enabled": ma_enabled,
        "ma_period": ma_period,
        "ma_unit": ma_unit,
        "ma_deviation_rate": ma_deviation,
        "entry_mode": entry_mode,
        "sr_target": params.get("sr_target"),
        "entry_spike_enabled": entry_spike_enabled,
        "entry_sr_enabled": entry_sr_enabled,
        "entry_momentum_enabled": entry_momentum_enabled,
        "entry_reverse_enabled": entry_reverse_enabled,
        "sr_ratio_join_mode": params.get("sr_ratio_join_mode"),
        "sr_move_ratio_enabled": params.get("sr_move_ratio_enabled"),
        "sr_move_speed_ratio_enabled": params.get("sr_move_speed_ratio_enabled"),
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
        self.view_range_months_var = tk.StringVar(value="1")
        self.view_month_label_var = tk.StringVar(
            value=f"{self.view_start_date.year}年{self.view_start_date.month:02d}月"
        )
        download_month_text = f"{self.start_date.year}-{self.start_date.month:02d}"
        self.download_start_month_var = tk.StringVar(value=download_month_text)
        self.download_end_month_var = tk.StringVar(value=download_month_text)
        month_text = f"{self.view_start_date.year}-{self.view_start_date.month:02d}"
        self.batch_start_month_var = tk.StringVar(value=month_text)
        self.batch_end_month_var = tk.StringVar(value=month_text)
        self.batch_months = []
        self.batch_index = 0
        self.batch_active = False
        self.exclude_weekends_var = tk.BooleanVar(value=True)
        self.status_var = tk.StringVar(value="準備完了")
        self.chart_info_var = tk.StringVar(value="")
        self.x_axis_mode_var = tk.StringVar(value="time")
        self.chart_type_var = tk.StringVar(value="tick")
        self.candle_interval_var = tk.IntVar(value=1)
        self.entry_spike_var = tk.BooleanVar(value=False)
        self.entry_sr_var = tk.BooleanVar(value=True)
        self.entry_momentum_var = tk.BooleanVar(value=False)
        self.entry_reverse_var = tk.BooleanVar(value=False)
        self.hide_chart_var = tk.BooleanVar(value=False)
        self.ma_filter_var = tk.BooleanVar(value=True)
        self.ma_period_var = tk.StringVar(value="200")
        self.ma_unit_var = tk.StringVar(value="min")
        self.ma_deviation_var = tk.StringVar(value="0.01")
        self.zigzag_show_var = tk.BooleanVar(value=False)
        self.sr_line_show_var = tk.BooleanVar(value=True)
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
        self.reverse_window_var = tk.StringVar(value="2")
        self.reverse_pips_var = tk.StringVar(value="3")
        self.reverse_hold_seconds_var = tk.StringVar(value="2")
        self.reverse_max_pullback_var = tk.StringVar(value="5")
        self.reverse_monitor_stop_enabled_var = tk.BooleanVar(value=False)
        self.reverse_monitor_stop_pips_var = tk.StringVar(value="0")
        self.momentum_window_var = tk.StringVar(value="1000")
        self.momentum_spike_pips_var = tk.StringVar(value="3.0")
        self.momentum_boundary_pct_var = tk.StringVar(value="50")
        self.momentum_tick_min_var = tk.StringVar(value="100")
        self.momentum_hold_seconds_var = tk.StringVar(value="2")
        self.momentum_max_pips_var = tk.StringVar(value="0")
        self.spread_var = tk.StringVar(value="1.0")
        self.stop_pips_var = tk.StringVar(value="10.0")
        self.take_pips_var = tk.StringVar(value="10.0")
        self.time_close_seconds_var = tk.StringVar(value="0")
        self.fixed_exit_price_var = tk.BooleanVar(value=True)
        self.fast_take_min_var = tk.StringVar(value="5")
        self.fast_take_window_ms_var = tk.StringVar(value="2000")
        self.fast_take_pips_var = tk.StringVar(value="5")
        self.common_stop_enabled_var = tk.BooleanVar(value=True)
        self.common_take_enabled_var = tk.BooleanVar(value=True)
        self.common_time_enabled_var = tk.BooleanVar(value=True)
        self.common_fast_take_enabled_var = tk.BooleanVar(value=True)
        self.common_stop_override_var = tk.BooleanVar(value=True)
        self.common_take_override_var = tk.BooleanVar(value=True)
        self.common_time_override_var = tk.BooleanVar(value=True)
        self.common_fixed_override_var = tk.BooleanVar(value=True)
        self.common_namping_override_var = tk.BooleanVar(value=True)
        self.common_fast_take_override_var = tk.BooleanVar(value=True)
        self.reverse_stop_pips_var = tk.StringVar(value="10.0")
        self.reverse_take_pips_var = tk.StringVar(value="10.0")
        self.reverse_time_close_seconds_var = tk.StringVar(value="0")
        self.reverse_fixed_exit_price_var = tk.BooleanVar(value=True)
        self.reverse_fast_take_min_var = tk.StringVar(value="5")
        self.reverse_fast_take_window_ms_var = tk.StringVar(value="2000")
        self.reverse_fast_take_pips_var = tk.StringVar(value="5")
        self.reverse_stop_enabled_var = tk.BooleanVar(value=True)
        self.reverse_take_enabled_var = tk.BooleanVar(value=True)
        self.reverse_time_enabled_var = tk.BooleanVar(value=True)
        self.reverse_fast_take_enabled_var = tk.BooleanVar(value=True)
        self.momentum_stop_pips_var = tk.StringVar(value="10.0")
        self.momentum_take_pips_var = tk.StringVar(value="10.0")
        self.momentum_time_close_seconds_var = tk.StringVar(value="0")
        self.momentum_fixed_exit_price_var = tk.BooleanVar(value=True)
        self.momentum_fast_take_min_var = tk.StringVar(value="5")
        self.momentum_fast_take_window_ms_var = tk.StringVar(value="2000")
        self.momentum_fast_take_pips_var = tk.StringVar(value="5")
        self.momentum_stop_enabled_var = tk.BooleanVar(value=True)
        self.momentum_take_enabled_var = tk.BooleanVar(value=True)
        self.momentum_time_enabled_var = tk.BooleanVar(value=True)
        self.momentum_fast_take_enabled_var = tk.BooleanVar(value=True)
        self.sr_stop_pips_var = tk.StringVar(value="10.0")
        self.sr_take_pips_var = tk.StringVar(value="10.0")
        self.sr_time_close_seconds_var = tk.StringVar(value="0")
        self.sr_fixed_exit_price_var = tk.BooleanVar(value=True)
        self.sr_fast_take_min_var = tk.StringVar(value="5")
        self.sr_fast_take_window_ms_var = tk.StringVar(value="2000")
        self.sr_fast_take_pips_var = tk.StringVar(value="5")
        self.sr_stop_enabled_var = tk.BooleanVar(value=True)
        self.sr_take_enabled_var = tk.BooleanVar(value=True)
        self.sr_time_enabled_var = tk.BooleanVar(value=True)
        self.sr_fast_take_enabled_var = tk.BooleanVar(value=True)
        self.spike_stop_pips_var = tk.StringVar(value="10.0")
        self.spike_take_pips_var = tk.StringVar(value="10.0")
        self.spike_time_close_seconds_var = tk.StringVar(value="0")
        self.spike_fixed_exit_price_var = tk.BooleanVar(value=True)
        self.spike_fast_take_min_var = tk.StringVar(value="5")
        self.spike_fast_take_window_ms_var = tk.StringVar(value="2000")
        self.spike_fast_take_pips_var = tk.StringVar(value="5")
        self.spike_stop_enabled_var = tk.BooleanVar(value=True)
        self.spike_take_enabled_var = tk.BooleanVar(value=True)
        self.spike_time_enabled_var = tk.BooleanVar(value=True)
        self.spike_fast_take_enabled_var = tk.BooleanVar(value=True)
        self.allow_same_direction_var = tk.BooleanVar(value=False)
        self.allow_opposite_direction_var = tk.BooleanVar(value=False)
        self.namping_first_entry_var = tk.BooleanVar(value=True)
        self.namping_step1_enabled_var = tk.BooleanVar(value=True)
        self.namping_step1_pips_var = tk.StringVar(value="5")
        self.namping_step1_lot_var = tk.StringVar(value="2")
        self.namping_step2_enabled_var = tk.BooleanVar(value=True)
        self.namping_step2_pips_var = tk.StringVar(value="5")
        self.namping_step2_lot_var = tk.StringVar(value="4")
        self.namping_step3_enabled_var = tk.BooleanVar(value=False)
        self.namping_step3_pips_var = tk.StringVar(value="5")
        self.namping_step3_lot_var = tk.StringVar(value="8")
        self.namping_step4_enabled_var = tk.BooleanVar(value=False)
        self.namping_step4_pips_var = tk.StringVar(value="5")
        self.namping_step4_lot_var = tk.StringVar(value="16")
        self.namping_step5_enabled_var = tk.BooleanVar(value=False)
        self.namping_step5_pips_var = tk.StringVar(value="5")
        self.namping_step5_lot_var = tk.StringVar(value="32")
        self.reverse_namping_first_entry_var = tk.BooleanVar(value=True)
        self.reverse_namping_step1_enabled_var = tk.BooleanVar(value=True)
        self.reverse_namping_step1_pips_var = tk.StringVar(value="5")
        self.reverse_namping_step1_lot_var = tk.StringVar(value="2")
        self.reverse_namping_step2_enabled_var = tk.BooleanVar(value=True)
        self.reverse_namping_step2_pips_var = tk.StringVar(value="5")
        self.reverse_namping_step2_lot_var = tk.StringVar(value="4")
        self.reverse_namping_step3_enabled_var = tk.BooleanVar(value=False)
        self.reverse_namping_step3_pips_var = tk.StringVar(value="5")
        self.reverse_namping_step3_lot_var = tk.StringVar(value="8")
        self.reverse_namping_step4_enabled_var = tk.BooleanVar(value=False)
        self.reverse_namping_step4_pips_var = tk.StringVar(value="5")
        self.reverse_namping_step4_lot_var = tk.StringVar(value="16")
        self.reverse_namping_step5_enabled_var = tk.BooleanVar(value=False)
        self.reverse_namping_step5_pips_var = tk.StringVar(value="5")
        self.reverse_namping_step5_lot_var = tk.StringVar(value="32")
        self.momentum_namping_first_entry_var = tk.BooleanVar(value=True)
        self.momentum_namping_step1_enabled_var = tk.BooleanVar(value=True)
        self.momentum_namping_step1_pips_var = tk.StringVar(value="5")
        self.momentum_namping_step1_lot_var = tk.StringVar(value="2")
        self.momentum_namping_step2_enabled_var = tk.BooleanVar(value=True)
        self.momentum_namping_step2_pips_var = tk.StringVar(value="5")
        self.momentum_namping_step2_lot_var = tk.StringVar(value="4")
        self.momentum_namping_step3_enabled_var = tk.BooleanVar(value=False)
        self.momentum_namping_step3_pips_var = tk.StringVar(value="5")
        self.momentum_namping_step3_lot_var = tk.StringVar(value="8")
        self.momentum_namping_step4_enabled_var = tk.BooleanVar(value=False)
        self.momentum_namping_step4_pips_var = tk.StringVar(value="5")
        self.momentum_namping_step4_lot_var = tk.StringVar(value="16")
        self.momentum_namping_step5_enabled_var = tk.BooleanVar(value=False)
        self.momentum_namping_step5_pips_var = tk.StringVar(value="5")
        self.momentum_namping_step5_lot_var = tk.StringVar(value="32")
        self.sr_namping_first_entry_var = tk.BooleanVar(value=True)
        self.sr_namping_step1_enabled_var = tk.BooleanVar(value=True)
        self.sr_namping_step1_pips_var = tk.StringVar(value="5")
        self.sr_namping_step1_lot_var = tk.StringVar(value="2")
        self.sr_namping_step2_enabled_var = tk.BooleanVar(value=True)
        self.sr_namping_step2_pips_var = tk.StringVar(value="5")
        self.sr_namping_step2_lot_var = tk.StringVar(value="4")
        self.sr_namping_step3_enabled_var = tk.BooleanVar(value=False)
        self.sr_namping_step3_pips_var = tk.StringVar(value="5")
        self.sr_namping_step3_lot_var = tk.StringVar(value="8")
        self.sr_namping_step4_enabled_var = tk.BooleanVar(value=False)
        self.sr_namping_step4_pips_var = tk.StringVar(value="5")
        self.sr_namping_step4_lot_var = tk.StringVar(value="16")
        self.sr_namping_step5_enabled_var = tk.BooleanVar(value=False)
        self.sr_namping_step5_pips_var = tk.StringVar(value="5")
        self.sr_namping_step5_lot_var = tk.StringVar(value="32")
        self.spike_namping_first_entry_var = tk.BooleanVar(value=True)
        self.spike_namping_step1_enabled_var = tk.BooleanVar(value=True)
        self.spike_namping_step1_pips_var = tk.StringVar(value="5")
        self.spike_namping_step1_lot_var = tk.StringVar(value="2")
        self.spike_namping_step2_enabled_var = tk.BooleanVar(value=True)
        self.spike_namping_step2_pips_var = tk.StringVar(value="5")
        self.spike_namping_step2_lot_var = tk.StringVar(value="4")
        self.spike_namping_step3_enabled_var = tk.BooleanVar(value=False)
        self.spike_namping_step3_pips_var = tk.StringVar(value="5")
        self.spike_namping_step3_lot_var = tk.StringVar(value="8")
        self.spike_namping_step4_enabled_var = tk.BooleanVar(value=False)
        self.spike_namping_step4_pips_var = tk.StringVar(value="5")
        self.spike_namping_step4_lot_var = tk.StringVar(value="16")
        self.spike_namping_step5_enabled_var = tk.BooleanVar(value=False)
        self.spike_namping_step5_pips_var = tk.StringVar(value="5")
        self.spike_namping_step5_lot_var = tk.StringVar(value="32")
        self.sr_zigzag_pips_var = tk.StringVar(value="10.0")
        self.sr_break_pips_var = tk.StringVar(value="0.01")
        self.sr_min_bars_var = tk.StringVar(value="10")
        self.sr_reentry_break_pips_var = tk.StringVar(value="10.0")
        self.sr_reentry_tick_limit_var = tk.StringVar(value="100")
        self.sr_reentry_tick_min_var = tk.StringVar(value="0")
        self.sr_reentry_tick_limit_enabled_var = tk.BooleanVar(value=True)
        self.sr_reentry_tick_min_enabled_var = tk.BooleanVar(value=True)
        self.sr_reentry_wait_bars_var = tk.StringVar(value="3")
        self.sr_reentry_min_seconds_var = tk.StringVar(value="30")
        self.sr_reentry_max_seconds_var = tk.StringVar(value="1800")
        self.sr_reentry_midpoint_var = tk.StringVar(value="50")
        self.sr_reentry_dominance_var = tk.StringVar(value="50")
        self.sr_reentry_move_ratio_var = tk.StringVar(value="100")
        self.sr_reentry_speed_ratio_var = tk.StringVar(value="100")
        self.sr_reentry_ratio_join_var = tk.StringVar(value="両方")
        self.sr_reentry_favored_tick_min_var = tk.StringVar(value="1.0")
        self.sr_reentry_midpoint_enabled_var = tk.BooleanVar(value=True)
        self.sr_reentry_dominance_enabled_var = tk.BooleanVar(value=True)
        self.sr_reentry_move_ratio_enabled_var = tk.BooleanVar(value=True)
        self.sr_reentry_speed_ratio_enabled_var = tk.BooleanVar(value=True)
        self.sr_reentry_favored_tick_min_enabled_var = tk.BooleanVar(value=True)
        self.sr_reentry_target_var = tk.StringVar(value="両方")
        self.signal_chain_enabled_var = tk.BooleanVar(value=True)
        self.signal_chain_pos_pips_var = tk.StringVar(value="10")
        self.signal_chain_neg_pips_var = tk.StringVar(value="5")
        self.signal_chain_count_var = tk.StringVar(value="3")
        self.signal_chain_monitor_minutes_var = tk.StringVar(value="240")
        self.signal_chain_count_reverse_var = tk.BooleanVar(value=True)
        self.signal_chain_count_momentum_var = tk.BooleanVar(value=True)
        self.signal_chain_count_sr_var = tk.BooleanVar(value=True)
        self.signal_chain_count_spike_var = tk.BooleanVar(value=True)
        self.signal_chain_ignore_reverse_var = tk.BooleanVar(value=False)
        self.signal_chain_ignore_momentum_var = tk.BooleanVar(value=False)
        self.signal_chain_ignore_sr_var = tk.BooleanVar(value=False)
        self.signal_chain_ignore_spike_var = tk.BooleanVar(value=False)
        self.signal_chain_trigger_reverse_var = tk.BooleanVar(value=True)
        self.signal_chain_trigger_momentum_var = tk.BooleanVar(value=True)
        self.signal_chain_trigger_sr_var = tk.BooleanVar(value=True)
        self.signal_chain_trigger_spike_var = tk.BooleanVar(value=True)
        self.backtest_info_var = tk.StringVar(value="バックテスト: 未実行")
        self.backtest_elapsed_var = tk.StringVar(value="計算時間: -")
        self.batch_elapsed_var = tk.StringVar(value="一括時間: -")
        self.pnl_info_var = tk.StringVar(value="損益: 未実行")
        self.pnl_log_enabled_var = tk.BooleanVar(value=True)
        self.pnl_log_path_var = tk.StringVar(value=str(project_root() / RESULTS_DIR_NAME))
        self.last_save_dir_var = tk.StringVar(value="")
        self.last_settings_path_var = tk.StringVar(value="")
        self.last_trade_path_var = tk.StringVar(value="")
        self.last_namping_path_var = tk.StringVar(value="")
        self.last_index_path_var = tk.StringVar(value="")
        self.saved_runs = []
        self.saved_run_var = tk.StringVar(value="")
        self.trade_jump_var = tk.StringVar(value="1")
        self.trade_nav_info_var = tk.StringVar(value="取引: 0件")
        self.trade_focus_index = None
        self.pnl_data = None
        self.pnl_view_start = 0
        self.pnl_view_end = 0
        self.backtest_ready = False
        self.analysis_cache_key = None
        self.analysis_cache = None
        self.backtest_timer_running = False
        self.backtest_started_at = None
        self.backtest_elapsed_last_seconds = None
        self.batch_timer_running = False
        self.batch_started_at = None
        self.batch_elapsed_last_seconds = None
        self.batch_results = []
        self.batch_total_equity = None
        self.batch_yearly_data = None
        self.batch_monthly_data = None
        self.last_view_range = None
        self.close_enabled_vars = {}
        self.close_toggle_widgets = {}
        self.close_widget_groups = {}
        self.close_override_vars = {
            "stop": self.common_stop_override_var,
            "take": self.common_take_override_var,
            "time": self.common_time_override_var,
            "fast": self.common_fast_take_override_var,
        }
        self.namping_var_sets = {
            "common": {
                "first_var": self.namping_first_entry_var,
                "step_enabled_vars": [
                    self.namping_step1_enabled_var,
                    self.namping_step2_enabled_var,
                    self.namping_step3_enabled_var,
                    self.namping_step4_enabled_var,
                    self.namping_step5_enabled_var,
                ],
                "step_pips_vars": [
                    self.namping_step1_pips_var,
                    self.namping_step2_pips_var,
                    self.namping_step3_pips_var,
                    self.namping_step4_pips_var,
                    self.namping_step5_pips_var,
                ],
                "step_lot_vars": [
                    self.namping_step1_lot_var,
                    self.namping_step2_lot_var,
                    self.namping_step3_lot_var,
                    self.namping_step4_lot_var,
                    self.namping_step5_lot_var,
                ],
            },
            "reverse": {
                "first_var": self.reverse_namping_first_entry_var,
                "step_enabled_vars": [
                    self.reverse_namping_step1_enabled_var,
                    self.reverse_namping_step2_enabled_var,
                    self.reverse_namping_step3_enabled_var,
                    self.reverse_namping_step4_enabled_var,
                    self.reverse_namping_step5_enabled_var,
                ],
                "step_pips_vars": [
                    self.reverse_namping_step1_pips_var,
                    self.reverse_namping_step2_pips_var,
                    self.reverse_namping_step3_pips_var,
                    self.reverse_namping_step4_pips_var,
                    self.reverse_namping_step5_pips_var,
                ],
                "step_lot_vars": [
                    self.reverse_namping_step1_lot_var,
                    self.reverse_namping_step2_lot_var,
                    self.reverse_namping_step3_lot_var,
                    self.reverse_namping_step4_lot_var,
                    self.reverse_namping_step5_lot_var,
                ],
            },
            "momentum": {
                "first_var": self.momentum_namping_first_entry_var,
                "step_enabled_vars": [
                    self.momentum_namping_step1_enabled_var,
                    self.momentum_namping_step2_enabled_var,
                    self.momentum_namping_step3_enabled_var,
                    self.momentum_namping_step4_enabled_var,
                    self.momentum_namping_step5_enabled_var,
                ],
                "step_pips_vars": [
                    self.momentum_namping_step1_pips_var,
                    self.momentum_namping_step2_pips_var,
                    self.momentum_namping_step3_pips_var,
                    self.momentum_namping_step4_pips_var,
                    self.momentum_namping_step5_pips_var,
                ],
                "step_lot_vars": [
                    self.momentum_namping_step1_lot_var,
                    self.momentum_namping_step2_lot_var,
                    self.momentum_namping_step3_lot_var,
                    self.momentum_namping_step4_lot_var,
                    self.momentum_namping_step5_lot_var,
                ],
            },
            "sr": {
                "first_var": self.sr_namping_first_entry_var,
                "step_enabled_vars": [
                    self.sr_namping_step1_enabled_var,
                    self.sr_namping_step2_enabled_var,
                    self.sr_namping_step3_enabled_var,
                    self.sr_namping_step4_enabled_var,
                    self.sr_namping_step5_enabled_var,
                ],
                "step_pips_vars": [
                    self.sr_namping_step1_pips_var,
                    self.sr_namping_step2_pips_var,
                    self.sr_namping_step3_pips_var,
                    self.sr_namping_step4_pips_var,
                    self.sr_namping_step5_pips_var,
                ],
                "step_lot_vars": [
                    self.sr_namping_step1_lot_var,
                    self.sr_namping_step2_lot_var,
                    self.sr_namping_step3_lot_var,
                    self.sr_namping_step4_lot_var,
                    self.sr_namping_step5_lot_var,
                ],
            },
            "spike": {
                "first_var": self.spike_namping_first_entry_var,
                "step_enabled_vars": [
                    self.spike_namping_step1_enabled_var,
                    self.spike_namping_step2_enabled_var,
                    self.spike_namping_step3_enabled_var,
                    self.spike_namping_step4_enabled_var,
                    self.spike_namping_step5_enabled_var,
                ],
                "step_pips_vars": [
                    self.spike_namping_step1_pips_var,
                    self.spike_namping_step2_pips_var,
                    self.spike_namping_step3_pips_var,
                    self.spike_namping_step4_pips_var,
                    self.spike_namping_step5_pips_var,
                ],
                "step_lot_vars": [
                    self.spike_namping_step1_lot_var,
                    self.spike_namping_step2_lot_var,
                    self.spike_namping_step3_lot_var,
                    self.spike_namping_step4_lot_var,
                    self.spike_namping_step5_lot_var,
                ],
            },
        }
        self.namping_widget_groups = {}
        self.entry_tab_info = {}

        self._build_ui()
        self._load_persistent_state()
        self._apply_ui_state()
        self._refresh_saved_runs()
        self.root.protocol("WM_DELETE_WINDOW", self._on_app_close)
        self._poll_queue()

    def _build_namping_rows(self, frame, key, row_start=0):
        vars_set = self.namping_var_sets.get(key)
        if not vars_set:
            return
        group = []
        pady = (6, 0) if row_start > 0 else (0, 0)
        ttk.Checkbutton(
            frame, text="初回エントリー", variable=vars_set["first_var"]
        ).grid(row=row_start, column=0, sticky="w", pady=pady)
        for idx in range(5):
            row = row_start + 1 + idx
            enabled_var = vars_set["step_enabled_vars"][idx]
            pips_var = vars_set["step_pips_vars"][idx]
            lot_var = vars_set["step_lot_vars"][idx]
            ttk.Checkbutton(
                frame,
                text=f"段階{idx + 1}",
                variable=enabled_var,
                command=lambda k=key: self._on_namping_toggle_group(k),
            ).grid(row=row, column=0, sticky="w", pady=(6, 0))
            ttk.Label(frame, text="幅pp").grid(row=row, column=1, sticky="w", pady=(6, 0))
            pips_entry = ttk.Entry(frame, textvariable=pips_var, width=6)
            pips_entry.grid(row=row, column=2, padx=(4, 12), pady=(6, 0), sticky="w")
            ttk.Label(frame, text="ロット").grid(row=row, column=3, sticky="w", pady=(6, 0))
            lot_entry = ttk.Entry(frame, textvariable=lot_var, width=6)
            lot_entry.grid(row=row, column=4, padx=(4, 0), pady=(6, 0), sticky="w")
            group.append((enabled_var, pips_entry, lot_entry))
        self.namping_widget_groups[key] = group

    def _build_ui(self):
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=0)

        notebook = ttk.Notebook(self.root)
        notebook.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)

        param_tab = ttk.Frame(notebook, padding=12)
        chart_tab = ttk.Frame(notebook, padding=12)
        download_tab = ttk.Frame(notebook, padding=12)
        pnl_tab = ttk.Frame(notebook, padding=12)
        notebook.add(param_tab, text="パラメータ")
        notebook.add(chart_tab, text="チャート")
        notebook.add(download_tab, text="ダウンロード")
        notebook.add(pnl_tab, text="損益")
        notebook.select(param_tab)

        status_bar = ttk.Frame(self.root)
        status_bar.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 8))
        status_bar.columnconfigure(0, weight=1)
        ttk.Label(status_bar, textvariable=self.status_var).grid(row=0, column=0, sticky="w")

        chart_tab.columnconfigure(0, weight=1)
        chart_tab.rowconfigure(8, weight=1)
        param_tab.columnconfigure(0, weight=1)
        param_tab.rowconfigure(3, weight=1)

        view_row = ttk.Frame(param_tab)
        view_row.grid(row=0, column=0, sticky="ew")
        view_row.columnconfigure(4, weight=1)

        ttk.Label(view_row, textvariable=self.view_month_label_var).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Button(view_row, text="前", command=self._move_view_prev).grid(
            row=0, column=1, padx=(12, 0), sticky="w"
        )
        ttk.Button(view_row, text="次", command=self._move_view_next).grid(
            row=0, column=2, padx=(6, 0), sticky="w"
        )

        ttk.Label(view_row, text="開始年月").grid(
            row=1, column=0, sticky="w", pady=(6, 0)
        )
        self.batch_start_entry = ttk.Entry(
            view_row, textvariable=self.batch_start_month_var, width=8
        )
        self.batch_start_entry.grid(row=1, column=1, padx=(4, 6), pady=(6, 0), sticky="w")
        ttk.Label(view_row, text="終了年月").grid(
            row=1, column=2, sticky="w", pady=(6, 0)
        )
        self.batch_end_entry = ttk.Entry(
            view_row, textvariable=self.batch_end_month_var, width=8
        )
        self.batch_end_entry.grid(row=1, column=3, padx=(4, 6), pady=(6, 0), sticky="w")
        self.batch_run_button = ttk.Button(
            view_row, text="一括実行", command=self._start_month_batch
        )
        self.batch_run_button.grid(row=1, column=4, padx=(6, 0), pady=(6, 0), sticky="w")

        run_row = ttk.Frame(param_tab)
        run_row.grid(row=1, column=0, sticky="w", pady=(6, 4))
        self.chart_button = ttk.Button(run_row, text="実行", command=self._show_chart)
        self.chart_button.grid(row=0, column=0, sticky="w")
        self.chart_cancel_button = ttk.Button(
            run_row,
            text="中止",
            command=self._cancel_chart,
            state="disabled",
        )
        self.chart_cancel_button.grid(row=0, column=1, padx=(6, 0), sticky="w")
        ttk.Label(run_row, textvariable=self.backtest_info_var).grid(
            row=1, column=0, columnspan=3, sticky="w", pady=(4, 0)
        )
        ttk.Label(run_row, textvariable=self.backtest_elapsed_var).grid(
            row=2, column=0, columnspan=3, sticky="w"
        )
        ttk.Label(run_row, textvariable=self.batch_elapsed_var).grid(
            row=3, column=0, columnspan=3, sticky="w"
        )

        chart_controls = ttk.Frame(chart_tab)
        chart_controls.grid(row=2, column=0, sticky="ew", pady=(4, 4))
        chart_controls.columnconfigure(4, weight=1)

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
        ttk.Label(chart_controls, text="取引").grid(row=0, column=6, padx=(12, 4), sticky="w")
        self.trade_jump_entry = ttk.Entry(
            chart_controls, textvariable=self.trade_jump_var, width=6
        )
        self.trade_jump_entry.grid(row=0, column=7, sticky="w")
        self.trade_jump_button = ttk.Button(
            chart_controls, text="移動", command=self._jump_to_trade
        )
        self.trade_jump_button.grid(row=0, column=8, padx=(4, 0), sticky="w")
        self.trade_prev_button = ttk.Button(
            chart_controls, text="前へ", command=self._focus_prev_trade
        )
        self.trade_prev_button.grid(row=0, column=9, padx=(6, 0), sticky="w")
        self.trade_next_button = ttk.Button(
            chart_controls, text="次へ", command=self._focus_next_trade
        )
        self.trade_next_button.grid(row=0, column=10, padx=(4, 0), sticky="w")
        ttk.Label(chart_controls, textvariable=self.trade_nav_info_var).grid(
            row=0, column=11, padx=(8, 0), sticky="w"
        )

        self.zigzag_check = ttk.Checkbutton(
            chart_controls,
            text="ジグザグ表示",
            variable=self.zigzag_show_var,
            command=self._on_zigzag_toggle,
        )
        self.zigzag_check.grid(row=1, column=5, padx=(12, 0), sticky="w")
        self.sr_line_check = ttk.Checkbutton(
            chart_controls,
            text="水平線",
            variable=self.sr_line_show_var,
            command=self._on_sr_line_toggle,
        )
        self.sr_line_check.grid(row=1, column=6, padx=(8, 0), sticky="w")
        self.range_band_check = ttk.Checkbutton(
            chart_controls,
            text="レンジ補助線",
            variable=self.range_band_show_var,
            command=self._on_range_band_toggle,
        )
        self.range_band_check.grid(row=1, column=7, padx=(8, 0), sticky="w")

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

        param_body = ttk.Frame(param_tab)
        param_body.grid(row=2, column=0, sticky="nsew", pady=(4, 4))
        param_tab.rowconfigure(2, weight=1)
        param_body.columnconfigure(0, weight=1)
        param_body.columnconfigure(1, weight=2)
        param_body.rowconfigure(0, weight=1)

        common_panel = ttk.Frame(param_body, padding=12)
        common_panel.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        common_panel.columnconfigure(0, weight=1)

        right_panel = ttk.Frame(param_body)
        right_panel.grid(row=0, column=1, sticky="nsew")
        right_panel.columnconfigure(0, weight=1)
        right_panel.rowconfigure(0, weight=1)

        param_notebook = ttk.Notebook(right_panel)
        param_notebook.grid(row=0, column=0, sticky="nsew")

        reverse_tab = ttk.Frame(param_notebook, padding=12)
        momentum_tab = ttk.Frame(param_notebook, padding=12)
        sr_tab = ttk.Frame(param_notebook, padding=12)
        spike_tab = ttk.Frame(param_notebook, padding=12)
        param_notebook.add(reverse_tab, text="秒逆張り")
        param_notebook.add(momentum_tab, text="勢い追随")
        param_notebook.add(sr_tab, text="水平線戻り")
        param_notebook.add(spike_tab, text="スパイク")

        for tab in (reverse_tab, momentum_tab, sr_tab, spike_tab):
            tab.columnconfigure(0, weight=1)

        common_close = ttk.LabelFrame(common_panel, text="決済条件（共通）")
        common_close.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        ttk.Label(common_close, text="スプレッド（ピップス）").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Entry(common_close, textvariable=self.spread_var, width=8).grid(
            row=0, column=1, padx=(4, 12), sticky="w"
        )
        self.common_stop_check = ttk.Checkbutton(
            common_close,
            text="",
            variable=self.common_stop_enabled_var,
            command=lambda: self._on_close_toggle("common", "stop"),
        )
        self.common_stop_check.grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Label(common_close, text="損切幅（ピップス）").grid(
            row=1, column=1, sticky="w", pady=(6, 0)
        )
        self.common_stop_entry = ttk.Entry(
            common_close, textvariable=self.stop_pips_var, width=8
        )
        self.common_stop_entry.grid(row=1, column=2, padx=(2, 12), pady=(6, 0), sticky="w")
        ttk.Checkbutton(
            common_close,
            text="共通優先",
            variable=self.common_stop_override_var,
            command=self._on_common_override_toggle,
        ).grid(row=1, column=3, padx=(6, 0), pady=(6, 0), sticky="w")
        self.common_take_check = ttk.Checkbutton(
            common_close,
            text="",
            variable=self.common_take_enabled_var,
            command=lambda: self._on_close_toggle("common", "take"),
        )
        self.common_take_check.grid(row=2, column=0, sticky="w", pady=(6, 0))
        ttk.Label(common_close, text="利確幅（ピップス）").grid(
            row=2, column=1, sticky="w", pady=(6, 0)
        )
        self.common_take_entry = ttk.Entry(
            common_close, textvariable=self.take_pips_var, width=8
        )
        self.common_take_entry.grid(row=2, column=2, padx=(2, 12), pady=(6, 0), sticky="w")
        ttk.Checkbutton(
            common_close,
            text="共通優先",
            variable=self.common_take_override_var,
            command=self._on_common_override_toggle,
        ).grid(row=2, column=3, padx=(6, 0), pady=(6, 0), sticky="w")
        self.common_time_check = ttk.Checkbutton(
            common_close,
            text="",
            variable=self.common_time_enabled_var,
            command=lambda: self._on_close_toggle("common", "time"),
        )
        self.common_time_check.grid(row=3, column=0, sticky="w", pady=(6, 0))
        ttk.Label(common_close, text="時間経過(秒)").grid(
            row=3, column=1, sticky="w", pady=(6, 0)
        )
        self.common_time_entry = ttk.Entry(
            common_close, textvariable=self.time_close_seconds_var, width=8
        )
        self.common_time_entry.grid(row=3, column=2, padx=(2, 12), pady=(6, 0), sticky="w")
        ttk.Checkbutton(
            common_close,
            text="共通優先",
            variable=self.common_time_override_var,
            command=self._on_common_override_toggle,
        ).grid(row=3, column=3, padx=(6, 0), pady=(6, 0), sticky="w")
        self.common_fast_take_check = ttk.Checkbutton(
            common_close,
            text="",
            variable=self.common_fast_take_enabled_var,
            command=lambda: self._on_close_toggle("common", "fast"),
        )
        self.common_fast_take_check.grid(row=4, column=0, sticky="w", pady=(6, 0))
        ttk.Label(common_close, text="急伸利確 最低幅（pp）").grid(
            row=4, column=1, sticky="w", pady=(6, 0)
        )
        self.common_fast_take_min_entry = ttk.Entry(
            common_close, textvariable=self.fast_take_min_var, width=8
        )
        self.common_fast_take_min_entry.grid(
            row=4, column=2, padx=(2, 12), pady=(6, 0), sticky="w"
        )
        ttk.Label(common_close, text="ミリ秒").grid(
            row=4, column=3, sticky="w", pady=(6, 0)
        )
        self.common_fast_take_window_entry = ttk.Entry(
            common_close, textvariable=self.fast_take_window_ms_var, width=8
        )
        self.common_fast_take_window_entry.grid(
            row=4, column=4, padx=(2, 12), pady=(6, 0), sticky="w"
        )
        ttk.Label(common_close, text="ピプス（pp）").grid(
            row=4, column=5, sticky="w", pady=(6, 0)
        )
        self.common_fast_take_pips_entry = ttk.Entry(
            common_close, textvariable=self.fast_take_pips_var, width=8
        )
        self.common_fast_take_pips_entry.grid(
            row=4, column=6, padx=(2, 12), pady=(6, 0), sticky="w"
        )
        ttk.Checkbutton(
            common_close,
            text="共通優先",
            variable=self.common_fast_take_override_var,
            command=self._on_common_override_toggle,
        ).grid(row=4, column=7, padx=(6, 0), pady=(6, 0), sticky="w")
        self.fixed_exit_price_check = ttk.Checkbutton(
            common_close,
            text="損切/利確を固定決済",
            variable=self.fixed_exit_price_var,
        )
        self.fixed_exit_price_check.grid(
            row=5, column=1, padx=(0, 8), pady=(6, 0), sticky="w"
        )
        ttk.Checkbutton(
            common_close, text="共通優先", variable=self.common_fixed_override_var
        ).grid(row=5, column=2, padx=(6, 0), pady=(6, 0), sticky="w")

        common_namping = ttk.LabelFrame(common_panel, text="ナンピン条件（共通）")
        common_namping.grid(row=1, column=0, sticky="ew", pady=(0, 6))
        ttk.Checkbutton(
            common_namping, text="共通優先", variable=self.common_namping_override_var
        ).grid(row=0, column=0, sticky="w")
        self._build_namping_rows(common_namping, "common", row_start=1)

        common_filter = ttk.LabelFrame(common_panel, text="共通フィルター")
        common_filter.grid(row=2, column=0, sticky="ew", pady=(0, 6))
        self.ma_check = ttk.Checkbutton(
            common_filter,
            text="移動平均フィルター",
            variable=self.ma_filter_var,
            command=self._on_ma_filter_toggle,
        )
        self.ma_check.grid(row=0, column=0, columnspan=2, sticky="w")
        ttk.Label(common_filter, text="期間").grid(row=0, column=2, sticky="w")
        self.ma_period_entry = ttk.Entry(
            common_filter, textvariable=self.ma_period_var, width=8
        )
        self.ma_period_entry.grid(row=0, column=3, padx=(4, 12), sticky="w")
        ttk.Label(common_filter, text="単位").grid(row=0, column=4, sticky="w")
        self.ma_unit_seconds_radio = ttk.Radiobutton(
            common_filter, text="秒", variable=self.ma_unit_var, value="sec"
        )
        self.ma_unit_seconds_radio.grid(row=0, column=5, sticky="w")
        self.ma_unit_minutes_radio = ttk.Radiobutton(
            common_filter, text="分", variable=self.ma_unit_var, value="min"
        )
        self.ma_unit_minutes_radio.grid(row=0, column=6, padx=(0, 12), sticky="w")
        ttk.Label(common_filter, text="乖離率（％）").grid(row=0, column=7, sticky="w")
        self.ma_deviation_entry = ttk.Entry(
            common_filter, textvariable=self.ma_deviation_var, width=8
        )
        self.ma_deviation_entry.grid(row=0, column=8, padx=(4, 0), sticky="w")
        self.backtest_exclude_check = ttk.Checkbutton(
            common_filter,
            text="時間帯除外",
            variable=self.backtest_exclude_var,
            command=self._on_backtest_exclude_toggle,
        )
        self.backtest_exclude_check.grid(row=1, column=0, sticky="w", pady=(6, 0))
        self.backtest_exclude_button = ttk.Button(
            common_filter, text="時間帯設定", command=self._open_backtest_exclude_hours
        )
        self.backtest_exclude_button.grid(
            row=1, column=1, padx=(4, 12), pady=(6, 0), sticky="w"
        )
        ttk.Label(common_filter, textvariable=self.backtest_exclude_label_var).grid(
            row=1, column=2, columnspan=4, sticky="w", pady=(6, 0)
        )
        self.allow_same_direction_check = ttk.Checkbutton(
            common_filter,
            text="同方向同時保有",
            variable=self.allow_same_direction_var,
        )
        self.allow_same_direction_check.grid(row=2, column=0, sticky="w", pady=(6, 0))
        self.allow_opposite_direction_check = ttk.Checkbutton(
            common_filter,
            text="逆方向同時保有",
            variable=self.allow_opposite_direction_var,
        )
        self.allow_opposite_direction_check.grid(row=2, column=1, sticky="w", pady=(6, 0))

        signal_chain_settings = ttk.LabelFrame(common_panel, text="連続点灯条件")
        signal_chain_settings.grid(row=3, column=0, sticky="ew", pady=(0, 6))
        self.signal_chain_enabled_check = ttk.Checkbutton(
            signal_chain_settings,
            text="連続点灯ON",
            variable=self.signal_chain_enabled_var,
            command=self._on_signal_chain_toggle,
        )
        self.signal_chain_enabled_check.grid(
            row=0, column=0, columnspan=2, sticky="w"
        )
        ttk.Label(signal_chain_settings, text="正方向幅（pp）").grid(
            row=1, column=0, sticky="w", pady=(6, 0)
        )
        self.signal_chain_pos_entry = ttk.Entry(
            signal_chain_settings, textvariable=self.signal_chain_pos_pips_var, width=8
        )
        self.signal_chain_pos_entry.grid(
            row=1, column=1, padx=(4, 12), pady=(6, 0), sticky="w"
        )
        ttk.Label(signal_chain_settings, text="逆方向幅（pp）").grid(
            row=1, column=2, sticky="w", pady=(6, 0)
        )
        self.signal_chain_neg_entry = ttk.Entry(
            signal_chain_settings, textvariable=self.signal_chain_neg_pips_var, width=8
        )
        self.signal_chain_neg_entry.grid(
            row=1, column=3, padx=(4, 12), pady=(6, 0), sticky="w"
        )
        ttk.Label(signal_chain_settings, text="連続回数").grid(
            row=1, column=4, sticky="w", pady=(6, 0)
        )
        self.signal_chain_count_entry = ttk.Entry(
            signal_chain_settings, textvariable=self.signal_chain_count_var, width=8
        )
        self.signal_chain_count_entry.grid(
            row=1, column=5, padx=(4, 0), pady=(6, 0), sticky="w"
        )
        ttk.Label(signal_chain_settings, text="監視時間（分）").grid(
            row=2, column=0, sticky="w", pady=(6, 0)
        )
        self.signal_chain_monitor_entry = ttk.Entry(
            signal_chain_settings,
            textvariable=self.signal_chain_monitor_minutes_var,
            width=8,
        )
        self.signal_chain_monitor_entry.grid(
            row=2, column=1, padx=(4, 12), pady=(6, 0), sticky="w"
        )
        ttk.Label(signal_chain_settings, text="逆方向サインで見送り").grid(
            row=3, column=0, sticky="w", pady=(6, 0)
        )
        self.signal_chain_ignore_reverse_check = ttk.Checkbutton(
            signal_chain_settings,
            text="秒逆張り",
            variable=self.signal_chain_ignore_reverse_var,
        )
        self.signal_chain_ignore_reverse_check.grid(
            row=3, column=1, sticky="w", pady=(6, 0)
        )
        self.signal_chain_ignore_momentum_check = ttk.Checkbutton(
            signal_chain_settings,
            text="勢い追随",
            variable=self.signal_chain_ignore_momentum_var,
        )
        self.signal_chain_ignore_momentum_check.grid(
            row=3, column=2, sticky="w", pady=(6, 0)
        )
        self.signal_chain_ignore_sr_check = ttk.Checkbutton(
            signal_chain_settings,
            text="水平線戻り",
            variable=self.signal_chain_ignore_sr_var,
        )
        self.signal_chain_ignore_sr_check.grid(
            row=3, column=3, sticky="w", pady=(6, 0)
        )
        self.signal_chain_ignore_spike_check = ttk.Checkbutton(
            signal_chain_settings,
            text="スパイク",
            variable=self.signal_chain_ignore_spike_var,
        )
        self.signal_chain_ignore_spike_check.grid(
            row=3, column=4, sticky="w", pady=(6, 0)
        )
        ttk.Label(signal_chain_settings, text="カウント対象").grid(
            row=4, column=0, sticky="w", pady=(6, 0)
        )
        self.signal_chain_count_reverse_check = ttk.Checkbutton(
            signal_chain_settings,
            text="秒逆張り",
            variable=self.signal_chain_count_reverse_var,
        )
        self.signal_chain_count_reverse_check.grid(
            row=4, column=1, sticky="w", pady=(6, 0)
        )
        self.signal_chain_count_momentum_check = ttk.Checkbutton(
            signal_chain_settings,
            text="勢い追随",
            variable=self.signal_chain_count_momentum_var,
        )
        self.signal_chain_count_momentum_check.grid(
            row=4, column=2, sticky="w", pady=(6, 0)
        )
        self.signal_chain_count_sr_check = ttk.Checkbutton(
            signal_chain_settings,
            text="水平線戻り",
            variable=self.signal_chain_count_sr_var,
        )
        self.signal_chain_count_sr_check.grid(
            row=4, column=3, sticky="w", pady=(6, 0)
        )
        self.signal_chain_count_spike_check = ttk.Checkbutton(
            signal_chain_settings,
            text="スパイク",
            variable=self.signal_chain_count_spike_var,
        )
        self.signal_chain_count_spike_check.grid(
            row=4, column=4, sticky="w", pady=(6, 0)
        )
        ttk.Label(signal_chain_settings, text="最終きっかけ").grid(
            row=5, column=0, sticky="w", pady=(6, 0)
        )
        self.signal_chain_trigger_reverse_check = ttk.Checkbutton(
            signal_chain_settings,
            text="秒逆張り",
            variable=self.signal_chain_trigger_reverse_var,
        )
        self.signal_chain_trigger_reverse_check.grid(
            row=5, column=1, sticky="w", pady=(6, 0)
        )
        self.signal_chain_trigger_momentum_check = ttk.Checkbutton(
            signal_chain_settings,
            text="勢い追随",
            variable=self.signal_chain_trigger_momentum_var,
        )
        self.signal_chain_trigger_momentum_check.grid(
            row=5, column=2, sticky="w", pady=(6, 0)
        )
        self.signal_chain_trigger_sr_check = ttk.Checkbutton(
            signal_chain_settings,
            text="水平線戻り",
            variable=self.signal_chain_trigger_sr_var,
        )
        self.signal_chain_trigger_sr_check.grid(
            row=5, column=3, sticky="w", pady=(6, 0)
        )
        self.signal_chain_trigger_spike_check = ttk.Checkbutton(
            signal_chain_settings,
            text="スパイク",
            variable=self.signal_chain_trigger_spike_var,
        )
        self.signal_chain_trigger_spike_check.grid(
            row=5, column=4, sticky="w", pady=(6, 0)
        )

        save_info_frame = ttk.LabelFrame(common_panel, text="保存結果（最新）")
        save_info_frame.grid(row=4, column=0, sticky="ew", pady=(0, 6))
        save_info_frame.columnconfigure(1, weight=1)

        ttk.Label(save_info_frame, text="保存先フォルダ").grid(
            row=0, column=0, sticky="w"
        )
        self.last_save_dir_entry = ttk.Entry(
            save_info_frame, textvariable=self.last_save_dir_var, width=40, state="readonly"
        )
        self.last_save_dir_entry.grid(row=0, column=1, padx=(4, 6), sticky="ew")
        ttk.Button(
            save_info_frame,
            text="開く",
            command=lambda: self._open_path_var(self.last_save_dir_var),
        ).grid(row=0, column=2, sticky="w")

        ttk.Label(save_info_frame, text="設定ファイル").grid(
            row=1, column=0, sticky="w", pady=(6, 0)
        )
        self.last_settings_entry = ttk.Entry(
            save_info_frame,
            textvariable=self.last_settings_path_var,
            width=40,
            state="readonly",
        )
        self.last_settings_entry.grid(row=1, column=1, padx=(4, 6), pady=(6, 0), sticky="ew")
        ttk.Button(
            save_info_frame,
            text="開く",
            command=lambda: self._open_path_var(self.last_settings_path_var),
        ).grid(row=1, column=2, pady=(6, 0), sticky="w")

        ttk.Label(save_info_frame, text="取引一覧").grid(
            row=2, column=0, sticky="w", pady=(6, 0)
        )
        self.last_trade_entry = ttk.Entry(
            save_info_frame, textvariable=self.last_trade_path_var, width=40, state="readonly"
        )
        self.last_trade_entry.grid(row=2, column=1, padx=(4, 6), pady=(6, 0), sticky="ew")
        ttk.Button(
            save_info_frame,
            text="開く",
            command=lambda: self._open_path_var(self.last_trade_path_var),
        ).grid(row=2, column=2, pady=(6, 0), sticky="w")

        ttk.Label(save_info_frame, text="ナンピン詳細").grid(
            row=3, column=0, sticky="w", pady=(6, 0)
        )
        self.last_namping_entry = ttk.Entry(
            save_info_frame,
            textvariable=self.last_namping_path_var,
            width=40,
            state="readonly",
        )
        self.last_namping_entry.grid(
            row=3, column=1, padx=(4, 6), pady=(6, 0), sticky="ew"
        )
        ttk.Button(
            save_info_frame,
            text="開く",
            command=lambda: self._open_path_var(self.last_namping_path_var),
        ).grid(row=3, column=2, pady=(6, 0), sticky="w")

        ttk.Label(save_info_frame, text="一覧").grid(
            row=4, column=0, sticky="w", pady=(6, 0)
        )
        self.last_index_entry = ttk.Entry(
            save_info_frame, textvariable=self.last_index_path_var, width=40, state="readonly"
        )
        self.last_index_entry.grid(row=4, column=1, padx=(4, 6), pady=(6, 0), sticky="ew")
        ttk.Button(
            save_info_frame,
            text="開く",
            command=lambda: self._open_path_var(self.last_index_path_var),
        ).grid(row=4, column=2, pady=(6, 0), sticky="w")

        def build_close_frame(
            parent,
            key,
            stop_var,
            take_var,
            time_var,
            fixed_var,
            fast_min_var,
            fast_window_var,
            fast_pips_var,
            stop_enabled_var,
            take_enabled_var,
            time_enabled_var,
            fast_enabled_var,
        ):
            frame = ttk.LabelFrame(parent, text="決済条件")
            stop_check = ttk.Checkbutton(
                frame,
                text="",
                variable=stop_enabled_var,
                command=lambda: self._on_close_toggle(key, "stop"),
            )
            stop_check.grid(row=0, column=0, sticky="w")
            stop_label = ttk.Label(frame, text="損切幅（ピップス）")
            stop_label.grid(row=0, column=1, sticky="w")
            stop_entry = ttk.Entry(frame, textvariable=stop_var, width=8)
            stop_entry.grid(row=0, column=2, padx=(4, 12), sticky="w")
            take_check = ttk.Checkbutton(
                frame,
                text="",
                variable=take_enabled_var,
                command=lambda: self._on_close_toggle(key, "take"),
            )
            take_check.grid(row=1, column=0, sticky="w", pady=(6, 0))
            take_label = ttk.Label(frame, text="利確幅（ピップス）")
            take_label.grid(row=1, column=1, sticky="w", pady=(6, 0))
            take_entry = ttk.Entry(frame, textvariable=take_var, width=8)
            take_entry.grid(row=1, column=2, padx=(4, 12), pady=(6, 0), sticky="w")
            time_check = ttk.Checkbutton(
                frame,
                text="",
                variable=time_enabled_var,
                command=lambda: self._on_close_toggle(key, "time"),
            )
            time_check.grid(row=2, column=0, sticky="w", pady=(6, 0))
            time_label = ttk.Label(frame, text="時間経過(秒)")
            time_label.grid(row=2, column=1, sticky="w", pady=(6, 0))
            time_entry = ttk.Entry(frame, textvariable=time_var, width=8)
            time_entry.grid(row=2, column=2, padx=(4, 12), pady=(6, 0), sticky="w")
            fast_check = ttk.Checkbutton(
                frame,
                text="",
                variable=fast_enabled_var,
                command=lambda: self._on_close_toggle(key, "fast"),
            )
            fast_check.grid(row=3, column=0, sticky="w", pady=(6, 0))
            fast_label = ttk.Label(frame, text="急伸利確 最低幅（pp）")
            fast_label.grid(row=3, column=1, sticky="w", pady=(6, 0))
            fast_min_entry = ttk.Entry(frame, textvariable=fast_min_var, width=8)
            fast_min_entry.grid(
                row=3, column=2, padx=(4, 12), pady=(6, 0), sticky="w"
            )
            fast_window_label = ttk.Label(frame, text="ミリ秒")
            fast_window_label.grid(row=3, column=3, sticky="w", pady=(6, 0))
            fast_window_entry = ttk.Entry(frame, textvariable=fast_window_var, width=8)
            fast_window_entry.grid(
                row=3, column=4, padx=(4, 12), pady=(6, 0), sticky="w"
            )
            fast_pips_label = ttk.Label(frame, text="ピプス（pp）")
            fast_pips_label.grid(row=3, column=5, sticky="w", pady=(6, 0))
            fast_pips_entry = ttk.Entry(frame, textvariable=fast_pips_var, width=8)
            fast_pips_entry.grid(
                row=3, column=6, padx=(4, 12), pady=(6, 0), sticky="w"
            )
            ttk.Checkbutton(
                frame, text="損切/利確を固定決済", variable=fixed_var
            ).grid(row=4, column=1, columnspan=2, pady=(6, 0), sticky="w")
            self._register_close_condition(
                key, "stop", stop_enabled_var, stop_check, [stop_label, stop_entry]
            )
            self._register_close_condition(
                key, "take", take_enabled_var, take_check, [take_label, take_entry]
            )
            self._register_close_condition(
                key, "time", time_enabled_var, time_check, [time_label, time_entry]
            )
            self._register_close_condition(
                key,
                "fast",
                fast_enabled_var,
                fast_check,
                [
                    fast_label,
                    fast_min_entry,
                    fast_window_label,
                    fast_window_entry,
                    fast_pips_label,
                    fast_pips_entry,
                ],
            )
            return frame

        self.entry_reverse_check = ttk.Checkbutton(
            reverse_tab,
            text="秒逆張り",
            variable=self.entry_reverse_var,
            command=lambda: self._on_entry_tab_toggle("reverse"),
        )
        self.entry_reverse_check.grid(row=0, column=0, sticky="w", pady=(0, 6))
        reverse_close = build_close_frame(
            reverse_tab,
            "reverse",
            self.reverse_stop_pips_var,
            self.reverse_take_pips_var,
            self.reverse_time_close_seconds_var,
            self.reverse_fixed_exit_price_var,
            self.reverse_fast_take_min_var,
            self.reverse_fast_take_window_ms_var,
            self.reverse_fast_take_pips_var,
            self.reverse_stop_enabled_var,
            self.reverse_take_enabled_var,
            self.reverse_time_enabled_var,
            self.reverse_fast_take_enabled_var,
        )
        reverse_close.grid(row=1, column=0, sticky="ew", pady=(0, 6))
        reverse_namping = ttk.LabelFrame(reverse_tab, text="ナンピン条件")
        reverse_namping.grid(row=2, column=0, sticky="ew", pady=(0, 6))
        self._build_namping_rows(reverse_namping, "reverse", row_start=0)
        reverse_settings = ttk.LabelFrame(reverse_tab, text="秒逆張り条件")
        reverse_settings.grid(row=3, column=0, sticky="ew")
        ttk.Label(reverse_settings, text="逆張り時間（秒）").grid(row=0, column=0, sticky="w")
        ttk.Entry(reverse_settings, textvariable=self.reverse_window_var, width=8).grid(
            row=0, column=1, padx=(4, 12), sticky="w"
        )
        ttk.Label(reverse_settings, text="逆張り幅（pp）").grid(row=0, column=2, sticky="w")
        ttk.Entry(reverse_settings, textvariable=self.reverse_pips_var, width=8).grid(
            row=0, column=3, padx=(4, 12), sticky="w"
        )
        ttk.Label(reverse_settings, text="停滞秒").grid(row=0, column=4, sticky="w")
        ttk.Entry(
            reverse_settings, textvariable=self.reverse_hold_seconds_var, width=8
        ).grid(row=0, column=5, padx=(4, 12), sticky="w")
        ttk.Label(reverse_settings, text="戻り上限（pp）").grid(row=0, column=6, sticky="w")
        ttk.Entry(
            reverse_settings, textvariable=self.reverse_max_pullback_var, width=8
        ).grid(row=0, column=7, padx=(4, 0), sticky="w")
        self.reverse_monitor_stop_check = ttk.Checkbutton(
            reverse_settings,
            text="監視抜け撤退",
            variable=self.reverse_monitor_stop_enabled_var,
            command=self._on_reverse_monitor_stop_toggle,
        )
        self.reverse_monitor_stop_check.grid(
            row=1, column=0, sticky="w", pady=(6, 0)
        )
        self.reverse_monitor_stop_label = ttk.Label(
            reverse_settings, text="抜け幅（pp）"
        )
        self.reverse_monitor_stop_label.grid(
            row=1, column=1, sticky="w", pady=(6, 0)
        )
        self.reverse_monitor_stop_pips_entry = ttk.Entry(
            reverse_settings, textvariable=self.reverse_monitor_stop_pips_var, width=8
        )
        self.reverse_monitor_stop_pips_entry.grid(
            row=1, column=2, padx=(4, 12), pady=(6, 0), sticky="w"
        )

        self.entry_momentum_check = ttk.Checkbutton(
            momentum_tab,
            text="勢い追随",
            variable=self.entry_momentum_var,
            command=lambda: self._on_entry_tab_toggle("momentum"),
        )
        self.entry_momentum_check.grid(row=0, column=0, sticky="w", pady=(0, 6))
        momentum_close = build_close_frame(
            momentum_tab,
            "momentum",
            self.momentum_stop_pips_var,
            self.momentum_take_pips_var,
            self.momentum_time_close_seconds_var,
            self.momentum_fixed_exit_price_var,
            self.momentum_fast_take_min_var,
            self.momentum_fast_take_window_ms_var,
            self.momentum_fast_take_pips_var,
            self.momentum_stop_enabled_var,
            self.momentum_take_enabled_var,
            self.momentum_time_enabled_var,
            self.momentum_fast_take_enabled_var,
        )
        momentum_close.grid(row=1, column=0, sticky="ew", pady=(0, 6))
        momentum_namping = ttk.LabelFrame(momentum_tab, text="ナンピン条件")
        momentum_namping.grid(row=2, column=0, sticky="ew", pady=(0, 6))
        self._build_namping_rows(momentum_namping, "momentum", row_start=0)
        momentum_settings = ttk.LabelFrame(momentum_tab, text="勢い条件")
        momentum_settings.grid(row=3, column=0, sticky="ew")
        ttk.Label(momentum_settings, text="監視時間（ms）").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Entry(momentum_settings, textvariable=self.momentum_window_var, width=8).grid(
            row=0, column=1, padx=(4, 12), sticky="w"
        )
        ttk.Label(momentum_settings, text="監視幅（pp）").grid(
            row=0, column=2, sticky="w"
        )
        ttk.Entry(momentum_settings, textvariable=self.momentum_spike_pips_var, width=8).grid(
            row=0, column=3, padx=(4, 12), sticky="w"
        )
        ttk.Label(momentum_settings, text="境界位置（％）").grid(
            row=0, column=4, sticky="w"
        )
        ttk.Entry(momentum_settings, textvariable=self.momentum_boundary_pct_var, width=8).grid(
            row=0, column=5, padx=(4, 0), sticky="w"
        )
        ttk.Label(momentum_settings, text="平均ティック/分下限").grid(
            row=1, column=0, sticky="w", pady=(6, 0)
        )
        ttk.Entry(momentum_settings, textvariable=self.momentum_tick_min_var, width=8).grid(
            row=1, column=1, padx=(4, 12), pady=(6, 0), sticky="w"
        )
        ttk.Label(momentum_settings, text="継続秒数").grid(
            row=1, column=2, sticky="w", pady=(6, 0)
        )
        ttk.Entry(momentum_settings, textvariable=self.momentum_hold_seconds_var, width=8).grid(
            row=1, column=3, padx=(4, 0), pady=(6, 0), sticky="w"
        )
        ttk.Label(momentum_settings, text="上限（pp）").grid(
            row=1, column=4, sticky="w", pady=(6, 0)
        )
        ttk.Entry(
            momentum_settings, textvariable=self.momentum_max_pips_var, width=8
        ).grid(row=1, column=5, padx=(4, 0), pady=(6, 0), sticky="w")

        self.entry_sr_check = ttk.Checkbutton(
            sr_tab,
            text="水平線戻り",
            variable=self.entry_sr_var,
            command=lambda: self._on_entry_tab_toggle("sr"),
        )
        self.entry_sr_check.grid(row=0, column=0, sticky="w", pady=(0, 6))
        sr_close = build_close_frame(
            sr_tab,
            "sr",
            self.sr_stop_pips_var,
            self.sr_take_pips_var,
            self.sr_time_close_seconds_var,
            self.sr_fixed_exit_price_var,
            self.sr_fast_take_min_var,
            self.sr_fast_take_window_ms_var,
            self.sr_fast_take_pips_var,
            self.sr_stop_enabled_var,
            self.sr_take_enabled_var,
            self.sr_time_enabled_var,
            self.sr_fast_take_enabled_var,
        )
        sr_close.grid(row=1, column=0, sticky="ew", pady=(0, 6))
        sr_namping = ttk.LabelFrame(sr_tab, text="ナンピン条件")
        sr_namping.grid(row=2, column=0, sticky="ew", pady=(0, 6))
        self._build_namping_rows(sr_namping, "sr", row_start=0)
        sr_settings = ttk.LabelFrame(sr_tab, text="水平線条件")
        sr_settings.grid(row=3, column=0, sticky="ew", pady=(0, 6))
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

        sr_reentry_settings = ttk.LabelFrame(sr_tab, text="水平線戻り条件")
        sr_reentry_settings.grid(row=4, column=0, sticky="ew")
        ttk.Label(sr_reentry_settings, text="抜け幅（pp）").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Entry(
            sr_reentry_settings,
            textvariable=self.sr_reentry_break_pips_var,
            width=8,
        ).grid(row=0, column=1, padx=(4, 12), sticky="w")
        self.sr_reentry_tick_limit_check = ttk.Checkbutton(
            sr_reentry_settings,
            text="ティック上限",
            variable=self.sr_reentry_tick_limit_enabled_var,
            command=self._on_sr_reentry_filter_toggle,
        )
        self.sr_reentry_tick_limit_check.grid(row=0, column=2, sticky="w")
        self.sr_reentry_tick_limit_entry = ttk.Entry(
            sr_reentry_settings,
            textvariable=self.sr_reentry_tick_limit_var,
            width=8,
        )
        self.sr_reentry_tick_limit_entry.grid(row=0, column=3, padx=(4, 12), sticky="w")
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
        self.sr_reentry_tick_min_check = ttk.Checkbutton(
            sr_reentry_settings,
            text="ティック下限",
            variable=self.sr_reentry_tick_min_enabled_var,
            command=self._on_sr_reentry_filter_toggle,
        )
        self.sr_reentry_tick_min_check.grid(row=2, column=0, sticky="w", pady=(6, 0))
        self.sr_reentry_tick_min_entry = ttk.Entry(
            sr_reentry_settings,
            textvariable=self.sr_reentry_tick_min_var,
            width=8,
        )
        self.sr_reentry_tick_min_entry.grid(row=2, column=1, padx=(4, 0), pady=(6, 0), sticky="w")
        self.sr_reentry_midpoint_check = ttk.Checkbutton(
            sr_reentry_settings,
            text="滞在位置",
            variable=self.sr_reentry_midpoint_enabled_var,
            command=self._on_sr_reentry_filter_toggle,
        )
        self.sr_reentry_midpoint_check.grid(row=2, column=2, sticky="w", pady=(6, 0))
        self.sr_reentry_midpoint_entry = ttk.Entry(
            sr_reentry_settings,
            textvariable=self.sr_reentry_midpoint_var,
            width=8,
        )
        self.sr_reentry_midpoint_entry.grid(row=2, column=3, padx=(4, 12), pady=(6, 0), sticky="w")
        self.sr_reentry_dominance_check = ttk.Checkbutton(
            sr_reentry_settings,
            text="優勢滞在率",
            variable=self.sr_reentry_dominance_enabled_var,
            command=self._on_sr_reentry_filter_toggle,
        )
        self.sr_reentry_dominance_check.grid(row=2, column=4, sticky="w", pady=(6, 0))
        self.sr_reentry_dominance_entry = ttk.Entry(
            sr_reentry_settings,
            textvariable=self.sr_reentry_dominance_var,
            width=8,
        )
        self.sr_reentry_dominance_entry.grid(row=2, column=5, padx=(4, 0), pady=(6, 0), sticky="w")
        self.sr_reentry_move_ratio_check = ttk.Checkbutton(
            sr_reentry_settings,
            text="平均幅比率",
            variable=self.sr_reentry_move_ratio_enabled_var,
            command=self._on_sr_reentry_filter_toggle,
        )
        self.sr_reentry_move_ratio_check.grid(row=3, column=0, sticky="w", pady=(6, 0))
        self.sr_reentry_move_ratio_entry = ttk.Entry(
            sr_reentry_settings,
            textvariable=self.sr_reentry_move_ratio_var,
            width=8,
        )
        self.sr_reentry_move_ratio_entry.grid(row=3, column=1, padx=(4, 0), pady=(6, 0), sticky="w")
        self.sr_reentry_speed_ratio_check = ttk.Checkbutton(
            sr_reentry_settings,
            text="速度比率",
            variable=self.sr_reentry_speed_ratio_enabled_var,
            command=self._on_sr_reentry_filter_toggle,
        )
        self.sr_reentry_speed_ratio_check.grid(row=3, column=2, sticky="w", pady=(6, 0))
        self.sr_reentry_speed_ratio_entry = ttk.Entry(
            sr_reentry_settings,
            textvariable=self.sr_reentry_speed_ratio_var,
            width=8,
        )
        self.sr_reentry_speed_ratio_entry.grid(row=3, column=3, padx=(4, 0), pady=(6, 0), sticky="w")
        ttk.Label(sr_reentry_settings, text="比率条件").grid(
            row=4, column=0, sticky="w", pady=(6, 0)
        )
        self.sr_reentry_ratio_join_combo = ttk.Combobox(
            sr_reentry_settings,
            textvariable=self.sr_reentry_ratio_join_var,
            values=["両方", "どちらか"],
            width=10,
            state="readonly",
        )
        self.sr_reentry_ratio_join_combo.grid(
            row=4, column=1, padx=(4, 0), pady=(6, 0), sticky="w"
        )
        self.sr_reentry_favored_tick_min_check = ttk.Checkbutton(
            sr_reentry_settings,
            text="有利1ティック幅下限（pp）",
            variable=self.sr_reentry_favored_tick_min_enabled_var,
            command=self._on_sr_reentry_filter_toggle,
        )
        self.sr_reentry_favored_tick_min_check.grid(row=3, column=4, sticky="w", pady=(6, 0))
        self.sr_reentry_favored_tick_min_entry = ttk.Entry(
            sr_reentry_settings,
            textvariable=self.sr_reentry_favored_tick_min_var,
            width=8,
        )
        self.sr_reentry_favored_tick_min_entry.grid(
            row=3, column=5, padx=(4, 0), pady=(6, 0), sticky="w"
        )

        self.entry_spike_check = ttk.Checkbutton(
            spike_tab,
            text="スパイク",
            variable=self.entry_spike_var,
            command=lambda: self._on_entry_tab_toggle("spike"),
        )
        self.entry_spike_check.grid(row=0, column=0, sticky="w", pady=(0, 6))
        spike_close = build_close_frame(
            spike_tab,
            "spike",
            self.spike_stop_pips_var,
            self.spike_take_pips_var,
            self.spike_time_close_seconds_var,
            self.spike_fixed_exit_price_var,
            self.spike_fast_take_min_var,
            self.spike_fast_take_window_ms_var,
            self.spike_fast_take_pips_var,
            self.spike_stop_enabled_var,
            self.spike_take_enabled_var,
            self.spike_time_enabled_var,
            self.spike_fast_take_enabled_var,
        )
        spike_close.grid(row=1, column=0, sticky="ew", pady=(0, 6))
        spike_namping = ttk.LabelFrame(spike_tab, text="ナンピン条件")
        spike_namping.grid(row=2, column=0, sticky="ew", pady=(0, 6))
        self._build_namping_rows(spike_namping, "spike", row_start=0)
        spike_settings = ttk.LabelFrame(spike_tab, text="スパイク条件")
        spike_settings.grid(row=3, column=0, sticky="ew", pady=(0, 6))
        ttk.Label(spike_settings, text="スパイク時間（ミリ秒）").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Entry(spike_settings, textvariable=self.spike_window_var, width=8).grid(
            row=0, column=1, padx=(4, 12), sticky="w"
        )
        ttk.Label(spike_settings, text="スパイク幅（ピップス）").grid(
            row=0, column=2, sticky="w"
        )
        ttk.Entry(spike_settings, textvariable=self.spike_pips_var, width=8).grid(
            row=0, column=3, padx=(4, 12), sticky="w"
        )
        ttk.Label(spike_settings, text="最小戻し率（％）").grid(
            row=0, column=4, sticky="w"
        )
        ttk.Entry(spike_settings, textvariable=self.retrace_var, width=8).grid(
            row=0, column=5, padx=(4, 0), sticky="w"
        )
        self.extreme_check = ttk.Checkbutton(
            spike_settings,
            text="天底フィルター",
            variable=self.extreme_filter_var,
            command=self._on_extreme_filter_toggle,
        )
        self.extreme_check.grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Label(spike_settings, text="天底維持ms").grid(row=1, column=1, sticky="w", pady=(6, 0))
        self.extreme_hold_entry = ttk.Entry(
            spike_settings, textvariable=self.extreme_hold_ms_var, width=8
        )
        self.extreme_hold_entry.grid(
            row=1, column=2, padx=(4, 12), pady=(6, 0), sticky="w"
        )
        ttk.Label(spike_settings, text="天底距離pips").grid(row=1, column=3, sticky="w", pady=(6, 0))
        self.extreme_distance_entry = ttk.Entry(
            spike_settings, textvariable=self.extreme_distance_pips_var, width=8
        )
        self.extreme_distance_entry.grid(
            row=1, column=4, padx=(4, 12), pady=(6, 0), sticky="w"
        )

        self.entry_tab_info = {
            "reverse": {
                "frame": reverse_tab,
                "var": self.entry_reverse_var,
                "toggle": self.entry_reverse_check,
            },
            "momentum": {
                "frame": momentum_tab,
                "var": self.entry_momentum_var,
                "toggle": self.entry_momentum_check,
            },
            "sr": {
                "frame": sr_tab,
                "var": self.entry_sr_var,
                "toggle": self.entry_sr_check,
            },
            "spike": {
                "frame": spike_tab,
                "var": self.entry_spike_var,
                "toggle": self.entry_spike_check,
            },
        }
        self._register_close_condition(
            "common",
            "stop",
            self.common_stop_enabled_var,
            self.common_stop_check,
            [self.common_stop_entry],
        )
        self._register_close_condition(
            "common",
            "take",
            self.common_take_enabled_var,
            self.common_take_check,
            [self.common_take_entry],
        )
        self._register_close_condition(
            "common",
            "time",
            self.common_time_enabled_var,
            self.common_time_check,
            [self.common_time_entry],
        )
        self._register_close_condition(
            "common",
            "fast",
            self.common_fast_take_enabled_var,
            self.common_fast_take_check,
            [
                self.common_fast_take_min_entry,
                self.common_fast_take_window_entry,
                self.common_fast_take_pips_entry,
            ],
        )
        ttk.Label(chart_tab, textvariable=self.chart_info_var).grid(
            row=4, column=0, sticky="w"
        )
        ttk.Label(chart_tab, textvariable=self.cursor_info_var).grid(
            row=7, column=0, sticky="w"
        )

        self.chart_canvas = tk.Canvas(chart_tab, bg="white")
        self.chart_canvas.grid(row=8, column=0, sticky="nsew", pady=(4, 0))
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

        ttk.Label(row, text="開始年月").grid(row=0, column=0, sticky="w")
        start_entry = ttk.Entry(
            row, textvariable=self.download_start_month_var, width=8
        )
        start_entry.grid(row=0, column=1, padx=6)

        ttk.Label(row, text="終了年月").grid(row=1, column=0, sticky="w", pady=(6, 0))
        end_entry = ttk.Entry(
            row, textvariable=self.download_end_month_var, width=8
        )
        end_entry.grid(row=1, column=1, padx=6, pady=(6, 0))

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
        pnl_tab.columnconfigure(1, weight=1)
        pnl_tab.rowconfigure(1, weight=3)
        pnl_tab.rowconfigure(3, weight=1)
        pnl_tab.rowconfigure(5, weight=1)

        ttk.Label(pnl_tab, textvariable=self.pnl_info_var).grid(row=0, column=0, sticky="w")
        pnl_select_frame = ttk.Frame(pnl_tab)
        pnl_select_frame.grid(row=0, column=1, sticky="e")
        ttk.Label(pnl_select_frame, text="保存結果").grid(row=0, column=0, sticky="w")
        self.saved_runs_combo = ttk.Combobox(
            pnl_select_frame, textvariable=self.saved_run_var, width=48, state="readonly"
        )
        self.saved_runs_combo.grid(row=0, column=1, padx=(4, 6), sticky="w")
        self.saved_runs_show_button = ttk.Button(
            pnl_select_frame, text="表示", command=self._load_selected_run
        )
        self.saved_runs_show_button.grid(row=0, column=2, sticky="w")
        self.saved_runs_refresh_button = ttk.Button(
            pnl_select_frame, text="更新", command=self._refresh_saved_runs
        )
        self.saved_runs_refresh_button.grid(row=0, column=3, padx=(6, 0), sticky="w")
        self.pnl_canvas = tk.Canvas(pnl_tab, bg="white")
        self.pnl_canvas.grid(row=1, column=0, sticky="nsew", pady=(6, 0))
        self.pnl_canvas.bind("<Configure>", self._on_pnl_resize)
        self.pnl_canvas.bind("<MouseWheel>", self._on_pnl_mouse_wheel)
        self.pnl_canvas.bind("<Button-4>", self._on_pnl_mouse_wheel)
        self.pnl_canvas.bind("<Button-5>", self._on_pnl_mouse_wheel)

        ttk.Label(pnl_tab, text="年別損益").grid(row=2, column=0, sticky="w", pady=(8, 0))
        self.pnl_year_canvas = tk.Canvas(pnl_tab, bg="white")
        self.pnl_year_canvas.grid(row=3, column=0, sticky="nsew", pady=(4, 0))
        self.pnl_year_canvas.bind("<Configure>", self._on_pnl_resize)

        ttk.Label(pnl_tab, text="月別損益").grid(row=4, column=0, sticky="w", pady=(8, 0))
        self.pnl_month_canvas = tk.Canvas(pnl_tab, bg="white")
        self.pnl_month_canvas.grid(row=5, column=0, sticky="nsew", pady=(4, 0))
        self.pnl_month_canvas.bind("<Configure>", self._on_pnl_resize)

    def _apply_ui_state(self):
        self._on_ma_filter_toggle()
        self._on_extreme_filter_toggle()
        self._on_backtest_exclude_toggle()
        self._on_sr_reentry_filter_toggle()
        self._on_signal_chain_toggle()
        self._on_namping_toggle_group("common")
        self._on_entry_tab_toggle("reverse")
        self._on_entry_tab_toggle("momentum")
        self._on_entry_tab_toggle("sr")
        self._on_entry_tab_toggle("spike")
        self._on_close_toggle("common", "stop")
        self._on_close_toggle("common", "take")
        self._on_close_toggle("common", "time")
        self._on_close_toggle("common", "fast")
        self._update_trade_nav_state()

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
        self.view_month_label_var.set(f"{picked.year}年{picked.month:02d}月")

    def _set_view_end(self, picked: date):
        self.view_end_date = picked
        self.view_end_var.set(picked.isoformat())

    def _get_view_range_months(self):
        self.view_range_months_var.set("1")
        return 1

    def _add_months(self, base_date: date, months: int) -> date:
        month_index = base_date.month - 1 + months
        year = base_date.year + month_index // 12
        month = month_index % 12 + 1
        return date(year, month, 1)

    def _month_end(self, base_date: date) -> date:
        last_day = calendar.monthrange(base_date.year, base_date.month)[1]
        return date(base_date.year, base_date.month, last_day)

    def _apply_view_month_range(self, start_date: date, months: int):
        start = start_date.replace(day=1)
        end_month_start = self._add_months(start, months - 1)
        end = self._month_end(end_month_start)
        self.view_start_date = start
        self.view_end_date = end
        self.view_start_var.set(start.isoformat())
        self.view_end_var.set(end.isoformat())
        self.view_month_label_var.set(f"{start.year}年{start.month:02d}月")

    def _apply_download_month_range(self, start_month: date, end_month: date):
        start = start_month.replace(day=1)
        end = self._month_end(end_month)
        self.start_date = start
        self.end_date = end
        self.start_var.set(start.isoformat())
        self.end_var.set(end.isoformat())

    def _shift_view_month_range(self, direction: int):
        months = self._get_view_range_months()
        if months is None:
            return
        base_date = self.view_start_date or date.today()
        start = base_date.replace(day=1)
        new_start = self._add_months(start, months * direction)
        self._apply_view_month_range(new_start, months)

    def _move_view_prev(self):
        self._shift_view_month_range(-1)

    def _move_view_next(self):
        self._shift_view_month_range(1)

    def _parse_month_text(self, value: str):
        text = value.strip()
        if not text:
            return None
        parts = [p for p in re.split(r"\D+", text) if p]
        year = None
        month = None
        if len(parts) >= 2:
            year = int(parts[0])
            month = int(parts[1])
        elif len(text) == 6 and text.isdigit():
            year = int(text[:4])
            month = int(text[4:])
        if year is None or month is None:
            return None
        if month < 1 or month > 12:
            return None
        return date(year, month, 1)

    def _normalize_month_label(self, text: str) -> str:
        if not text:
            return ""
        parts = [int(p) for p in re.findall(r"\d+", text)]
        if len(parts) >= 2:
            return f"{parts[0]}年{parts[1]:02d}月"
        return text

    def _month_label_key(self, label: str):
        parts = [int(p) for p in re.findall(r"\d+", label)]
        if len(parts) >= 2:
            return parts[0], parts[1]
        return 9999, 99

    def _start_month_batch(self):
        start = self._parse_month_text(self.batch_start_month_var.get())
        end = self._parse_month_text(self.batch_end_month_var.get())
        if start is None or end is None:
            messagebox.showerror("エラー", "開始年月と終了年月を正しく入力してください。")
            return
        if end < start:
            messagebox.showerror("エラー", "終了年月は開始年月より後にしてください。")
            return
        months = []
        current = start
        while current <= end:
            months.append(current)
            current = self._add_months(current, 1)
        if not months:
            return
        self.batch_months = months
        self.batch_index = 0
        self.batch_active = True
        self.batch_results = []
        self.batch_total_equity = None
        self.batch_yearly_data = None
        self.batch_monthly_data = None
        if hasattr(self, "batch_run_button"):
            self.batch_run_button.config(state="disabled")
        self._start_batch_timer()
        self._run_next_batch()

    def _run_next_batch(self):
        if not self.batch_active:
            return
        if self.chart_worker and self.chart_worker.is_alive():
            return
        if self.batch_index >= len(self.batch_months):
            self.batch_active = False
            if hasattr(self, "batch_run_button"):
                self.batch_run_button.config(state="normal")
            self._stop_batch_timer()
            self._finalize_batch_results()
            self._notify_batch_done()
            return
        target = self.batch_months[self.batch_index]
        self.batch_index += 1
        self._apply_view_month_range(target, 1)
        started = self._show_chart()
        if not started:
            self.batch_active = False
            if hasattr(self, "batch_run_button"):
                self.batch_run_button.config(state="normal")
            self._stop_batch_timer()

    def _parse_number(self, value: str) -> float:
        cleaned = value.strip().replace(",", ".").replace("，", ".")
        filtered = "".join(ch for ch in cleaned if ch.isdigit() or ch in ".-")
        return float(filtered)

    def _get_backtest_params(self):
        entry_spike_enabled = self.entry_spike_var.get()
        entry_sr_enabled = self.entry_sr_var.get()
        entry_momentum_enabled = self.entry_momentum_var.get()
        entry_reverse_enabled = self.entry_reverse_var.get()
        any_entry_enabled = (
            entry_spike_enabled
            or entry_sr_enabled
            or entry_momentum_enabled
            or entry_reverse_enabled
        )
        try:
            window_ms = self._parse_number(self.spike_window_var.get())
            spike_pips = self._parse_number(self.spike_pips_var.get())
            retrace_pct = self._parse_number(self.retrace_var.get())
            reverse_window_seconds = self._parse_number(self.reverse_window_var.get())
            reverse_pips = self._parse_number(self.reverse_pips_var.get())
            reverse_hold_seconds = self._parse_number(self.reverse_hold_seconds_var.get())
            reverse_max_pullback_pips = self._parse_number(
                self.reverse_max_pullback_var.get()
            )
            reverse_monitor_stop_enabled = self.reverse_monitor_stop_enabled_var.get()
            reverse_monitor_stop_pips = self._parse_number(
                self.reverse_monitor_stop_pips_var.get()
            )
            spread_pips = self._parse_number(self.spread_var.get())
            common_stop_pips = self._parse_number(self.stop_pips_var.get())
            common_take_pips = self._parse_number(self.take_pips_var.get())
            common_time_close_seconds = self._parse_number(
                self.time_close_seconds_var.get()
            )
            common_fixed_exit_price = self.fixed_exit_price_var.get()
            common_fast_take_min = self._parse_number(self.fast_take_min_var.get())
            common_fast_take_window_ms = self._parse_number(
                self.fast_take_window_ms_var.get()
            )
            common_fast_take_pips = self._parse_number(self.fast_take_pips_var.get())
            common_stop_enabled = self.common_stop_enabled_var.get()
            common_take_enabled = self.common_take_enabled_var.get()
            common_time_enabled = self.common_time_enabled_var.get()
            common_fast_take_enabled = self.common_fast_take_enabled_var.get()
            common_stop_override = self.common_stop_override_var.get()
            common_take_override = self.common_take_override_var.get()
            common_time_override = self.common_time_override_var.get()
            common_fixed_override = self.common_fixed_override_var.get()
            common_namping_override = self.common_namping_override_var.get()
            common_fast_take_override = self.common_fast_take_override_var.get()
            reverse_stop_pips = self._parse_number(self.reverse_stop_pips_var.get())
            reverse_take_pips = self._parse_number(self.reverse_take_pips_var.get())
            reverse_time_close_seconds = self._parse_number(
                self.reverse_time_close_seconds_var.get()
            )
            reverse_fixed_exit_price = self.reverse_fixed_exit_price_var.get()
            reverse_fast_take_min = self._parse_number(
                self.reverse_fast_take_min_var.get()
            )
            reverse_fast_take_window_ms = self._parse_number(
                self.reverse_fast_take_window_ms_var.get()
            )
            reverse_fast_take_pips = self._parse_number(
                self.reverse_fast_take_pips_var.get()
            )
            reverse_stop_enabled = self.reverse_stop_enabled_var.get()
            reverse_take_enabled = self.reverse_take_enabled_var.get()
            reverse_time_enabled = self.reverse_time_enabled_var.get()
            reverse_fast_take_enabled = self.reverse_fast_take_enabled_var.get()
            momentum_stop_pips = self._parse_number(self.momentum_stop_pips_var.get())
            momentum_take_pips = self._parse_number(self.momentum_take_pips_var.get())
            momentum_time_close_seconds = self._parse_number(
                self.momentum_time_close_seconds_var.get()
            )
            momentum_fixed_exit_price = self.momentum_fixed_exit_price_var.get()
            momentum_fast_take_min = self._parse_number(
                self.momentum_fast_take_min_var.get()
            )
            momentum_fast_take_window_ms = self._parse_number(
                self.momentum_fast_take_window_ms_var.get()
            )
            momentum_fast_take_pips = self._parse_number(
                self.momentum_fast_take_pips_var.get()
            )
            momentum_stop_enabled = self.momentum_stop_enabled_var.get()
            momentum_take_enabled = self.momentum_take_enabled_var.get()
            momentum_time_enabled = self.momentum_time_enabled_var.get()
            momentum_fast_take_enabled = self.momentum_fast_take_enabled_var.get()
            sr_stop_pips = self._parse_number(self.sr_stop_pips_var.get())
            sr_take_pips = self._parse_number(self.sr_take_pips_var.get())
            sr_time_close_seconds = self._parse_number(self.sr_time_close_seconds_var.get())
            sr_fixed_exit_price = self.sr_fixed_exit_price_var.get()
            sr_fast_take_min = self._parse_number(self.sr_fast_take_min_var.get())
            sr_fast_take_window_ms = self._parse_number(
                self.sr_fast_take_window_ms_var.get()
            )
            sr_fast_take_pips = self._parse_number(self.sr_fast_take_pips_var.get())
            sr_stop_enabled = self.sr_stop_enabled_var.get()
            sr_take_enabled = self.sr_take_enabled_var.get()
            sr_time_enabled = self.sr_time_enabled_var.get()
            sr_fast_take_enabled = self.sr_fast_take_enabled_var.get()
            spike_stop_pips = self._parse_number(self.spike_stop_pips_var.get())
            spike_take_pips = self._parse_number(self.spike_take_pips_var.get())
            spike_time_close_seconds = self._parse_number(
                self.spike_time_close_seconds_var.get()
            )
            spike_fixed_exit_price = self.spike_fixed_exit_price_var.get()
            spike_fast_take_min = self._parse_number(self.spike_fast_take_min_var.get())
            spike_fast_take_window_ms = self._parse_number(
                self.spike_fast_take_window_ms_var.get()
            )
            spike_fast_take_pips = self._parse_number(
                self.spike_fast_take_pips_var.get()
            )
            spike_stop_enabled = self.spike_stop_enabled_var.get()
            spike_take_enabled = self.spike_take_enabled_var.get()
            spike_time_enabled = self.spike_time_enabled_var.get()
            spike_fast_take_enabled = self.spike_fast_take_enabled_var.get()
            ma_enabled = self.ma_filter_var.get()
            ma_period = self._parse_number(self.ma_period_var.get())
            ma_unit = self.ma_unit_var.get()
            if ma_unit not in ("sec", "min"):
                ma_unit = "min"
            ma_deviation_pct = self._parse_number(self.ma_deviation_var.get())
            signal_chain_pos_pips = self._parse_number(self.signal_chain_pos_pips_var.get())
            signal_chain_neg_pips = self._parse_number(self.signal_chain_neg_pips_var.get())
            signal_chain_count = int(self._parse_number(self.signal_chain_count_var.get()))
            signal_chain_monitor_minutes = self._parse_number(
                self.signal_chain_monitor_minutes_var.get()
            )
            signal_chain_enabled = self.signal_chain_enabled_var.get()
            signal_chain_count_reverse = self.signal_chain_count_reverse_var.get()
            signal_chain_count_momentum = self.signal_chain_count_momentum_var.get()
            signal_chain_count_sr = self.signal_chain_count_sr_var.get()
            signal_chain_count_spike = self.signal_chain_count_spike_var.get()
            signal_chain_ignore_reverse = self.signal_chain_ignore_reverse_var.get()
            signal_chain_ignore_momentum = self.signal_chain_ignore_momentum_var.get()
            signal_chain_ignore_sr = self.signal_chain_ignore_sr_var.get()
            signal_chain_ignore_spike = self.signal_chain_ignore_spike_var.get()
            signal_chain_trigger_reverse = self.signal_chain_trigger_reverse_var.get()
            signal_chain_trigger_momentum = (
                self.signal_chain_trigger_momentum_var.get()
            )
            signal_chain_trigger_sr = self.signal_chain_trigger_sr_var.get()
            signal_chain_trigger_spike = self.signal_chain_trigger_spike_var.get()
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
            allow_same_direction = self.allow_same_direction_var.get()
            allow_opposite_direction = self.allow_opposite_direction_var.get()

            def read_namping(key):
                vars_set = self.namping_var_sets[key]
                first_enabled = vars_set["first_var"].get()
                step_enabled = [var.get() for var in vars_set["step_enabled_vars"]]
                step_pips = [
                    self._parse_number(var.get())
                    for var in vars_set["step_pips_vars"]
                ]
                step_lots = [
                    self._parse_number(var.get())
                    for var in vars_set["step_lot_vars"]
                ]
                return first_enabled, step_enabled, step_pips, step_lots

            common_namping = read_namping("common")
            reverse_namping = read_namping("reverse")
            momentum_namping = read_namping("momentum")
            sr_namping = read_namping("sr")
            spike_namping = read_namping("spike")
        except ValueError:
            messagebox.showerror("エラー", "数値の入力が正しくありません。")
            return None

        if entry_spike_enabled:
            if window_ms <= 0:
                messagebox.showerror("エラー", "スパイク時間は0より大きくしてください。")
                return None
            if spike_pips <= 0:
                messagebox.showerror("エラー", "スパイク幅は0より大きくしてください。")
                return None
            if retrace_pct < 0:
                messagebox.showerror("エラー", "最小戻し率は0以上にしてください。")
                return None
        if entry_reverse_enabled:
            if reverse_window_seconds <= 0:
                messagebox.showerror("エラー", "逆張り時間は0より大きくしてください。")
                return None
            if reverse_pips <= 0:
                messagebox.showerror("エラー", "逆張り幅は0より大きくしてください。")
                return None
            if reverse_hold_seconds < 0:
                messagebox.showerror("エラー", "停滞秒は0以上にしてください。")
                return None
            if reverse_max_pullback_pips < 0:
                messagebox.showerror("エラー", "戻り上限は0以上にしてください。")
                return None
            if reverse_monitor_stop_enabled and reverse_monitor_stop_pips < 0:
                messagebox.showerror(
                    "エラー", "監視抜け撤退の抜け幅は0以上にしてください。"
                )
                return None
        if spread_pips < 0:
            messagebox.showerror("エラー", "スプレッドは0以上にしてください。")
            return None

        def validate_namping(label, data):
            first_enabled, step_enabled, step_pips, step_lots = data
            if not first_enabled and not any(step_enabled):
                messagebox.showerror(
                    "エラー", f"{label}の初回/段階のエントリーが全てオフです。"
                )
                return False
            for idx, enabled in enumerate(step_enabled):
                if not enabled:
                    continue
                if step_pips[idx] <= 0:
                    messagebox.showerror(
                        "エラー", f"{label}の段階{idx + 1}の幅は0より大きくしてください。"
                    )
                    return False
                if step_lots[idx] <= 0:
                    messagebox.showerror(
                        "エラー", f"{label}の段階{idx + 1}のロットは0より大きくしてください。"
                    )
                    return False
            return True

        if any_entry_enabled:
            if common_stop_override and common_stop_enabled and common_stop_pips <= 0:
                messagebox.showerror("エラー", "損切幅は0より大きくしてください。")
                return None
            if common_take_override and common_take_enabled and common_take_pips <= 0:
                messagebox.showerror("エラー", "利確幅は0より大きくしてください。")
                return None
            if common_time_override and common_time_enabled and common_time_close_seconds < 0:
                messagebox.showerror("エラー", "時間経過クローズは0以上にしてください。")
                return None
            if common_fast_take_enabled and common_fast_take_min <= 0:
                messagebox.showerror("エラー", "急伸利確の最低幅は0以上にしてください。")
                return None
            if common_fast_take_enabled and common_fast_take_window_ms <= 0:
                messagebox.showerror("エラー", "急伸利確のミリ秒は0以上にしてください。")
                return None
            if common_fast_take_enabled and common_fast_take_pips <= 0:
                messagebox.showerror("エラー", "急伸利確のピプスは0以上にしてください。")
                return None
            if common_namping_override:
                if not validate_namping("共通", common_namping):
                    return None

        if entry_reverse_enabled:
            if (
                not common_stop_override
                and reverse_stop_enabled
                and reverse_stop_pips <= 0
            ):
                messagebox.showerror("エラー", "秒逆張りの損切幅は0より大きくしてください。")
                return None
            if (
                not common_take_override
                and reverse_take_enabled
                and reverse_take_pips <= 0
            ):
                messagebox.showerror("エラー", "秒逆張りの利確幅は0より大きくしてください。")
                return None
            if (
                not common_time_override
                and reverse_time_enabled
                and reverse_time_close_seconds < 0
            ):
                messagebox.showerror("エラー", "秒逆張りの時間経過クローズは0以上にしてください。")
                return None
            if not common_fast_take_override:
                if reverse_fast_take_enabled and reverse_fast_take_min <= 0:
                    messagebox.showerror(
                        "エラー", "秒逆張りの急伸利確最低幅は0以上にしてください。"
                    )
                    return None
                if reverse_fast_take_enabled and reverse_fast_take_window_ms <= 0:
                    messagebox.showerror(
                        "エラー", "秒逆張りの急伸利確ミリ秒は0以上にしてください。"
                    )
                    return None
                if reverse_fast_take_enabled and reverse_fast_take_pips <= 0:
                    messagebox.showerror(
                        "エラー", "秒逆張りの急伸利確ピプスは0以上にしてください。"
                    )
                    return None
            if not common_namping_override:
                if not validate_namping("秒逆張り", reverse_namping):
                    return None

        if entry_momentum_enabled:
            if (
                not common_stop_override
                and momentum_stop_enabled
                and momentum_stop_pips <= 0
            ):
                messagebox.showerror("エラー", "勢い追随の損切幅は0より大きくしてください。")
                return None
            if (
                not common_take_override
                and momentum_take_enabled
                and momentum_take_pips <= 0
            ):
                messagebox.showerror("エラー", "勢い追随の利確幅は0より大きくしてください。")
                return None
            if (
                not common_time_override
                and momentum_time_enabled
                and momentum_time_close_seconds < 0
            ):
                messagebox.showerror("エラー", "勢い追随の時間経過クローズは0以上にしてください。")
                return None
            if not common_fast_take_override:
                if momentum_fast_take_enabled and momentum_fast_take_min <= 0:
                    messagebox.showerror(
                        "エラー", "勢い追随の急伸利確最低幅は0以上にしてください。"
                    )
                    return None
                if momentum_fast_take_enabled and momentum_fast_take_window_ms <= 0:
                    messagebox.showerror(
                        "エラー", "勢い追随の急伸利確ミリ秒は0以上にしてください。"
                    )
                    return None
                if momentum_fast_take_enabled and momentum_fast_take_pips <= 0:
                    messagebox.showerror(
                        "エラー", "勢い追随の急伸利確ピプスは0以上にしてください。"
                    )
                    return None
            if not common_namping_override:
                if not validate_namping("勢い追随", momentum_namping):
                    return None

        if entry_sr_enabled:
            if (
                not common_stop_override
                and sr_stop_enabled
                and sr_stop_pips <= 0
            ):
                messagebox.showerror("エラー", "水平線戻りの損切幅は0より大きくしてください。")
                return None
            if (
                not common_take_override
                and sr_take_enabled
                and sr_take_pips <= 0
            ):
                messagebox.showerror("エラー", "水平線戻りの利確幅は0より大きくしてください。")
                return None
            if (
                not common_time_override
                and sr_time_enabled
                and sr_time_close_seconds < 0
            ):
                messagebox.showerror("エラー", "水平線戻りの時間経過クローズは0以上にしてください。")
                return None
            if not common_fast_take_override:
                if sr_fast_take_enabled and sr_fast_take_min <= 0:
                    messagebox.showerror(
                        "エラー", "水平線戻りの急伸利確最低幅は0以上にしてください。"
                    )
                    return None
                if sr_fast_take_enabled and sr_fast_take_window_ms <= 0:
                    messagebox.showerror(
                        "エラー", "水平線戻りの急伸利確ミリ秒は0以上にしてください。"
                    )
                    return None
                if sr_fast_take_enabled and sr_fast_take_pips <= 0:
                    messagebox.showerror(
                        "エラー", "水平線戻りの急伸利確ピプスは0以上にしてください。"
                    )
                    return None
            if not common_namping_override:
                if not validate_namping("水平線戻り", sr_namping):
                    return None

        if entry_spike_enabled:
            if (
                not common_stop_override
                and spike_stop_enabled
                and spike_stop_pips <= 0
            ):
                messagebox.showerror("エラー", "スパイクの損切幅は0より大きくしてください。")
                return None
            if (
                not common_take_override
                and spike_take_enabled
                and spike_take_pips <= 0
            ):
                messagebox.showerror("エラー", "スパイクの利確幅は0より大きくしてください。")
                return None
            if (
                not common_time_override
                and spike_time_enabled
                and spike_time_close_seconds < 0
            ):
                messagebox.showerror("エラー", "スパイクの時間経過クローズは0以上にしてください。")
                return None
            if not common_fast_take_override:
                if spike_fast_take_enabled and spike_fast_take_min <= 0:
                    messagebox.showerror(
                        "エラー", "スパイクの急伸利確最低幅は0以上にしてください。"
                    )
                    return None
                if spike_fast_take_enabled and spike_fast_take_window_ms <= 0:
                    messagebox.showerror(
                        "エラー", "スパイクの急伸利確ミリ秒は0以上にしてください。"
                    )
                    return None
                if spike_fast_take_enabled and spike_fast_take_pips <= 0:
                    messagebox.showerror(
                        "エラー", "スパイクの急伸利確ピプスは0以上にしてください。"
                    )
                    return None
            if not common_namping_override:
                if not validate_namping("スパイク", spike_namping):
                    return None

        if signal_chain_enabled:
            if signal_chain_pos_pips < 0:
                messagebox.showerror("エラー", "正方向幅は0以上にしてください。")
                return None
            if signal_chain_neg_pips < 0:
                messagebox.showerror("エラー", "逆方向幅は0以上にしてください。")
                return None
            if signal_chain_count < 1:
                messagebox.showerror("エラー", "連続回数は1以上にしてください。")
                return None
            if signal_chain_monitor_minutes < 0:
                messagebox.showerror("エラー", "監視時間は0以上にしてください。")
                return None

        min_ma_period = 1 if ma_unit == "sec" else 2
        if ma_period < min_ma_period:
            if ma_unit == "sec":
                messagebox.showerror("エラー", "移動平均の期間は1以上にしてください。")
            else:
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

        def add_namping_params(prefix, data, params):
            first_enabled, step_enabled, step_pips, step_lots = data
            params[f"{prefix}namping_first_enabled"] = first_enabled
            for idx in range(5):
                step_no = idx + 1
                params[f"{prefix}namping_step{step_no}_enabled"] = step_enabled[idx]
                params[f"{prefix}namping_step{step_no}_pips"] = step_pips[idx]
                params[f"{prefix}namping_step{step_no}_lot"] = step_lots[idx]

        params = {
            "window_ms": window_ms,
            "spike_pips": spike_pips,
            "retrace_rate": retrace_pct / 100.0,
            "reverse_window_seconds": reverse_window_seconds,
            "reverse_pips": reverse_pips,
            "reverse_hold_seconds": reverse_hold_seconds,
            "reverse_max_pullback_pips": reverse_max_pullback_pips,
            "reverse_monitor_stop_enabled": reverse_monitor_stop_enabled,
            "reverse_monitor_stop_pips": reverse_monitor_stop_pips,
            "spread_pips": spread_pips,
            "stop_pips": common_stop_pips,
            "take_pips": common_take_pips,
            "time_close_seconds": common_time_close_seconds,
            "fixed_exit_price": common_fixed_exit_price,
            "common_stop_pips": common_stop_pips,
            "common_take_pips": common_take_pips,
            "common_time_close_seconds": common_time_close_seconds,
            "common_fixed_exit_price": common_fixed_exit_price,
            "common_fast_take_min": common_fast_take_min,
            "common_fast_take_window_ms": common_fast_take_window_ms,
            "common_fast_take_pips": common_fast_take_pips,
            "common_stop_enabled": common_stop_enabled,
            "common_take_enabled": common_take_enabled,
            "common_time_enabled": common_time_enabled,
            "common_fast_take_enabled": common_fast_take_enabled,
            "common_stop_override": common_stop_override,
            "common_take_override": common_take_override,
            "common_time_override": common_time_override,
            "common_fixed_override": common_fixed_override,
            "common_namping_override": common_namping_override,
            "common_fast_take_override": common_fast_take_override,
            "reverse_stop_pips": reverse_stop_pips,
            "reverse_take_pips": reverse_take_pips,
            "reverse_time_close_seconds": reverse_time_close_seconds,
            "reverse_fixed_exit_price": reverse_fixed_exit_price,
            "reverse_fast_take_min": reverse_fast_take_min,
            "reverse_fast_take_window_ms": reverse_fast_take_window_ms,
            "reverse_fast_take_pips": reverse_fast_take_pips,
            "reverse_stop_enabled": reverse_stop_enabled,
            "reverse_take_enabled": reverse_take_enabled,
            "reverse_time_enabled": reverse_time_enabled,
            "reverse_fast_take_enabled": reverse_fast_take_enabled,
            "momentum_stop_pips": momentum_stop_pips,
            "momentum_take_pips": momentum_take_pips,
            "momentum_time_close_seconds": momentum_time_close_seconds,
            "momentum_fixed_exit_price": momentum_fixed_exit_price,
            "momentum_fast_take_min": momentum_fast_take_min,
            "momentum_fast_take_window_ms": momentum_fast_take_window_ms,
            "momentum_fast_take_pips": momentum_fast_take_pips,
            "momentum_stop_enabled": momentum_stop_enabled,
            "momentum_take_enabled": momentum_take_enabled,
            "momentum_time_enabled": momentum_time_enabled,
            "momentum_fast_take_enabled": momentum_fast_take_enabled,
            "sr_stop_pips": sr_stop_pips,
            "sr_take_pips": sr_take_pips,
            "sr_time_close_seconds": sr_time_close_seconds,
            "sr_fixed_exit_price": sr_fixed_exit_price,
            "sr_fast_take_min": sr_fast_take_min,
            "sr_fast_take_window_ms": sr_fast_take_window_ms,
            "sr_fast_take_pips": sr_fast_take_pips,
            "sr_stop_enabled": sr_stop_enabled,
            "sr_take_enabled": sr_take_enabled,
            "sr_time_enabled": sr_time_enabled,
            "sr_fast_take_enabled": sr_fast_take_enabled,
            "spike_stop_pips": spike_stop_pips,
            "spike_take_pips": spike_take_pips,
            "spike_time_close_seconds": spike_time_close_seconds,
            "spike_fixed_exit_price": spike_fixed_exit_price,
            "spike_fast_take_min": spike_fast_take_min,
            "spike_fast_take_window_ms": spike_fast_take_window_ms,
            "spike_fast_take_pips": spike_fast_take_pips,
            "spike_stop_enabled": spike_stop_enabled,
            "spike_take_enabled": spike_take_enabled,
            "spike_time_enabled": spike_time_enabled,
            "spike_fast_take_enabled": spike_fast_take_enabled,
            "ma_enabled": ma_enabled,
            "ma_period": int(ma_period),
            "ma_unit": ma_unit,
            "ma_deviation_rate": ma_deviation_pct / 100.0,
            "signal_chain_pos_pips": signal_chain_pos_pips,
            "signal_chain_neg_pips": signal_chain_neg_pips,
            "signal_chain_count": signal_chain_count,
            "signal_chain_monitor_minutes": signal_chain_monitor_minutes,
            "signal_chain_ignore_reverse": signal_chain_ignore_reverse,
            "signal_chain_ignore_momentum": signal_chain_ignore_momentum,
            "signal_chain_ignore_sr": signal_chain_ignore_sr,
            "signal_chain_ignore_spike": signal_chain_ignore_spike,
            "signal_chain_enabled": signal_chain_enabled,
            "signal_chain_count_reverse": signal_chain_count_reverse,
            "signal_chain_count_momentum": signal_chain_count_momentum,
            "signal_chain_count_sr": signal_chain_count_sr,
            "signal_chain_count_spike": signal_chain_count_spike,
            "signal_chain_trigger_reverse": signal_chain_trigger_reverse,
            "signal_chain_trigger_momentum": signal_chain_trigger_momentum,
            "signal_chain_trigger_sr": signal_chain_trigger_sr,
            "signal_chain_trigger_spike": signal_chain_trigger_spike,
            "extreme_enabled": extreme_enabled,
            "extreme_hold_ms": extreme_hold_ms,
            "extreme_distance_pips": extreme_distance_pips,
            "exclude_enabled": exclude_enabled,
            "exclude_hours": exclude_hours,
            "allow_same_direction": allow_same_direction,
            "allow_opposite_direction": allow_opposite_direction,
        }

        add_namping_params("common_", common_namping, params)
        add_namping_params("reverse_", reverse_namping, params)
        add_namping_params("momentum_", momentum_namping, params)
        add_namping_params("sr_", sr_namping, params)
        add_namping_params("spike_", spike_namping, params)

        return params

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
            wait_bars = int(self._parse_number(self.sr_reentry_wait_bars_var.get()))
            min_seconds = self._parse_number(self.sr_reentry_min_seconds_var.get())
            max_seconds = self._parse_number(self.sr_reentry_max_seconds_var.get())
            tick_limit_enabled = self.sr_reentry_tick_limit_enabled_var.get()
            tick_min_enabled = self.sr_reentry_tick_min_enabled_var.get()
            midpoint_enabled = self.sr_reentry_midpoint_enabled_var.get()
            dominance_enabled = self.sr_reentry_dominance_enabled_var.get()
            move_ratio_enabled = self.sr_reentry_move_ratio_enabled_var.get()
            speed_ratio_enabled = self.sr_reentry_speed_ratio_enabled_var.get()
            favored_tick_min_enabled = (
                self.sr_reentry_favored_tick_min_enabled_var.get()
            )
            ratio_join_label = self.sr_reentry_ratio_join_var.get()

            tick_limit = (
                int(self._parse_number(self.sr_reentry_tick_limit_var.get()))
                if tick_limit_enabled
                else 1_000_000_000
            )
            tick_min = (
                self._parse_number(self.sr_reentry_tick_min_var.get())
                if tick_min_enabled
                else 0.0
            )
            midpoint_pct = (
                self._parse_number(self.sr_reentry_midpoint_var.get())
                if midpoint_enabled
                else 50.0
            )
            dominance_pct = (
                self._parse_number(self.sr_reentry_dominance_var.get())
                if dominance_enabled
                else 0.0
            )
            move_ratio_pct = (
                self._parse_number(self.sr_reentry_move_ratio_var.get())
                if move_ratio_enabled
                else 0.0
            )
            speed_ratio_pct = (
                self._parse_number(self.sr_reentry_speed_ratio_var.get())
                if speed_ratio_enabled
                else 0.0
            )
            favored_tick_min_pips = (
                self._parse_number(self.sr_reentry_favored_tick_min_var.get())
                if favored_tick_min_enabled
                else 0.0
            )
        except ValueError:
            messagebox.showerror("エラー", "水平線戻りの数値入力が正しくありません。")
            return None

        if break_pips <= 0:
            messagebox.showerror("エラー", "抜け幅は0より大きくしてください。")
            return None
        if tick_limit_enabled and tick_limit < 1:
            messagebox.showerror("エラー", "ティック数上限は1以上にしてください。")
            return None
        if tick_min_enabled and tick_min < 0:
            messagebox.showerror("エラー", "ティック数下限は0以上にしてください。")
            return None
        if tick_limit_enabled and tick_min_enabled and tick_limit < tick_min:
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
        if midpoint_enabled and (midpoint_pct < 0 or midpoint_pct > 100):
            messagebox.showerror("エラー", "滞在判定位置は0〜100の範囲にしてください。")
            return None
        if dominance_enabled and (dominance_pct < 0 or dominance_pct > 100):
            messagebox.showerror("エラー", "優勢滞在率は0〜100の範囲にしてください。")
            return None
        if move_ratio_enabled and move_ratio_pct < 0:
            messagebox.showerror("エラー", "有利平均幅比率は0以上にしてください。")
            return None
        if speed_ratio_enabled and speed_ratio_pct < 0:
            messagebox.showerror("エラー", "有利速度比率は0以上にしてください。")
            return None
        if favored_tick_min_enabled and favored_tick_min_pips < 0:
            messagebox.showerror("エラー", "有利1ティック幅下限は0以上にしてください。")
            return None

        target_label = self.sr_reentry_target_var.get()
        target_map = {
            "水平線": "sr",
            "補助線": "range",
            "両方": "both",
        }
        target_kind = target_map.get(target_label, "both")
        ratio_join_map = {
            "両方": "and",
            "どちらか": "or",
        }
        ratio_join_mode = ratio_join_map.get(ratio_join_label, "and")

        return {
            "sr_break_pips": break_pips,
            "sr_tick_limit": tick_limit,
            "sr_tick_min": tick_min,
            "sr_wait_bars": wait_bars,
            "sr_min_seconds": min_seconds,
            "sr_max_seconds": max_seconds,
            "sr_midpoint_pct": midpoint_pct,
            "sr_dominance_pct": dominance_pct,
            "sr_move_ratio_pct": move_ratio_pct,
            "sr_move_speed_ratio_pct": speed_ratio_pct,
            "sr_favored_tick_min_pips": favored_tick_min_pips,
            "sr_move_ratio_enabled": move_ratio_enabled,
            "sr_move_speed_ratio_enabled": speed_ratio_enabled,
            "sr_ratio_join_mode": ratio_join_mode,
            "sr_target": target_kind,
        }

    def _get_momentum_params(self):
        try:
            window_ms = self._parse_number(self.momentum_window_var.get())
            spike_pips = self._parse_number(self.momentum_spike_pips_var.get())
            boundary_pct = self._parse_number(self.momentum_boundary_pct_var.get())
            tick_min = self._parse_number(self.momentum_tick_min_var.get())
            hold_seconds = self._parse_number(self.momentum_hold_seconds_var.get())
            max_pips = self._parse_number(self.momentum_max_pips_var.get())
        except ValueError:
            messagebox.showerror("エラー", "勢い条件の数値入力が正しくありません。")
            return None

        if window_ms <= 0:
            messagebox.showerror("エラー", "監視時間は0より大きくしてください。")
            return None
        if spike_pips <= 0:
            messagebox.showerror("エラー", "監視幅は0より大きくしてください。")
            return None
        if boundary_pct < 0 or boundary_pct > 100:
            messagebox.showerror("エラー", "境界位置は0〜100の範囲にしてください。")
            return None
        if tick_min < 0:
            messagebox.showerror("エラー", "平均ティック/分下限は0以上にしてください。")
            return None
        if hold_seconds < 0:
            messagebox.showerror("エラー", "継続秒数は0以上にしてください。")
            return None
        if max_pips < 0:
            messagebox.showerror("エラー", "上限は0以上にしてください。")
            return None

        return {
            "momentum_window_ms": window_ms,
            "momentum_spike_pips": spike_pips,
            "momentum_boundary_pct": boundary_pct,
            "momentum_tick_min_per_min": tick_min,
            "momentum_hold_seconds": hold_seconds,
            "momentum_max_pips": max_pips,
        }

    def _start_download(self):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("お知らせ", "ダウンロード中です。")
            return
        start_month = self._parse_month_text(self.download_start_month_var.get())
        end_month = self._parse_month_text(self.download_end_month_var.get())
        if start_month is None or end_month is None:
            messagebox.showerror("エラー", "開始年月と終了年月を正しく入力してください。")
            return
        if end_month < start_month:
            messagebox.showerror("エラー", "終了年月は開始年月より後にしてください。")
            return
        self._apply_download_month_range(start_month, end_month)
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
        if self.batch_active:
            self.batch_active = False
            self.batch_months = []
            self.batch_index = 0
            if hasattr(self, "batch_run_button"):
                self.batch_run_button.config(state="normal")
            self._stop_batch_timer()
        else:
            self.chart_cancel_button.config(state="disabled")

    def _state_file_path(self):
        return project_root() / "ui_state.json"

    def _load_persistent_state(self):
        path = self._state_file_path()
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            backup_path = path.with_suffix(path.suffix + ".bak")
            if not backup_path.exists():
                return
            try:
                data = json.loads(backup_path.read_text(encoding="utf-8"))
            except Exception:
                return

        def set_var(var, value):
            if value is None:
                return
            try:
                var.set(value)
            except Exception:
                pass

        def set_bool(var, value):
            if value is None:
                return
            try:
                var.set(bool(value))
            except Exception:
                pass

        def set_date_value(key, attr_name, var):
            raw = data.get(key)
            if not raw:
                return
            try:
                parsed = date.fromisoformat(raw)
            except Exception:
                return
            setattr(self, attr_name, parsed)
            var.set(parsed.isoformat())

        set_date_value("start_date", "start_date", self.start_var)
        set_date_value("end_date", "end_date", self.end_var)
        set_date_value("view_start_date", "view_start_date", self.view_start_var)
        set_date_value("view_end_date", "view_end_date", self.view_end_var)
        set_var(self.view_range_months_var, data.get("view_range_months"))
        set_var(self.download_start_month_var, data.get("download_start_month"))
        set_var(self.download_end_month_var, data.get("download_end_month"))
        set_var(self.batch_start_month_var, data.get("batch_start_month"))
        set_var(self.batch_end_month_var, data.get("batch_end_month"))
        if self.view_start_date:
            self.view_month_label_var.set(
                f"{self.view_start_date.year}年{self.view_start_date.month:02d}月"
            )
        if not (self.download_start_month_var.get() or "").strip():
            if self.start_date:
                self.download_start_month_var.set(
                    f"{self.start_date.year}-{self.start_date.month:02d}"
                )
        if not (self.download_end_month_var.get() or "").strip():
            if self.end_date:
                self.download_end_month_var.set(
                    f"{self.end_date.year}-{self.end_date.month:02d}"
                )

        set_bool(self.exclude_weekends_var, data.get("exclude_weekends"))
        set_var(self.x_axis_mode_var, data.get("x_axis_mode"))
        set_var(self.chart_type_var, data.get("chart_type"))
        set_var(self.candle_interval_var, data.get("candle_interval"))
        entry_spike = data.get("entry_spike_enabled")
        entry_sr = data.get("entry_sr_enabled")
        entry_momentum = data.get("entry_momentum_enabled")
        entry_reverse = data.get("entry_reverse_enabled")
        if (
            entry_spike is None
            and entry_sr is None
            and entry_momentum is None
            and entry_reverse is None
        ):
            entry_mode = data.get("entry_mode")
            if entry_mode == "spike":
                entry_spike = True
                entry_sr = False
                entry_momentum = False
                entry_reverse = False
            elif entry_mode == "sr_reentry":
                entry_spike = False
                entry_sr = True
                entry_momentum = False
                entry_reverse = False
            elif entry_mode == "both":
                entry_spike = True
                entry_sr = True
                entry_momentum = False
                entry_reverse = False
            elif entry_mode == "momentum":
                entry_spike = False
                entry_sr = False
                entry_momentum = True
                entry_reverse = False
            elif entry_mode == "reverse":
                entry_spike = False
                entry_sr = False
                entry_momentum = False
                entry_reverse = True
            elif entry_mode == "multi":
                entry_spike = True
                entry_sr = True
                entry_momentum = True
                entry_reverse = False
        if entry_spike is not None:
            set_bool(self.entry_spike_var, entry_spike)
        if entry_sr is not None:
            set_bool(self.entry_sr_var, entry_sr)
        if entry_momentum is not None:
            set_bool(self.entry_momentum_var, entry_momentum)
        if entry_reverse is not None:
            set_bool(self.entry_reverse_var, entry_reverse)
        set_bool(self.hide_chart_var, data.get("hide_chart"))
        self.pnl_log_enabled_var.set(True)
        self.pnl_log_path_var.set(str(project_root() / RESULTS_DIR_NAME))
        set_bool(self.ma_filter_var, data.get("ma_filter"))
        set_var(self.ma_period_var, data.get("ma_period"))
        set_var(self.ma_unit_var, data.get("ma_unit"))
        set_var(self.ma_deviation_var, data.get("ma_deviation"))
        set_bool(self.zigzag_show_var, data.get("zigzag_show"))
        set_bool(self.sr_line_show_var, data.get("sr_line_show"))
        set_bool(self.range_band_show_var, data.get("range_band_show"))
        set_var(self.range_band_bars_var, data.get("range_band_bars"))
        set_bool(self.extreme_filter_var, data.get("extreme_filter"))
        set_var(self.extreme_hold_ms_var, data.get("extreme_hold_ms"))
        set_var(self.extreme_distance_pips_var, data.get("extreme_distance_pips"))
        set_bool(self.backtest_exclude_var, data.get("backtest_exclude"))

        hours = data.get("backtest_exclude_hours")
        if isinstance(hours, list):
            for i, value in enumerate(hours[: len(self.backtest_exclude_hours_vars)]):
                self.backtest_exclude_hours_vars[i].set(bool(value))

        set_var(self.spike_window_var, data.get("spike_window"))
        set_var(self.spike_pips_var, data.get("spike_pips"))
        set_var(self.retrace_var, data.get("retrace"))
        set_var(self.reverse_window_var, data.get("reverse_window_seconds"))
        set_var(self.reverse_pips_var, data.get("reverse_pips"))
        set_var(self.reverse_hold_seconds_var, data.get("reverse_hold_seconds"))
        set_var(self.reverse_max_pullback_var, data.get("reverse_max_pullback_pips"))
        set_bool(
            self.reverse_monitor_stop_enabled_var,
            data.get("reverse_monitor_stop_enabled"),
        )
        set_var(
            self.reverse_monitor_stop_pips_var,
            data.get("reverse_monitor_stop_pips"),
        )
        set_var(self.momentum_window_var, data.get("momentum_window_ms"))
        set_var(self.momentum_spike_pips_var, data.get("momentum_spike_pips"))
        set_var(self.momentum_boundary_pct_var, data.get("momentum_boundary_pct"))
        set_var(self.momentum_tick_min_var, data.get("momentum_tick_min_per_min"))
        set_var(self.momentum_hold_seconds_var, data.get("momentum_hold_seconds"))
        max_pips = data.get("momentum_max_pips")
        if max_pips is None:
            old_long = data.get("momentum_max_long_pips")
            old_short = data.get("momentum_max_short_pips")
            candidates = [v for v in (old_long, old_short) if v is not None]
            if candidates:
                try:
                    max_pips = max(candidates)
                except Exception:
                    max_pips = None
        set_var(self.momentum_max_pips_var, max_pips)
        set_var(self.spread_var, data.get("spread"))
        set_var(self.stop_pips_var, data.get("stop_pips"))
        set_var(self.take_pips_var, data.get("take_pips"))
        set_var(self.fast_take_min_var, data.get("fast_take_min"))
        set_var(self.fast_take_window_ms_var, data.get("fast_take_window_ms"))
        set_var(self.fast_take_pips_var, data.get("fast_take_pips"))
        time_close_seconds = data.get("time_close_seconds")
        if time_close_seconds is None:
            old_minutes = data.get("time_close_minutes")
            if old_minutes is not None:
                try:
                    time_close_seconds = float(old_minutes) * 60.0
                except Exception:
                    time_close_seconds = None
        set_var(self.time_close_seconds_var, time_close_seconds)
        set_bool(self.fixed_exit_price_var, data.get("fixed_exit_price"))
        set_bool(self.common_stop_enabled_var, data.get("common_stop_enabled"))
        set_bool(self.common_take_enabled_var, data.get("common_take_enabled"))
        set_bool(self.common_time_enabled_var, data.get("common_time_enabled"))
        set_bool(self.common_fast_take_enabled_var, data.get("common_fast_take_enabled"))
        set_bool(self.common_stop_override_var, data.get("common_stop_override"))
        set_bool(self.common_take_override_var, data.get("common_take_override"))
        set_bool(self.common_time_override_var, data.get("common_time_override"))
        set_bool(self.common_fixed_override_var, data.get("common_fixed_override"))
        set_bool(self.common_namping_override_var, data.get("common_namping_override"))
        set_bool(self.common_fast_take_override_var, data.get("common_fast_take_override"))
        set_var(self.reverse_stop_pips_var, data.get("reverse_stop_pips"))
        set_var(self.reverse_take_pips_var, data.get("reverse_take_pips"))
        set_var(
            self.reverse_time_close_seconds_var,
            data.get("reverse_time_close_seconds"),
        )
        set_bool(self.reverse_fixed_exit_price_var, data.get("reverse_fixed_exit_price"))
        set_var(self.reverse_fast_take_min_var, data.get("reverse_fast_take_min"))
        set_var(
            self.reverse_fast_take_window_ms_var,
            data.get("reverse_fast_take_window_ms"),
        )
        set_var(self.reverse_fast_take_pips_var, data.get("reverse_fast_take_pips"))
        set_bool(self.reverse_stop_enabled_var, data.get("reverse_stop_enabled"))
        set_bool(self.reverse_take_enabled_var, data.get("reverse_take_enabled"))
        set_bool(self.reverse_time_enabled_var, data.get("reverse_time_enabled"))
        set_bool(self.reverse_fast_take_enabled_var, data.get("reverse_fast_take_enabled"))
        set_var(self.momentum_stop_pips_var, data.get("momentum_stop_pips"))
        set_var(self.momentum_take_pips_var, data.get("momentum_take_pips"))
        set_var(
            self.momentum_time_close_seconds_var,
            data.get("momentum_time_close_seconds"),
        )
        set_bool(
            self.momentum_fixed_exit_price_var, data.get("momentum_fixed_exit_price")
        )
        set_var(self.momentum_fast_take_min_var, data.get("momentum_fast_take_min"))
        set_var(
            self.momentum_fast_take_window_ms_var,
            data.get("momentum_fast_take_window_ms"),
        )
        set_var(self.momentum_fast_take_pips_var, data.get("momentum_fast_take_pips"))
        set_bool(self.momentum_stop_enabled_var, data.get("momentum_stop_enabled"))
        set_bool(self.momentum_take_enabled_var, data.get("momentum_take_enabled"))
        set_bool(self.momentum_time_enabled_var, data.get("momentum_time_enabled"))
        set_bool(self.momentum_fast_take_enabled_var, data.get("momentum_fast_take_enabled"))
        set_var(self.sr_stop_pips_var, data.get("sr_stop_pips"))
        set_var(self.sr_take_pips_var, data.get("sr_take_pips"))
        set_var(self.sr_time_close_seconds_var, data.get("sr_time_close_seconds"))
        set_bool(self.sr_fixed_exit_price_var, data.get("sr_fixed_exit_price"))
        set_var(self.sr_fast_take_min_var, data.get("sr_fast_take_min"))
        set_var(self.sr_fast_take_window_ms_var, data.get("sr_fast_take_window_ms"))
        set_var(self.sr_fast_take_pips_var, data.get("sr_fast_take_pips"))
        set_bool(self.sr_stop_enabled_var, data.get("sr_stop_enabled"))
        set_bool(self.sr_take_enabled_var, data.get("sr_take_enabled"))
        set_bool(self.sr_time_enabled_var, data.get("sr_time_enabled"))
        set_bool(self.sr_fast_take_enabled_var, data.get("sr_fast_take_enabled"))
        set_var(self.spike_stop_pips_var, data.get("spike_stop_pips"))
        set_var(self.spike_take_pips_var, data.get("spike_take_pips"))
        set_var(
            self.spike_time_close_seconds_var,
            data.get("spike_time_close_seconds"),
        )
        set_bool(self.spike_fixed_exit_price_var, data.get("spike_fixed_exit_price"))
        set_var(self.spike_fast_take_min_var, data.get("spike_fast_take_min"))
        set_var(
            self.spike_fast_take_window_ms_var,
            data.get("spike_fast_take_window_ms"),
        )
        set_var(self.spike_fast_take_pips_var, data.get("spike_fast_take_pips"))
        set_bool(self.spike_stop_enabled_var, data.get("spike_stop_enabled"))
        set_bool(self.spike_take_enabled_var, data.get("spike_take_enabled"))
        set_bool(self.spike_time_enabled_var, data.get("spike_time_enabled"))
        set_bool(self.spike_fast_take_enabled_var, data.get("spike_fast_take_enabled"))
        set_bool(self.allow_same_direction_var, data.get("allow_same_direction"))
        set_bool(self.allow_opposite_direction_var, data.get("allow_opposite_direction"))
        set_bool(self.namping_first_entry_var, data.get("namping_first_enabled"))
        set_bool(self.namping_step1_enabled_var, data.get("namping_step1_enabled"))
        set_bool(self.namping_step2_enabled_var, data.get("namping_step2_enabled"))
        set_var(self.namping_step1_pips_var, data.get("namping_step1_pips"))
        set_var(self.namping_step2_pips_var, data.get("namping_step2_pips"))
        set_var(self.namping_step1_lot_var, data.get("namping_step1_lot"))
        set_var(self.namping_step2_lot_var, data.get("namping_step2_lot"))
        set_bool(self.namping_step3_enabled_var, data.get("namping_step3_enabled"))
        set_bool(self.namping_step4_enabled_var, data.get("namping_step4_enabled"))
        set_bool(self.namping_step5_enabled_var, data.get("namping_step5_enabled"))
        set_var(self.namping_step3_pips_var, data.get("namping_step3_pips"))
        set_var(self.namping_step4_pips_var, data.get("namping_step4_pips"))
        set_var(self.namping_step5_pips_var, data.get("namping_step5_pips"))
        set_var(self.namping_step3_lot_var, data.get("namping_step3_lot"))
        set_var(self.namping_step4_lot_var, data.get("namping_step4_lot"))
        set_var(self.namping_step5_lot_var, data.get("namping_step5_lot"))
        set_bool(
            self.reverse_namping_first_entry_var,
            data.get("reverse_namping_first_enabled"),
        )
        set_bool(
            self.reverse_namping_step1_enabled_var,
            data.get("reverse_namping_step1_enabled"),
        )
        set_bool(
            self.reverse_namping_step2_enabled_var,
            data.get("reverse_namping_step2_enabled"),
        )
        set_bool(
            self.reverse_namping_step3_enabled_var,
            data.get("reverse_namping_step3_enabled"),
        )
        set_bool(
            self.reverse_namping_step4_enabled_var,
            data.get("reverse_namping_step4_enabled"),
        )
        set_bool(
            self.reverse_namping_step5_enabled_var,
            data.get("reverse_namping_step5_enabled"),
        )
        set_var(
            self.reverse_namping_step1_pips_var,
            data.get("reverse_namping_step1_pips"),
        )
        set_var(
            self.reverse_namping_step2_pips_var,
            data.get("reverse_namping_step2_pips"),
        )
        set_var(
            self.reverse_namping_step3_pips_var,
            data.get("reverse_namping_step3_pips"),
        )
        set_var(
            self.reverse_namping_step4_pips_var,
            data.get("reverse_namping_step4_pips"),
        )
        set_var(
            self.reverse_namping_step5_pips_var,
            data.get("reverse_namping_step5_pips"),
        )
        set_var(
            self.reverse_namping_step1_lot_var,
            data.get("reverse_namping_step1_lot"),
        )
        set_var(
            self.reverse_namping_step2_lot_var,
            data.get("reverse_namping_step2_lot"),
        )
        set_var(
            self.reverse_namping_step3_lot_var,
            data.get("reverse_namping_step3_lot"),
        )
        set_var(
            self.reverse_namping_step4_lot_var,
            data.get("reverse_namping_step4_lot"),
        )
        set_var(
            self.reverse_namping_step5_lot_var,
            data.get("reverse_namping_step5_lot"),
        )
        set_bool(
            self.momentum_namping_first_entry_var,
            data.get("momentum_namping_first_enabled"),
        )
        set_bool(
            self.momentum_namping_step1_enabled_var,
            data.get("momentum_namping_step1_enabled"),
        )
        set_bool(
            self.momentum_namping_step2_enabled_var,
            data.get("momentum_namping_step2_enabled"),
        )
        set_bool(
            self.momentum_namping_step3_enabled_var,
            data.get("momentum_namping_step3_enabled"),
        )
        set_bool(
            self.momentum_namping_step4_enabled_var,
            data.get("momentum_namping_step4_enabled"),
        )
        set_bool(
            self.momentum_namping_step5_enabled_var,
            data.get("momentum_namping_step5_enabled"),
        )
        set_var(
            self.momentum_namping_step1_pips_var,
            data.get("momentum_namping_step1_pips"),
        )
        set_var(
            self.momentum_namping_step2_pips_var,
            data.get("momentum_namping_step2_pips"),
        )
        set_var(
            self.momentum_namping_step3_pips_var,
            data.get("momentum_namping_step3_pips"),
        )
        set_var(
            self.momentum_namping_step4_pips_var,
            data.get("momentum_namping_step4_pips"),
        )
        set_var(
            self.momentum_namping_step5_pips_var,
            data.get("momentum_namping_step5_pips"),
        )
        set_var(
            self.momentum_namping_step1_lot_var,
            data.get("momentum_namping_step1_lot"),
        )
        set_var(
            self.momentum_namping_step2_lot_var,
            data.get("momentum_namping_step2_lot"),
        )
        set_var(
            self.momentum_namping_step3_lot_var,
            data.get("momentum_namping_step3_lot"),
        )
        set_var(
            self.momentum_namping_step4_lot_var,
            data.get("momentum_namping_step4_lot"),
        )
        set_var(
            self.momentum_namping_step5_lot_var,
            data.get("momentum_namping_step5_lot"),
        )
        set_bool(
            self.sr_namping_first_entry_var,
            data.get("sr_namping_first_enabled"),
        )
        set_bool(
            self.sr_namping_step1_enabled_var,
            data.get("sr_namping_step1_enabled"),
        )
        set_bool(
            self.sr_namping_step2_enabled_var,
            data.get("sr_namping_step2_enabled"),
        )
        set_bool(
            self.sr_namping_step3_enabled_var,
            data.get("sr_namping_step3_enabled"),
        )
        set_bool(
            self.sr_namping_step4_enabled_var,
            data.get("sr_namping_step4_enabled"),
        )
        set_bool(
            self.sr_namping_step5_enabled_var,
            data.get("sr_namping_step5_enabled"),
        )
        set_var(
            self.sr_namping_step1_pips_var,
            data.get("sr_namping_step1_pips"),
        )
        set_var(
            self.sr_namping_step2_pips_var,
            data.get("sr_namping_step2_pips"),
        )
        set_var(
            self.sr_namping_step3_pips_var,
            data.get("sr_namping_step3_pips"),
        )
        set_var(
            self.sr_namping_step4_pips_var,
            data.get("sr_namping_step4_pips"),
        )
        set_var(
            self.sr_namping_step5_pips_var,
            data.get("sr_namping_step5_pips"),
        )
        set_var(
            self.sr_namping_step1_lot_var,
            data.get("sr_namping_step1_lot"),
        )
        set_var(
            self.sr_namping_step2_lot_var,
            data.get("sr_namping_step2_lot"),
        )
        set_var(
            self.sr_namping_step3_lot_var,
            data.get("sr_namping_step3_lot"),
        )
        set_var(
            self.sr_namping_step4_lot_var,
            data.get("sr_namping_step4_lot"),
        )
        set_var(
            self.sr_namping_step5_lot_var,
            data.get("sr_namping_step5_lot"),
        )
        set_bool(
            self.spike_namping_first_entry_var,
            data.get("spike_namping_first_enabled"),
        )
        set_bool(
            self.spike_namping_step1_enabled_var,
            data.get("spike_namping_step1_enabled"),
        )
        set_bool(
            self.spike_namping_step2_enabled_var,
            data.get("spike_namping_step2_enabled"),
        )
        set_bool(
            self.spike_namping_step3_enabled_var,
            data.get("spike_namping_step3_enabled"),
        )
        set_bool(
            self.spike_namping_step4_enabled_var,
            data.get("spike_namping_step4_enabled"),
        )
        set_bool(
            self.spike_namping_step5_enabled_var,
            data.get("spike_namping_step5_enabled"),
        )
        set_var(
            self.spike_namping_step1_pips_var,
            data.get("spike_namping_step1_pips"),
        )
        set_var(
            self.spike_namping_step2_pips_var,
            data.get("spike_namping_step2_pips"),
        )
        set_var(
            self.spike_namping_step3_pips_var,
            data.get("spike_namping_step3_pips"),
        )
        set_var(
            self.spike_namping_step4_pips_var,
            data.get("spike_namping_step4_pips"),
        )
        set_var(
            self.spike_namping_step5_pips_var,
            data.get("spike_namping_step5_pips"),
        )
        set_var(
            self.spike_namping_step1_lot_var,
            data.get("spike_namping_step1_lot"),
        )
        set_var(
            self.spike_namping_step2_lot_var,
            data.get("spike_namping_step2_lot"),
        )
        set_var(
            self.spike_namping_step3_lot_var,
            data.get("spike_namping_step3_lot"),
        )
        set_var(
            self.spike_namping_step4_lot_var,
            data.get("spike_namping_step4_lot"),
        )
        set_var(
            self.spike_namping_step5_lot_var,
            data.get("spike_namping_step5_lot"),
        )

        set_var(self.sr_zigzag_pips_var, data.get("sr_zigzag_pips"))
        set_var(self.sr_break_pips_var, data.get("sr_break_pips"))
        set_var(self.sr_min_bars_var, data.get("sr_min_bars"))
        set_var(self.sr_reentry_break_pips_var, data.get("sr_reentry_break_pips"))
        set_var(self.sr_reentry_tick_limit_var, data.get("sr_reentry_tick_limit"))
        set_var(self.sr_reentry_tick_min_var, data.get("sr_reentry_tick_min"))
        set_bool(
            self.sr_reentry_tick_limit_enabled_var,
            data.get("sr_reentry_tick_limit_enabled"),
        )
        set_bool(
            self.sr_reentry_tick_min_enabled_var,
            data.get("sr_reentry_tick_min_enabled"),
        )
        set_var(self.sr_reentry_wait_bars_var, data.get("sr_reentry_wait_bars"))
        set_var(self.sr_reentry_min_seconds_var, data.get("sr_reentry_min_seconds"))
        set_var(self.sr_reentry_max_seconds_var, data.get("sr_reentry_max_seconds"))
        set_var(self.sr_reentry_midpoint_var, data.get("sr_reentry_midpoint"))
        set_var(self.sr_reentry_dominance_var, data.get("sr_reentry_dominance"))
        set_var(self.sr_reentry_move_ratio_var, data.get("sr_reentry_move_ratio"))
        set_var(self.sr_reentry_speed_ratio_var, data.get("sr_reentry_speed_ratio"))
        set_var(self.sr_reentry_ratio_join_var, data.get("sr_reentry_ratio_join"))
        set_var(
            self.sr_reentry_favored_tick_min_var,
            data.get("sr_reentry_favored_tick_min"),
        )
        set_bool(
            self.sr_reentry_midpoint_enabled_var,
            data.get("sr_reentry_midpoint_enabled"),
        )
        set_bool(
            self.sr_reentry_dominance_enabled_var,
            data.get("sr_reentry_dominance_enabled"),
        )
        set_bool(
            self.sr_reentry_move_ratio_enabled_var,
            data.get("sr_reentry_move_ratio_enabled"),
        )
        set_bool(
            self.sr_reentry_speed_ratio_enabled_var,
            data.get("sr_reentry_speed_ratio_enabled"),
        )
        set_bool(
            self.sr_reentry_favored_tick_min_enabled_var,
            data.get("sr_reentry_favored_tick_min_enabled"),
        )
        set_var(self.sr_reentry_target_var, data.get("sr_reentry_target"))
        set_var(self.signal_chain_pos_pips_var, data.get("signal_chain_pos_pips"))
        set_var(self.signal_chain_neg_pips_var, data.get("signal_chain_neg_pips"))
        set_var(self.signal_chain_count_var, data.get("signal_chain_count"))
        set_var(
            self.signal_chain_monitor_minutes_var,
            data.get("signal_chain_monitor_minutes"),
        )
        set_bool(
            self.signal_chain_enabled_var,
            data.get("signal_chain_enabled"),
        )
        set_bool(
            self.signal_chain_count_reverse_var,
            data.get("signal_chain_count_reverse"),
        )
        set_bool(
            self.signal_chain_count_momentum_var,
            data.get("signal_chain_count_momentum"),
        )
        set_bool(
            self.signal_chain_count_sr_var,
            data.get("signal_chain_count_sr"),
        )
        set_bool(
            self.signal_chain_count_spike_var,
            data.get("signal_chain_count_spike"),
        )
        old_ignore = data.get("signal_chain_ignore_opposite")
        ignore_reverse = data.get("signal_chain_ignore_reverse")
        ignore_momentum = data.get("signal_chain_ignore_momentum")
        ignore_sr = data.get("signal_chain_ignore_sr")
        ignore_spike = data.get("signal_chain_ignore_spike")
        if (
            ignore_reverse is None
            and ignore_momentum is None
            and ignore_sr is None
            and ignore_spike is None
            and old_ignore is not None
        ):
            ignore_reverse = old_ignore
            ignore_momentum = old_ignore
            ignore_sr = old_ignore
            ignore_spike = old_ignore
        set_bool(self.signal_chain_ignore_reverse_var, ignore_reverse)
        set_bool(self.signal_chain_ignore_momentum_var, ignore_momentum)
        set_bool(self.signal_chain_ignore_sr_var, ignore_sr)
        set_bool(self.signal_chain_ignore_spike_var, ignore_spike)
        set_bool(
            self.signal_chain_trigger_reverse_var,
            data.get("signal_chain_trigger_reverse"),
        )
        set_bool(
            self.signal_chain_trigger_momentum_var,
            data.get("signal_chain_trigger_momentum"),
        )
        set_bool(
            self.signal_chain_trigger_sr_var,
            data.get("signal_chain_trigger_sr"),
        )
        set_bool(
            self.signal_chain_trigger_spike_var,
            data.get("signal_chain_trigger_spike"),
        )

    def _collect_persistent_state(self):
        return {
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "view_start_date": self.view_start_date.isoformat(),
            "view_end_date": self.view_end_date.isoformat(),
            "view_range_months": self.view_range_months_var.get(),
            "download_start_month": self.download_start_month_var.get(),
            "download_end_month": self.download_end_month_var.get(),
            "batch_start_month": self.batch_start_month_var.get(),
            "batch_end_month": self.batch_end_month_var.get(),
            "exclude_weekends": self.exclude_weekends_var.get(),
            "x_axis_mode": self.x_axis_mode_var.get(),
            "chart_type": self.chart_type_var.get(),
            "candle_interval": int(self.candle_interval_var.get()),
            "entry_spike_enabled": self.entry_spike_var.get(),
            "entry_sr_enabled": self.entry_sr_var.get(),
            "entry_momentum_enabled": self.entry_momentum_var.get(),
            "entry_reverse_enabled": self.entry_reverse_var.get(),
            "hide_chart": self.hide_chart_var.get(),
            "pnl_log_enabled": self.pnl_log_enabled_var.get(),
            "pnl_log_path": self.pnl_log_path_var.get(),
            "ma_filter": self.ma_filter_var.get(),
            "ma_period": self.ma_period_var.get(),
            "ma_unit": self.ma_unit_var.get(),
            "ma_deviation": self.ma_deviation_var.get(),
            "zigzag_show": self.zigzag_show_var.get(),
            "sr_line_show": self.sr_line_show_var.get(),
            "range_band_show": self.range_band_show_var.get(),
            "range_band_bars": self.range_band_bars_var.get(),
            "extreme_filter": self.extreme_filter_var.get(),
            "extreme_hold_ms": self.extreme_hold_ms_var.get(),
            "extreme_distance_pips": self.extreme_distance_pips_var.get(),
            "backtest_exclude": self.backtest_exclude_var.get(),
            "backtest_exclude_hours": [
                var.get() for var in self.backtest_exclude_hours_vars
            ],
            "spike_window": self.spike_window_var.get(),
            "spike_pips": self.spike_pips_var.get(),
            "retrace": self.retrace_var.get(),
            "reverse_window_seconds": self.reverse_window_var.get(),
            "reverse_pips": self.reverse_pips_var.get(),
            "reverse_hold_seconds": self.reverse_hold_seconds_var.get(),
            "reverse_max_pullback_pips": self.reverse_max_pullback_var.get(),
            "reverse_monitor_stop_enabled": self.reverse_monitor_stop_enabled_var.get(),
            "reverse_monitor_stop_pips": self.reverse_monitor_stop_pips_var.get(),
            "momentum_window_ms": self.momentum_window_var.get(),
            "momentum_spike_pips": self.momentum_spike_pips_var.get(),
            "momentum_boundary_pct": self.momentum_boundary_pct_var.get(),
            "momentum_tick_min_per_min": self.momentum_tick_min_var.get(),
            "momentum_hold_seconds": self.momentum_hold_seconds_var.get(),
            "momentum_max_pips": self.momentum_max_pips_var.get(),
            "spread": self.spread_var.get(),
            "stop_pips": self.stop_pips_var.get(),
            "take_pips": self.take_pips_var.get(),
            "time_close_seconds": self.time_close_seconds_var.get(),
            "fixed_exit_price": self.fixed_exit_price_var.get(),
            "fast_take_min": self.fast_take_min_var.get(),
            "fast_take_window_ms": self.fast_take_window_ms_var.get(),
            "fast_take_pips": self.fast_take_pips_var.get(),
            "common_stop_enabled": self.common_stop_enabled_var.get(),
            "common_take_enabled": self.common_take_enabled_var.get(),
            "common_time_enabled": self.common_time_enabled_var.get(),
            "common_fast_take_enabled": self.common_fast_take_enabled_var.get(),
            "common_stop_override": self.common_stop_override_var.get(),
            "common_take_override": self.common_take_override_var.get(),
            "common_time_override": self.common_time_override_var.get(),
            "common_fixed_override": self.common_fixed_override_var.get(),
            "common_namping_override": self.common_namping_override_var.get(),
            "common_fast_take_override": self.common_fast_take_override_var.get(),
            "reverse_stop_pips": self.reverse_stop_pips_var.get(),
            "reverse_take_pips": self.reverse_take_pips_var.get(),
            "reverse_time_close_seconds": self.reverse_time_close_seconds_var.get(),
            "reverse_fixed_exit_price": self.reverse_fixed_exit_price_var.get(),
            "reverse_fast_take_min": self.reverse_fast_take_min_var.get(),
            "reverse_fast_take_window_ms": self.reverse_fast_take_window_ms_var.get(),
            "reverse_fast_take_pips": self.reverse_fast_take_pips_var.get(),
            "reverse_stop_enabled": self.reverse_stop_enabled_var.get(),
            "reverse_take_enabled": self.reverse_take_enabled_var.get(),
            "reverse_time_enabled": self.reverse_time_enabled_var.get(),
            "reverse_fast_take_enabled": self.reverse_fast_take_enabled_var.get(),
            "momentum_stop_pips": self.momentum_stop_pips_var.get(),
            "momentum_take_pips": self.momentum_take_pips_var.get(),
            "momentum_time_close_seconds": self.momentum_time_close_seconds_var.get(),
            "momentum_fixed_exit_price": self.momentum_fixed_exit_price_var.get(),
            "momentum_fast_take_min": self.momentum_fast_take_min_var.get(),
            "momentum_fast_take_window_ms": self.momentum_fast_take_window_ms_var.get(),
            "momentum_fast_take_pips": self.momentum_fast_take_pips_var.get(),
            "momentum_stop_enabled": self.momentum_stop_enabled_var.get(),
            "momentum_take_enabled": self.momentum_take_enabled_var.get(),
            "momentum_time_enabled": self.momentum_time_enabled_var.get(),
            "momentum_fast_take_enabled": self.momentum_fast_take_enabled_var.get(),
            "sr_stop_pips": self.sr_stop_pips_var.get(),
            "sr_take_pips": self.sr_take_pips_var.get(),
            "sr_time_close_seconds": self.sr_time_close_seconds_var.get(),
            "sr_fixed_exit_price": self.sr_fixed_exit_price_var.get(),
            "sr_fast_take_min": self.sr_fast_take_min_var.get(),
            "sr_fast_take_window_ms": self.sr_fast_take_window_ms_var.get(),
            "sr_fast_take_pips": self.sr_fast_take_pips_var.get(),
            "sr_stop_enabled": self.sr_stop_enabled_var.get(),
            "sr_take_enabled": self.sr_take_enabled_var.get(),
            "sr_time_enabled": self.sr_time_enabled_var.get(),
            "sr_fast_take_enabled": self.sr_fast_take_enabled_var.get(),
            "spike_stop_pips": self.spike_stop_pips_var.get(),
            "spike_take_pips": self.spike_take_pips_var.get(),
            "spike_time_close_seconds": self.spike_time_close_seconds_var.get(),
            "spike_fixed_exit_price": self.spike_fixed_exit_price_var.get(),
            "spike_fast_take_min": self.spike_fast_take_min_var.get(),
            "spike_fast_take_window_ms": self.spike_fast_take_window_ms_var.get(),
            "spike_fast_take_pips": self.spike_fast_take_pips_var.get(),
            "spike_stop_enabled": self.spike_stop_enabled_var.get(),
            "spike_take_enabled": self.spike_take_enabled_var.get(),
            "spike_time_enabled": self.spike_time_enabled_var.get(),
            "spike_fast_take_enabled": self.spike_fast_take_enabled_var.get(),
            "allow_same_direction": self.allow_same_direction_var.get(),
            "allow_opposite_direction": self.allow_opposite_direction_var.get(),
            "namping_first_enabled": self.namping_first_entry_var.get(),
            "namping_step1_enabled": self.namping_step1_enabled_var.get(),
            "namping_step2_enabled": self.namping_step2_enabled_var.get(),
            "namping_step1_pips": self.namping_step1_pips_var.get(),
            "namping_step2_pips": self.namping_step2_pips_var.get(),
            "namping_step1_lot": self.namping_step1_lot_var.get(),
            "namping_step2_lot": self.namping_step2_lot_var.get(),
            "namping_step3_enabled": self.namping_step3_enabled_var.get(),
            "namping_step4_enabled": self.namping_step4_enabled_var.get(),
            "namping_step5_enabled": self.namping_step5_enabled_var.get(),
            "namping_step3_pips": self.namping_step3_pips_var.get(),
            "namping_step4_pips": self.namping_step4_pips_var.get(),
            "namping_step5_pips": self.namping_step5_pips_var.get(),
            "namping_step3_lot": self.namping_step3_lot_var.get(),
            "namping_step4_lot": self.namping_step4_lot_var.get(),
            "namping_step5_lot": self.namping_step5_lot_var.get(),
            "reverse_namping_first_enabled": (
                self.reverse_namping_first_entry_var.get()
            ),
            "reverse_namping_step1_enabled": self.reverse_namping_step1_enabled_var.get(),
            "reverse_namping_step2_enabled": self.reverse_namping_step2_enabled_var.get(),
            "reverse_namping_step3_enabled": self.reverse_namping_step3_enabled_var.get(),
            "reverse_namping_step4_enabled": self.reverse_namping_step4_enabled_var.get(),
            "reverse_namping_step5_enabled": self.reverse_namping_step5_enabled_var.get(),
            "reverse_namping_step1_pips": self.reverse_namping_step1_pips_var.get(),
            "reverse_namping_step2_pips": self.reverse_namping_step2_pips_var.get(),
            "reverse_namping_step3_pips": self.reverse_namping_step3_pips_var.get(),
            "reverse_namping_step4_pips": self.reverse_namping_step4_pips_var.get(),
            "reverse_namping_step5_pips": self.reverse_namping_step5_pips_var.get(),
            "reverse_namping_step1_lot": self.reverse_namping_step1_lot_var.get(),
            "reverse_namping_step2_lot": self.reverse_namping_step2_lot_var.get(),
            "reverse_namping_step3_lot": self.reverse_namping_step3_lot_var.get(),
            "reverse_namping_step4_lot": self.reverse_namping_step4_lot_var.get(),
            "reverse_namping_step5_lot": self.reverse_namping_step5_lot_var.get(),
            "momentum_namping_first_enabled": (
                self.momentum_namping_first_entry_var.get()
            ),
            "momentum_namping_step1_enabled": (
                self.momentum_namping_step1_enabled_var.get()
            ),
            "momentum_namping_step2_enabled": (
                self.momentum_namping_step2_enabled_var.get()
            ),
            "momentum_namping_step3_enabled": (
                self.momentum_namping_step3_enabled_var.get()
            ),
            "momentum_namping_step4_enabled": (
                self.momentum_namping_step4_enabled_var.get()
            ),
            "momentum_namping_step5_enabled": (
                self.momentum_namping_step5_enabled_var.get()
            ),
            "momentum_namping_step1_pips": self.momentum_namping_step1_pips_var.get(),
            "momentum_namping_step2_pips": self.momentum_namping_step2_pips_var.get(),
            "momentum_namping_step3_pips": self.momentum_namping_step3_pips_var.get(),
            "momentum_namping_step4_pips": self.momentum_namping_step4_pips_var.get(),
            "momentum_namping_step5_pips": self.momentum_namping_step5_pips_var.get(),
            "momentum_namping_step1_lot": self.momentum_namping_step1_lot_var.get(),
            "momentum_namping_step2_lot": self.momentum_namping_step2_lot_var.get(),
            "momentum_namping_step3_lot": self.momentum_namping_step3_lot_var.get(),
            "momentum_namping_step4_lot": self.momentum_namping_step4_lot_var.get(),
            "momentum_namping_step5_lot": self.momentum_namping_step5_lot_var.get(),
            "sr_namping_first_enabled": self.sr_namping_first_entry_var.get(),
            "sr_namping_step1_enabled": self.sr_namping_step1_enabled_var.get(),
            "sr_namping_step2_enabled": self.sr_namping_step2_enabled_var.get(),
            "sr_namping_step3_enabled": self.sr_namping_step3_enabled_var.get(),
            "sr_namping_step4_enabled": self.sr_namping_step4_enabled_var.get(),
            "sr_namping_step5_enabled": self.sr_namping_step5_enabled_var.get(),
            "sr_namping_step1_pips": self.sr_namping_step1_pips_var.get(),
            "sr_namping_step2_pips": self.sr_namping_step2_pips_var.get(),
            "sr_namping_step3_pips": self.sr_namping_step3_pips_var.get(),
            "sr_namping_step4_pips": self.sr_namping_step4_pips_var.get(),
            "sr_namping_step5_pips": self.sr_namping_step5_pips_var.get(),
            "sr_namping_step1_lot": self.sr_namping_step1_lot_var.get(),
            "sr_namping_step2_lot": self.sr_namping_step2_lot_var.get(),
            "sr_namping_step3_lot": self.sr_namping_step3_lot_var.get(),
            "sr_namping_step4_lot": self.sr_namping_step4_lot_var.get(),
            "sr_namping_step5_lot": self.sr_namping_step5_lot_var.get(),
            "spike_namping_first_enabled": self.spike_namping_first_entry_var.get(),
            "spike_namping_step1_enabled": self.spike_namping_step1_enabled_var.get(),
            "spike_namping_step2_enabled": self.spike_namping_step2_enabled_var.get(),
            "spike_namping_step3_enabled": self.spike_namping_step3_enabled_var.get(),
            "spike_namping_step4_enabled": self.spike_namping_step4_enabled_var.get(),
            "spike_namping_step5_enabled": self.spike_namping_step5_enabled_var.get(),
            "spike_namping_step1_pips": self.spike_namping_step1_pips_var.get(),
            "spike_namping_step2_pips": self.spike_namping_step2_pips_var.get(),
            "spike_namping_step3_pips": self.spike_namping_step3_pips_var.get(),
            "spike_namping_step4_pips": self.spike_namping_step4_pips_var.get(),
            "spike_namping_step5_pips": self.spike_namping_step5_pips_var.get(),
            "spike_namping_step1_lot": self.spike_namping_step1_lot_var.get(),
            "spike_namping_step2_lot": self.spike_namping_step2_lot_var.get(),
            "spike_namping_step3_lot": self.spike_namping_step3_lot_var.get(),
            "spike_namping_step4_lot": self.spike_namping_step4_lot_var.get(),
            "spike_namping_step5_lot": self.spike_namping_step5_lot_var.get(),
            "sr_zigzag_pips": self.sr_zigzag_pips_var.get(),
            "sr_break_pips": self.sr_break_pips_var.get(),
            "sr_min_bars": self.sr_min_bars_var.get(),
            "sr_reentry_break_pips": self.sr_reentry_break_pips_var.get(),
            "sr_reentry_tick_limit": self.sr_reentry_tick_limit_var.get(),
            "sr_reentry_tick_min": self.sr_reentry_tick_min_var.get(),
            "sr_reentry_tick_limit_enabled": self.sr_reentry_tick_limit_enabled_var.get(),
            "sr_reentry_tick_min_enabled": self.sr_reentry_tick_min_enabled_var.get(),
            "sr_reentry_wait_bars": self.sr_reentry_wait_bars_var.get(),
            "sr_reentry_min_seconds": self.sr_reentry_min_seconds_var.get(),
            "sr_reentry_max_seconds": self.sr_reentry_max_seconds_var.get(),
            "sr_reentry_midpoint": self.sr_reentry_midpoint_var.get(),
            "sr_reentry_dominance": self.sr_reentry_dominance_var.get(),
            "sr_reentry_move_ratio": self.sr_reentry_move_ratio_var.get(),
            "sr_reentry_speed_ratio": self.sr_reentry_speed_ratio_var.get(),
            "sr_reentry_ratio_join": self.sr_reentry_ratio_join_var.get(),
            "sr_reentry_favored_tick_min": self.sr_reentry_favored_tick_min_var.get(),
            "sr_reentry_midpoint_enabled": self.sr_reentry_midpoint_enabled_var.get(),
            "sr_reentry_dominance_enabled": self.sr_reentry_dominance_enabled_var.get(),
            "sr_reentry_move_ratio_enabled": self.sr_reentry_move_ratio_enabled_var.get(),
            "sr_reentry_speed_ratio_enabled": self.sr_reentry_speed_ratio_enabled_var.get(),
            "sr_reentry_favored_tick_min_enabled": (
                self.sr_reentry_favored_tick_min_enabled_var.get()
            ),
            "sr_reentry_target": self.sr_reentry_target_var.get(),
            "signal_chain_pos_pips": self.signal_chain_pos_pips_var.get(),
            "signal_chain_neg_pips": self.signal_chain_neg_pips_var.get(),
            "signal_chain_count": self.signal_chain_count_var.get(),
            "signal_chain_monitor_minutes": self.signal_chain_monitor_minutes_var.get(),
            "signal_chain_ignore_reverse": self.signal_chain_ignore_reverse_var.get(),
            "signal_chain_ignore_momentum": self.signal_chain_ignore_momentum_var.get(),
            "signal_chain_ignore_sr": self.signal_chain_ignore_sr_var.get(),
            "signal_chain_ignore_spike": self.signal_chain_ignore_spike_var.get(),
            "signal_chain_enabled": self.signal_chain_enabled_var.get(),
            "signal_chain_count_reverse": self.signal_chain_count_reverse_var.get(),
            "signal_chain_count_momentum": self.signal_chain_count_momentum_var.get(),
            "signal_chain_count_sr": self.signal_chain_count_sr_var.get(),
            "signal_chain_count_spike": self.signal_chain_count_spike_var.get(),
            "signal_chain_trigger_reverse": self.signal_chain_trigger_reverse_var.get(),
            "signal_chain_trigger_momentum": (
                self.signal_chain_trigger_momentum_var.get()
            ),
            "signal_chain_trigger_sr": self.signal_chain_trigger_sr_var.get(),
            "signal_chain_trigger_spike": self.signal_chain_trigger_spike_var.get(),
        }

    def _save_persistent_state(self):
        path = self._state_file_path()
        try:
            payload = self._collect_persistent_state()
            tmp_path = path.with_suffix(path.suffix + ".tmp")
            backup_path = path.with_suffix(path.suffix + ".bak")
            tmp_path.write_text(
                json.dumps(payload, ensure_ascii=True, indent=2),
                encoding="utf-8",
            )
            if path.exists():
                try:
                    if backup_path.exists():
                        backup_path.unlink()
                    path.replace(backup_path)
                except Exception:
                    pass
            tmp_path.replace(path)
        except Exception:
            pass

    def _on_app_close(self):
        self._save_persistent_state()
        self.root.destroy()

    def _format_elapsed_text(self, seconds):
        total_seconds = int(seconds)
        if total_seconds < 60:
            return f"計算時間: {total_seconds}秒"
        minutes = total_seconds // 60
        remain = total_seconds - minutes * 60
        return f"計算時間: {minutes}分{remain}秒"

    def _format_batch_elapsed_text(self, seconds):
        total_seconds = int(seconds)
        if total_seconds < 60:
            return f"一括時間: {total_seconds}秒"
        minutes = total_seconds // 60
        remain = total_seconds - minutes * 60
        return f"一括時間: {minutes}分{remain}秒"

    def _append_batch_result(self, payload):
        summary = payload.get("summary", {})
        self.batch_results.append(
            {
                "start": self.view_start_date,
                "end": self.view_end_date,
                "summary": summary,
                "equity_curve": payload.get("equity_curve") or [],
            }
        )

    def _finalize_batch_results(self):
        if not self.batch_results:
            self.batch_total_equity = []
            self.batch_yearly_data = []
            self.batch_monthly_data = []
            self._draw_batch_year_chart()
            self._draw_batch_month_chart()
            return

        results_sorted = sorted(
            self.batch_results,
            key=lambda r: (r.get("start") or date.min, r.get("end") or date.min),
        )
        combined = []
        offset = 0.0
        total = 0
        wins = 0
        losses = 0
        draws = 0
        total_pips = 0.0
        year_totals = {}
        month_totals = {}

        for result in results_sorted:
            summary = result.get("summary", {})
            total += int(summary.get("total", 0))
            wins += int(summary.get("wins", 0))
            losses += int(summary.get("losses", 0))
            draws += int(summary.get("draws", 0))
            total_pips += float(summary.get("total_pips", 0.0))

            start_date = result.get("start")
            if start_date:
                year_totals[start_date.year] = year_totals.get(start_date.year, 0.0) + float(
                    summary.get("total_pips", 0.0)
                )
                month_key = f"{start_date.year}-{start_date.month:02d}"
                month_totals[month_key] = month_totals.get(month_key, 0.0) + float(
                    summary.get("total_pips", 0.0)
                )

            curve = result.get("equity_curve") or []
            if curve:
                for ts, value in curve:
                    combined.append((ts, value + offset))
                offset += curve[-1][1]

        self.batch_total_equity = combined
        self.batch_yearly_data = sorted(year_totals.items(), key=lambda x: x[0])
        self.batch_monthly_data = sorted(month_totals.items(), key=lambda x: x[0])

        if combined:
            max_dd = compute_max_drawdown(combined)
            draw_text = f" 引き分け{draws}件" if draws else ""
            self.pnl_info_var.set(
                f"一括損益: 取引{total}件 勝ち{wins}件 負け{losses}件"
                f"{draw_text} 合計損益{total_pips:.1f}ピップス 最大DD{max_dd:.1f}ピップス"
            )
            self.pnl_data = combined
            self._reset_pnl_view()
            self.backtest_ready = True
            self._draw_pnl_chart()
        else:
            self.pnl_data = None
            self.backtest_ready = False
            self.pnl_info_var.set("一括損益: データなし")
            self._draw_pnl_chart()

        self._draw_batch_year_chart()
        self._draw_batch_month_chart()

    def _start_backtest_timer(self):
        self.backtest_started_at = pytime.perf_counter()
        self.backtest_timer_running = True
        self.backtest_elapsed_last_seconds = 0
        self.backtest_elapsed_var.set("計算時間: 0秒")

    def _update_backtest_timer(self):
        if not self.backtest_timer_running or self.backtest_started_at is None:
            return
        elapsed = pytime.perf_counter() - self.backtest_started_at
        elapsed_seconds = int(elapsed)
        if elapsed_seconds == self.backtest_elapsed_last_seconds:
            return
        self.backtest_elapsed_last_seconds = elapsed_seconds
        self.backtest_elapsed_var.set(self._format_elapsed_text(elapsed_seconds))

    def _stop_backtest_timer(self):
        if self.backtest_started_at is None:
            self.backtest_timer_running = False
            return
        elapsed = pytime.perf_counter() - self.backtest_started_at
        elapsed_seconds = int(elapsed)
        self.backtest_elapsed_last_seconds = elapsed_seconds
        self.backtest_elapsed_var.set(self._format_elapsed_text(elapsed_seconds))
        self.backtest_timer_running = False

    def _start_batch_timer(self):
        self.batch_started_at = pytime.perf_counter()
        self.batch_timer_running = True
        self.batch_elapsed_last_seconds = 0
        self.batch_elapsed_var.set("一括時間: 0秒")

    def _update_batch_timer(self):
        if not self.batch_timer_running or self.batch_started_at is None:
            return
        elapsed = pytime.perf_counter() - self.batch_started_at
        elapsed_seconds = int(elapsed)
        if elapsed_seconds == self.batch_elapsed_last_seconds:
            return
        self.batch_elapsed_last_seconds = elapsed_seconds
        self.batch_elapsed_var.set(self._format_batch_elapsed_text(elapsed_seconds))

    def _stop_batch_timer(self):
        if self.batch_started_at is None:
            self.batch_timer_running = False
            return
        elapsed = pytime.perf_counter() - self.batch_started_at
        elapsed_seconds = int(elapsed)
        self.batch_elapsed_last_seconds = elapsed_seconds
        self.batch_elapsed_var.set(self._format_batch_elapsed_text(elapsed_seconds))
        self.batch_timer_running = False

    def _show_chart(self):
        if self.chart_worker and self.chart_worker.is_alive():
            messagebox.showinfo("お知らせ", "表示処理中です。")
            return False
        months = self._get_view_range_months()
        if months is None:
            return False
        if not self.batch_active:
            self.batch_results = []
            self.batch_total_equity = None
            self.batch_yearly_data = None
            self.batch_monthly_data = None
            self._draw_batch_year_chart()
            self._draw_batch_month_chart()
        self._apply_view_month_range(self.view_start_date or date.today(), months)
        if self.view_end_date < self.view_start_date:
            messagebox.showerror("エラー", "終了日は開始日より後にしてください。")
            return False
        params = self._get_backtest_params()
        if not params:
            return False
        entry_spike_enabled = self.entry_spike_var.get()
        entry_sr_enabled = self.entry_sr_var.get()
        entry_momentum_enabled = self.entry_momentum_var.get()
        entry_reverse_enabled = self.entry_reverse_var.get()
        sr_params = {}
        range_params = {}
        if entry_sr_enabled:
            sr_params = self._get_sr_params()
            range_params = self._get_range_params()
            if not sr_params or not range_params:
                return
        if (
            not entry_spike_enabled
            and not entry_sr_enabled
            and not entry_momentum_enabled
            and not entry_reverse_enabled
        ):
            messagebox.showerror("エラー", "戦略は最低1つ選択してください。")
            return False
        sr_reentry_params = {}
        if entry_sr_enabled:
            sr_reentry_params = self._get_sr_reentry_params()
            if not sr_reentry_params:
                return False
        momentum_params = {}
        if entry_momentum_enabled:
            momentum_params = self._get_momentum_params()
            if not momentum_params:
                return False
        try:
            line_interval = int(self.candle_interval_var.get())
        except Exception:
            line_interval = 1
        enabled_count = sum(
            1
            for flag in (
                entry_spike_enabled,
                entry_sr_enabled,
                entry_momentum_enabled,
                entry_reverse_enabled,
            )
            if flag
        )
        if enabled_count >= 2:
            if (
                entry_spike_enabled
                and entry_sr_enabled
                and not entry_momentum_enabled
                and not entry_reverse_enabled
            ):
                entry_mode = "both"
            else:
                entry_mode = "multi"
        elif entry_sr_enabled:
            entry_mode = "sr_reentry"
        elif entry_momentum_enabled:
            entry_mode = "momentum"
        elif entry_reverse_enabled:
            entry_mode = "reverse"
        else:
            entry_mode = "spike"
        params["entry_mode"] = entry_mode
        params["entry_spike_enabled"] = entry_spike_enabled
        params["entry_sr_enabled"] = entry_sr_enabled
        params["entry_momentum_enabled"] = entry_momentum_enabled
        params["entry_reverse_enabled"] = entry_reverse_enabled
        params["line_interval"] = max(1, line_interval)
        params["sr_params"] = sr_params
        params["range_params"] = range_params
        params.update(sr_reentry_params)
        params.update(momentum_params)
        self.chart_cancel_event.clear()
        self.chart_button.config(state="disabled")
        self.chart_cancel_button.config(state="normal")
        self.status_var.set("表示準備中...")
        self.backtest_info_var.set("バックテスト: 計算中...")
        self.pnl_info_var.set("損益: 計算中...")
        self._start_backtest_timer()
        self.backtest_ready = False
        self.pnl_data = None
        self._draw_pnl_chart()
        self.chart_worker = threading.Thread(
            target=self._chart_worker,
            args=(self.view_start_date, self.view_end_date, params, sr_params, range_params),
            daemon=True,
        )
        self.chart_worker.start()
        return True

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
            backtest["run_params"] = params
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
                    if self.batch_active:
                        self._run_next_batch()
                elif kind == "chart_error":
                    messagebox.showerror("エラー", payload)
                    self.backtest_info_var.set("バックテスト: データなし")
                    self.pnl_info_var.set("損益: データなし")
                    self._stop_backtest_timer()
                    self.backtest_ready = False
                    self.pnl_data = None
                    self._draw_pnl_chart()
                    self.chart_cancel_button.config(state="disabled")
                    if self.batch_active:
                        self.batch_active = False
                        self.batch_months = []
                        self.batch_index = 0
                        if hasattr(self, "batch_run_button"):
                            self.batch_run_button.config(state="normal")
                        self._stop_batch_timer()
                elif kind == "chart_data":
                    self._render_chart(payload)
                elif kind == "backtest_data":
                    self._render_backtest(payload)
                elif kind == "backtest_error":
                    messagebox.showerror("エラー", f"バックテストで問題が起きました: {payload}")
                    self.backtest_info_var.set("バックテスト: エラー")
                    self.pnl_info_var.set("損益: エラー")
                    self._stop_backtest_timer()
                    self.backtest_ready = False
                    self.pnl_data = None
                    self._draw_pnl_chart()
                    self.chart_cancel_button.config(state="disabled")
                    if self.batch_active:
                        self.batch_active = False
                        self.batch_months = []
                        self.batch_index = 0
                        if hasattr(self, "batch_run_button"):
                            self.batch_run_button.config(state="normal")
                        self._stop_batch_timer()
                elif kind == "chart_cancelled":
                    self.status_var.set("表示計算を中止しました")
                    self.backtest_info_var.set("バックテスト: 中止")
                    self.pnl_info_var.set("損益: 中止")
                    self._stop_backtest_timer()
                    self.backtest_ready = False
                    self.pnl_data = None
                    self._draw_pnl_chart()
                    if self.batch_active:
                        self.batch_active = False
                        self.batch_months = []
                        self.batch_index = 0
                        if hasattr(self, "batch_run_button"):
                            self.batch_run_button.config(state="normal")
                        self._stop_batch_timer()
                elif kind == "cancelled":
                    self.status_var.set("キャンセルしました")
                    self.run_button.config(state="normal")
                    self.cancel_button.config(state="disabled")
                    self._clear_analysis_cache()
        except queue.Empty:
            pass
        self._update_backtest_timer()
        self._update_batch_timer()
        self.root.after(200, self._poll_queue)

    def _render_chart(self, payload):
        points = payload["points"]
        start = payload["start"]
        end = payload["end"]
        missing_count = payload["missing_count"]
        sr_params = payload.get("sr_params") or {}
        range_params = payload.get("range_params") or {}

        current_range = (start, end)
        reset_trade_jump = current_range != self.last_view_range
        self.last_view_range = current_range

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
        self.trade_focus_index = None
        if reset_trade_jump:
            self.trade_jump_var.set("1")
        self._update_trade_nav_state()
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
        max_dd = summary.get("max_dd", 0.0)
        entry_mode = payload.get("entry_mode")
        sr_target = payload.get("sr_target")
        entry_spike_enabled = payload.get(
            "entry_spike_enabled", entry_mode in ("spike", "both", "multi")
        )
        entry_sr_enabled = payload.get(
            "entry_sr_enabled", entry_mode in ("sr_reentry", "both", "multi")
        )
        entry_momentum_enabled = payload.get(
            "entry_momentum_enabled", entry_mode in ("momentum", "multi")
        )
        entry_reverse_enabled = payload.get(
            "entry_reverse_enabled", entry_mode in ("reverse", "multi")
        )

        if total == 0:
            self.backtest_info_var.set("バックテスト: 取引0件 最大DD0.0ピップス")
            self.pnl_info_var.set("損益: 取引0件 最大DD0.0ピップス")
        else:
            draw_text = f" 引き分け{draws}件" if draws else ""
            self.backtest_info_var.set(
                f"バックテスト: 取引{total}件 勝ち{wins}件 負け{losses}件"
                f"{draw_text} 勝率{win_rate:.1f}% 合計損益{total_pips:.1f}ピップス"
                f" 平均損益{avg_pips:.2f}ピップス 最大DD{max_dd:.1f}ピップス"
            )
            draw_text_short = f" / 引き分け{draws}" if draws else ""
            self.pnl_info_var.set(
                f"合計損益: {total_pips:.1f}ピップス 取引: {total}件"
                f"（勝ち{wins} / 負け{losses}{draw_text_short}） 最大DD{max_dd:.1f}ピップス"
            )

        if entry_sr_enabled and sr_target == "both":
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

        ratio_join_mode = payload.get("sr_ratio_join_mode")
        move_ratio_enabled = payload.get("sr_move_ratio_enabled")
        speed_ratio_enabled = payload.get("sr_move_speed_ratio_enabled")
        if (
            entry_sr_enabled
            and ratio_join_mode == "or"
            and move_ratio_enabled
            and speed_ratio_enabled
        ):
            trades = payload.get("trades", [])
            move_trades = [t for t in trades if t.get("move_ratio_ok")]
            speed_trades = [t for t in trades if t.get("speed_ratio_ok")]
            move_pips = sum(t.get("pips", 0.0) for t in move_trades)
            speed_pips = sum(t.get("pips", 0.0) for t in speed_trades)
            ratio_breakdown = (
                f"\n比率内訳: 平均幅OK 取引{len(move_trades)}件 合計損益{move_pips:.1f}ピップス"
                f" / 速度OK 取引{len(speed_trades)}件 合計損益{speed_pips:.1f}ピップス"
            )
            self.pnl_info_var.set(self.pnl_info_var.get() + ratio_breakdown)

        trades = payload.get("trades", [])
        if trades:
            long_trades = [t for t in trades if t.get("side") == "long"]
            short_trades = [t for t in trades if t.get("side") == "short"]
            long_pips = sum(t.get("pips", 0.0) for t in long_trades)
            short_pips = sum(t.get("pips", 0.0) for t in short_trades)
            side_breakdown = (
                f"\n内訳: ロング 取引{len(long_trades)}件 合計損益{long_pips:.1f}ピップス"
                f" / ショート 取引{len(short_trades)}件 合計損益{short_pips:.1f}ピップス"
            )
            self.pnl_info_var.set(self.pnl_info_var.get() + side_breakdown)

        self.pnl_data = payload.get("equity_curve") or []
        self._reset_pnl_view()
        self.backtest_ready = True
        if self.chart_data is not None:
            self.chart_data["trades"] = payload.get("trades") or []
            self.chart_data["ma_series"] = payload.get("ma_series") or []
            self.chart_data["ma_enabled"] = payload.get("ma_enabled", False)
            self._draw_chart()
        self._update_trade_nav_state()
        self._draw_pnl_chart()
        self._stop_backtest_timer()
        self._append_pnl_log(payload)
        if self.batch_active:
            self._append_batch_result(payload)
        self._notify_backtest_done()

    def _notify_backtest_done(self):
        if self.batch_active:
            return
        try:
            self.root.bell()
        except Exception:
            pass

    def _notify_batch_done(self):
        try:
            self.root.bell()
        except Exception:
            pass

    def _update_trade_nav_state(self):
        trades = []
        if self.chart_data is not None:
            trades = self.chart_data.get("trades") or []
        total = len(trades)
        if total <= 0:
            self.trade_focus_index = None
            self.trade_nav_info_var.set("取引: 0件")
            if hasattr(self, "trade_prev_button"):
                self.trade_prev_button.config(state="disabled")
            if hasattr(self, "trade_next_button"):
                self.trade_next_button.config(state="disabled")
            if hasattr(self, "trade_jump_button"):
                self.trade_jump_button.config(state="disabled")
            return

        if self.trade_focus_index is not None:
            if self.trade_focus_index < 0:
                self.trade_focus_index = 0
            elif self.trade_focus_index >= total:
                self.trade_focus_index = total - 1
            self.trade_nav_info_var.set(
                f"取引: {self.trade_focus_index + 1}/{total}"
            )
            self.trade_jump_var.set(str(self.trade_focus_index + 1))
        else:
            self.trade_nav_info_var.set(f"取引: 0/{total}")

        if hasattr(self, "trade_prev_button"):
            self.trade_prev_button.config(state="normal")
        if hasattr(self, "trade_next_button"):
            self.trade_next_button.config(state="normal")
        if hasattr(self, "trade_jump_button"):
            self.trade_jump_button.config(state="normal")

    def _focus_trade_index(self, index):
        if not self.chart_data:
            return
        trades = self.chart_data.get("trades") or []
        if not trades:
            return
        total = len(trades)
        if index < 0:
            index = 0
        elif index >= total:
            index = total - 1

        trade = trades[index]
        times = self.chart_data.get("times") or []
        if not times:
            return
        target_time = trade.get("entry_time")
        entry_idx = trade.get("entry_idx")
        if entry_idx is None and isinstance(target_time, datetime):
            entry_idx = bisect_left(times, target_time)
            if entry_idx >= len(times):
                entry_idx = len(times) - 1
            if entry_idx < 0:
                entry_idx = 0
        elif entry_idx is None:
            entry_idx = 0

        chart_type = (
            self.chart_type_var.get() if hasattr(self, "chart_type_var") else "tick"
        )
        mode = self.chart_data.get("mode", "time")
        if chart_type == "candle" or mode == "time":
            view_start_time = self.chart_data.get("view_start_time", times[0])
            view_end_time = self.chart_data.get("view_end_time", times[-1])
            span = view_end_time - view_start_time
            if span.total_seconds() <= 0:
                span = timedelta(hours=1)
            center_time = target_time if isinstance(target_time, datetime) else times[entry_idx]
            half = span / 2
            new_start = center_time - half
            new_end = center_time + half
            if new_start < times[0]:
                new_start = times[0]
                new_end = new_start + span
            if new_end > times[-1]:
                new_end = times[-1]
                new_start = new_end - span
            if new_start < times[0]:
                new_start = times[0]
            self.chart_data["view_start_time"] = new_start
            self.chart_data["view_end_time"] = new_end
        else:
            view_start = self.chart_data.get("view_start", 0)
            view_end = self.chart_data.get("view_end", len(times) - 1)
            visible = max(1, view_end - view_start + 1)
            max_start = max(0, len(times) - visible)
            new_start = entry_idx - visible // 2
            if new_start < 0:
                new_start = 0
            elif new_start > max_start:
                new_start = max_start
            self.chart_data["view_start"] = new_start
            self.chart_data["view_end"] = new_start + visible - 1

        self.trade_focus_index = index
        self._update_trade_nav_state()
        self._draw_chart()

    def _jump_to_trade(self):
        if not self.chart_data:
            return
        trades = self.chart_data.get("trades") or []
        if not trades:
            return
        try:
            value = int(self._parse_number(self.trade_jump_var.get()))
        except ValueError:
            messagebox.showerror("エラー", "取引番号の入力が正しくありません。")
            return
        if value < 1 or value > len(trades):
            messagebox.showerror("エラー", f"取引番号は1～{len(trades)}の範囲で指定してください。")
            return
        self._focus_trade_index(value - 1)

    def _focus_prev_trade(self):
        if not self.chart_data:
            return
        trades = self.chart_data.get("trades") or []
        if not trades:
            return
        if self.trade_focus_index is None:
            self._focus_trade_index(len(trades) - 1)
            return
        self._focus_trade_index(self.trade_focus_index - 1)

    def _focus_next_trade(self):
        if not self.chart_data:
            return
        trades = self.chart_data.get("trades") or []
        if not trades:
            return
        if self.trade_focus_index is None:
            self._focus_trade_index(0)
            return
        self._focus_trade_index(self.trade_focus_index + 1)

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
        self.ma_unit_seconds_radio.config(state=state)
        self.ma_unit_minutes_radio.config(state=state)
        if self.chart_data:
            self._draw_chart()

    def _on_zigzag_toggle(self):
        if self.chart_data:
            self._draw_chart()

    def _on_sr_line_toggle(self):
        if self.chart_data:
            self._draw_chart()

    def _on_range_band_toggle(self):
        if self.chart_data:
            self._draw_chart()

    def _apply_sr_entry_visibility(self, enabled: bool):
        if not enabled:
            self.zigzag_show_var.set(False)
            self.sr_line_show_var.set(False)
            self.range_band_show_var.set(False)
        state = "normal" if enabled else "disabled"
        if hasattr(self, "zigzag_check"):
            self.zigzag_check.config(state=state)
        if hasattr(self, "sr_line_check"):
            self.sr_line_check.config(state=state)
        if hasattr(self, "range_band_check"):
            self.range_band_check.config(state=state)
        if self.chart_data:
            self._draw_chart()

    def _on_sr_reentry_filter_toggle(self):
        if hasattr(self, "sr_reentry_tick_limit_entry"):
            state = "normal" if self.sr_reentry_tick_limit_enabled_var.get() else "disabled"
            self.sr_reentry_tick_limit_entry.config(state=state)
        if hasattr(self, "sr_reentry_tick_min_entry"):
            state = "normal" if self.sr_reentry_tick_min_enabled_var.get() else "disabled"
            self.sr_reentry_tick_min_entry.config(state=state)
        if hasattr(self, "sr_reentry_midpoint_entry"):
            state = "normal" if self.sr_reentry_midpoint_enabled_var.get() else "disabled"
            self.sr_reentry_midpoint_entry.config(state=state)
        if hasattr(self, "sr_reentry_dominance_entry"):
            state = "normal" if self.sr_reentry_dominance_enabled_var.get() else "disabled"
            self.sr_reentry_dominance_entry.config(state=state)
        if hasattr(self, "sr_reentry_move_ratio_entry"):
            state = "normal" if self.sr_reentry_move_ratio_enabled_var.get() else "disabled"
            self.sr_reentry_move_ratio_entry.config(state=state)
        if hasattr(self, "sr_reentry_speed_ratio_entry"):
            state = "normal" if self.sr_reentry_speed_ratio_enabled_var.get() else "disabled"
            self.sr_reentry_speed_ratio_entry.config(state=state)
        if hasattr(self, "sr_reentry_favored_tick_min_entry"):
            state = (
                "normal"
                if self.sr_reentry_favored_tick_min_enabled_var.get()
                else "disabled"
            )
            self.sr_reentry_favored_tick_min_entry.config(state=state)

    def _on_signal_chain_toggle(self):
        enabled = self.signal_chain_enabled_var.get()
        state = "normal" if enabled else "disabled"
        if hasattr(self, "signal_chain_pos_entry"):
            self.signal_chain_pos_entry.config(state=state)
        if hasattr(self, "signal_chain_neg_entry"):
            self.signal_chain_neg_entry.config(state=state)
        if hasattr(self, "signal_chain_count_entry"):
            self.signal_chain_count_entry.config(state=state)
        if hasattr(self, "signal_chain_monitor_entry"):
            self.signal_chain_monitor_entry.config(state=state)
        if hasattr(self, "signal_chain_ignore_reverse_check"):
            self.signal_chain_ignore_reverse_check.config(state=state)
        if hasattr(self, "signal_chain_ignore_momentum_check"):
            self.signal_chain_ignore_momentum_check.config(state=state)
        if hasattr(self, "signal_chain_ignore_sr_check"):
            self.signal_chain_ignore_sr_check.config(state=state)
        if hasattr(self, "signal_chain_ignore_spike_check"):
            self.signal_chain_ignore_spike_check.config(state=state)
        if hasattr(self, "signal_chain_count_reverse_check"):
            self.signal_chain_count_reverse_check.config(state=state)
        if hasattr(self, "signal_chain_count_momentum_check"):
            self.signal_chain_count_momentum_check.config(state=state)
        if hasattr(self, "signal_chain_count_sr_check"):
            self.signal_chain_count_sr_check.config(state=state)
        if hasattr(self, "signal_chain_count_spike_check"):
            self.signal_chain_count_spike_check.config(state=state)
        if hasattr(self, "signal_chain_trigger_reverse_check"):
            self.signal_chain_trigger_reverse_check.config(state=state)
        if hasattr(self, "signal_chain_trigger_momentum_check"):
            self.signal_chain_trigger_momentum_check.config(state=state)
        if hasattr(self, "signal_chain_trigger_sr_check"):
            self.signal_chain_trigger_sr_check.config(state=state)
        if hasattr(self, "signal_chain_trigger_spike_check"):
            self.signal_chain_trigger_spike_check.config(state=state)

    def _set_widget_enabled(self, widget, enabled: bool):
        try:
            if enabled:
                widget.state(["!disabled"])
            else:
                widget.state(["disabled"])
            return
        except Exception:
            pass
        try:
            widget.config(state="normal" if enabled else "disabled")
        except Exception:
            pass

    def _apply_state_recursive(self, parent, enabled: bool, skip=None):
        for child in parent.winfo_children():
            if child is skip:
                continue
            self._set_widget_enabled(child, enabled)
            self._apply_state_recursive(child, enabled, skip)

    def _on_entry_tab_toggle(self, key: str):
        info = self.entry_tab_info.get(key)
        if not info:
            return
        enabled = bool(info["var"].get())
        self._apply_state_recursive(info["frame"], enabled, skip=info["toggle"])
        if key == "sr":
            self._apply_sr_entry_visibility(enabled)
        if enabled:
            self._on_namping_toggle_group(key)
            if key == "reverse":
                self._on_reverse_monitor_stop_toggle()
            self._apply_close_states_for_key(key)

    def _on_namping_toggle_group(self, key: str):
        group = self.namping_widget_groups.get(key)
        if not group:
            return
        for enabled_var, pips_entry, lot_entry in group:
            state = enabled_var.get()
            self._set_widget_enabled(pips_entry, state)
            self._set_widget_enabled(lot_entry, state)

    def _on_namping_toggle(self):
        self._on_namping_toggle_group("common")

    def _register_close_condition(self, key, condition, enabled_var, toggle, widgets):
        self.close_enabled_vars[(key, condition)] = enabled_var
        self.close_toggle_widgets[(key, condition)] = toggle
        self.close_widget_groups[(key, condition)] = widgets

    def _set_close_widgets(self, key, condition, enabled: bool):
        widgets = self.close_widget_groups.get((key, condition)) or []
        for widget in widgets:
            self._set_widget_enabled(widget, enabled)

    def _on_close_toggle(self, key, condition):
        enabled_var = self.close_enabled_vars.get((key, condition))
        if not enabled_var:
            return
        if key != "common":
            override_var = self.close_override_vars.get(condition)
            if override_var and override_var.get():
                common_enabled_var = self.close_enabled_vars.get(("common", condition))
                common_enabled = bool(common_enabled_var.get()) if common_enabled_var else False
                enabled_var.set(common_enabled)
                self._set_close_widgets(key, condition, common_enabled)
                return
        enabled = bool(enabled_var.get())
        self._set_close_widgets(key, condition, enabled)
        if key == "common":
            self._apply_close_overrides(condition)

    def _apply_close_overrides(self, condition=None):
        conditions = [condition] if condition else ["stop", "take", "time", "fast"]
        for cond in conditions:
            override_var = self.close_override_vars.get(cond)
            if not override_var:
                continue
            override = bool(override_var.get())
            common_enabled = bool(
                self.close_enabled_vars.get(("common", cond)).get()
            )
            for key in ("reverse", "momentum", "sr", "spike"):
                enabled_var = self.close_enabled_vars.get((key, cond))
                toggle = self.close_toggle_widgets.get((key, cond))
                if enabled_var is None or toggle is None:
                    continue
                if override:
                    enabled_var.set(common_enabled)
                self._set_close_widgets(key, cond, bool(enabled_var.get()))

    def _on_common_override_toggle(self):
        self._apply_close_overrides()

    def _apply_close_states_for_key(self, key: str):
        for cond in ("stop", "take", "time", "fast"):
            self._on_close_toggle(key, cond)

    def _on_reverse_monitor_stop_toggle(self):
        enabled = self.reverse_monitor_stop_enabled_var.get()
        state = "normal" if enabled else "disabled"
        if hasattr(self, "reverse_monitor_stop_pips_entry"):
            self.reverse_monitor_stop_pips_entry.config(state=state)
        if hasattr(self, "reverse_monitor_stop_label"):
            self._set_widget_enabled(self.reverse_monitor_stop_label, enabled)

    def _on_extreme_filter_toggle(self):
        enabled = self.extreme_filter_var.get()
        state = "normal" if enabled else "disabled"
        self.extreme_hold_entry.config(state=state)
        self.extreme_distance_entry.config(state=state)

    def _on_pnl_log_toggle(self):
        enabled = self.pnl_log_enabled_var.get()
        if hasattr(self, "pnl_log_path_entry"):
            self._set_widget_enabled(self.pnl_log_path_entry, enabled)
        if hasattr(self, "pnl_log_select_button"):
            self._set_widget_enabled(self.pnl_log_select_button, enabled)

    def _select_pnl_log_path(self):
        current = (self.pnl_log_path_var.get() or "").strip()
        initial_dir = ""
        if current:
            try:
                path = Path(current)
                if path.suffix.lower() == ".csv":
                    initial_dir = str(path.parent)
                else:
                    initial_dir = str(path)
            except Exception:
                initial_dir = ""
        chosen = filedialog.askdirectory(
            title="損益記録の保存先フォルダ",
            initialdir=initial_dir or None,
        )
        if not chosen:
            return
        self.pnl_log_path_var.set(chosen)

    def _format_csv_time(self, ts: datetime) -> str:
        if ts is None:
            return ""
        return ts.strftime("%Y-%m-%d %H:%M:%S.%f")

    def _parse_csv_time(self, value: str):
        if not value:
            return None
        text = value.strip()
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(text, fmt)
            except Exception:
                continue
        try:
            return datetime.fromisoformat(text)
        except Exception:
            return None

    def _normalize_json_value(self, value):
        if isinstance(value, dict):
            return {k: self._normalize_json_value(v) for k, v in sorted(value.items())}
        if isinstance(value, set):
            return [
                self._normalize_json_value(v) for v in sorted(value, key=lambda x: str(x))
            ]
        if isinstance(value, (list, tuple)):
            return [self._normalize_json_value(v) for v in value]
        return value

    def _results_root(self) -> Path:
        return project_root() / RESULTS_DIR_NAME

    def _strategy_label(self, payload) -> str:
        labels = []
        if payload.get("entry_spike_enabled"):
            labels.append("スパイク")
        if payload.get("entry_sr_enabled"):
            labels.append("水平線")
        if payload.get("entry_momentum_enabled"):
            labels.append("勢い追随")
        if payload.get("entry_reverse_enabled"):
            labels.append("秒逆張り")
        if not labels:
            return "未設定"
        return "+".join(labels)

    def _settings_id(self, params) -> str:
        normalized = self._normalize_json_value(params or {})
        text = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha1(text.encode("utf-8")).hexdigest()[:8]

    def _set_last_save_paths(
        self,
        base_dir: Path,
        settings_path: Path,
        trade_path: Path,
        namping_path: Path,
        index_path: Path,
    ):
        self.last_save_dir_var.set(str(base_dir))
        self.last_settings_path_var.set(str(settings_path))
        self.last_trade_path_var.set(str(trade_path))
        self.last_namping_path_var.set(str(namping_path))
        self.last_index_path_var.set(str(index_path))

    def _open_path_var(self, path_var: tk.StringVar):
        raw = (path_var.get() or "").strip()
        if not raw:
            messagebox.showinfo("お知らせ", "保存先がありません。")
            return
        try:
            path = Path(raw)
            if not path.exists():
                messagebox.showerror("エラー", "指定したパスが見つかりません。")
                return
            os.startfile(str(path))
        except Exception as e:
            messagebox.showerror("エラー", f"開くことに失敗しました: {e}")

    def _append_pnl_log(self, payload):
        base_dir = self._results_root()
        try:
            base_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        run_at = datetime.now(JST)
        run_id = run_at.strftime("%Y%m%d_%H%M%S")
        strategy_label = self._strategy_label(payload)
        run_params = payload.get("run_params") or {}
        settings_id = self._settings_id(run_params)

        settings_dir = base_dir / PAIR / strategy_label / f"設定_{settings_id}"
        try:
            settings_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        settings_path = settings_dir / "設定.json"
        ui_state = self._collect_persistent_state()
        for key in ("start_date", "end_date", "view_start_date", "view_end_date"):
            ui_state.pop(key, None)
        settings_payload = {
            "通貨": PAIR,
            "戦略": strategy_label,
            "設定ID": settings_id,
            "保存日時": self._format_csv_time(run_at),
            "パラメータ": self._normalize_json_value(run_params),
            "画面設定": self._normalize_json_value(ui_state),
        }
        try:
            settings_path.write_text(
                json.dumps(settings_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

        summary = payload.get("summary", {})
        total = summary.get("total", 0)
        wins = summary.get("wins", 0)
        losses = summary.get("losses", 0)
        total_pips = summary.get("total_pips", 0.0)
        max_dd = summary.get("max_dd", 0.0)

        start_date = self.view_start_date.isoformat() if self.view_start_date else ""
        end_date = self.view_end_date.isoformat() if self.view_end_date else ""

        trades = payload.get("trades", [])
        def trade_time_key(trade):
            fallback = datetime.min.replace(tzinfo=JST)
            return trade.get("exit_time") or trade.get("entry_time") or fallback

        trades_sorted = sorted(trades, key=trade_time_key)

        trade_path = settings_dir / "取引一覧.csv"
        trade_header = [
            "開始日",
            "終了日",
            "実行ID",
            "取引ID",
            "in時刻",
            "out時刻",
            "方向",
            "in価格",
            "out価格",
            "ロット合計",
            "損益",
            "累計損益",
            "in理由",
            "out理由",
            "ライン種別",
            "ナンピン回数",
            "平均in価格",
        ]

        last_cum = 0.0
        if trade_path.exists():
            try:
                with trade_path.open(encoding="utf-8", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        value = row.get("累計損益")
                        if value is None or value == "":
                            continue
                        try:
                            last_cum = float(value)
                        except Exception:
                            continue
            except Exception:
                last_cum = 0.0

        new_rows = []
        cumulative = last_cum

        def safe_float(value, default=0.0):
            try:
                return float(value)
            except Exception:
                return default

        for idx, trade in enumerate(trades_sorted):
            pips = safe_float(trade.get("pips", 0.0))
            cumulative += pips
            trade_id = f"{run_id}-{idx + 1:04d}"
            side = trade.get("side", "")
            if side == "long":
                side_label = "買い"
            elif side == "short":
                side_label = "売り"
            else:
                side_label = side
            line_source = trade.get("line_source")
            if line_source == "sr":
                line_label = "支持抵抗"
            elif line_source == "range":
                line_label = "補助線"
            else:
                line_label = ""
            namping_entries = trade.get("namping_entries") or []
            namping_count = max(0, len(namping_entries) - 1)
            new_rows.append(
                [
                    start_date,
                    end_date,
                    run_id,
                    trade_id,
                    self._format_csv_time(trade.get("entry_time")),
                    self._format_csv_time(trade.get("exit_time")),
                    side_label,
                    f"{safe_float(trade.get('entry_price', 0.0)):.5f}",
                    f"{safe_float(trade.get('exit_price', 0.0)):.5f}",
                    f"{safe_float(trade.get('lot_total', 0.0)):.2f}",
                    f"{pips:.2f}",
                    f"{cumulative:.2f}",
                    trade.get("entry_reason", ""),
                    trade.get("exit_reason", ""),
                    line_label,
                    str(namping_count),
                    f"{safe_float(trade.get('avg_entry_price', 0.0)):.5f}",
                ]
            )

        try:
            write_header = not trade_path.exists() or trade_path.stat().st_size == 0
            with trade_path.open("a", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(trade_header)
                writer.writerows(new_rows)
        except Exception as e:
            messagebox.showerror("エラー", f"損益記録の保存に失敗しました: {e}")

        namping_path = settings_dir / "ナンピン詳細.csv"
        namping_header = [
            "開始日",
            "終了日",
            "実行ID",
            "取引ID",
            "段階",
            "種別",
            "in時刻",
            "in価格",
            "ロット",
        ]
        points_all = None
        if self.chart_data is not None:
            points_all = self.chart_data.get("all_points")

        namping_rows = []
        for idx, trade in enumerate(trades_sorted):
            trade_id = f"{run_id}-{idx + 1:04d}"
            entries = trade.get("namping_entries") or []
            for step_idx, entry in enumerate(entries):
                entry_time = None
                entry_idx = entry.get("idx")
                if points_all and entry_idx is not None and 0 <= entry_idx < len(points_all):
                    entry_time = points_all[entry_idx][0]
                if entry_time is None:
                    entry_time = trade.get("entry_time")
                namping_rows.append(
                    [
                        start_date,
                        end_date,
                        run_id,
                        trade_id,
                        str(step_idx + 1),
                        entry.get("kind", ""),
                        self._format_csv_time(entry_time),
                        f"{safe_float(entry.get('price', 0.0)):.5f}",
                        f"{safe_float(entry.get('lot', 0.0)):.2f}",
                    ]
                )

        if namping_rows:
            try:
                write_header = not namping_path.exists() or namping_path.stat().st_size == 0
                with namping_path.open("a", encoding="utf-8", newline="") as f:
                    writer = csv.writer(f)
                    if write_header:
                        writer.writerow(namping_header)
                    writer.writerows(namping_rows)
            except Exception:
                pass

        index_path = base_dir / "index.csv"
        index_header = [
            "実行時刻",
            "通貨",
            "戦略",
            "設定ID",
            "保存先",
            "開始日",
            "終了日",
            "取引数",
            "勝ち",
            "負け",
            "合計損益",
            "最大DD",
        ]
        index_row = [
            self._format_csv_time(run_at),
            PAIR,
            strategy_label,
            settings_id,
            str(trade_path.relative_to(base_dir)) if trade_path.exists() else "",
            start_date,
            end_date,
            str(total),
            str(wins),
            str(losses),
            f"{safe_float(total_pips):.2f}",
            f"{safe_float(max_dd):.2f}",
        ]
        try:
            write_header = not index_path.exists() or index_path.stat().st_size == 0
            with index_path.open("a", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(index_header)
                writer.writerow(index_row)
        except Exception:
            pass

        self._set_last_save_paths(
            base_dir=base_dir,
            settings_path=settings_path,
            trade_path=trade_path,
            namping_path=namping_path,
            index_path=index_path,
        )
        self._refresh_saved_runs()

    def _format_saved_run_label(self, row):
        run_time = (row.get("実行時刻") or "").strip()
        pair = (row.get("通貨") or "").strip()
        strategy = (row.get("戦略") or "").strip()
        settings_id = (row.get("設定ID") or "").strip()
        start_date = (row.get("開始日") or "").strip()
        end_date = (row.get("終了日") or "").strip()
        total_pips = (row.get("合計損益") or "").strip()

        range_text = ""
        if start_date or end_date:
            range_text = f"{start_date}〜{end_date}".strip("〜")
        total_text = ""
        if total_pips:
            total_text = f"損益{total_pips}ピップス"

        pieces = [
            run_time,
            pair,
            strategy,
            f"設定{settings_id}" if settings_id else "",
            range_text,
            total_text,
        ]
        return " | ".join([p for p in pieces if p])

    def _refresh_saved_runs(self):
        index_path = self._results_root() / "index.csv"
        self.saved_runs = []
        values = []
        if index_path.exists():
            try:
                with index_path.open("r", encoding="utf-8", newline="") as f:
                    rows = list(csv.DictReader(f))
            except Exception:
                rows = []
            for row in reversed(rows):
                rel_path = (row.get("保存先") or "").strip()
                if not rel_path:
                    continue
                try:
                    trade_path = project_root() / rel_path
                except Exception:
                    continue
                display = self._format_saved_run_label(row)
                if not display:
                    display = rel_path
                self.saved_runs.append(
                    {
                        "display": display,
                        "trade_path": trade_path,
                    }
                )
                values.append(display)

        current = self.saved_run_var.get()
        self.saved_runs_combo["values"] = values
        if values:
            self.saved_runs_combo.config(state="readonly")
            self.saved_runs_show_button.config(state="normal")
            if current in values:
                self.saved_runs_combo.set(current)
            else:
                self.saved_runs_combo.current(0)
        else:
            self.saved_runs_combo.set("")
            self.saved_runs_combo.config(state="disabled")
            self.saved_runs_show_button.config(state="disabled")

    def _load_selected_run(self):
        if not self.saved_runs:
            messagebox.showinfo("お知らせ", "保存結果がありません。")
            return
        selection = self.saved_runs_combo.current()
        if selection is None or selection < 0 or selection >= len(self.saved_runs):
            messagebox.showinfo("お知らせ", "保存結果を選択してください。")
            return
        target = self.saved_runs[selection]
        trade_path = target.get("trade_path")
        if not trade_path:
            messagebox.showerror("エラー", "保存先が見つかりません。")
            return
        self._apply_pnl_csv(trade_path, source_label="保存結果")

    def _load_pnl_csv(self):
        chosen = filedialog.askopenfilename(
            title="損益CSVを選択",
            filetypes=[("CSV", "*.csv"), ("すべて", "*.*")],
        )
        if not chosen:
            return
        self._apply_pnl_csv(Path(chosen), source_label="CSV読込")

    def _apply_pnl_csv(self, path: Path, source_label: str = "CSV読込"):
        if not path.exists():
            messagebox.showerror("エラー", "指定したCSVが見つかりません。")
            return
        try:
            with path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                rows = [row for row in reader]
        except Exception as e:
            messagebox.showerror("エラー", f"CSVの読み込みに失敗しました: {e}")
            return

        def pick(row, keys):
            for key in keys:
                if key in row and row.get(key):
                    return row.get(key)
            return ""

        equity_curve = []
        total = 0
        wins = 0
        losses = 0
        cumulative = 0.0
        for row in rows:
            out_time_raw = pick(row, ["out時刻", "out_time", "決済時刻"])
            ts = self._parse_csv_time(out_time_raw)
            if ts is None:
                continue
            cum_raw = pick(row, ["累計損益", "cum_pips"])
            pips_raw = pick(row, ["損益", "pips"])
            if cum_raw:
                try:
                    cumulative = float(cum_raw)
                except Exception:
                    continue
            else:
                try:
                    cumulative += float(pips_raw)
                except Exception:
                    continue
            total += 1
            try:
                pips_value = float(pips_raw)
                if pips_value > 0:
                    wins += 1
                elif pips_value < 0:
                    losses += 1
            except Exception:
                pass
            equity_curve.append((ts, cumulative))

        if not equity_curve:
            messagebox.showerror("エラー", "有効な損益データが見つかりません。")
            return

        max_dd = compute_max_drawdown(equity_curve)
        self.pnl_data = equity_curve
        self._reset_pnl_view()
        self.backtest_ready = True
        self.batch_results = []
        self.batch_total_equity = None
        self.batch_yearly_data = None
        self.batch_monthly_data = None
        self.pnl_info_var.set(
            f"損益: {source_label} 取引{total}件 合計損益{cumulative:.1f}ピップス 最大DD{max_dd:.1f}ピップス"
        )
        self._draw_pnl_chart()
        self._draw_batch_year_chart()
        self._draw_batch_month_chart()

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

    def _find_nearest_trade_marker(self, x, y, radius=10):
        data = self.chart_data or {}
        markers = data.get("trade_markers") or []
        if not markers:
            return None

        best = None
        best_dist2 = radius * radius
        for marker in markers:
            mx = marker.get("x")
            my = marker.get("y")
            if mx is None or my is None:
                continue
            dx = x - mx
            dy = y - my
            dist2 = dx * dx + dy * dy
            if dist2 <= best_dist2:
                best = marker
                best_dist2 = dist2
        return best

    def _format_trade_hover_text(self, marker):
        trade = marker.get("trade") or {}
        side = trade.get("side")
        side_text = "ロング" if side == "long" else "ショート"
        marker_kind = marker.get("kind")
        marker_text = "エントリー" if marker_kind == "entry" else "クローズ"
        entry_reason = trade.get("entry_reason") or "-"
        ratio_labels = []
        if trade.get("move_ratio_ok"):
            ratio_labels.append("平均幅比率")
        if trade.get("speed_ratio_ok"):
            ratio_labels.append("速度比率")
        ratio_text = f" 比率:{'/'.join(ratio_labels)}" if ratio_labels else ""
        exit_reason = trade.get("reason") or "-"
        pips = trade.get("pips")
        if isinstance(pips, (int, float)):
            result_text = f"{pips:+.2f}ピップス"
        else:
            result_text = "-"
        return (
            f"{marker_text} {side_text} "
            f"理由:{entry_reason}{ratio_text} 決済:{exit_reason} 結果:{result_text}"
        )

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
        base_text = ""
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
            base_text = (
                f"カーソル: {ts.strftime('%m/%d %H:%M')} "
                f"始値{open_p:.3f} 高値{high_p:.3f} "
                f"安値{low_p:.3f} 終値{close_p:.3f}"
            )
        else:
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
            base_text = f"カーソル: {ts.strftime('%m/%d %H:%M:%S')} 価格{price:.3f}"

        marker = self._find_nearest_trade_marker(event.x, event.y)
        if marker:
            self.cursor_info_var.set(base_text + " | " + self._format_trade_hover_text(marker))
        else:
            self.cursor_info_var.set(base_text)

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
        sr_entry_enabled = self.entry_sr_var.get()
        show_zigzag = sr_entry_enabled and self.zigzag_show_var.get()
        show_sr_line = sr_entry_enabled and self.sr_line_show_var.get()
        show_range_band = sr_entry_enabled and self.range_band_show_var.get()
        need_overlay_data = (
            chart_type == "candle" or show_zigzag or show_sr_line or show_range_band
        )
        if need_overlay_data:
            overlay_cache = data.get("overlay_cache")
            if overlay_cache is None:
                overlay_cache = {}
                data["overlay_cache"] = overlay_cache

            cache_entry = overlay_cache.get(candle_interval)
            if (
                cache_entry is None
                or cache_entry.get("source_len") != len(points_all)
                or cache_entry.get("sr_enabled") != sr_entry_enabled
            ):
                full_candles = build_timeframe_candles(points_all, candle_interval)
                if sr_entry_enabled:
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
                else:
                    zigzag_points = []
                    range_segments = []
                    sr_segments = []

                cache_entry = {
                    "source_len": len(points_all),
                    "candles": full_candles,
                    "zigzag_points": zigzag_points,
                    "range_segments": range_segments,
                    "sr_segments": sr_segments,
                    "sr_enabled": sr_entry_enabled,
                }
                overlay_cache[candle_interval] = cache_entry

            data["zigzag_points"] = cache_entry.get("zigzag_points") or []
            data["range_segments"] = cache_entry.get("range_segments") or []
            data["sr_segments"] = cache_entry.get("sr_segments") or []

            if chart_type == "candle":
                full_candles = cache_entry.get("candles") or []
                view_candles = [
                    c for c in full_candles if view_start_time <= c[0] <= view_end_time
                ]
                candles = view_candles
                data["view_candles"] = candles
                data["view_candle_times"] = [c[0] for c in candles]
                data["view_candle_interval"] = candle_interval

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
        if show_zigzag and chart_type == "candle" and zigzag_points:
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

        def line_time_to_x(ts, as_end=False):
            if mode == "time":
                if span_seconds <= 0:
                    return None
                if ts < view_start_time:
                    ts = view_start_time
                if ts > view_end_time:
                    ts = view_end_time
                return (
                    left
                    + (ts - view_start_time).total_seconds()
                    / span_seconds
                    * plot_width
                )
            if n <= 1:
                return None
            idx = bisect_right(times, ts) - 1 if as_end else bisect_left(times, ts)
            if idx < view_start_idx or idx > view_end_idx:
                return None
            return left + (idx - view_start_idx) / (n - 1) * plot_width

        range_segments = data.get("range_segments") or []
        if show_range_band and range_segments:
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
                draw_start = start_ts if start_ts > view_start_time else view_start_time
                draw_end = end_ts if end_ts < view_end_time else view_end_time
                x1 = line_time_to_x(draw_start, as_end=False)
                x2 = line_time_to_x(draw_end, as_end=True)
                if x1 is None or x2 is None:
                    continue
                if x2 < x1:
                    x1, x2 = x2, x1
                y_high = price_to_y(high)
                y_low = price_to_y(low)
                canvas.create_line(x1, y_high, x2, y_high, fill="#1f77b4", width=1, dash=(2, 2))
                canvas.create_line(x1, y_low, x2, y_low, fill="#1f77b4", width=1, dash=(2, 2))

        sr_segments = data.get("sr_segments") or []
        if show_sr_line and sr_segments:
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
                x1 = line_time_to_x(draw_start, as_end=False)
                x2 = line_time_to_x(draw_end, as_end=True)
                if x1 is None or x2 is None:
                    continue
                if x2 < x1:
                    x1, x2 = x2, x1
                y = price_to_y(price)
                color = "#2ca02c" if kind == "support" else "#d62728"
                canvas.create_line(x1, y, x2, y, fill=color, width=1, dash=(3, 3))

        trades = data.get("trades") or []
        data["trade_markers"] = []
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

                entry_y = price_to_y(entry_price)
                exit_y = price_to_y(exit_price)

                if side == "short":
                    color = "#d62728"
                    entry_dir = "down"
                    exit_dir = "up"
                    chain_color = "#f5b483"
                else:
                    color = "#2ca02c"
                    entry_dir = "up"
                    exit_dir = "down"
                    chain_color = "#b8e9a5"

                chain_signals = trade.get("signal_chain") or []
                if chain_signals and entry_x is not None and entry_y is not None:
                    seen_chain = set()
                    base_entry_idx = trade.get("entry_idx")
                    for chain in chain_signals:
                        chain_idx = chain.get("entry_idx")
                        if chain_idx is None or chain_idx == base_entry_idx:
                            continue
                        if chain_idx in seen_chain:
                            continue
                        seen_chain.add(chain_idx)
                        if not (0 <= chain_idx < len(points_all)):
                            continue
                        chain_time = points_all[chain_idx][0]
                        chain_price = points_all[chain_idx][1]
                        chain_x = time_to_x(chain_time)
                        if chain_x is None:
                            continue
                        chain_y = price_to_y(chain_price)
                        canvas.create_line(
                            chain_x,
                            chain_y,
                            entry_x,
                            entry_y,
                            fill=chain_color,
                            dash=(2, 3),
                        )
                        draw_triangle(chain_x, chain_y, size, entry_dir, chain_color)

                if entry_x is not None and exit_x is not None:
                    canvas.create_line(
                        entry_x,
                        entry_y,
                        exit_x,
                        exit_y,
                        fill=color,
                        dash=(4, 3),
                    )
                if entry_x is not None:
                    draw_triangle(entry_x, entry_y, size, entry_dir, color)
                    data["trade_markers"].append(
                        {"x": entry_x, "y": entry_y, "kind": "entry", "trade": trade}
                    )
                if exit_x is not None:
                    draw_triangle(exit_x, exit_y, size, exit_dir, color)
                    data["trade_markers"].append(
                        {"x": exit_x, "y": exit_y, "kind": "exit", "trade": trade}
                    )

                extra_entries = trade.get("namping_entries") or []
                if extra_entries:
                    base_entry_idx = trade.get("entry_idx")
                    for entry in extra_entries:
                        entry_idx = entry.get("idx")
                        entry_price = entry.get("price")
                        if entry_idx is None or entry_price is None:
                            continue
                        if base_entry_idx is not None and entry_idx == base_entry_idx:
                            continue
                        if not (0 <= entry_idx < len(points_all)):
                            continue
                        extra_time = points_all[entry_idx][0]
                        extra_x = time_to_x(extra_time)
                        if extra_x is None:
                            continue
                        extra_y = price_to_y(entry_price)
                        if exit_x is not None:
                            canvas.create_line(
                                extra_x,
                                extra_y,
                                exit_x,
                                exit_y,
                                fill=color,
                                dash=(4, 3),
                            )
                        draw_triangle(extra_x, extra_y, size, entry_dir, color)
                        data["trade_markers"].append(
                            {"x": extra_x, "y": extra_y, "kind": "entry", "trade": trade}
                        )

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
        self._draw_batch_year_chart()
        self._draw_batch_month_chart()

    def _reset_pnl_view(self):
        if not self.pnl_data:
            self.pnl_view_start = 0
            self.pnl_view_end = 0
            return
        self.pnl_view_start = 0
        self.pnl_view_end = len(self.pnl_data) - 1

    def _get_pnl_view_data(self):
        if not self.pnl_data:
            self.pnl_view_start = 0
            self.pnl_view_end = 0
            return []
        n = len(self.pnl_data)
        if self.pnl_view_start is None or self.pnl_view_end is None:
            self.pnl_view_start = 0
            self.pnl_view_end = n - 1
        self.pnl_view_start = max(0, min(self.pnl_view_start, n - 1))
        self.pnl_view_end = max(self.pnl_view_start, min(self.pnl_view_end, n - 1))
        return self.pnl_data[self.pnl_view_start : self.pnl_view_end + 1]

    def _on_pnl_mouse_wheel(self, event):
        if not self.pnl_data or len(self.pnl_data) < 2:
            return
        if hasattr(event, "delta") and event.delta:
            direction = 1 if event.delta > 0 else -1
        elif hasattr(event, "num"):
            direction = 1 if event.num == 4 else -1
        else:
            return

        total = len(self.pnl_data)
        start = self.pnl_view_start
        end = self.pnl_view_end
        if start is None or end is None or end < start:
            start = 0
            end = total - 1
        start = max(0, min(start, total - 1))
        end = max(start, min(end, total - 1))
        visible = end - start + 1
        if visible <= 2:
            return

        width, _height, left, _top, right, _bottom, plot_width, _plot_height = (
            self._get_plot_area(self.pnl_canvas)
        )
        if plot_width <= 1:
            return
        if left <= event.x <= width - right:
            ratio = (event.x - left) / plot_width
            ratio = min(max(ratio, 0.0), 1.0)
        else:
            ratio = 0.5

        min_visible = 10
        if direction > 0:
            new_visible = max(min_visible, int(visible * 0.8))
        else:
            new_visible = min(total, int(visible * 1.25))
        if new_visible == visible:
            return

        anchor = start + int(ratio * (visible - 1))
        new_start = anchor - int(ratio * (new_visible - 1))
        new_start = max(0, min(new_start, total - new_visible))
        self.pnl_view_start = new_start
        self.pnl_view_end = new_start + new_visible - 1
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

        view_data = self._get_pnl_view_data()
        if not view_data or len(view_data) < 2:
            canvas.create_text(
                canvas.winfo_width() // 2,
                canvas.winfo_height() // 2,
                text="取引がありません。",
                fill="#666666",
            )
            return

        times = [ts for ts, _v in view_data]
        values = [v for _ts, v in view_data]

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
        for ts, value in view_data:
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

    def _draw_bar_chart(self, canvas, data, empty_message):
        if not canvas:
            return
        canvas.delete("all")
        if not data:
            canvas.create_text(
                canvas.winfo_width() // 2,
                canvas.winfo_height() // 2,
                text=empty_message,
                fill="#666666",
            )
            return

        labels = [str(label) for label, _ in data]
        values = [float(value) for _label, value in data]
        min_v = min(values + [0.0])
        max_v = max(values + [0.0])
        if min_v == max_v:
            min_v -= 1.0
            max_v += 1.0

        width, height, left, top, right, bottom, plot_width, plot_height = (
            self._get_plot_area(canvas)
        )
        canvas.create_rectangle(
            left, top, width - right, height - bottom, outline="#888888"
        )

        span = max_v - min_v
        zero_y = height - bottom - (0.0 - min_v) / span * plot_height
        canvas.create_line(left, zero_y, width - right, zero_y, fill="#888888")

        count = len(values)
        if count <= 0:
            return
        bar_space = plot_width / count
        bar_width = bar_space * 0.6
        label_step = max(1, count // 12)

        for idx, (label, value) in enumerate(zip(labels, values)):
            x_center = left + bar_space * (idx + 0.5)
            x0 = x_center - bar_width / 2
            x1 = x_center + bar_width / 2
            y_value = height - bottom - (value - min_v) / span * plot_height
            if value >= 0:
                y0 = y_value
                y1 = zero_y
                color = "#2ca02c"
            else:
                y0 = zero_y
                y1 = y_value
                color = "#d62728"
            canvas.create_rectangle(x0, y0, x1, y1, fill=color, outline=color)

            if idx % label_step == 0:
                canvas.create_text(
                    x_center,
                    height - bottom + 6,
                    text=label,
                    anchor="n",
                    fill="#333333",
                )

        ticks = 4
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

    def _draw_batch_year_chart(self):
        if not hasattr(self, "pnl_year_canvas"):
            return
        data = self.batch_yearly_data or []
        self._draw_bar_chart(self.pnl_year_canvas, data, "一括未実行")

    def _draw_batch_month_chart(self):
        if not hasattr(self, "pnl_month_canvas"):
            return
        data = self.batch_monthly_data or []
        self._draw_bar_chart(self.pnl_month_canvas, data, "一括未実行")


def main():
    root = tk.Tk()
    Step1App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
