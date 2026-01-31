import calendar
import csv
import lzma
import queue
import struct
import threading
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
        notebook.add(chart_tab, text="チャート")
        notebook.add(download_tab, text="ダウンロード")

        status_bar = ttk.Frame(self.root)
        status_bar.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 8))
        status_bar.columnconfigure(0, weight=1)
        ttk.Label(status_bar, textvariable=self.status_var).grid(row=0, column=0, sticky="w")

        chart_tab.columnconfigure(0, weight=1)
        chart_tab.rowconfigure(4, weight=1)

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

        self.chart_button = ttk.Button(chart_tab, text="表示", command=self._show_chart)
        self.chart_button.grid(row=2, column=0, sticky="w", pady=(8, 6))

        ttk.Label(chart_tab, textvariable=self.chart_info_var).grid(
            row=3, column=0, sticky="w"
        )

        self.chart_canvas = tk.Canvas(chart_tab, bg="white")
        self.chart_canvas.grid(row=4, column=0, sticky="nsew", pady=(6, 0))
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
        self.chart_button.config(state="disabled")
        self.status_var.set("表示準備中...")
        self.chart_worker = threading.Thread(
            target=self._chart_worker,
            args=(self.view_start_date, self.view_end_date),
            daemon=True,
        )
        self.chart_worker.start()

    def _chart_worker(self, start: date, end: date):
        points, missing = load_ticks_from_csv(start, end)
        if missing:
            self.queue.put(("log", f"[表示] CSV不足 {len(missing)}件"))
        if not points:
            self.queue.put(("chart_error", "表示できるデータがありません。"))
            self.queue.put(("chart_done", None))
            return
        payload = {
            "start": start,
            "end": end,
            "points": points,
            "missing_count": len(missing),
        }
        self.queue.put(("chart_data", payload))
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
                elif kind == "chart_data":
                    self._render_chart(payload)
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
        self.chart_data = {
            "all_points": points,
            "view_start": 0,
            "view_end": len(points) - 1,
            "count": len(points),
            "start": start,
            "end": end,
            "missing": missing_count,
        }
        self._draw_chart()

    def _get_plot_area(self):
        canvas = self.chart_canvas
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

        anchor = view_start + int(ratio * (visible - 1))
        new_start = anchor - int(ratio * (new_visible - 1))
        new_start = max(0, min(new_start, total - new_visible))
        data["view_start"] = new_start
        data["view_end"] = new_start + new_visible - 1
        self._draw_chart()

    def _on_drag_start(self, event):
        if not self.chart_data:
            return
        self.drag_start_x = event.x
        self.drag_start_view = (
            self.chart_data["view_start"],
            self.chart_data["view_end"],
        )

    def _on_drag_move(self, event):
        if not self.chart_data or self.drag_start_x is None or not self.drag_start_view:
            return
        width, _height, left, _top, right, _bottom, plot_width, _plot_height = (
            self._get_plot_area()
        )
        if plot_width <= 1:
            return
        view_start, view_end = self.drag_start_view
        visible = view_end - view_start + 1
        dx = event.x - self.drag_start_x
        shift = int(-dx / plot_width * visible)
        total = len(self.chart_data["all_points"])
        new_start = max(0, min(view_start + shift, total - visible))
        self.chart_data["view_start"] = new_start
        self.chart_data["view_end"] = new_start + visible - 1
        self._draw_chart()

    def _on_canvas_resize(self, _event):
        if self.chart_data:
            self._draw_chart()

    def _draw_chart(self):
        data = self.chart_data
        if not data:
            return

        points_all = data["all_points"]
        view_start = data["view_start"]
        view_end = data["view_end"]
        start = data["start"]
        end = data["end"]
        missing_count = data["missing"]

        view_points = points_all[view_start : view_end + 1]
        if not view_points:
            self.chart_info_var.set("表示できるデータがありません。")
            return

        prices_full = [p for _, p in view_points]
        min_p = min(prices_full)
        max_p = max(prices_full)
        if min_p == max_p:
            min_p -= 0.01
            max_p += 0.01

        info_text = (
            f"表示期間: {start.isoformat()}〜{end.isoformat()}  "
            f"件数: {data['count']}  "
            f"表示中: {len(view_points)}  "
            f"最小: {min_p:.3f}  最大: {max_p:.3f}"
        )
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

        n = len(view_points)
        if n < 2:
            return

        sampled = downsample_points(view_points, 5000)
        coords = []
        for idx, (_ts, price) in sampled:
            x = left + idx / (n - 1) * plot_width
            y = height - bottom - (price - min_p) / (max_p - min_p) * plot_height
            coords.extend([x, y])

        if coords:
            canvas.create_line(coords, fill="#1f77b4", width=1)

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
            idx = int((n - 1) * i / time_ticks)
            ts = view_points[idx][0]
            label = ts.strftime("%m/%d %H:%M")
            x = left + idx / (n - 1) * plot_width
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
