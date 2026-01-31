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
except Exception:
    JST = timezone(timedelta(hours=9))

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

        today_jst = datetime.now(JST).date()
        self.start_date = today_jst
        self.end_date = today_jst

        self.start_var = tk.StringVar(value=self.start_date.isoformat())
        self.end_var = tk.StringVar(value=self.end_date.isoformat())
        self.status_var = tk.StringVar(value="準備完了")

        self._build_ui()
        self._poll_queue()

    def _build_ui(self):
        frame = ttk.Frame(self.root, padding=12)
        frame.grid(row=0, column=0, sticky="nsew")
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        row = ttk.Frame(frame)
        row.grid(row=0, column=0, sticky="ew")
        row.columnconfigure(1, weight=1)

        ttk.Label(row, text="開始日（JST）").grid(row=0, column=0, sticky="w")
        start_entry = ttk.Entry(row, textvariable=self.start_var, width=12, state="readonly")
        start_entry.grid(row=0, column=1, padx=6)
        ttk.Button(row, text="選択", command=self._pick_start).grid(row=0, column=2)

        ttk.Label(row, text="終了日（JST）").grid(row=1, column=0, sticky="w", pady=(6, 0))
        end_entry = ttk.Entry(row, textvariable=self.end_var, width=12, state="readonly")
        end_entry.grid(row=1, column=1, padx=6, pady=(6, 0))
        ttk.Button(row, text="選択", command=self._pick_end).grid(row=1, column=2, pady=(6, 0))

        self.run_button = ttk.Button(frame, text="ダウンロード", command=self._start_download)
        self.run_button.grid(row=1, column=0, sticky="w", pady=(10, 6))

        ttk.Label(frame, textvariable=self.status_var).grid(row=2, column=0, sticky="w")

        self.log = tk.Text(frame, height=14, width=80)
        self.log.grid(row=3, column=0, sticky="nsew", pady=(6, 0))
        frame.rowconfigure(3, weight=1)

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

    def _start_download(self):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("お知らせ", "ダウンロード中です。")
            return
        if self.end_date < self.start_date:
            messagebox.showerror("エラー", "終了日は開始日より後にしてください。")
            return
        self.run_button.config(state="disabled")
        self.status_var.set("準備中...")
        self.log.delete("1.0", tk.END)
        self.worker = threading.Thread(
            target=self._download_worker, args=(self.start_date, self.end_date), daemon=True
        )
        self.worker.start()

    def _download_worker(self, start: date, end: date):
        hours = to_utc_hour_range(start, end)
        total = len(hours)
        self.queue.put(("status", f"ダウンロード中...（{total}件）"))

        for idx, dt_utc in enumerate(hours, start=1):
            url = hour_to_url(dt_utc)
            path = hour_to_path(dt_utc)
            path.parent.mkdir(parents=True, exist_ok=True)

            if path.exists():
                if path.stat().st_size == 0:
                    try:
                        path.unlink()
                    except Exception as e:
                        self.queue.put(("log", f"[{idx}/{total}] 削除失敗 {path} {e}"))
                        continue
                else:
                    self.queue.put(("log", f"[{idx}/{total}] スキップ {path}"))
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
                    self.queue.put(("log", f"[{idx}/{total}] 0バイト {url}"))
                    continue
                path.write_bytes(data)
                if path.stat().st_size == 0:
                    path.unlink()
                    self.queue.put(("log", f"[{idx}/{total}] 0バイト {url}"))
                    continue
                self.queue.put(("log", f"[{idx}/{total}] 成功 {path}"))
            except HTTPError as e:
                self.queue.put(("log", f"[{idx}/{total}] HTTP {e.code} {url}"))
            except URLError as e:
                self.queue.put(("log", f"[{idx}/{total}] URLエラー {e.reason}"))
            except Exception as e:
                self.queue.put(("log", f"[{idx}/{total}] エラー {e}"))

        day_groups = group_hours_by_jst_day(hours)
        self.queue.put(("status", f"CSV作成中...（{len(day_groups)}日）"))

        for jst_day, day_hours in day_groups.items():
            csv_path = day_to_csv_path(jst_day)
            if csv_path.exists() and csv_path.stat().st_size > 0:
                self.queue.put(("log", f"[CSV] スキップ {csv_path}"))
                continue

            missing = []
            for dt_utc in day_hours:
                src = hour_to_path(dt_utc)
                if (not src.exists()) or src.stat().st_size == 0:
                    missing.append(src)
            if missing:
                self.queue.put(
                    ("log", f"[CSV] スキップ {jst_day.isoformat()} 不足 {len(missing)}件")
                )
                continue

            csv_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with csv_path.open("w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(
                        ["timestamp_jst", "bid", "ask", "bid_volume", "ask_volume"]
                    )
                    for dt_utc in day_hours:
                        src = hour_to_path(dt_utc)
                        for row in iter_ticks(src, dt_utc):
                            writer.writerow(row)
                self.queue.put(("log", f"[CSV] 成功 {csv_path}"))
            except Exception as e:
                try:
                    if csv_path.exists():
                        csv_path.unlink()
                except Exception:
                    pass
                self.queue.put(("log", f"[CSV] エラー {jst_day.isoformat()} {e}"))

        self.queue.put(("status", "完了"))
        self.queue.put(("done", None))

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
        except queue.Empty:
            pass
        self.root.after(200, self._poll_queue)


def main():
    root = tk.Tk()
    Step1App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
