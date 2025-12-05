import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
import time
import os
import re
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


def extract_station_name(filename):
    match = re.search(r"([A-Z]{4}\d{2}[A-Z])", filename.upper())
    return match.group(1) if match else "UNKNOWN"


def format_size(bytes_val):
    if bytes_val <= 0:
        return "—"
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes_val < 1024:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.1f} TB"


class FTPSiteGUI:
    def __init__(self, manager):
        self.manager = manager
        self.root = tk.Tk()
        self.root.title(
            "DGnet FTP Monitor - GREEK NATIONAL v9.999.9.7 - OFFICIAL FINAL - 10 Nov 2025 03:09 PM EET"
        )
        self.root.geometry("1950x1080")
        self.root.minsize(1700, 950)
        self.days_var = tk.IntVar(value=1)
        self.summary_days_var = tk.IntVar(value=7)
        self.show_issues = tk.BooleanVar(value=True)
        self.filter_site = tk.StringVar(value="All Stations")
        self.summary_filter = tk.StringVar(value="All Stations")
        self.auto_refresh = tk.BooleanVar(value=True)
        self.status_var = tk.StringVar(value="Ready")
        self.scheduler_var = tk.StringVar(value="Scheduler: Stopped")
        self.delay_minutes = tk.IntVar(value=15)
        self.full_log = None
        self.scheduler_running = False
        self.scheduler_thread = None
        self.next_run_time = None
        self.scheduler_lock = threading.Lock()  # Protect scheduler state
        self.missing_text = None
        self.missing_files_data = {}  # Store missing files separately
        self._build_ui()
        self._refresh_sites()

    def _build_ui(self):
        paned = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        paned.pack(fill="both", expand=True)

        left_frame = ttk.Frame(paned)
        paned.add(left_frame, weight=1)

        ttk.Label(
            left_frame,
            text="Greek GNSS Network - Grouped View",
            font=("Arial", 16, "bold"),
        ).pack(pady=15)
        self.tree_sites = ttk.Treeview(left_frame, show="tree", selectmode="extended")
        self.tree_sites.pack(fill="both", expand=True, padx=20, pady=10)
        self.tree_sites.bind("<<TreeviewSelect>>", self._on_tree_select)

        btns = ttk.Frame(left_frame)
        btns.pack(pady=8)
        ttk.Button(btns, text="Add Station", command=self._add_site).pack(
            side=tk.LEFT, padx=6
        )
        ttk.Button(btns, text="Edit", command=self._edit_site).pack(
            side=tk.LEFT, padx=6
        )
        ttk.Button(btns, text="Delete", command=self._delete_site).pack(
            side=tk.LEFT, padx=6
        )
        ttk.Button(
            btns,
            text="Collapse All",
            command=lambda: [
                self.tree_sites.item(i, open=False)
                for i in self.tree_sites.get_children()
            ],
        ).pack(side=tk.LEFT, padx=6)
        ttk.Button(
            btns,
            text="Expand All",
            command=lambda: [
                self.tree_sites.item(i, open=True)
                for i in self.tree_sites.get_children()
            ],
        ).pack(side=tk.LEFT, padx=6)

        right = ttk.Frame(paned)
        paned.add(right, weight=5)

        self.notebook = ttk.Notebook(right)
        self.notebook.pack(fill="both", expand=True, padx=15, pady=10)

        # TAB 1: File Monitor
        tab1 = ttk.Frame(self.notebook)
        self.notebook.add(tab1, text=" File Monitor ")

        ctrl = ttk.LabelFrame(
            tab1, text=" Controls - Greek National Standard v9.999.9.7 "
        )
        ctrl.pack(fill="x", padx=10, pady=8)

        row1 = ttk.Frame(ctrl)
        row1.pack(fill="x", pady=6)
        ttk.Label(row1, text="Days:").pack(side=tk.LEFT, padx=5)
        ttk.Spinbox(
            row1,
            from_=1,
            to=30,
            textvariable=self.days_var,
            width=5,
            command=self._refresh_table,
        ).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(
            row1,
            text="Issues only",
            variable=self.show_issues,
            command=self._filter_only,
        ).pack(side=tk.LEFT, padx=20)
        ttk.Label(row1, text="Station:").pack(side=tk.LEFT, padx=10)
        self.combo = ttk.Combobox(
            row1, textvariable=self.filter_site, state="readonly", width=30
        )
        self.combo.pack(side=tk.LEFT, padx=5)
        self.combo.bind("<<ComboboxSelected>>", lambda e: self._filter_only())
        ttk.Checkbutton(
            row1, text="Auto-Refresh after download", variable=self.auto_refresh
        ).pack(side=tk.LEFT, padx=30)

        row2 = ttk.Frame(ctrl)
        row2.pack(fill="x", pady=8)
        self.scan_btn = ttk.Button(
            row2, text="SCAN", command=self._refresh_table, style="Accent.TButton"
        )
        self.scan_btn.pack(side=tk.LEFT, padx=5)
        self.dl_btn = ttk.Button(
            row2, text="Download Completed Files", command=self._download
        )
        self.dl_btn.pack(side=tk.LEFT, padx=5)
        self.scheduler_btn = ttk.Button(
            row2, text="START SCHEDULER", command=self._toggle_scheduler
        )
        self.scheduler_btn.pack(side=tk.LEFT, padx=10)
        self.led = tk.Canvas(
            row2, width=18, height=18, bg="#f0f0f0", highlightthickness=0
        )
        self.led.pack(side=tk.LEFT, padx=6)
        self.led.create_oval(4, 4, 14, 14, fill="red", tags="dot")
        ttk.Label(row2, text="Minutes after hour:").pack(side=tk.LEFT, padx=8)
        ttk.Spinbox(
            row2, from_=1, to=59, textvariable=self.delay_minutes, width=5
        ).pack(side=tk.LEFT, padx=5)

        ttk.Label(
            ctrl,
            textvariable=self.scheduler_var,
            foreground="#006400",
            font=("Segoe UI", 10, "bold"),
        ).pack(pady=4)
        self.progress = ttk.Progressbar(ctrl, mode="determinate")
        self.progress.pack(fill="x", padx=15, pady=8)

        columns = (
            "Log Name",
            "Station",
            "Date (UTC)",
            "File",
            "Local",
            "Local Size",
            "Remote",
            "Remote Size",
            "Status",
            "Type",
        )
        tree_frame = ttk.Frame(tab1)
        tree_frame.pack(fill="both", expand=True, padx=15, pady=10)
        h_scroll = ttk.Scrollbar(tree_frame, orient="horizontal")
        self.tree = ttk.Treeview(
            tree_frame, columns=columns, show="headings", xscrollcommand=h_scroll.set
        )
        h_scroll.config(command=self.tree.xview)
        h_scroll.pack(side="bottom", fill="x")
        self.tree.pack(fill="both", expand=True)

        widths = [120, 110, 150, 450, 60, 90, 60, 90, 160, 100]
        for c, w in zip(columns, widths):
            self.tree.heading(c, text=c)
            self.tree.column(c, width=w, anchor="center")
        self.tree.column("File", anchor="w")
        self.tree.column("Local Size", anchor="e")
        self.tree.column("Remote Size", anchor="e")

        self.tree.tag_configure("missing_local", background="#ffcccc", foreground="red")
        self.tree.tag_configure(
            "missing_remote", background="#ffffcc", foreground="darkorange"
        )
        self.tree.tag_configure("mismatch", background="#ff9999", foreground="darkred")
        self.tree.tag_configure("scheduled", background="#ccffcc", foreground="green")
        self.tree.tag_configure(
            "current_growing",
            background="#e6f3ff",
            foreground="blue",
            font=("Segoe UI", 9, "bold"),
        )

        # TAB 2: Network Summary
        tab2 = ttk.Frame(self.notebook)
        self.notebook.add(tab2, text=" Network Summary ")

        sum_ctrl = ttk.LabelFrame(tab2, text=" Summary Settings ")
        sum_ctrl.pack(fill="x", padx=15, pady=10)
        ttk.Label(sum_ctrl, text="Last").pack(side=tk.LEFT, padx=10)
        days_spin = ttk.Spinbox(
            sum_ctrl, from_=1, to=365, textvariable=self.summary_days_var, width=6
        )
        days_spin.pack(side=tk.LEFT, padx=5)
        ttk.Label(sum_ctrl, text="days").pack(side=tk.LEFT)
        ttk.Label(sum_ctrl, text="  Show:").pack(side=tk.LEFT, padx=15)
        self.summary_combo = ttk.Combobox(
            sum_ctrl, textvariable=self.summary_filter, state="readonly", width=35
        )
        self.summary_combo.pack(side=tk.LEFT, padx=5)
        self.summary_combo.bind(
            "<<ComboboxSelected>>", lambda e: self._refresh_summary()
        )
        ttk.Button(
            sum_ctrl,
            text="SCAN & REFRESH SUMMARY",
            command=self._refresh_summary_full,
            style="Accent.TButton",
        ).pack(side=tk.LEFT, padx=20)
        ttk.Checkbutton(
            sum_ctrl, text="Auto-Refresh after download", variable=self.auto_refresh
        ).pack(side=tk.LEFT, padx=30)

        sum_paned = ttk.PanedWindow(tab2, orient=tk.HORIZONTAL)
        sum_paned.pack(fill="both", expand=True, padx=15, pady=10)

        tree_frame_sum = ttk.Frame(sum_paned)
        sum_paned.add(tree_frame_sum, weight=1)
        h_scroll_sum = ttk.Scrollbar(tree_frame_sum, orient="horizontal")
        self.summary_tree = ttk.Treeview(
            tree_frame_sum,
            columns=("Group", "Last Download", "Last File", "Missing Count"),
            show="headings",
            xscrollcommand=h_scroll_sum.set,
        )
        h_scroll_sum.config(command=self.summary_tree.xview)
        h_scroll_sum.pack(side="bottom", fill="x")
        self.summary_tree.pack(fill="both", expand=True)

        widths_sum = [520, 180, 220, 110]
        for c, w in zip(self.summary_tree["columns"], widths_sum):
            self.summary_tree.heading(c, text=c)
            self.summary_tree.column(c, width=w, anchor="center")
        self.summary_tree.column("Group", anchor="w")

        missing_frame = ttk.LabelFrame(
            sum_paned, text=" ROLLING MISSING FILES LIST (LIVE & 100% ACCURATE) "
        )
        sum_paned.add(missing_frame, weight=1)
        self.missing_text = scrolledtext.ScrolledText(
            missing_frame,
            width=70,
            height=30,
            font=("Consolas", 10),
            bg="#fff8f8",
            wrap="none",
        )
        h_scroll_text = ttk.Scrollbar(
            missing_frame, orient="horizontal", command=self.missing_text.xview
        )
        self.missing_text.configure(xscrollcommand=h_scroll_text.set)
        self.missing_text.pack(fill="both", expand=True, padx=10, pady=(10, 0))
        h_scroll_text.pack(fill="x", padx=10, pady=(0, 10))

        self.summary_tree.tag_configure("ok", foreground="darkgreen")
        self.summary_tree.tag_configure(
            "missing", foreground="red", font=("Segoe UI", 9, "bold")
        )
        self.summary_tree.bind("<<TreeviewSelect>>", self._show_missing_details)

        status_frame = ttk.Frame(self.root)
        status_frame.pack(fill="x", side="bottom")
        ttk.Label(
            status_frame,
            textvariable=self.status_var,
            relief="sunken",
            anchor="w",
            padding=10,
            font=("Segoe UI", 10),
        ).pack(fill="x")

        style = ttk.Style()
        style.configure(
            "Accent.TButton",
            foreground="white",
            background="#0066cc",
            font=("Segoe UI", 10, "bold"),
        )
        style.configure("Treeview", rowheight=26, font=("Consolas", 10))

    def _show_missing_details(self, event=None):
        sel = self.summary_tree.selection()
        self.missing_text.delete(1.0, tk.END)
        if not sel:
            self.missing_text.insert(
                tk.END, "Click a station above to view missing files..."
            )
            return
        item = self.summary_tree.item(sel[0])
        values = item["values"]
        group = values[0]
        # Get missing files from our separate dictionary
        missing_files = self.missing_files_data.get(sel[0], [])
        if not missing_files:
            self.missing_text.insert(
                tk.END,
                f"PERFECT! NO MISSING FILES\n\n{group}\n\nAll files present and correct!",
            )
            return
        self.missing_text.insert(tk.END, f"MISSING FILES FOR:\n{group}\n\n")
        for f in sorted(missing_files):
            self.missing_text.insert(tk.END, f"{f}\n")
        self.missing_text.insert(tk.END, f"\nTOTAL: {len(missing_files)} files missing")

    def _refresh_summary(self):
        self.notebook.select(1)
        for i in self.summary_tree.get_children():
            self.summary_tree.delete(i)
        self.missing_files_data.clear()  # Clear stored missing files data
        self.missing_text.delete(1.0, tk.END)
        self.missing_text.insert(tk.END, "Building 100% accurate summary...")

        if not self.full_log:
            self.status_var.set("Run SCAN first")
            return

        days = self.summary_days_var.get()
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        groups = {}

        target_log = self.summary_filter.get()
        if target_log == "All Stations":
            target_log = None

        for site_items in self.full_log.log.values():
            for item in site_items:
                site = item["site_obj"]
                if target_log and site.name != target_log:
                    continue
                station = getattr(
                    site, "station_code", extract_station_name(item["file"])
                )
                rate_key = (
                    f"{site.rate} {'[ExtClk]' if site.external_clock else ''}".strip()
                )
                group_key = f"{site.network} | {station} | {site.name} | {rate_key}"
                if group_key not in groups:
                    groups[group_key] = {
                        "last_dt": None,
                        "last_file": "",
                        "missing": [],
                    }

                file_dt = item.get("file_dt")
                if (
                    not file_dt
                    and item["local"] == "yes"
                    and os.path.exists(item["local_path"])
                ):
                    try:
                        mtime = os.path.getmtime(item["local_path"])
                        file_dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
                        item["file_dt"] = file_dt
                    except Exception as e:
                        logger.warning(
                            f"Could not get mtime for {item['local_path']}: {e}"
                        )

                if file_dt and (
                    groups[group_key]["last_dt"] is None
                    or file_dt > groups[group_key]["last_dt"]
                ):
                    groups[group_key]["last_dt"] = file_dt
                    groups[group_key]["last_file"] = item["file"]

                if item["status"] in [
                    "missing locally",
                    "missing remotely",
                    "size mismatch",
                ] and not item.get("is_current_utc", False):
                    if file_dt is None or file_dt >= cutoff:
                        groups[group_key]["missing"].append(item["file"])

        for group, data in sorted(groups.items()):
            last_str = (
                data["last_dt"].strftime("%Y-%m-%d %H:%M UTC")
                if data["last_dt"]
                else "Never"
            )
            missing_files = data["missing"]
            missing_count = len(missing_files)
            tag = "ok" if missing_count == 0 else "missing"
            iid = self.summary_tree.insert(
                "",
                "end",
                values=(group, last_str, data["last_file"] or "—", missing_count),
                tags=(tag,),
            )
            # Store missing files separately using the iid as key
            self.missing_files_data[iid] = missing_files

        total_missing = sum(len(g["missing"]) for g in groups.values())
        filter_text = f" (filtered: {target_log})" if target_log else ""
        self.status_var.set(
            f"Summary: {len(groups)} stations | {total_missing} missing files{filter_text}"
        )

    def _refresh_summary_full(self):
        self.status_var.set("Scanning full Greek network...")

        def task():
            log = self.manager.scan_all(self.summary_days_var.get())
            self.full_log = log

            def finish():
                self._refresh_summary()
                self.status_var.set("Summary updated – v9.999.9.7 100% ACCURATE")

            self.root.after(0, finish)

        threading.Thread(target=task, daemon=True).start()

    def _refresh_after_download(self):
        if self.auto_refresh.get():
            self._refresh_table()
            self._refresh_summary()

    def _scan_and_download(self, auto=False):
        for i in self.tree.get_children():
            self.tree.delete(i)
        self.status_var.set("Scanning Greek network...")
        self.scan_btn.config(state="disabled")

        def task():
            def status_callback(msg):
                self.root.after(0, lambda m=msg: self.status_var.set(m))

            log = self.manager.scan_all(self.days_var.get(), status_callback)
            self.full_log = log

            def finish():
                self.scan_btn.config(state="normal")
                self._filter_only()
                if auto:
                    self.manager.auto_download_completed(log, self.delay_minutes.get())
                self._refresh_summary()
                self._refresh_sites()
                self.status_var.set("Scan complete – v9.999.9.7")

            self.root.after(0, finish)

        threading.Thread(target=task, daemon=True).start()

    def _filter_only(self):
        if not self.full_log:
            return
        for i in self.tree.get_children():
            self.tree.delete(i)
        items = []
        now_utc = datetime.now(timezone.utc)

        for site_items in self.full_log.log.values():
            for item in site_items:
                if (
                    self.show_issues.get()
                    and item["status"] in ["ok", "scheduled"]
                    and not item.get("is_current_utc")
                ):
                    continue
                if (
                    self.filter_site.get() != "All Stations"
                    and item["site"] != self.filter_site.get()
                ):
                    continue

                if "file_dt" not in item:
                    try:
                        if " " in item["date"]:
                            item["file_dt"] = datetime.strptime(
                                item["date"], "%Y-%m-%d %H:%M"
                            ).replace(tzinfo=timezone.utc)
                        else:
                            item["file_dt"] = datetime.strptime(
                                item["date"], "%Y-%m-%d"
                            ).replace(tzinfo=timezone.utc)
                    except Exception as e:
                        logger.warning(
                            f"Could not parse date '{item['date']}' for {item['file']}: {e}"
                        )
                        item["file_dt"] = None

                is_current_utc = (
                    " " in item["date"]
                    and item["date"].split()[0] == now_utc.strftime("%Y-%m-%d")
                    and item["date"].split()[1][:2] == now_utc.strftime("%H")
                )
                if is_current_utc and item["remote"] == "yes":
                    item["is_current_utc"] = True
                    item["status"] = "new"

                items.append(item)

        for item in sorted(
            items,
            key=lambda x: (
                extract_station_name(x["file"]),
                x["site"],
                x["date"],
                x["file"],
            ),
        ):
            tag = (
                "current_growing"
                if item.get("is_current_utc")
                else (
                    "missing_local"
                    if item["status"] == "missing locally"
                    else (
                        "missing_remote"
                        if item["status"] == "missing remotely"
                        else (
                            "mismatch"
                            if item["status"] == "size mismatch"
                            else "scheduled"
                        )
                    )
                )
            )

            log_name = item["site"]
            station_name = getattr(
                item["site_obj"], "station_code", extract_station_name(item["file"])
            )
            local_size_str = (
                format_size(item["local_size"]) if item["local"] == "yes" else "—"
            )
            remote_size_str = (
                format_size(item["remote_size"]) if item["remote"] == "yes" else "—"
            )

            self.tree.insert(
                "",
                "end",
                values=(
                    log_name,
                    station_name,
                    item["date"],
                    item["file"],
                    item["local"],
                    local_size_str,
                    item["remote"],
                    remote_size_str,
                    item["status"],
                    (
                        "CURRENT (growing)"
                        if item.get("is_current_utc")
                        else "Future" if item["future"] else "Past"
                    ),
                ),
                tags=(tag,),
            )

    def _download(self):
        if not self.full_log:
            return
        items = [
            item
            for sl in self.full_log.log.values()
            for item in sl
            if item["status"] in ["missing locally", "size mismatch"]
            and not item.get("is_current_utc")
        ]
        if not items:
            messagebox.showinfo("Done", "No completed files to download")
            return

        self.dl_btn.config(state="disabled")
        self.progress["maximum"] = len(items)
        self.progress["value"] = 0

        def dl():
            def progress_callback(msg):
                def update():
                    self.progress["value"] = self.progress["value"] + 1
                    self.status_var.set(msg)

                self.root.after(0, update)

            self.manager.download_missing(items, progress_callback)

            def finish():
                self.dl_btn.config(state="normal")
                self._refresh_after_download()
                messagebox.showinfo("Success", f"Downloaded {len(items)} files!")

            self.root.after(0, finish)

        threading.Thread(target=dl, daemon=True).start()

    def _toggle_scheduler(self):
        with self.scheduler_lock:
            if not self.scheduler_running:
                self.scheduler_running = True
                self.scheduler_btn.config(text="STOP SCHEDULER")
                self.led.delete("dot")
                self.led.create_oval(
                    4, 4, 14, 14, fill="lime", outline="green", width=3, tags="dot"
                )
                self._schedule_next_run()
                self.scheduler_thread = threading.Thread(
                    target=self._scheduler_loop, daemon=True
                )
                self.scheduler_thread.start()
                self._scan_and_download(auto=True)
            else:
                self.scheduler_running = False
                self.scheduler_btn.config(text="START SCHEDULER")
                self.led.delete("dot")
                self.led.create_oval(4, 4, 14, 14, fill="red", tags="dot")
                self.scheduler_var.set("Scheduler stopped")

    def _schedule_next_run(self):
        now = datetime.now()
        delay = self.delay_minutes.get()
        next_hour = (now + timedelta(hours=1)).replace(
            minute=delay, second=0, microsecond=0
        )
        # Ensure next_run_time is always in the future
        if next_hour <= now:
            next_hour += timedelta(hours=1)
        with self.scheduler_lock:
            self.next_run_time = next_hour
        remaining = int((next_hour - now).total_seconds())
        self.scheduler_var.set(
            f"Next run: {next_hour.strftime('%H:%M')} (in {self._format_countdown(remaining)})"
        )

    def _scheduler_loop(self):
        while True:
            with self.scheduler_lock:
                if not self.scheduler_running:
                    break
                next_run = self.next_run_time

            now = datetime.now()
            if next_run and now >= next_run:
                self.root.after(0, lambda: self._scan_and_download(auto=True))
                self._schedule_next_run()
            else:
                # Only update countdown if not at run time
                if next_run:
                    remaining = int((next_run - now).total_seconds())
                    if remaining > 0:
                        # Capture values in local scope to avoid closure issues
                        next_time_str = next_run.strftime("%H:%M")
                        countdown_str = self._format_countdown(remaining)
                        status_text = f"Next run: {next_time_str} (in {countdown_str})"
                        self.root.after(
                            0, lambda msg=status_text: self.scheduler_var.set(msg)
                        )
            time.sleep(1)

    def _format_countdown(self, seconds):
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        return f"{h:02d}h {m:02d}m {s:02d}s"

    def _on_tree_select(self, event):
        sel = self.tree_sites.selection()
        if sel:
            item = self.tree_sites.item(sel[-1])
            if item["values"]:
                self.selected_log_name = item["values"][0]
                if self.summary_filter.get() == self.selected_log_name:
                    self._refresh_summary()

    def _refresh_sites(self):
        for item in self.tree_sites.get_children():
            self.tree_sites.delete(item)

        networks = {}
        for site in self.manager.sites:
            net = site.network or "Unknown"
            if net not in networks:
                networks[net] = {}
            try:
                for item in self.manager.scanner.scan_site(site, 1):
                    if item["local"] == "yes" or item["remote"] == "yes":
                        station = getattr(
                            site, "station_code", extract_station_name(item["file"])
                        )
                        rate_key = f"{site.rate} {'[ExtClk]' if site.external_clock else ''}".strip()
                        key = f"{station} | {site.name} | {rate_key}"
                        if key not in networks[net]:
                            networks[net][key] = []
                        networks[net][key].append(site)
                        break
            except Exception as e:
                logger.warning(f"Failed to scan site {site.name} for tree refresh: {e}")

        for net, stations in sorted(networks.items()):
            net_id = self.tree_sites.insert(
                "", "end", text=f" {net.upper()}", open=True
            )
            for station_key, sites in sorted(stations.items()):
                parts = station_key.split(" | ")
                station_name = parts[0]
                log_name = parts[1]
                rate = parts[2]
                station_id = self.tree_sites.insert(
                    net_id, "end", text=f"  {station_name}", open=True
                )
                self.tree_sites.insert(
                    station_id,
                    "end",
                    text=f"   {log_name} - {rate}",
                    values=(log_name,),
                )

        station_list = ["All Stations"] + sorted([s.name for s in self.manager.sites])
        self.summary_combo["values"] = station_list
        if self.summary_filter.get() not in station_list:
            self.summary_filter.set("All Stations")
        self.combo["values"] = station_list
        if self.manager.sites:
            self.combo.current(0)

    def _edit_dialog(self, site=None, idx=None):
        win = tk.Toplevel(self.root)
        win.title("Add Station" if not site else "Edit Station")
        win.geometry("720x1100")

        detected_station = ""
        if site:
            try:
                sample = self.manager.scanner.scan_site(site, 1)
                if sample and (
                    sample[0]["local"] == "yes" or sample[0]["remote"] == "yes"
                ):
                    detected_station = extract_station_name(sample[0]["file"])
            except Exception as e:
                logger.warning(f"Failed to detect station for {site.name}: {e}")

        fields = [
            ("network", "Network (e.g. NOA)"),
            (
                "station_code",
                (
                    f"Station Name (4-letter) → Auto: {detected_station}"
                    if detected_station
                    else "Station Name (4-letter)"
                ),
            ),
            ("name", "Log Name (e.g. NOA1)"),
            ("rate", "Rate (1s/30s)"),
            ("format", "Format"),
            ("host", "Host"),
            ("port", "Port (default: 21 for FTP, 22 for SFTP)"),
            ("protocol", "Protocol"),
            ("user", "User"),
            ("password", "Password"),
            ("path", "Path"),
            ("pattern", "Pattern"),
            ("frequency", "Frequency"),
            ("output_dir", "Local Folder"),
        ]
        ents = {}
        ext_clk = tk.BooleanVar(value=site.external_clock if site else False)
        letter = tk.BooleanVar(value=site.use_letter_hour if site else False)
        format_var = tk.StringVar(value=getattr(site, "format", "Topcon"))
        protocol_var = tk.StringVar(
            value=getattr(site, "protocol", "ftp") if site else "ftp"
        )
        frequency_var = tk.StringVar(
            value=getattr(site, "frequency", "hourly") if site else "hourly"
        )

        for i, (key, label) in enumerate(fields):
            if key == "format":
                ttk.Label(win, text=label + ":").grid(
                    row=i, column=0, sticky="w", padx=20, pady=8
                )
                combo = ttk.Combobox(
                    win,
                    textvariable=format_var,
                    values=["Topcon", "Trimble", "South"],
                    state="readonly",
                    width=53,
                )
                combo.grid(row=i, column=1, padx=20, pady=8)
                ents[key] = format_var
            elif key == "protocol":
                ttk.Label(win, text=label + ":").grid(
                    row=i, column=0, sticky="w", padx=20, pady=8
                )
                combo = ttk.Combobox(
                    win,
                    textvariable=protocol_var,
                    values=["ftp", "sftp"],
                    state="readonly",
                    width=53,
                )
                combo.grid(row=i, column=1, padx=20, pady=8)
                ents[key] = protocol_var
            elif key == "frequency":
                ttk.Label(win, text=label + ":").grid(
                    row=i, column=0, sticky="w", padx=20, pady=8
                )
                combo = ttk.Combobox(
                    win,
                    textvariable=frequency_var,
                    values=["hourly", "daily"],
                    state="readonly",
                    width=53,
                )
                combo.grid(row=i, column=1, padx=20, pady=8)
                ents[key] = frequency_var
            else:
                ttk.Label(win, text=label + ":").grid(
                    row=i, column=0, sticky="w", padx=20, pady=8
                )
                e = ttk.Entry(win, width=55)
                if site and key in site.__dict__:
                    e.insert(0, getattr(site, key, ""))
                elif (
                    key == "station_code"
                    and detected_station
                    and not getattr(site, "station_code", "")
                ):
                    e.insert(0, detected_station)
                e.grid(row=i, column=1, padx=20, pady=8)
                ents[key] = e

        ttk.Checkbutton(win, text="External Clock", variable=ext_clk).grid(
            row=len(fields), column=0, columnspan=2, pady=10
        )
        ttk.Checkbutton(win, text="Use letter hour (a-x)", variable=letter).grid(
            row=len(fields) + 1, column=0, columnspan=2, pady=10
        )

        def save():
            data = {
                k: (v.get() if isinstance(v, tk.StringVar) else v.get().strip())
                for k, v in ents.items()
            }
            data["external_clock"] = ext_clk.get()
            data["use_letter_hour"] = letter.get()

            # Validation
            errors = []

            # Required fields
            if not data.get("name"):
                errors.append("Log Name is required")
            if not data.get("host"):
                errors.append("Host is required")
            if not data.get("protocol"):
                errors.append("Protocol is required")

            # Port validation and conversion
            if data.get("port"):
                try:
                    port = int(data["port"])
                    if port < 1 or port > 65535:
                        errors.append("Port must be between 1 and 65535")
                    else:
                        data["port"] = port  # Store the converted integer
                except ValueError:
                    errors.append("Port must be a valid number")

            # Pattern validation (basic check for strftime compatibility)
            if data.get("pattern"):
                try:
                    # Test pattern with current date
                    datetime.now().strftime(data["pattern"])
                except Exception:
                    errors.append("Pattern contains invalid strftime codes")
            else:
                errors.append("Pattern is required")

            if errors:
                messagebox.showerror("Validation Error", "\n".join(errors))
                return

            try:
                if site:
                    self.manager.edit_site(idx, **data)
                else:
                    self.manager.add_site(**data)
                self._refresh_sites()
                win.destroy()
            except Exception as e:
                messagebox.showerror("Error", str(e))

        ttk.Button(win, text="Save Station", command=save).grid(
            row=len(fields) + 2, column=0, columnspan=2, pady=20
        )

    def _add_site(self):
        self._edit_dialog()

    def _edit_site(self):
        sel = self.tree_sites.selection()
        if not sel:
            return
        item = self.tree_sites.item(sel[-1])
        if item["values"]:
            log_name = item["values"][0]
            for idx, site in enumerate(self.manager.sites):
                if site.name == log_name:
                    self._edit_dialog(site, idx)
                    break

    def _delete_site(self):
        sel = self.tree_sites.selection()
        if not sel:
            return
        item = self.tree_sites.item(sel[-1])
        if not item["values"]:
            return
        log_name = item["values"][0]
        if messagebox.askyesno("Delete", f"Delete station {log_name} permanently?"):
            for idx, site in enumerate(self.manager.sites):
                if site.name == log_name:
                    self.manager.delete_site(idx)
                    self._refresh_sites()
                    self._refresh_table()
                    break

    def _refresh_table(self):
        self.notebook.select(0)
        self._scan_and_download(auto=False)

    def run(self):
        self.root.mainloop()
