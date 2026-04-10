#!/usr/bin/env python3
from __future__ import annotations

import queue
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any

import media_cleaner


class MediaCleanerApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Media Cleaner")
        self.root.geometry("980x680")
        self.root.minsize(860, 560)
        self.root.configure(bg="#0b1220")

        self.log_queue: queue.Queue[Any] = queue.Queue()
        self.worker_thread: threading.Thread | None = None
        self.running = False

        self._configure_style()
        self._build_ui()
        self._poll_queue()

    def _configure_style(self) -> None:
        style = ttk.Style()
        style.theme_use("clam")

        style.configure("Card.TFrame", background="#111827")
        style.configure("Header.TLabel", background="#0b1220", foreground="#f9fafb", font=("Segoe UI Semibold", 20))
        style.configure("Muted.TLabel", background="#111827", foreground="#9ca3af", font=("Segoe UI", 10))
        style.configure("Title.TLabel", background="#111827", foreground="#f9fafb", font=("Segoe UI Semibold", 12))
        style.configure("Value.TLabel", background="#111827", foreground="#e5e7eb", font=("Segoe UI", 11))
        style.configure("TEntry", fieldbackground="#0f172a", foreground="#e5e7eb")

        style.configure(
            "Accent.TButton",
            background="#2563eb",
            foreground="#ffffff",
            font=("Segoe UI Semibold", 10),
            borderwidth=0,
            focusthickness=0,
            padding=(14, 8),
        )
        style.map("Accent.TButton", background=[("active", "#1d4ed8"), ("disabled", "#334155")])

        style.configure(
            "Ghost.TButton",
            background="#1f2937",
            foreground="#e5e7eb",
            font=("Segoe UI", 10),
            borderwidth=0,
            focusthickness=0,
            padding=(14, 8),
        )
        style.map("Ghost.TButton", background=[("active", "#374151"), ("disabled", "#1f2937")])

        style.configure(
            "Horizontal.TProgressbar",
            troughcolor="#0f172a",
            background="#22c55e",
            bordercolor="#0f172a",
            lightcolor="#22c55e",
            darkcolor="#16a34a",
        )

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, style="Card.TFrame", padding=18)
        container.pack(fill="both", expand=True, padx=18, pady=18)

        ttk.Label(container, text="Media Cleaner", style="Header.TLabel").pack(anchor="w", pady=(0, 14))

        path_card = ttk.Frame(container, style="Card.TFrame")
        path_card.pack(fill="x")

        ttk.Label(path_card, text="Directory", style="Title.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(path_card, text="MKVToolNix cleanup for all media to MKV. Optional FFmpeg H.265 NVENC transcode.", style="Muted.TLabel").grid(
            row=1, column=0, columnspan=3, sticky="w", pady=(4, 12)
        )

        self.path_var = tk.StringVar()
        self.path_entry = ttk.Entry(path_card, textvariable=self.path_var, width=96)
        self.path_entry.grid(row=2, column=0, sticky="ew", padx=(0, 10))

        browse_btn = ttk.Button(path_card, text="Browse", style="Ghost.TButton", command=self.browse_directory)
        browse_btn.grid(row=2, column=1, sticky="ew", padx=(0, 8))

        self.start_btn = ttk.Button(path_card, text="Start Processing", style="Accent.TButton", command=self.start_processing)
        self.start_btn.grid(row=2, column=2, sticky="ew")

        self.transcode_h265_var = tk.BooleanVar(value=True)
        transcode_checkbox = ttk.Checkbutton(
            path_card,
            text="Transcode video to H.265 (NVENC)",
            variable=self.transcode_h265_var,
        )
        transcode_checkbox.grid(row=3, column=0, columnspan=3, sticky="w", pady=(10, 0))

        path_card.columnconfigure(0, weight=1)

        status_card = ttk.Frame(container, style="Card.TFrame")
        status_card.pack(fill="x", pady=(14, 10))

        ttk.Label(status_card, text="Status", style="Title.TLabel").grid(row=0, column=0, sticky="w")
        self.status_var = tk.StringVar(value="Idle")
        ttk.Label(status_card, textvariable=self.status_var, style="Value.TLabel").grid(row=0, column=1, sticky="w", padx=(10, 0))

        self.overall_pct_var = tk.StringVar(value="Overall: 0%")
        ttk.Label(status_card, textvariable=self.overall_pct_var, style="Muted.TLabel").grid(
            row=1, column=0, columnspan=2, sticky="w", pady=(8, 4)
        )
        self.overall_progress = ttk.Progressbar(status_card, orient="horizontal", mode="determinate", maximum=100)
        self.overall_progress.grid(row=2, column=0, columnspan=2, sticky="ew")

        self.file_pct_var = tk.StringVar(value="Current File: 0%")
        ttk.Label(status_card, textvariable=self.file_pct_var, style="Muted.TLabel").grid(
            row=3, column=0, columnspan=2, sticky="w", pady=(8, 4)
        )
        self.file_progress = ttk.Progressbar(status_card, orient="horizontal", mode="determinate", maximum=100)
        self.file_progress.grid(row=4, column=0, columnspan=2, sticky="ew")

        self.progress_detail_var = tk.StringVar(value="")
        ttk.Label(status_card, textvariable=self.progress_detail_var, style="Muted.TLabel").grid(
            row=5, column=0, columnspan=2, sticky="w", pady=(8, 0)
        )
        status_card.columnconfigure(1, weight=1)

        log_card = ttk.Frame(container, style="Card.TFrame")
        log_card.pack(fill="both", expand=True, pady=(10, 0))
        ttk.Label(log_card, text="Live Log", style="Title.TLabel").pack(anchor="w")

        text_frame = tk.Frame(log_card, bg="#0f172a", highlightthickness=1, highlightbackground="#1f2937")
        text_frame.pack(fill="both", expand=True, pady=(8, 0))

        self.log_text = tk.Text(
            text_frame,
            wrap="word",
            bg="#0f172a",
            fg="#d1d5db",
            insertbackground="#d1d5db",
            borderwidth=0,
            font=("Consolas", 10),
            padx=10,
            pady=10,
        )
        self.log_text.pack(side="left", fill="both", expand=True)

        scrollbar = ttk.Scrollbar(text_frame, orient="vertical", command=self.log_text.yview)
        scrollbar.pack(side="right", fill="y")
        self.log_text.configure(yscrollcommand=scrollbar.set)

    def browse_directory(self) -> None:
        selected = filedialog.askdirectory(title="Select directory to process")
        if selected:
            self.path_var.set(selected)

    def log(self, message: str) -> None:
        self.log_queue.put(("log", message))

    def progress(self, scope: str, percent: float, message: str) -> None:
        bounded = max(0.0, min(100.0, float(percent)))
        self.log_queue.put(("progress", scope, bounded, message))

    def _poll_queue(self) -> None:
        try:
            while True:
                message = self.log_queue.get_nowait()
                if not message:
                    continue
                kind = message[0]
                if kind == "log":
                    line = str(message[1])
                    self.log_text.insert("end", line + "\n")
                    self.log_text.see("end")
                elif kind == "progress":
                    _, scope, percent, detail = message
                    if scope == "overall":
                        self.overall_progress["value"] = percent
                        self.overall_pct_var.set(f"Overall: {percent:.0f}%")
                    elif scope == "file":
                        self.file_progress["value"] = percent
                        self.file_pct_var.set(f"Current File: {percent:.0f}%")
                    self.progress_detail_var.set(str(detail))
        except queue.Empty:
            pass
        self.root.after(120, self._poll_queue)

    def _set_running(self, running: bool) -> None:
        self.running = running
        if running:
            self.start_btn.state(["disabled"])
            self.status_var.set("Processing...")
            self.overall_progress["value"] = 0
            self.file_progress["value"] = 0
            self.overall_pct_var.set("Overall: 0%")
            self.file_pct_var.set("Current File: 0%")
            self.progress_detail_var.set("Starting...")
        else:
            self.start_btn.state(["!disabled"])
            self.status_var.set("Idle")

    def start_processing(self) -> None:
        if self.running:
            return

        raw = self.path_var.get().strip().strip('"')
        if not raw:
            messagebox.showerror("Missing Directory", "Please select a directory first.")
            return

        root = Path(raw).expanduser().resolve()
        if not root.exists() or not root.is_dir():
            messagebox.showerror("Invalid Directory", f"Invalid directory:\n{root}")
            return

        self.log_text.delete("1.0", "end")
        self.log(f"[INFO] Starting scan in: {root}")
        self._set_running(True)

        self.worker_thread = threading.Thread(target=self._worker_run, args=(root,), daemon=True)
        self.worker_thread.start()

    def _worker_run(self, root: Path) -> None:
        try:
            transcode_h265 = bool(self.transcode_h265_var.get())
            mode = "H.265 transcode enabled" if transcode_h265 else "H.265 transcode disabled (video copy)"
            self.log(f"[INFO] {mode}")
            total, processed, skipped = media_cleaner.process_directory(
                root,
                transcode_h265=transcode_h265,
                log=self.log,
                progress=self.progress,
            )
            self.log(f"[DONE] Finished. Total={total}, Processed={processed}, Skipped/Failed={skipped}")
        except Exception as exc:
            self.log(f"[FATAL] {exc}")
        finally:
            self.root.after(0, lambda: self._set_running(False))


def main() -> int:
    root = tk.Tk()
    app = MediaCleanerApp(root)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
