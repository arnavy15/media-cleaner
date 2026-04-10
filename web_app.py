#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import queue
import shutil
import subprocess
import tempfile
import threading
import traceback
import urllib.parse
import urllib.request
import uuid
import zipfile
import hashlib
import re
import errno
import mimetypes
from pathlib import Path
from typing import Callable

from flask import Flask, Response, jsonify, render_template_string, request, stream_with_context

MEDIA_EXTENSIONS = {
    ".mkv",
    ".mp4",
    ".mov",
    ".avi",
    ".m4v",
    ".wmv",
    ".flv",
    ".webm",
    ".mpg",
    ".mpeg",
    ".ts",
    ".m2ts",
}

ENGLISH_LANGUAGE_TAGS = {"eng", "en", "english", "en-us", "en-gb", "enus", "eng-us", "eng-gb"}

DEFAULT_DOWNLOAD_DIR = "/download"
DEFAULT_MEDIA_DIR = "/media"
MOVIES_OUTPUT_DIRNAME = "Movies"
TV_OUTPUT_DIRNAME = "TV Shows"
DEFAULT_OVERWRITE = False


def load_app_version() -> str:
    version_path = Path(__file__).with_name("VERSION")
    try:
        raw = version_path.read_text(encoding="utf-8").strip()
        return raw or "0.0.0"
    except OSError:
        return "0.0.0"


APP_VERSION = load_app_version()

app = Flask(__name__)

JOBS: dict[str, dict] = {}
JOBS_LOCK = threading.Lock()

PAGE_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Media Cleaner</title>
  <style>
    :root {
      --bg: #0a1020;
      --bg-soft: #121b34;
      --card: rgba(15, 23, 42, 0.72);
      --card-strong: rgba(15, 23, 42, 0.92);
      --text: #e2e8f0;
      --muted: #9fb0ca;
      --accent: #38bdf8;
      --accent-2: #22c55e;
      --border: rgba(148, 163, 184, 0.22);
      --input-bg: rgba(15, 23, 42, 0.66);
      --shadow: 0 30px 80px rgba(2, 8, 23, 0.45);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", "Inter", "SF Pro Text", Tahoma, sans-serif;
      color: var(--text);
      background:
        radial-gradient(1200px 600px at -10% -20%, #1d4ed8 0%, transparent 58%),
        radial-gradient(900px 640px at 110% -10%, #0ea5e9 0%, transparent 55%),
        radial-gradient(700px 520px at 50% 120%, #16a34a 0%, transparent 50%),
        linear-gradient(180deg, var(--bg-soft), var(--bg));
      min-height: 100vh;
    }
    .wrap { max-width: 1080px; margin: 30px auto; padding: 0 18px; }
    .hero {
      margin-bottom: 14px;
      padding: 14px 18px;
      border: 1px solid var(--border);
      border-radius: 14px;
      background: linear-gradient(145deg, rgba(56, 189, 248, 0.12), rgba(34, 197, 94, 0.09));
      backdrop-filter: blur(8px);
      box-shadow: var(--shadow);
    }
    .hero h1 { margin: 0 0 6px; font-size: 1.42rem; letter-spacing: 0.2px; }
    .hero p { margin: 0; color: var(--muted); }
    .chip {
      display: inline-block;
      margin-top: 8px;
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 0.78rem;
      font-weight: 700;
      color: #022c22;
      background: linear-gradient(135deg, var(--accent), var(--accent-2));
    }
    .card {
      background: linear-gradient(180deg, var(--card), var(--card-strong));
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 20px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }
    h2 { margin: 0 0 12px; font-size: 1.05rem; color: #cbd5e1; }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 14px;
    }
    .full { grid-column: 1 / -1; }
    label {
      display: block;
      margin-bottom: 6px;
      color: var(--muted);
      font-size: 0.86rem;
      font-weight: 600;
      letter-spacing: 0.25px;
      text-transform: uppercase;
    }
    input, select {
      width: 100%;
      background: var(--input-bg);
      border: 1px solid var(--border);
      border-radius: 12px;
      color: var(--text);
      padding: 11px 13px;
      outline: none;
      transition: border-color .15s ease, box-shadow .15s ease, transform .12s ease;
    }
    input:focus, select:focus {
      border-color: var(--accent);
      box-shadow: 0 0 0 4px rgba(56, 189, 248, 0.18);
      transform: translateY(-1px);
    }
    .check {
      display: flex;
      gap: 8px;
      align-items: center;
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid var(--border);
      background: rgba(2, 6, 23, 0.35);
    }
    .check input { width: auto; }
    .check label {
      margin: 0;
      text-transform: none;
      letter-spacing: 0;
      font-weight: 500;
      color: #bfd0ea;
      font-size: 0.92rem;
    }
    .actions {
      margin-top: 4px;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    button {
      border: 0;
      border-radius: 12px;
      padding: 12px 18px;
      cursor: pointer;
      background: linear-gradient(135deg, var(--accent), #0ea5e9);
      color: #082f49;
      font-weight: 700;
      letter-spacing: 0.2px;
      transition: transform .14s ease, box-shadow .14s ease, opacity .14s ease;
      box-shadow: 0 10px 26px rgba(14, 165, 233, 0.32);
    }
    button:hover { transform: translateY(-1px); }
    button[disabled] { opacity: 0.6; cursor: not-allowed; }
    .progress-panel {
      margin-top: 14px;
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 13px;
      background: rgba(2, 6, 23, 0.65);
    }
    .progress-row { margin-bottom: 12px; }
    .progress-row:last-child { margin-bottom: 0; }
    .progress-head {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      margin-bottom: 7px;
      font-size: 0.88rem;
      color: #cbd5e1;
    }
    .progress-meta { color: #93c5fd; font-weight: 600; font-size: 0.8rem; }
    .bar {
      width: 100%;
      height: 12px;
      border-radius: 999px;
      background: rgba(148, 163, 184, 0.2);
      overflow: hidden;
      border: 1px solid rgba(148, 163, 184, 0.25);
    }
    .bar-fill {
      height: 100%;
      width: 0%;
      background: linear-gradient(90deg, #38bdf8, #22c55e);
      transition: width 0.2s ease;
    }
    .status-line {
      margin-top: 8px;
      font-size: 0.86rem;
      color: #9fb0ca;
    }
    .subnote {
      color: var(--muted);
      font-size: 0.86rem;
      margin-top: 6px;
    }
    @media (max-width: 760px) {
      .wrap { margin: 18px auto; }
      .card { padding: 15px; }
      .hero { padding: 12px 14px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>Media Cleaner Control Panel</h1>
      <p>Download, clean, and route files with live progress tracking.</p>
      <span class="chip">v{{ app_version }} | MKVToolNix Only</span>
    </div>
    <div class="card">
      <h2>Pipeline Settings</h2>
      <form id="pipeline-form">
        <div class="grid">
          <div>
            <label for="download_type">Download Type</label>
            <select id="download_type" name="download_type">
              <option value="movie">Movie</option>
              <option value="tv">TV Show</option>
            </select>
          </div>
          <div>
            <label for="overwrite">Output behavior</label>
            <select id="overwrite" name="overwrite">
              <option value="false" {% if not overwrite %}selected{% endif %}>Skip if output exists</option>
              <option value="true" {% if overwrite %}selected{% endif %}>Overwrite output files</option>
            </select>
          </div>
          <div class="full">
            <label for="download_url">Download URL (optional)</label>
            <input id="download_url" name="download_url" value="" placeholder="https://..." />
          </div>
          <div class="full" id="movie-name-wrap">
            <label for="movie_final_name">Movie Final File Name (inside /media/Movies)</label>
            <input id="movie_final_name" name="movie_final_name" value="" placeholder="Movie Title (2026)" />
            <div class="subnote">No extension needed. Original extension is reused.</div>
          </div>
          <div class="full" id="tv-dest-wrap" style="display:none;">
            <label for="tv_download_subfolder">TV Download Destination (inside /media/TV Shows)</label>
            <input id="tv_download_subfolder" name="tv_download_subfolder" value="" placeholder="Show Name/Season 01" />
          </div>
          <div class="full check">
            <input type="checkbox" id="delete_after_clean" name="delete_after_clean" />
            <label for="delete_after_clean">Delete input file after successful clean</label>
          </div>
        </div>
        <div class="actions">
          <button id="run-btn" type="submit">Run Pipeline</button>
        </div>
        <div class="subnote">Volumes: <code>/download</code> for input and <code>/media</code> for cleaned output.</div>
      </form>
      <div class="progress-panel">
        <div class="progress-row">
          <div class="progress-head">
            <span>Overall Progress</span>
            <span id="overall-meta" class="progress-meta">0%</span>
          </div>
          <div class="bar"><div id="overall-bar" class="bar-fill"></div></div>
        </div>
        <div id="download-row" class="progress-row" style="display:none;">
          <div class="progress-head">
            <span id="download-label">Download Progress</span>
            <span id="download-meta" class="progress-meta">0%</span>
          </div>
          <div class="bar"><div id="download-bar" class="bar-fill"></div></div>
        </div>
        <div id="status-line" class="status-line">Ready.</div>
      </div>
    </div>
  </div>
  <script>
    const form = document.getElementById("pipeline-form");
    const runBtn = document.getElementById("run-btn");
    const downloadType = document.getElementById("download_type");
    const movieNameWrap = document.getElementById("movie-name-wrap");
    const tvDestWrap = document.getElementById("tv-dest-wrap");
    const overallBar = document.getElementById("overall-bar");
    const overallMeta = document.getElementById("overall-meta");
    const downloadRow = document.getElementById("download-row");
    const downloadBar = document.getElementById("download-bar");
    const downloadMeta = document.getElementById("download-meta");
    const downloadLabel = document.getElementById("download-label");
    const statusLine = document.getElementById("status-line");
    let stream = null;

    function setOverall(percent, text) {
      const clamped = Math.max(0, Math.min(100, Number(percent || 0)));
      overallBar.style.width = clamped + "%";
      overallMeta.textContent = clamped.toFixed(0) + "%";
      if (text) statusLine.textContent = text;
    }

    function setDownload(payload) {
      downloadRow.style.display = "block";
      const clamped = Math.max(0, Math.min(100, Number(payload.percent || 0)));
      downloadBar.style.width = clamped + "%";
      downloadMeta.textContent = clamped.toFixed(0) + "%";
      if (payload.filename) downloadLabel.textContent = "Downloading: " + payload.filename;
      if (payload.status) statusLine.textContent = payload.status;
    }

    function hideDownload() {
      downloadRow.style.display = "none";
    }

    function refreshDownloadFields() {
      if (downloadType.value === "tv") {
        movieNameWrap.style.display = "none";
        tvDestWrap.style.display = "block";
      } else {
        movieNameWrap.style.display = "block";
        tvDestWrap.style.display = "none";
      }
    }
    downloadType.addEventListener("change", refreshDownloadFields);
    refreshDownloadFields();

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (stream) {
        stream.close();
        stream = null;
      }
      setOverall(0, "Starting...");
      hideDownload();
      runBtn.disabled = true;
      runBtn.textContent = "Running...";

      try {
        const response = await fetch("/start", {
          method: "POST",
          body: new FormData(form),
        });
        if (!response.ok) {
          statusLine.textContent = "Failed to start job.";
          runBtn.disabled = false;
          runBtn.textContent = "Run Pipeline";
          return;
        }

        const payload = await response.json();
        if (!payload.job_id) {
          statusLine.textContent = "Missing job id.";
          runBtn.disabled = false;
          runBtn.textContent = "Run Pipeline";
          return;
        }

        stream = new EventSource(`/stream/${payload.job_id}`);
        stream.addEventListener("status", (evt) => {
          const data = JSON.parse(evt.data || "{}");
          if (data.message) statusLine.textContent = data.message;
        });
        stream.addEventListener("overall_progress", (evt) => {
          const data = JSON.parse(evt.data || "{}");
          setOverall(data.percent || 0, data.status || "");
        });
        stream.addEventListener("download_progress", (evt) => {
          const data = JSON.parse(evt.data || "{}");
          setDownload(data);
        });
        stream.addEventListener("done", () => {
          setOverall(100, "Pipeline complete.");
          hideDownload();
          runBtn.disabled = false;
          runBtn.textContent = "Run Pipeline";
          if (stream) {
            stream.close();
            stream = null;
          }
        });
        stream.onerror = () => {
          statusLine.textContent = "Stream disconnected.";
          runBtn.disabled = false;
          runBtn.textContent = "Run Pipeline";
          if (stream) {
            stream.close();
            stream = null;
          }
        };
      } catch (err) {
        statusLine.textContent = String(err);
        runBtn.disabled = false;
        runBtn.textContent = "Run Pipeline";
      }
    });
  </script>
</body>
</html>
"""


def run_command(cmd: list[str]) -> tuple[int, str, str]:
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return proc.returncode, proc.stdout, proc.stderr


def resolve_tools() -> tuple[str, str]:
    mkvmerge_bin = shutil.which("mkvmerge")
    mkvpropedit_bin = shutil.which("mkvpropedit")
    if not mkvmerge_bin or not mkvpropedit_bin:
        raise RuntimeError("mkvtoolnix not found in PATH. Expected mkvmerge and mkvpropedit.")
    return mkvmerge_bin, mkvpropedit_bin


def probe_tracks(file_path: Path, mkvmerge_bin: str) -> dict:
    code, out, err = run_command([mkvmerge_bin, "-J", str(file_path)])
    if code != 0:
        raise RuntimeError(f"mkvmerge -J failed: {err.strip() or out.strip()}")
    return json.loads(out)


def is_english_track(track: dict) -> bool:
    props = track.get("properties") or {}
    language = str(props.get("language", "")).strip().lower()
    language_ietf = str(props.get("language_ietf", "")).strip().lower()
    name = str(props.get("track_name", "")).strip().lower()
    return language in ENGLISH_LANGUAGE_TAGS or language_ietf in ENGLISH_LANGUAGE_TAGS or "english" in name


def analyze_tracks(info: dict) -> tuple[list[int], list[int], list[int]]:
    videos: list[int] = []
    audios: list[int] = []
    english_audios: list[int] = []
    for track in info.get("tracks", []) or []:
        track_id = track.get("id")
        track_type = track.get("type")
        if not isinstance(track_id, int):
            continue
        if track_type == "video":
            videos.append(track_id)
        elif track_type == "audio":
            audios.append(track_id)
            if is_english_track(track):
                english_audios.append(track_id)
    return sorted(videos), sorted(audios), sorted(english_audios)


def pick_audio_track(audios: list[int], english_audios: list[int]) -> int | None:
    if english_audios:
        return english_audios[0]
    if len(audios) == 1:
        return audios[0]
    return None


def join_ids(ids: list[int]) -> str:
    return ",".join(str(x) for x in ids)


def clean_file_to_output(
    source_file: Path,
    source_root: Path,
    output_root: Path,
    mkvmerge_bin: str,
    mkvpropedit_bin: str,
    overwrite: bool,
    delete_after_clean: bool,
    output_override_file: Path | None,
    log: Callable[[str], None],
) -> bool:
    info = probe_tracks(source_file, mkvmerge_bin)
    videos, audios, english_audios = analyze_tracks(info)
    if not videos:
        log(f"[SKIP] No video track: {source_file}")
        return False

    selected_audio = pick_audio_track(audios, english_audios)
    if selected_audio is None:
        log(f"[SKIP] No usable audio track (need English or single audio): {source_file}")
        return False

    if output_override_file is not None:
        dest_file = output_override_file
    else:
        rel_path = source_file.relative_to(source_root)
        dest_file = (output_root / rel_path).with_suffix(".mkv")
    dest_file.parent.mkdir(parents=True, exist_ok=True)

    if dest_file.exists() and not overwrite:
        log(f"[SKIP] Destination exists: {dest_file}")
        return False

    tmp_fd, tmp_name = tempfile.mkstemp(prefix=f"{source_file.stem}.", suffix=".tmp.mkv", dir=str(dest_file.parent))
    os.close(tmp_fd)
    tmp_output = Path(tmp_name)

    cmd = [
        mkvmerge_bin,
        "--output",
        str(tmp_output),
        "--title",
        "",
        "--video-tracks",
        join_ids(videos),
        "--no-subtitles",
        "--no-global-tags",
        "--no-track-tags",
        "--no-chapters",
        "--audio-tracks",
        str(selected_audio),
        "--language",
        f"{selected_audio}:eng",
        str(source_file),
    ]
    code, out, err = run_command(cmd)
    if code != 0:
        log(f"[ERROR] mkvmerge failed for {source_file}: {err.strip() or out.strip()}")
        try:
            tmp_output.unlink(missing_ok=True)
        except OSError:
            pass
        return False

    propedit_cmd = [mkvpropedit_bin, str(tmp_output), "--edit", "info", "--set", "title=", "--tags", "all:"]
    for i in range(1, len(videos) + 1):
        propedit_cmd.extend(["--edit", f"track:v{i}", "--set", "name="])
    propedit_cmd.extend(["--edit", "track:a1", "--set", "name="])
    run_command(propedit_cmd)

    try:
        if dest_file.exists() and overwrite:
            dest_file.unlink()
        os.replace(tmp_output, dest_file)
        log(f"[OK] Cleaned: {source_file} -> {dest_file}")
        if delete_after_clean:
            source_file.unlink(missing_ok=True)
            log(f"[OK] Deleted input: {source_file}")
        return True
    except OSError as exc:
        log(f"[ERROR] Finalize failed for {source_file}: {exc}")
        try:
            tmp_output.unlink(missing_ok=True)
        except OSError:
            pass
        return False


def safe_extract_zip(zip_path: Path, target_dir: Path) -> None:
    target_dir = target_dir.resolve()
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            target_path = (target_dir / member.filename).resolve()
            if not str(target_path).startswith(str(target_dir)):
                raise RuntimeError(f"Unsafe ZIP entry path: {member.filename}")
        zf.extractall(target_dir)


def extract_tv_zips(input_dir: Path, log: Callable[[str], None]) -> list[tuple[Path, Path]]:
    extracted_roots: list[tuple[Path, Path]] = []
    zip_files = sorted(p for p in input_dir.rglob("*") if p.is_file() and is_zip_payload(p))
    if not zip_files:
        log("[ZIP] No season ZIP files found.")
        return extracted_roots

    for zip_path in zip_files:
        extract_root = zip_path.parent / f"{zip_path.stem}_extracted"
        suffix = 1
        while extract_root.exists():
            extract_root = zip_path.parent / f"{zip_path.stem}_extracted_{suffix}"
            suffix += 1
        extract_root.mkdir(parents=True, exist_ok=True)
        safe_extract_zip(zip_path, extract_root)
        extracted_roots.append((zip_path.resolve(), extract_root))
        log(f"[ZIP] Extracted {zip_path} -> {extract_root}")
    return extracted_roots


def find_media_files(root: Path, skip_dirs: set[Path] | None = None) -> list[Path]:
    files: list[Path] = []
    skip_dirs = {p.resolve() for p in (skip_dirs or set())}
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower() not in MEDIA_EXTENSIONS:
            continue
        if any(parent in skip_dirs for parent in p.resolve().parents):
            continue
        files.append(p)
    return files


def is_zip_payload(file_path: Path) -> bool:
    if file_path.suffix.lower() == ".zip":
        return True
    try:
        return zipfile.is_zipfile(file_path)
    except OSError:
        return False


def is_media_payload(file_path: Path, mkvmerge_bin: str) -> bool:
    if file_path.suffix.lower() in MEDIA_EXTENSIONS:
        return True
    try:
        info = probe_tracks(file_path, mkvmerge_bin)
    except Exception:
        return False
    videos, _, _ = analyze_tracks(info)
    return bool(videos)


def safe_download_filename(download_url: str) -> str:
    parsed = urllib.parse.urlparse(download_url)
    raw_path_name = Path(parsed.path).name
    inferred_suffix = Path(raw_path_name).suffix
    candidate = raw_path_name or "downloaded_media"
    candidate = re.sub(r"[^A-Za-z0-9._\- ()\[\]]+", "_", candidate).strip(" .")
    if not candidate:
        candidate = "downloaded_media"

    suffix = Path(candidate).suffix.lower()
    if len(candidate) > 120:
        url_hash = hashlib.sha1(download_url.encode("utf-8")).hexdigest()[:12]
        ext = suffix if suffix and len(suffix) <= 10 else (inferred_suffix if len(inferred_suffix) <= 10 else "")
        candidate = f"download_{url_hash}{ext}"
    return candidate


def sanitize_download_name(name: str) -> str:
    candidate = Path(name).name
    candidate = re.sub(r"[^A-Za-z0-9._\\- ()\\[\\]]+", "_", candidate).strip(" .")
    if not candidate:
        return "downloaded_media"
    if len(candidate) > 120:
        stem = Path(candidate).stem[:100].rstrip(" .") or "downloaded_media"
        suffix = Path(candidate).suffix[:10]
        return f"{stem}{suffix}"
    return candidate


def infer_extension_from_headers(response) -> str:
    content_disposition = response.headers.get("Content-Disposition", "")
    disposition_match = re.search(r'filename\\*?=(?:UTF-8\'\')?"?([^\";]+)"?', content_disposition, re.IGNORECASE)
    if disposition_match:
        header_name = urllib.parse.unquote(disposition_match.group(1).strip())
        header_suffix = Path(header_name).suffix.lower()
        if header_suffix:
            return header_suffix

    content_type = response.headers.get_content_type()
    extension = mimetypes.guess_extension(content_type or "")
    if extension == ".ksh":
        return ".mkv"
    if extension:
        return extension

    return ""


def maybe_relabel_downloaded_file(file_path: Path, mkvmerge_bin: str, status: Callable[[str], None]) -> Path:
    current_suffix = file_path.suffix.lower()
    if current_suffix and current_suffix not in {".bin", ".file"}:
        return file_path

    target_suffix = ""
    if is_zip_payload(file_path):
        target_suffix = ".zip"
    elif is_media_payload(file_path, mkvmerge_bin):
        target_suffix = ".mkv"

    if not target_suffix:
        return file_path

    renamed_path = file_path.with_suffix(target_suffix)
    if renamed_path == file_path:
        return file_path

    counter = 1
    while renamed_path.exists():
        renamed_path = file_path.with_name(f"{file_path.stem}_{counter}{target_suffix}")
        counter += 1

    file_path.rename(renamed_path)
    status(f"Detected payload type, renamed download to: {renamed_path.name}")
    return renamed_path


def normalized_final_name(name: str, fallback_stem: str, fallback_suffix: str) -> str:
    clean_name = Path(name.strip()).name
    if not clean_name or clean_name in {".", ".."}:
        clean_name = fallback_stem
    if not Path(clean_name).suffix:
        clean_name = f"{clean_name}{fallback_suffix}"
    return clean_name


def format_bytes(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(num_bytes)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{num_bytes} B"


def download_to_input(
    download_url: str,
    input_dir: Path,
    mkvmerge_bin: str,
    status: Callable[[str], None],
    download_progress: Callable[[dict], None],
) -> Path:
    initial_name = safe_download_filename(download_url)
    destination = input_dir / initial_name
    destination.parent.mkdir(parents=True, exist_ok=True)

    status(f"Starting download: {initial_name}")
    try:
        with urllib.request.urlopen(download_url, timeout=120) as response:
            if not Path(initial_name).suffix:
                inferred_suffix = infer_extension_from_headers(response)
                if inferred_suffix:
                    destination = input_dir / sanitize_download_name(f"{Path(initial_name).name}{inferred_suffix}")
            with destination.open("wb") as out_file:
                total_raw = response.headers.get("Content-Length")
                total = int(total_raw) if total_raw and total_raw.isdigit() else 0
                downloaded = 0
                download_progress({
                    "percent": 0,
                    "filename": destination.name,
                    "status": "Download started",
                })
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    out_file.write(chunk)
                    downloaded += len(chunk)
                    percent = (downloaded / total * 100.0) if total > 0 else 0.0
                    status_text = f"{format_bytes(downloaded)} downloaded"
                    if total > 0:
                        status_text = f"{format_bytes(downloaded)} / {format_bytes(total)}"
                    download_progress({
                        "percent": percent,
                        "filename": destination.name,
                        "status": status_text,
                    })
        destination = maybe_relabel_downloaded_file(destination, mkvmerge_bin, status)
        download_progress({
            "percent": 100,
            "filename": destination.name,
            "status": "Download complete",
        })
        status(f"Download complete: {destination.name}")
        return destination
    except OSError as exc:
        if exc.errno != errno.ENAMETOOLONG:
            raise
        url_hash = hashlib.sha1(download_url.encode("utf-8")).hexdigest()[:12]
        short_name = f"download_{url_hash}.bin"
        destination = input_dir / short_name
        status(f"Filename too long, retrying as: {short_name}")
        with urllib.request.urlopen(download_url, timeout=120) as response, destination.open("wb") as out_file:
            total_raw = response.headers.get("Content-Length")
            total = int(total_raw) if total_raw and total_raw.isdigit() else 0
            downloaded = 0
            download_progress({
                "percent": 0,
                "filename": destination.name,
                "status": "Download restarted",
            })
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                out_file.write(chunk)
                downloaded += len(chunk)
                percent = (downloaded / total * 100.0) if total > 0 else 0.0
                status_text = f"{format_bytes(downloaded)} downloaded"
                if total > 0:
                    status_text = f"{format_bytes(downloaded)} / {format_bytes(total)}"
                download_progress({
                    "percent": percent,
                    "filename": destination.name,
                    "status": status_text,
                })
        destination = maybe_relabel_downloaded_file(destination, mkvmerge_bin, status)
        download_progress({
            "percent": 100,
            "filename": destination.name,
            "status": "Download complete",
        })
        status(f"Download complete: {destination.name}")
        return destination


def resolve_output_subfolder(media_root: Path, subfolder: str) -> Path:
    clean_subfolder = subfolder.strip().replace("\\", "/").strip("/")
    candidate = (media_root / clean_subfolder).resolve() if clean_subfolder else media_root.resolve()
    media_root_resolved = media_root.resolve()
    try:
        candidate.relative_to(media_root_resolved)
    except ValueError:
        raise RuntimeError(f"Invalid media subfolder path: {subfolder}")
    return candidate


def resolve_tv_destination(tv_base_root: Path, subfolder: str) -> Path:
    clean_subfolder = subfolder.strip().replace("\\", "/").strip("/")
    lower = clean_subfolder.lower()
    if lower.startswith("tv shows/"):
        clean_subfolder = clean_subfolder[9:]
    elif lower == "tv shows":
        clean_subfolder = ""
    elif lower.startswith("tv/"):
        clean_subfolder = clean_subfolder[3:]
    elif lower == "tv":
        clean_subfolder = ""
    return resolve_output_subfolder(tv_base_root, clean_subfolder)


def run_pipeline(
    params: dict[str, object],
    status: Callable[[str], None],
    overall_progress: Callable[[float, str], None],
    download_progress: Callable[[dict], None],
) -> None:
    input_root = Path(DEFAULT_DOWNLOAD_DIR).resolve()
    media_root = Path(DEFAULT_MEDIA_DIR).resolve()
    download_type = str(params["download_type"])
    movie_final_name = str(params["movie_final_name"]).strip()
    tv_download_subfolder = str(params["tv_download_subfolder"]).strip()
    movies_output_root = resolve_output_subfolder(media_root, MOVIES_OUTPUT_DIRNAME)
    tv_base_output_root = resolve_output_subfolder(media_root, TV_OUTPUT_DIRNAME)
    tv_output_root = resolve_tv_destination(tv_base_output_root, tv_download_subfolder)
    download_url = str(params["download_url"]).strip()
    overwrite = bool(params["overwrite"])
    delete_after_clean = bool(params["delete_after_clean"])

    input_root.mkdir(parents=True, exist_ok=True)
    media_root.mkdir(parents=True, exist_ok=True)
    movies_output_root.mkdir(parents=True, exist_ok=True)
    tv_output_root.mkdir(parents=True, exist_ok=True)

    mkvmerge_bin, mkvpropedit_bin = resolve_tools()

    overall_progress(2, "Preparing pipeline")
    status("Pipeline started")

    downloaded_file: Path | None = None
    downloaded_movie_override: Path | None = None
    downloaded_tv_override_root: Path | None = None
    if download_url:
        overall_progress(5, "Downloading input")
        downloaded_file = download_to_input(download_url, input_root, mkvmerge_bin, status, download_progress)
        if download_type == "movie":
            final_name = normalized_final_name(
                name=movie_final_name,
                fallback_stem=downloaded_file.stem or "movie",
                fallback_suffix=downloaded_file.suffix or ".mkv",
            )
            downloaded_movie_override = movies_output_root / final_name
            status(f"Movie final output name: {downloaded_movie_override.name}")
        elif download_type == "tv":
            downloaded_tv_override_root = tv_output_root
            status(f"TV destination: {downloaded_tv_override_root}")
        overall_progress(25, "Download completed")
    else:
        overall_progress(20, "No download selected")
    total = 0
    cleaned = 0
    skipped = 0
    processed = 0

    files_to_process: list[tuple[Path, Path, Path, Path | None]] = []

    if download_type == "movie":
        status("Scanning movie files")
        if downloaded_file is not None and is_media_payload(downloaded_file, mkvmerge_bin):
            override = downloaded_movie_override
            files_to_process.append((downloaded_file, downloaded_file.parent, movies_output_root, override))
        movie_files = find_media_files(input_root)
        for media_file in movie_files:
            if downloaded_file is not None and media_file.resolve() == downloaded_file.resolve():
                continue
            override = downloaded_movie_override if downloaded_file and media_file.resolve() == downloaded_file.resolve() else None
            files_to_process.append((media_file, input_root, movies_output_root, override))

    if download_type == "tv":
        if downloaded_file and is_media_payload(downloaded_file, mkvmerge_bin):
            status("Downloaded TV item is a single episode file")
            files_to_process.append((downloaded_file, downloaded_file.parent, downloaded_tv_override_root or tv_output_root, None))
        else:
            status("Extracting TV ZIP files (if present)")
            extracted = extract_tv_zips(input_root, status)
            tv_root_targets: list[tuple[Path, Path]] = []
            if extracted:
                for zip_path, season_root in extracted:
                    root_target = tv_output_root
                    if downloaded_file and zip_path == downloaded_file.resolve() and downloaded_tv_override_root is not None:
                        root_target = downloaded_tv_override_root
                    tv_root_targets.append((season_root, root_target))
            else:
                tv_root_targets.append((input_root, downloaded_tv_override_root or tv_output_root))

            for season_root, root_target in tv_root_targets:
                season_files = find_media_files(season_root)
                for media_file in season_files:
                    files_to_process.append((media_file, season_root, root_target, None))

    total = len(files_to_process)
    if total == 0:
        overall_progress(100, "No media files found")
        status("No media files found to clean")
        return

    overall_progress(30, f"Processing {total} file(s)")
    for media_file, source_root, target_root, override in files_to_process:
        status(f"Cleaning: {media_file.name}")
        ok = clean_file_to_output(
            source_file=media_file,
            source_root=source_root,
            output_root=target_root,
            mkvmerge_bin=mkvmerge_bin,
            mkvpropedit_bin=mkvpropedit_bin,
            overwrite=overwrite,
            delete_after_clean=delete_after_clean,
            output_override_file=override,
            log=status,
        )
        processed += 1
        if ok:
            cleaned += 1
        else:
            skipped += 1
        progress = 30.0 + (processed / total) * 70.0
        overall_progress(progress, f"Processed {processed}/{total}")

    status(f"Done. Cleaned: {cleaned}, Skipped/Failed: {skipped}")


def emit_event(job_id: str, event_name: str, payload: dict) -> None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        return
    job["queue"].put((event_name, payload))


def mark_done(job_id: str) -> None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return
        job["done"] = True
    job["queue"].put(("done", {"message": "Pipeline complete."}))


def job_runner(job_id: str, params: dict[str, object]) -> None:
    def status(message: str) -> None:
        emit_event(job_id, "status", {"message": message})

    def overall_progress(percent: float, message: str) -> None:
        emit_event(job_id, "overall_progress", {"percent": max(0.0, min(100.0, percent)), "status": message})

    def download_progress(payload: dict) -> None:
        emit_event(job_id, "download_progress", payload)

    try:
        run_pipeline(params, status, overall_progress, download_progress)
    except Exception:
        emit_event(job_id, "status", {"message": "Unhandled exception occurred."})
        emit_event(job_id, "status", {"message": traceback.format_exc()})
    finally:
        mark_done(job_id)


def sse_event(payload: dict, event: str) -> str:
    data = json.dumps(payload, ensure_ascii=False)
    safe_data = data.replace("\r", "")
    lines = safe_data.split("\n")
    parts: list[str] = []
    parts.append(f"event: {event}")
    for line in lines:
        parts.append(f"data: {line}")
    return "\n".join(parts) + "\n\n"


@app.get("/")
def index():
    return render_template_string(PAGE_TEMPLATE, overwrite=DEFAULT_OVERWRITE, app_version=APP_VERSION)


@app.get("/version")
def version():
    return jsonify({"version": APP_VERSION})


@app.post("/start")
def start_job():
    download_type = request.form.get("download_type", "movie").strip().lower()
    if download_type not in {"movie", "tv"}:
        download_type = "movie"

    params: dict[str, object] = {
        "download_url": request.form.get("download_url", "").strip(),
        "download_type": download_type,
        "movie_final_name": request.form.get("movie_final_name", "").strip(),
        "tv_download_subfolder": request.form.get("tv_download_subfolder", "").strip(),
        "overwrite": request.form.get("overwrite", "false").strip().lower() == "true",
        "delete_after_clean": request.form.get("delete_after_clean") == "on",
    }

    job_id = uuid.uuid4().hex
    job_state = {"queue": queue.Queue(), "done": False}
    with JOBS_LOCK:
        JOBS[job_id] = job_state

    thread = threading.Thread(target=job_runner, args=(job_id, params), daemon=True)
    thread.start()
    return jsonify({"job_id": job_id})


@app.get("/stream/<job_id>")
def stream(job_id: str):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        return Response("Job not found", status=404)

    @stream_with_context
    def generate():
        q: queue.Queue = job["queue"]
        while True:
            event_name, payload = q.get()
            if event_name == "done":
                yield sse_event(payload, event="done")
                break
            yield sse_event(payload, event=event_name)

        with JOBS_LOCK:
            JOBS.pop(job_id, None)

    return Response(generate(), mimetype="text/event-stream")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5050"))
    app.run(host="0.0.0.0", port=port, threaded=True)
