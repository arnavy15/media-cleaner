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
DEFAULT_OVERWRITE = False

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
    pre {
      margin: 14px 0 0;
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 13px;
      max-height: 420px;
      overflow: auto;
      background: rgba(2, 6, 23, 0.65);
      white-space: pre-wrap;
      line-height: 1.42;
      color: #cfe2ff;
      font-size: 0.9rem;
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
      <p>Download, clean, and route files with live streaming logs.</p>
      <span class="chip">MKVToolNix Only</span>
    </div>
    <div class="card">
      <h2>Pipeline Settings</h2>
      <form id="pipeline-form">
        <div class="grid">
          <div>
            <label for="mode">Mode</label>
            <select id="mode" name="mode">
              <option value="movies">Movies</option>
              <option value="tv">TV Shows</option>
              <option value="both">Both</option>
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
            <label for="movies_subfolder">Movies Subfolder (inside /media)</label>
            <input id="movies_subfolder" name="movies_subfolder" value="movies" required />
          </div>
          <div class="full">
            <label for="tv_subfolder">TV Subfolder (inside /media)</label>
            <input id="tv_subfolder" name="tv_subfolder" value="tv" required />
          </div>
          <div class="full">
            <label for="download_url">Download URL (optional)</label>
            <input id="download_url" name="download_url" value="" placeholder="https://..." />
          </div>
          <div class="full">
            <label for="download_name">Download File Name (optional)</label>
            <input id="download_name" name="download_name" value="" placeholder="movie.mkv or season1.zip" />
          </div>
          <div class="full">
            <label for="download_type">Downloaded Item Type</label>
            <select id="download_type" name="download_type">
              <option value="movie">Movie</option>
              <option value="tv">TV Show</option>
            </select>
          </div>
          <div class="full" id="movie-name-wrap">
            <label for="movie_final_name">Movie Final File Name (inside movies output)</label>
            <input id="movie_final_name" name="movie_final_name" value="" placeholder="Movie Title (2026).mkv" />
          </div>
          <div class="full" id="tv-dest-wrap" style="display:none;">
            <label for="tv_download_subfolder">TV Download Destination (inside /media)</label>
            <input id="tv_download_subfolder" name="tv_download_subfolder" value="" placeholder="TV/Show Name/Season 01" />
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
      <pre id="logs">Waiting...</pre>
    </div>
  </div>
  <script>
    const form = document.getElementById("pipeline-form");
    const logs = document.getElementById("logs");
    const runBtn = document.getElementById("run-btn");
    const downloadType = document.getElementById("download_type");
    const movieNameWrap = document.getElementById("movie-name-wrap");
    const tvDestWrap = document.getElementById("tv-dest-wrap");
    let stream = null;

    function appendLog(line) {
      logs.textContent += (logs.textContent === "Waiting..." ? "" : "\\n") + line;
      logs.scrollTop = logs.scrollHeight;
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
      logs.textContent = "";
      runBtn.disabled = true;
      runBtn.textContent = "Running...";

      try {
        const response = await fetch("/start", {
          method: "POST",
          body: new FormData(form),
        });
        if (!response.ok) {
          appendLog("[FATAL] Failed to start job.");
          runBtn.disabled = false;
          runBtn.textContent = "Run Pipeline";
          return;
        }

        const payload = await response.json();
        if (!payload.job_id) {
          appendLog("[FATAL] Missing job id.");
          runBtn.disabled = false;
          runBtn.textContent = "Run Pipeline";
          return;
        }

        stream = new EventSource(`/stream/${payload.job_id}`);
        stream.onmessage = (evt) => appendLog(evt.data);
        stream.addEventListener("done", () => {
          appendLog("[UI] Job complete.");
          runBtn.disabled = false;
          runBtn.textContent = "Run Pipeline";
          if (stream) {
            stream.close();
            stream = null;
          }
        });
        stream.onerror = () => {
          appendLog("[UI] Stream disconnected.");
          runBtn.disabled = false;
          runBtn.textContent = "Run Pipeline";
          if (stream) {
            stream.close();
            stream = null;
          }
        };
      } catch (err) {
        appendLog("[FATAL] " + err);
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
        dest_file = output_override_file.with_suffix(".mkv")
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
    zip_files = sorted(input_dir.rglob("*.zip"))
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


def normalized_filename(name: str, fallback_name: str) -> str:
    clean_name = Path(name.strip()).name
    if not clean_name or clean_name in {".", ".."}:
        return fallback_name
    return clean_name


def normalized_final_mkv_name(name: str, fallback_stem: str) -> str:
    clean_name = Path(name.strip()).name
    if not clean_name or clean_name in {".", ".."}:
        clean_name = fallback_stem
    return str(Path(clean_name).with_suffix(".mkv"))


def download_to_input(download_url: str, download_name: str, input_dir: Path, log: Callable[[str], None]) -> Path:
    parsed = urllib.parse.urlparse(download_url)
    fallback_name = Path(parsed.path).name or "downloaded_media"
    final_name = normalized_filename(download_name, fallback_name)
    destination = input_dir / final_name
    destination.parent.mkdir(parents=True, exist_ok=True)

    log(f"[DOWNLOAD] Starting: {download_url}")
    with urllib.request.urlopen(download_url, timeout=120) as response, destination.open("wb") as out_file:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            out_file.write(chunk)
    log(f"[DOWNLOAD] Saved to: {destination}")
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


def run_pipeline(params: dict[str, object], log: Callable[[str], None]) -> None:
    mode = str(params["mode"])
    input_root = Path(DEFAULT_DOWNLOAD_DIR).resolve()
    media_root = Path(DEFAULT_MEDIA_DIR).resolve()
    movies_subfolder = str(params["movies_subfolder"])
    tv_subfolder = str(params["tv_subfolder"])
    download_type = str(params["download_type"])
    movie_final_name = str(params["movie_final_name"]).strip()
    tv_download_subfolder = str(params["tv_download_subfolder"]).strip()
    movies_output_root = resolve_output_subfolder(media_root, movies_subfolder)
    tv_output_root = resolve_output_subfolder(media_root, tv_subfolder)
    download_url = str(params["download_url"]).strip()
    download_name = str(params["download_name"]).strip()
    overwrite = bool(params["overwrite"])
    delete_after_clean = bool(params["delete_after_clean"])

    input_root.mkdir(parents=True, exist_ok=True)
    media_root.mkdir(parents=True, exist_ok=True)
    movies_output_root.mkdir(parents=True, exist_ok=True)
    tv_output_root.mkdir(parents=True, exist_ok=True)

    log(f"[START] Mode: {mode}")
    log(f"[START] Download/input root: {input_root}")
    log(f"[START] Media/output root: {media_root}")
    log(f"[START] Movies output: {movies_output_root}")
    log(f"[START] TV output: {tv_output_root}")

    downloaded_file: Path | None = None
    downloaded_movie_override: Path | None = None
    downloaded_tv_override_root: Path | None = None
    if download_url:
        downloaded_file = download_to_input(download_url, download_name, input_root, log)
        if download_type == "movie":
            final_name = normalized_final_mkv_name(movie_final_name, downloaded_file.stem or "movie")
            downloaded_movie_override = movies_output_root / final_name
            log(f"[DOWNLOAD] Movie final output name: {downloaded_movie_override}")
        elif download_type == "tv" and tv_download_subfolder:
            downloaded_tv_override_root = resolve_output_subfolder(media_root, tv_download_subfolder)
            downloaded_tv_override_root.mkdir(parents=True, exist_ok=True)
            log(f"[DOWNLOAD] TV output override for this download: {downloaded_tv_override_root}")

    mkvmerge_bin, mkvpropedit_bin = resolve_tools()
    total = 0
    cleaned = 0
    skipped = 0

    if mode in {"movies", "both"}:
        log("[MOVIES] Scanning media files from input folder.")
        movie_files = find_media_files(input_root)
        if not movie_files:
            log("[MOVIES] No media files found.")
        for media_file in movie_files:
            total += 1
            ok = clean_file_to_output(
                source_file=media_file,
                source_root=input_root,
                output_root=movies_output_root,
                mkvmerge_bin=mkvmerge_bin,
                mkvpropedit_bin=mkvpropedit_bin,
                overwrite=overwrite,
                delete_after_clean=delete_after_clean,
                output_override_file=downloaded_movie_override if downloaded_file and media_file.resolve() == downloaded_file.resolve() else None,
                log=log,
            )
            if ok:
                cleaned += 1
            else:
                skipped += 1

    if mode in {"tv", "both"}:
        if downloaded_file and download_type == "tv" and downloaded_file.suffix.lower() in MEDIA_EXTENSIONS:
            log("[TV] Downloaded TV item is a single media file (episode).")
            total += 1
            ok = clean_file_to_output(
                source_file=downloaded_file,
                source_root=downloaded_file.parent,
                output_root=downloaded_tv_override_root or tv_output_root,
                mkvmerge_bin=mkvmerge_bin,
                mkvpropedit_bin=mkvpropedit_bin,
                overwrite=overwrite,
                delete_after_clean=delete_after_clean,
                output_override_file=None,
                log=log,
            )
            if ok:
                cleaned += 1
            else:
                skipped += 1
        else:
            log("[TV] Extracting season ZIP files (if any), then scanning TV files.")
            extracted = extract_tv_zips(input_root, log)
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
                log(f"[TV] Scanning extracted season: {season_root}")
                season_files = find_media_files(season_root)
                if not season_files:
                    log(f"[TV] No media files found in {season_root}")
                    continue
                for media_file in season_files:
                    total += 1
                    ok = clean_file_to_output(
                        source_file=media_file,
                        source_root=season_root,
                        output_root=root_target,
                        mkvmerge_bin=mkvmerge_bin,
                        mkvpropedit_bin=mkvpropedit_bin,
                        overwrite=overwrite,
                        delete_after_clean=delete_after_clean,
                        output_override_file=None,
                        log=log,
                    )
                    if ok:
                        cleaned += 1
                    else:
                        skipped += 1

    log(f"[SUMMARY] Total files seen: {total}")
    log(f"[SUMMARY] Cleaned: {cleaned}")
    log(f"[SUMMARY] Skipped/Failed: {skipped}")


def emit_log(job_id: str, message: str) -> None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        return
    job["queue"].put(message)


def mark_done(job_id: str) -> None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return
        job["done"] = True
    job["queue"].put("[DONE]")


def job_runner(job_id: str, params: dict[str, object]) -> None:
    def log(message: str) -> None:
        emit_log(job_id, message)

    try:
        run_pipeline(params, log)
    except Exception:
        log("[FATAL] Unhandled exception:")
        for line in traceback.format_exc().splitlines():
            log(line)
    finally:
        mark_done(job_id)


def sse_event(data: str, event: str | None = None) -> str:
    safe_data = data.replace("\r", "")
    lines = safe_data.split("\n")
    parts: list[str] = []
    if event:
        parts.append(f"event: {event}")
    for line in lines:
        parts.append(f"data: {line}")
    return "\n".join(parts) + "\n\n"


@app.get("/")
def index():
    return render_template_string(PAGE_TEMPLATE, overwrite=DEFAULT_OVERWRITE)


@app.post("/start")
def start_job():
    mode = request.form.get("mode", "movies").strip().lower()
    if mode not in {"movies", "tv", "both"}:
        mode = "movies"
    download_type = request.form.get("download_type", "movie").strip().lower()
    if download_type not in {"movie", "tv"}:
        download_type = "movie"

    params: dict[str, object] = {
        "mode": mode,
        "movies_subfolder": request.form.get("movies_subfolder", "movies").strip() or "movies",
        "tv_subfolder": request.form.get("tv_subfolder", "tv").strip() or "tv",
        "download_url": request.form.get("download_url", "").strip(),
        "download_name": request.form.get("download_name", "").strip(),
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
            message = q.get()
            if message == "[DONE]":
                yield sse_event("Pipeline complete.", event="done")
                break
            yield sse_event(message)

        with JOBS_LOCK:
            JOBS.pop(job_id, None)

    return Response(generate(), mimetype="text/event-stream")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5050"))
    app.run(host="0.0.0.0", port=port, threaded=True)
