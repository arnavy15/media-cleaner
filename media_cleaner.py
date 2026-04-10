#!/usr/bin/env python3
"""
Batch media cleaner (FFmpeg + MKVToolNix):
- Recursively scans a user-provided directory
- Keeps all video tracks
- Keeps exactly one English audio track (first English match)
- Optionally transcodes kept video tracks to H.265 with NVENC (ffmpeg)
- Removes subtitles, chapters, global tags, and track tags
- Clears file title and all track names
- Always outputs MKV on success
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from glob import glob
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

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


def _default_logger(message: str) -> None:
    print(message)


def _default_progress(scope: str, percent: float, message: str) -> None:
    return


def run_command(cmd: List[str]) -> Tuple[int, str, str]:
    kwargs = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
    }
    if os.name == "nt":
        # Prevent console windows from flashing when launched from the GUI EXE.
        kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
    proc = subprocess.run(cmd, **kwargs)
    return proc.returncode, proc.stdout, proc.stderr


def resolve_ffmpeg_bin() -> Optional[str]:
    ffmpeg_path = shutil.which("ffmpeg")
    if ffmpeg_path:
        return ffmpeg_path
    return None


def ensure_hevc_nvenc_available(ffmpeg_bin: str) -> None:
    code, out, err = run_command([ffmpeg_bin, "-hide_banner", "-encoders"])
    if code != 0:
        raise RuntimeError(f"Unable to query FFmpeg encoders: {err.strip() or out.strip()}")
    encoders_text = f"{out}\n{err}".lower()
    if "hevc_nvenc" not in encoders_text:
        raise RuntimeError(
            "NVIDIA H.265 encoder not available (hevc_nvenc). "
            "Install an FFmpeg build with NVENC support and NVIDIA drivers."
        )


def resolve_mkvtoolnix_tools() -> Optional[Tuple[str, str]]:
    # 1) PATH
    mkvmerge_path = shutil.which("mkvmerge")
    mkvpropedit_path = shutil.which("mkvpropedit")
    if mkvmerge_path and mkvpropedit_path:
        return mkvmerge_path, mkvpropedit_path

    # 2) User override
    mkv_bin_env = os.environ.get("MKVTOOLNIX_BIN", "").strip().strip('"')
    if mkv_bin_env:
        m1 = Path(mkv_bin_env) / "mkvmerge.exe"
        m2 = Path(mkv_bin_env) / "mkvpropedit.exe"
        if m1.exists() and m2.exists():
            return str(m1), str(m2)

    # 3) Common install paths
    candidates = [
        Path(r"C:\Program Files\MKVToolNix"),
        Path(r"C:\Program Files (x86)\MKVToolNix"),
    ]
    local_app_data = os.environ.get("LOCALAPPDATA", "").strip()
    if local_app_data:
        pattern = (
            Path(local_app_data)
            / "Microsoft"
            / "WinGet"
            / "Packages"
            / "MoritzBunkus.MKVToolNix_Microsoft.Winget.Source_8wekyb3d8bbwe"
            / "*"
        )
        for p in sorted(glob(str(pattern))):
            candidates.append(Path(p))

    for base in candidates:
        m1 = base / "mkvmerge.exe"
        m2 = base / "mkvpropedit.exe"
        if m1.exists() and m2.exists():
            return str(m1), str(m2)

    return None


def probe_tracks(file_path: Path, mkvmerge_bin: str) -> Dict:
    code, out, err = run_command([mkvmerge_bin, "-J", str(file_path)])
    if code != 0:
        raise RuntimeError(f"mkvmerge -J failed: {err.strip() or out.strip()}")
    try:
        return json.loads(out)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid mkvmerge JSON output: {e}") from e


def is_english_track(track: Dict) -> bool:
    props = track.get("properties") or {}
    language = str(props.get("language", "")).strip().lower()
    language_ietf = str(props.get("language_ietf", "")).strip().lower()
    name = str(props.get("track_name", "")).strip().lower()
    return (
        language in ENGLISH_LANGUAGE_TAGS
        or language_ietf in ENGLISH_LANGUAGE_TAGS
        or "english" in name
    )


def analyze_tracks(info: Dict) -> Dict[str, List[int]]:
    videos: List[int] = []
    audios: List[int] = []
    english_audios: List[int] = []
    subtitles: List[int] = []

    for t in info.get("tracks", []) or []:
        tid = t.get("id")
        ttype = t.get("type")
        if not isinstance(tid, int):
            continue
        if ttype == "video":
            videos.append(tid)
        elif ttype == "audio":
            audios.append(tid)
            if is_english_track(t):
                english_audios.append(tid)
        elif ttype == "subtitles":
            subtitles.append(tid)

    return {
        "video": sorted(videos),
        "audio": sorted(audios),
        "english_audio": sorted(english_audios),
        "subtitle": sorted(subtitles),
    }


def join_ids(ids: List[int]) -> str:
    return ",".join(str(i) for i in ids)


def process_file(
    file_path: Path,
    ffmpeg_bin: str,
    mkvmerge_bin: str,
    mkvpropedit_bin: str,
    transcode_h265: bool = True,
    log: Callable[[str], None] = _default_logger,
    progress: Callable[[str, float, str], None] = _default_progress,
) -> bool:
    log(f"\n[PROCESS] {file_path}")
    progress("file", 0.0, f"{file_path.name}: starting")

    try:
        info = probe_tracks(file_path, mkvmerge_bin)
    except Exception as e:
        log(f"[ERROR] Track probe failed: {e}")
        progress("file", 100.0, f"{file_path.name}: failed during track probe")
        return False

    track_info = analyze_tracks(info)
    videos = track_info["video"]
    audios = track_info["audio"]
    english_audios = track_info["english_audio"]
    subtitles = track_info["subtitle"]
    chapters = info.get("chapters") or []
    attachments = info.get("attachments") or []

    log(
        f"[INFO] Tracks - video: {len(videos)}, audio: {len(audios)}, "
        f"english audio: {len(english_audios)}, subtitles: {len(subtitles)}, "
        f"chapters: {len(chapters)}, attachments: {len(attachments)}"
    )

    if not videos:
        log("[SKIP] No video track found.")
        progress("file", 100.0, f"{file_path.name}: skipped (no video)")
        return False

    selected_audio: Optional[int] = None
    if english_audios:
        selected_audio = english_audios[0]
        if len(english_audios) > 1:
            log(f"[INFO] Multiple English audio tracks found; keeping track id {selected_audio}.")
    elif len(audios) == 1:
        selected_audio = audios[0]
        log("[WARN] Single audio track has no English tag; keeping it and setting language to eng.")
    else:
        log("[SKIP] No English audio track found.")
        progress("file", 100.0, f"{file_path.name}: skipped (no English audio)")
        return False

    progress("file", 15.0, f"{file_path.name}: analyzed tracks")

    out_suffix = ".mkv"
    output_path = file_path.with_suffix(out_suffix)
    replacing_same_path = output_path == file_path
    tmp_clean_fd, tmp_clean_name = tempfile.mkstemp(
        prefix=f"{file_path.stem}.tmp.",
        suffix=".clean.mkv",
        dir=str(file_path.parent),
    )
    os.close(tmp_clean_fd)
    tmp_clean_path = Path(tmp_clean_name)

    tmp_video_fd, tmp_video_name = tempfile.mkstemp(
        prefix=f"{file_path.stem}.tmp.",
        suffix=".video.mkv",
        dir=str(file_path.parent),
    )
    os.close(tmp_video_fd)
    tmp_video_path = Path(tmp_video_name)

    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=f"{file_path.stem}.tmp.",
        suffix=out_suffix,
        dir=str(file_path.parent),
    )
    os.close(tmp_fd)
    tmp_path = Path(tmp_name)

    # Step 1: MKVToolNix cleanup first (remove extra audio/subtitles/tags/chapters, clear title/names).
    clean_cmd = [
        mkvmerge_bin,
        "--output", str(tmp_clean_path),
        "--title", "",
        "--video-tracks", join_ids(videos),
        "--audio-tracks", str(selected_audio),
        "--no-subtitles",
        "--no-global-tags",
        "--no-track-tags",
        "--no-chapters",
        "--language", f"{selected_audio}:eng",
        str(file_path),
    ]
    code, out, err = run_command(clean_cmd)
    if code != 0:
        log(f"[ERROR] mkvmerge cleanup failed: {err.strip() or out.strip()}")
        try:
            if tmp_clean_path.exists():
                tmp_clean_path.unlink()
            if tmp_video_path.exists():
                tmp_video_path.unlink()
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        progress("file", 100.0, f"{file_path.name}: failed during cleanup")
        return False

    clean_propedit_cmd = [
        mkvpropedit_bin,
        str(tmp_clean_path),
        "--edit", "info",
        "--set", "title=",
        "--tags", "all:",
    ]
    for i in range(1, len(videos) + 1):
        clean_propedit_cmd.extend(["--edit", f"track:v{i}", "--set", "name="])
    clean_propedit_cmd.extend(["--edit", "track:a1", "--set", "name="])
    run_command(clean_propedit_cmd)
    progress("file", 40.0, f"{file_path.name}: cleanup complete")

    if transcode_h265:
        # Step 2: ffmpeg transcodes video streams only to H.265 NVENC from cleaned source.
        transcode_cmd = [
            ffmpeg_bin,
            "-y",
            "-i", str(tmp_clean_path),
            "-map", "0:v",
            "-c:v", "hevc_nvenc",
            "-preset", "p5",
            "-rc", "vbr",
            "-cq", "23",
            "-b:v", "0",
            "-an",
            "-sn",
            "-dn",
            "-map_chapters", "-1",
            "-map_metadata", "-1",
            str(tmp_video_path),
        ]
        code, out, err = run_command(transcode_cmd)
        if code != 0:
            log(f"[ERROR] ffmpeg transcode failed: {err.strip() or out.strip()}")
            try:
                if tmp_clean_path.exists():
                    tmp_clean_path.unlink()
                if tmp_video_path.exists():
                    tmp_video_path.unlink()
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass
            progress("file", 100.0, f"{file_path.name}: failed during transcode")
            return False
        progress("file", 75.0, f"{file_path.name}: transcode complete")
    else:
        # Keep original video tracks from cleaned source without re-encoding.
        copy_cmd = [
            ffmpeg_bin,
            "-y",
            "-i", str(tmp_clean_path),
            "-map", "0:v",
            "-c:v", "copy",
            "-an",
            "-sn",
            "-dn",
            "-map_chapters", "-1",
            "-map_metadata", "-1",
            str(tmp_video_path),
        ]
        code, out, err = run_command(copy_cmd)
        if code != 0:
            log(f"[ERROR] ffmpeg video copy failed: {err.strip() or out.strip()}")
            try:
                if tmp_clean_path.exists():
                    tmp_clean_path.unlink()
                if tmp_video_path.exists():
                    tmp_video_path.unlink()
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass
            progress("file", 100.0, f"{file_path.name}: failed during video copy")
            return False
        progress("file", 75.0, f"{file_path.name}: video stream copy complete")

    # Step 3: mkvmerge muxes transcoded video + cleaned audio only.
    merge_cmd = [
        mkvmerge_bin,
        "--output", str(tmp_path),
        "--title", "",
        "--no-audio",
        "--no-subtitles",
        "--no-global-tags",
        "--no-track-tags",
        "--no-chapters",
        str(tmp_video_path),
        "--no-video",
        "--no-subtitles",
        "--no-global-tags",
        "--no-track-tags",
        "--no-chapters",
        str(tmp_clean_path),
    ]

    code, out, err = run_command(merge_cmd)
    if code != 0:
        log(f"[ERROR] mkvmerge failed: {err.strip() or out.strip()}")
        try:
            if tmp_clean_path.exists():
                tmp_clean_path.unlink()
            if tmp_video_path.exists():
                tmp_video_path.unlink()
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        progress("file", 100.0, f"{file_path.name}: failed during final mux")
        return False

    # Extra hardening for MKV tags in-place.
    cleanup_cmd = [
        mkvpropedit_bin,
        str(tmp_path),
        "--edit", "info",
        "--set", "title=",
        "--tags", "all:",
    ]
    for i in range(1, len(videos) + 1):
        cleanup_cmd.extend(["--edit", f"track:v{i}", "--set", "name="])
    cleanup_cmd.extend(["--edit", "track:a1", "--set", "name="])
    run_command(cleanup_cmd)
    progress("file", 90.0, f"{file_path.name}: final cleanup complete")

    try:
        if tmp_clean_path.exists():
            tmp_clean_path.unlink()
        if tmp_video_path.exists():
            tmp_video_path.unlink()
    except OSError:
        pass

    if not tmp_path.exists() or tmp_path.stat().st_size == 0:
        log("[ERROR] Temporary output missing or empty.")
        try:
            if tmp_path.exists():
                tmp_path.unlink()
            if tmp_clean_path.exists():
                tmp_clean_path.unlink()
            if tmp_video_path.exists():
                tmp_video_path.unlink()
        except OSError:
            pass
        progress("file", 100.0, f"{file_path.name}: failed (empty output)")
        return False

    # Always output MKV. Non-MKV inputs are replaced by <stem>.mkv.
    try:
        if not replacing_same_path and file_path.exists():
            file_path.unlink()
        os.replace(tmp_path, output_path)
        if replacing_same_path:
            log("[OK] Replaced original file successfully.")
        else:
            log(f"[OK] Replaced original with MKV: {output_path.name}")
        progress("file", 100.0, f"{file_path.name}: done")
        return True
    except Exception as e:
        log(f"[ERROR] Failed to replace original file: {e}")
        log(f"[INFO] Processed temp file left at: {tmp_path}")
        progress("file", 100.0, f"{file_path.name}: failed to replace original")
        return False


def find_media_files(root: Path) -> List[Path]:
    files: List[Path] = []
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            p = Path(dirpath) / name
            if p.suffix.lower() in MEDIA_EXTENSIONS:
                files.append(p)
    return files


def process_directory(
    root: Path,
    transcode_h265: bool = True,
    log: Callable[[str], None] = _default_logger,
    progress: Callable[[str, float, str], None] = _default_progress,
) -> Tuple[int, int, int]:
    ffmpeg_bin = resolve_ffmpeg_bin()
    if not ffmpeg_bin:
        raise RuntimeError("FFmpeg not found (ffmpeg).")
    if transcode_h265:
        ensure_hevc_nvenc_available(ffmpeg_bin)

    tools = resolve_mkvtoolnix_tools()
    if not tools:
        raise RuntimeError("MKVToolNix tools not found (mkvmerge/mkvpropedit).")
    mkvmerge_bin, mkvpropedit_bin = tools

    if not root.exists() or not root.is_dir():
        raise ValueError(f"Invalid directory: {root}")

    media_files = find_media_files(root)
    if not media_files:
        log("[INFO] No matching media files found.")
        progress("overall", 100.0, "No media files found")
        return 0, 0, 0

    log(f"[INFO] Found {len(media_files)} media files.\n")
    progress("overall", 0.0, f"Found {len(media_files)} media files")
    processed = 0
    skipped_or_failed = 0

    for i, file_path in enumerate(media_files, start=1):
        log(f"--- [{i}/{len(media_files)}] ---")
        progress("file", 0.0, f"{file_path.name}: queued ({i}/{len(media_files)})")
        ok = process_file(
            file_path,
            ffmpeg_bin,
            mkvmerge_bin,
            mkvpropedit_bin,
            transcode_h265=transcode_h265,
            log=log,
            progress=progress,
        )
        if ok:
            processed += 1
        else:
            skipped_or_failed += 1
        overall_pct = (i / len(media_files)) * 100.0
        progress("overall", overall_pct, f"Completed {i}/{len(media_files)} files")

    log("\n=== Summary ===")
    log(f"Total files found: {len(media_files)}")
    log(f"Processed/Compliant: {processed}")
    log(f"Skipped/Failed: {skipped_or_failed}")
    progress("overall", 100.0, "All files processed")
    return len(media_files), processed, skipped_or_failed


def main() -> int:
    user_input = input("Enter directory path to scan: ").strip().strip('"')
    if not user_input:
        print("[FATAL] No directory provided.")
        return 1

    root = Path(user_input).expanduser().resolve()
    try:
        process_directory(root, log=print)
        return 0
    except ValueError as e:
        print(f"[FATAL] {e}")
        return 1
    except RuntimeError as e:
        print(f"[FATAL] {e}")
        print("[HINT] Add ffmpeg, mkvmerge, and mkvpropedit to PATH (or set MKVTOOLNIX_BIN).")
        return 1


if __name__ == "__main__":
    sys.exit(main())
