#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

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


def env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def run_command(cmd: list[str]) -> tuple[int, str, str]:
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return proc.returncode, proc.stdout, proc.stderr


def resolve_tools() -> tuple[str, str]:
    mkvmerge = shutil.which("mkvmerge")
    mkvpropedit = shutil.which("mkvpropedit")
    if not mkvmerge or not mkvpropedit:
        raise RuntimeError("mkvtoolnix not found in PATH. Expected mkvmerge and mkvpropedit.")
    return mkvmerge, mkvpropedit


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


def join_ids(ids: list[int]) -> str:
    return ",".join(str(x) for x in ids)


def pick_audio_track(audios: list[int], english_audios: list[int], keep_english_only: bool) -> int | None:
    if keep_english_only:
        if english_audios:
            return english_audios[0]
        return None
    if english_audios:
        return english_audios[0]
    if audios:
        return audios[0]
    return None


def is_file_stable(file_path: Path, wait_seconds: int) -> bool:
    try:
        size_1 = file_path.stat().st_size
        time.sleep(wait_seconds)
        size_2 = file_path.stat().st_size
        return size_1 == size_2 and size_2 > 0
    except OSError:
        return False


def clean_file(
    source_file: Path,
    source_root: Path,
    dest_root: Path,
    mkvmerge_bin: str,
    mkvpropedit_bin: str,
    keep_english_only: bool,
    overwrite: bool,
) -> bool:
    print(f"[PROCESS] {source_file}")
    info = probe_tracks(source_file, mkvmerge_bin)
    videos, audios, english_audios = analyze_tracks(info)

    if not videos:
        print("[SKIP] No video track.")
        return False

    selected_audio = pick_audio_track(audios, english_audios, keep_english_only)
    rel_path = source_file.relative_to(source_root)
    dest_file = (dest_root / rel_path).with_suffix(".mkv")
    dest_file.parent.mkdir(parents=True, exist_ok=True)

    if dest_file.exists() and not overwrite:
        print(f"[SKIP] Destination exists: {dest_file}")
        return False

    tmp_fd, tmp_name = tempfile.mkstemp(prefix=f"{source_file.stem}.", suffix=".mkv", dir=str(dest_file.parent))
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
    ]
    if selected_audio is None:
        cmd.append("--no-audio")
    else:
        cmd.extend(["--audio-tracks", str(selected_audio), "--language", f"{selected_audio}:eng"])
    cmd.append(str(source_file))

    code, out, err = run_command(cmd)
    if code != 0:
        print(f"[ERROR] mkvmerge failed: {err.strip() or out.strip()}")
        try:
            tmp_output.unlink(missing_ok=True)
        except OSError:
            pass
        return False

    propedit_cmd = [mkvpropedit_bin, str(tmp_output), "--edit", "info", "--set", "title=", "--tags", "all:"]
    for i in range(1, len(videos) + 1):
        propedit_cmd.extend(["--edit", f"track:v{i}", "--set", "name="])
    if selected_audio is not None:
        propedit_cmd.extend(["--edit", "track:a1", "--set", "name="])
    run_command(propedit_cmd)

    try:
        if dest_file.exists() and overwrite:
            dest_file.unlink()
        os.replace(tmp_output, dest_file)
        source_file.unlink()
        print(f"[OK] Moved cleaned file to: {dest_file}")
        return True
    except OSError as exc:
        print(f"[ERROR] Finalize failed: {exc}")
        try:
            tmp_output.unlink(missing_ok=True)
        except OSError:
            pass
        return False


def find_media_files(root: Path) -> list[Path]:
    results: list[Path] = []
    for dirpath, _, filenames in os.walk(root):
        base = Path(dirpath)
        for name in filenames:
            p = base / name
            if p.suffix.lower() in MEDIA_EXTENSIONS:
                results.append(p)
    return results


def main() -> int:
    source_root = Path(os.environ.get("SOURCE_DIR", "/watch")).resolve()
    dest_root = Path(os.environ.get("DEST_DIR", "/output")).resolve()
    config_root = Path(os.environ.get("CONFIG_DIR", "/config")).resolve()
    poll_seconds = int(os.environ.get("POLL_SECONDS", "15"))
    stable_wait_seconds = int(os.environ.get("STABLE_WAIT_SECONDS", "5"))
    keep_english_only = env_bool("KEEP_ENGLISH_ONLY", True)
    overwrite = env_bool("OVERWRITE_OUTPUT", False)

    if not source_root.exists():
        print(f"[FATAL] SOURCE_DIR does not exist: {source_root}")
        return 1
    config_root.mkdir(parents=True, exist_ok=True)
    dest_root.mkdir(parents=True, exist_ok=True)

    mkvmerge_bin, mkvpropedit_bin = resolve_tools()
    print(f"[START] Watching: {source_root}")
    print(f"[START] Output:   {dest_root}")
    print(f"[START] Config:   {config_root}")
    print(f"[START] Poll:     {poll_seconds}s")

    while True:
        try:
            media_files = find_media_files(source_root)
            if media_files:
                print(f"[SCAN] Found {len(media_files)} candidate file(s).")
            for file_path in media_files:
                if not file_path.exists():
                    continue
                if not is_file_stable(file_path, stable_wait_seconds):
                    print(f"[WAIT] Still changing: {file_path}")
                    continue
                try:
                    clean_file(
                        source_file=file_path,
                        source_root=source_root,
                        dest_root=dest_root,
                        mkvmerge_bin=mkvmerge_bin,
                        mkvpropedit_bin=mkvpropedit_bin,
                        keep_english_only=keep_english_only,
                        overwrite=overwrite,
                    )
                except Exception as exc:
                    print(f"[ERROR] Unexpected failure for {file_path}: {exc}")
            time.sleep(poll_seconds)
        except KeyboardInterrupt:
            print("[STOP] Interrupted.")
            return 0
        except Exception as exc:
            print(f"[ERROR] Loop failure: {exc}")
            time.sleep(poll_seconds)


if __name__ == "__main__":
    sys.exit(main())
