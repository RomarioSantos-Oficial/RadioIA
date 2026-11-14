from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import shutil
import sqlite3
import subprocess
import threading
import time
import unicodedata
from contextlib import asynccontextmanager
from datetime import datetime, time as clock_time, timedelta
from pathlib import Path
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "playout.sqlite3"
WORKSPACE_POINTER = DATA_DIR / "workspace.json"
SUPPORTED = {".mp4", ".webm", ".mov", ".mp3", ".wav", ".ogg"}
IMAGE_TYPES = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
WEEKDAYS = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("playout")


def load_workspace_pointer() -> None:
    global DATA_DIR, DB_PATH
    try:
        workspace = json.loads(WORKSPACE_POINTER.read_text(encoding="utf-8")).get("path")
        if workspace and Path(workspace).is_dir():
            DATA_DIR = Path(workspace) / ".playout"
            DB_PATH = DATA_DIR / "playout.sqlite3"
    except (OSError, ValueError, AttributeError):
        pass


load_workspace_pointer()


def db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=15)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA journal_mode=WAL")
    return con


def init_db() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    with db() as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS folders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            path TEXT NOT NULL DEFAULT '',
            recursive INTEGER NOT NULL DEFAULT 1,
            mode TEXT NOT NULL DEFAULT 'sequential',
            limit_seconds REAL,
            jingle_every INTEGER NOT NULL DEFAULT 0,
            sponsor_every INTEGER NOT NULL DEFAULT 0,
            commercial_every INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS media (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folder_id INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            path TEXT NOT NULL,
            kind TEXT NOT NULL,
            duration REAL NOT NULL DEFAULT 0,
            sort_order INTEGER NOT NULL DEFAULT 0,
            active INTEGER NOT NULL DEFAULT 1,
            loop_mode TEXT NOT NULL DEFAULT 'once',
            repeat_count INTEGER NOT NULL DEFAULT 1,
            category TEXT NOT NULL DEFAULT 'music'
        );
        CREATE TABLE IF NOT EXISTS flows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            root_path TEXT NOT NULL UNIQUE,
            mode TEXT NOT NULL DEFAULT 'sequential',
            recursive INTEGER NOT NULL DEFAULT 1,
            program_share INTEGER NOT NULL DEFAULT 90,
            jingle_folder_id INTEGER REFERENCES folders(id) ON DELETE SET NULL,
            commercial_folder_id INTEGER REFERENCES folders(id) ON DELETE SET NULL
        );
        CREATE TABLE IF NOT EXISTS flow_programs (
            flow_id INTEGER NOT NULL REFERENCES flows(id) ON DELETE CASCADE,
            folder_id INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
            jingle_folder_id INTEGER REFERENCES folders(id) ON DELETE SET NULL,
            commercial_folder_id INTEGER REFERENCES folders(id) ON DELETE SET NULL,
            sort_order INTEGER NOT NULL DEFAULT 0,
            active INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY(flow_id, folder_id)
        );
        CREATE TABLE IF NOT EXISTS schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            folder_id INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
            flow_id INTEGER REFERENCES flows(id) ON DELETE SET NULL,
            kind TEXT NOT NULL DEFAULT 'program',
            weekdays TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            transition TEXT NOT NULL DEFAULT 'cut',
            active INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        INSERT OR IGNORE INTO settings(key,value) VALUES('automation','0');
        INSERT OR IGNORE INTO settings(key,value) VALUES('background_path','');
        """)
        # Add new playlist fields when opening a database created by an earlier version.
        folder_columns = {row["name"] for row in con.execute("PRAGMA table_info(folders)")}
        for column in ("jingle_every", "sponsor_every", "commercial_every"):
            if column not in folder_columns:
                con.execute(f"ALTER TABLE folders ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0")
        media_columns = {row["name"] for row in con.execute("PRAGMA table_info(media)")}
        if "category" not in media_columns:
            con.execute("ALTER TABLE media ADD COLUMN category TEXT NOT NULL DEFAULT 'music'")
        unique_path = False
        for index in con.execute("PRAGMA index_list(media)").fetchall():
            if index["unique"]:
                columns = [r["name"] for r in con.execute(f"PRAGMA index_info('{index['name']}')").fetchall()]
                if columns == ["path"]:
                    unique_path = True
                    break
        if unique_path:
            # A shared station asset (for example, one sponsor spot) can belong to
            # more than one program playlist, so paths must not be globally unique.
            con.execute("PRAGMA foreign_keys=OFF")
            con.execute("BEGIN IMMEDIATE")
            con.execute("""CREATE TABLE media_rebuilt (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                folder_id INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                path TEXT NOT NULL,
                kind TEXT NOT NULL,
                duration REAL NOT NULL DEFAULT 0,
                sort_order INTEGER NOT NULL DEFAULT 0,
                active INTEGER NOT NULL DEFAULT 1,
                loop_mode TEXT NOT NULL DEFAULT 'once',
                repeat_count INTEGER NOT NULL DEFAULT 1,
                category TEXT NOT NULL DEFAULT 'music'
            )""")
            con.execute("""INSERT INTO media_rebuilt(id,folder_id,name,path,kind,duration,sort_order,active,loop_mode,repeat_count,category)
                SELECT id,folder_id,name,path,kind,duration,sort_order,active,loop_mode,repeat_count,category FROM media""")
            con.execute("DROP TABLE media")
            con.execute("ALTER TABLE media_rebuilt RENAME TO media")
            con.commit()
            con.execute("PRAGMA foreign_keys=ON")
        schedule_columns = {row["name"] for row in con.execute("PRAGMA table_info(schedules)")}
        if "flow_id" not in schedule_columns:
            con.execute("ALTER TABLE schedules ADD COLUMN flow_id INTEGER REFERENCES flows(id) ON DELETE SET NULL")
        program_columns = {row["name"] for row in con.execute("PRAGMA table_info(flow_programs)")}
        if "jingle_folder_id" not in program_columns:
            con.execute("ALTER TABLE flow_programs ADD COLUMN jingle_folder_id INTEGER REFERENCES folders(id) ON DELETE SET NULL")
        if "commercial_folder_id" not in program_columns:
            con.execute("ALTER TABLE flow_programs ADD COLUMN commercial_folder_id INTEGER REFERENCES folders(id) ON DELETE SET NULL")
        if "active" not in program_columns:
            con.execute("ALTER TABLE flow_programs ADD COLUMN active INTEGER NOT NULL DEFAULT 1")
        flow_columns = {row["name"] for row in con.execute("PRAGMA table_info(flows)")}
        if "recursive" not in flow_columns:
            con.execute("ALTER TABLE flows ADD COLUMN recursive INTEGER NOT NULL DEFAULT 1")


def switch_workspace(root: Path) -> None:
    global DATA_DIR, DB_PATH
    root = root.expanduser().resolve()
    if not root.is_dir():
        raise HTTPException(400, "A pasta escolhida não está acessível.")
    target_dir = root / ".playout"
    target_db = target_dir / "playout.sqlite3"
    target_dir.mkdir(parents=True, exist_ok=True)
    old_db = DB_PATH
    if target_db.resolve() != old_db.resolve() and not target_db.exists() and old_db.exists():
        with sqlite3.connect(old_db) as source, sqlite3.connect(target_db) as destination:
            source.backup(destination)
    DATA_DIR = target_dir
    DB_PATH = target_db
    init_db()
    WORKSPACE_POINTER.parent.mkdir(parents=True, exist_ok=True)
    WORKSPACE_POINTER.write_text(json.dumps({"path": str(root)}, ensure_ascii=False), encoding="utf-8")


def get_setting(key: str, default: str = "") -> str:
    with db() as con:
        row = con.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def set_setting(key: str, value: str) -> None:
    with db() as con:
        con.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))


def row_dict(row: sqlite3.Row) -> dict[str, Any]:
    return dict(row)


def get_folder(folder_id: int) -> dict[str, Any]:
    with db() as con:
        row = con.execute("SELECT * FROM folders WHERE id=?", (folder_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Programa/pasta não encontrado.")
    result = row_dict(row)
    result["recursive"] = bool(result["recursive"])
    result["limit_seconds"] = float(result["limit_seconds"]) if result["limit_seconds"] is not None else None
    return result


def get_media(media_id: int) -> dict[str, Any]:
    with db() as con:
        row = con.execute("SELECT * FROM media WHERE id=?", (media_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Arquivo não encontrado na biblioteca.")
    result = row_dict(row)
    result["active"] = bool(result["active"])
    result["duration"] = float(result["duration"])
    return result


def get_folder_media(folder_id: int, active_only: bool = False) -> list[dict[str, Any]]:
    with db() as con:
        sql = "SELECT * FROM media WHERE folder_id=?"
        if active_only:
            sql += " AND active=1"
        sql += " ORDER BY sort_order,id"
        rows = con.execute(sql, (folder_id,)).fetchall()
    return [row_dict(r) for r in rows]


def infer_category(path: Path, source_root: Path | None = None) -> str:
    if source_root is not None:
        try:
            relative_parts = path.relative_to(source_root).parts[:-1]
        except ValueError:
            relative_parts = ()
        category_parts = (*relative_parts, source_root.name)
    else:
        category_parts = (path.parent.name,)
    raw = " ".join(part.casefold() for part in category_parts)
    text = "".join(char for char in unicodedata.normalize("NFKD", raw)
                   if not unicodedata.combining(char))
    if "patroc" in text or "sponsor" in text:
        return "sponsor"
    if "vinhet" in text or "jingle" in text:
        return "jingle"
    if "comercial" in text or "commercial" in text or "anuncio" in text or "propaganda" in text or "advert" in text:
        return "commercial"
    return "music"


def compose_program(media: list[dict[str, Any]], folder: dict[str, Any]) -> list[dict[str, Any]]:
    """Build a radio sequence from songs and optional timed interstitial categories."""
    media = sorted((dict(item) for item in media if item.get("active", True)),
                   key=lambda item: (int(item.get("sort_order", 0)), int(item.get("id", 0))))
    if not media:
        return []
    categories = {name: [item for item in media if item.get("category", "music") == name]
                  for name in ("music", "jingle", "sponsor", "commercial", "other")}
    songs = categories["music"]
    if not songs:
        result = media
        if folder.get("mode") == "random":
            random.shuffle(result)
        return result
    if folder.get("mode") == "random":
        random.shuffle(songs)
    intervals = {
        "jingle": int(folder.get("jingle_every", 0) or 0),
        "sponsor": int(folder.get("sponsor_every", 0) or 0),
        "commercial": int(folder.get("commercial_every", 0) or 0),
    }
    cursors = {name: 0 for name in intervals}
    result: list[dict[str, Any]] = []
    for count, song in enumerate(songs, start=1):
        result.append(song)
        for category in ("jingle", "sponsor", "commercial"):
            interval = intervals[category]
            items = categories[category]
            cursor = cursors[category]
            if interval > 0 and count % interval == 0 and cursor < len(items):
                result.append(items[cursor])
                cursors[category] += 1
    # Include assets whose interval is disabled, or extras beyond the configured count.
    for category in ("jingle", "sponsor", "commercial", "other"):
        result.extend(categories[category][cursors.get(category, 0):])
    return result


def probe_media(path: Path) -> tuple[float, str]:
    ffprobe = os.environ.get("FFPROBE_PATH") or shutil.which("ffprobe")
    if not ffprobe:
        raise RuntimeError("FFprobe não encontrado. Instale o FFmpeg e inclua ffprobe no PATH.")
    result = subprocess.run(
        [ffprobe, "-v", "error", "-show_entries", "format=duration:stream=codec_type,disposition", "-of", "json", str(path)],
        capture_output=True, text=True, timeout=30, check=False,
    )
    if result.returncode != 0:
        raise ValueError(result.stderr.strip() or "FFprobe não conseguiu ler o arquivo.")
    data = json.loads(result.stdout or "{}")
    duration = float((data.get("format") or {}).get("duration") or 0)
    has_video = any(
        s.get("codec_type") == "video" and not int((s.get("disposition") or {}).get("attached_pic", 0))
        for s in data.get("streams", [])
    )
    if duration <= 0:
        raise ValueError("Duração não identificada.")
    return duration, "video" if has_video else "audio"


def add_file(folder_id: int, path_text: str, order: int | None = None,
             source_root: Path | None = None) -> bool:
    path = Path(path_text).expanduser().resolve()
    if not path.is_file() or path.suffix.lower() not in SUPPORTED:
        return False
    try:
        duration, kind = probe_media(path)
    except (ValueError, RuntimeError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        log.warning("Não foi possível importar %s: %s", path, exc)
        return False
    with db() as con:
        if order is None:
            order = con.execute("SELECT COALESCE(MAX(sort_order),-1)+1 FROM media WHERE folder_id=?", (folder_id,)).fetchone()[0]
        existing = con.execute("SELECT id FROM media WHERE folder_id=? AND path=?", (folder_id, str(path))).fetchone()
        if existing:
            con.execute("UPDATE media SET name=?,kind=?,duration=? WHERE id=?",
                        (path.name, kind, duration, existing["id"]))
        else:
            con.execute("""INSERT INTO media(folder_id,name,path,kind,duration,sort_order,category)
                VALUES(?,?,?,?,?,?,?)""",
                (folder_id, path.name, str(path), kind, duration, order, infer_category(path, source_root)))
    return True


def folder_scan(folder_id: int) -> tuple[int, int]:
    folder = get_folder(folder_id)
    root = Path(folder["path"]).resolve()
    if not root.is_dir():
        raise HTTPException(400, "A pasta selecionada não está acessível.")
    iterator = root.rglob("*") if folder["recursive"] else root.iterdir()
    paths = sorted((p for p in iterator if p.is_file() and p.suffix.lower() in SUPPORTED), key=lambda p: str(p).casefold())
    present = {os.path.normcase(str(path.resolve())) for path in paths}
    added = 0
    for path in paths:
        if add_file(folder_id, str(path), source_root=root):
            added += 1
    # Remove da biblioteca arquivos apagados ou movidos para fora da pasta.
    # Mídias selecionadas manualmente de outros diretórios são preservadas.
    with db() as con:
        rows = con.execute("SELECT id,path FROM media WHERE folder_id=?", (folder_id,)).fetchall()
        stale_ids = []
        for row in rows:
            media_path = Path(row["path"]).resolve()
            try:
                media_path.relative_to(root)
            except ValueError:
                continue
            if os.path.normcase(str(media_path)) not in present:
                stale_ids.append((row["id"],))
        if stale_ids:
            con.executemany("DELETE FROM media WHERE id=?", stale_ids)
    return added, len(paths)


def list_library() -> list[dict[str, Any]]:
    with db() as con:
        folders = con.execute("""SELECT f.* FROM folders f
            WHERE NOT EXISTS (
                SELECT 1 FROM flow_programs fp
                WHERE fp.folder_id=f.id OR fp.jingle_folder_id=f.id OR fp.commercial_folder_id=f.id
            ) AND NOT EXISTS (
                SELECT 1 FROM flows fl
                WHERE fl.jingle_folder_id=f.id OR fl.commercial_folder_id=f.id
            )
            ORDER BY f.name COLLATE NOCASE,f.id""").fetchall()
        result = []
        for folder in folders:
            item = row_dict(folder)
            item["recursive"] = bool(item["recursive"])
            item["limit_seconds"] = float(item["limit_seconds"]) if item["limit_seconds"] is not None else None
            rows = con.execute("SELECT * FROM media WHERE folder_id=? ORDER BY sort_order,id", (item["id"],)).fetchall()
            item["media"] = [row_dict(r) for r in rows]
            for media in item["media"]:
                media["active"] = bool(media["active"])
                media["duration"] = float(media["duration"])
            result.append(item)
    return result


def as_seconds(hhmm: str) -> int:
    parsed = clock_time.fromisoformat(hhmm)
    return parsed.hour * 3600 + parsed.minute * 60 + parsed.second


def event_datetime(event: dict[str, Any], day: datetime) -> tuple[datetime, datetime]:
    start = datetime.combine(day.date(), clock_time.fromisoformat(event["start_time"])).astimezone()
    end = datetime.combine(day.date(), clock_time.fromisoformat(event["end_time"])).astimezone()
    if end <= start:
        end += timedelta(days=1)
    return start, end


def scheduled_events() -> list[dict[str, Any]]:
    with db() as con:
        rows = con.execute("""SELECT s.*,COALESCE(fl.name,f.name) AS folder_name FROM schedules s
            JOIN folders f ON f.id=s.folder_id LEFT JOIN flows fl ON fl.id=s.flow_id
            ORDER BY s.start_time,s.id""").fetchall()
    output = [row_dict(r) for r in rows]
    for event in output:
        event["weekdays"] = [int(d) for d in event["weekdays"].split(",") if d != ""]
        event["active"] = bool(event["active"])
    return output


def normalized_label(value: str) -> str:
    raw = value.casefold()
    return "".join(ch for ch in unicodedata.normalize("NFKD", raw) if not unicodedata.combining(ch))


def ensure_media_folder(path: Path, name: str, recursive: bool = True,
                        mode: str = "sequential") -> int:
    path = path.resolve()
    with db() as con:
        row = con.execute("SELECT id FROM folders WHERE path=? ORDER BY id LIMIT 1", (str(path),)).fetchone()
        if row:
            folder_id = int(row["id"])
            con.execute("UPDATE folders SET name=?,recursive=?,mode=? WHERE id=?",
                        (name, int(recursive), mode, folder_id))
        else:
            cur = con.execute("INSERT INTO folders(name,path,recursive,mode) VALUES(?,?,?,?)",
                              (name, str(path), int(recursive), mode))
            folder_id = int(cur.lastrowid)
    return folder_id


def import_source(folder_id: int, root: Path, recursive: bool = True) -> tuple[int, int]:
    root = root.resolve()
    if not root.is_dir():
        return 0, 0
    iterator = root.rglob("*") if recursive else root.iterdir()
    paths = sorted((p for p in iterator if p.is_file() and p.suffix.lower() in SUPPORTED),
                   key=lambda p: str(p).casefold())
    added = sum(1 for path in paths if add_file(folder_id, str(path), source_root=root))
    return added, len(paths)


def get_flow(flow_id: int) -> dict[str, Any]:
    with db() as con:
        row = con.execute("SELECT * FROM flows WHERE id=?", (flow_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Fluxo de programação não encontrado.")
    return row_dict(row)


def get_flow_programs(flow_id: int) -> list[dict[str, Any]]:
    with db() as con:
        rows = con.execute("""SELECT fp.*,f.name,f.path FROM flow_programs fp
            JOIN folders f ON f.id=fp.folder_id WHERE fp.flow_id=? ORDER BY fp.sort_order,fp.folder_id""",
            (flow_id,)).fetchall()
    output = [row_dict(row) for row in rows]
    for item in output:
        item["active"] = bool(item["active"])
    return output


def flow_snapshot(flow_id: int) -> dict[str, Any]:
    flow = get_flow(flow_id)
    flow["recursive"] = bool(flow["recursive"])
    programs = []
    program_duration = 0.0
    ad_duration = 0.0
    cycle_estimate = 0.0
    for item in get_flow_programs(flow_id):
        if not item["active"]:
            programs.append({**item, "media_count": 0, "duration": 0.0,
                             "jingle_count": 0, "jingle_duration": 0.0,
                             "commercial_count": 0, "commercial_duration": 0.0})
            continue
        media = get_folder_media(int(item["folder_id"]), active_only=True)
        duration = sum(float(m["duration"]) for m in media)
        program_duration += duration
        jingle_id = item["jingle_folder_id"] or flow["jingle_folder_id"]
        commercial_id = item["commercial_folder_id"] or flow["commercial_folder_id"]
        jingles = get_folder_media(int(jingle_id), active_only=True) if jingle_id else []
        commercials = get_folder_media(int(commercial_id), active_only=True) if commercial_id else []
        jingle_duration = sum(float(m["duration"]) for m in jingles)
        commercial_duration = sum(float(m["duration"]) for m in commercials)
        ad_duration += jingle_duration + commercial_duration
        cycle_estimate += duration * 100 / int(flow["program_share"]) if (jingle_duration + commercial_duration) and int(flow["program_share"]) < 100 else duration
        programs.append({**item, "media_count": len(media), "duration": duration,
                         "jingle_count": len(jingles), "jingle_duration": jingle_duration,
                         "commercial_count": len(commercials), "commercial_duration": commercial_duration})
    return {**flow, "programs": programs, "program_duration": program_duration,
            "interstitial_duration": ad_duration, "media_duration": program_duration + ad_duration,
            "cycle_duration_estimate": cycle_estimate}


def build_flow_queue(flow_id: int, ad_cursor: int = 0) -> tuple[list[dict[str, Any]], int]:
    flow = get_flow(flow_id)
    share = max(50, min(100, int(flow["program_share"])))
    queue: list[dict[str, Any]] = []
    for program in get_flow_programs(flow_id):
        if not program["active"]:
            continue
        program_media = get_folder_media(int(program["folder_id"]), active_only=True)
        if flow["mode"] == "random":
            random.shuffle(program_media)
        program_media = [dict(m, loop_mode="once", repeat_count=1) for m in program_media]
        jingle_id = program["jingle_folder_id"] or flow["jingle_folder_id"]
        commercial_id = program["commercial_folder_id"] or flow["commercial_folder_id"]
        jingles = get_folder_media(int(jingle_id), active_only=True) if jingle_id else []
        commercials = get_folder_media(int(commercial_id), active_only=True) if commercial_id else []
        ad_media = [dict(m, loop_mode="once", repeat_count=1) for m in (*jingles, *commercials)]
        program_seconds = 0.0
        ad_seconds = 0.0
        for media_index, media in enumerate(program_media):
            queue.append(media)
            program_seconds += float(media["duration"])
            target_ad_seconds = program_seconds * (100 - share) / share
            while ad_media:
                ad = ad_media[ad_cursor % len(ad_media)]
                next_ad_seconds = float(ad["duration"])
                current_error = abs(target_ad_seconds - ad_seconds)
                next_error = abs(target_ad_seconds - (ad_seconds + next_ad_seconds))
                fits_target = ad_seconds + next_ad_seconds <= target_ad_seconds + 0.01
                at_program_end = media_index == len(program_media) - 1
                if not fits_target and (not at_program_end or next_error + 0.01 >= current_error):
                    break
                queue.append(ad)
                ad_cursor += 1
                ad_seconds += next_ad_seconds
    return queue, ad_cursor


def directory_role(name: str) -> str:
    label = normalized_label(name).replace(" ", "")
    if "vinhet" in label or "jingle" in label:
        return "jingle"
    if any(term in label for term in ("comercia", "commercial", "anuncio", "anunci", "advert", "patroc", "sponsor")):
        return "commercial"
    if label in ("programas", "programs", "shows", "grade"):
        return "programs"
    if label in ("videos", "video", "conteudo", "midia", "media"):
        return "videos"
    return "other"


def has_media_files(path: Path, recursive: bool = False) -> bool:
    iterator = path.rglob("*") if recursive else path.iterdir()
    return any(file.is_file() and file.suffix.lower() in SUPPORTED for file in iterator)


def create_asset_group(flow_name: str, label: str, paths: list[Path], mode: str) -> int | None:
    if not paths:
        return None
    first = paths[0].resolve()
    folder_id = ensure_media_folder(first, f"{flow_name} · {label}", recursive=True, mode=mode)
    folder_scan(folder_id)
    for source in paths[1:]:
        import_source(folder_id, source, recursive=True)
    return folder_id


def create_flow_program(flow_name: str, program_root: Path, recursive: bool,
                        mode: str) -> dict[str, Any]:
    children = sorted((p for p in program_root.iterdir() if p.is_dir() and p.name != ".playout"),
                      key=lambda p: p.name.casefold())
    jingle_dirs = [p for p in children if directory_role(p.name) == "jingle"]
    commercial_dirs = [p for p in children if directory_role(p.name) == "commercial"]
    video_dirs = [p for p in children if directory_role(p.name) == "videos"]
    nested_program_dirs = [p for p in children if directory_role(p.name) == "programs"]
    other_dirs = [p for p in children if directory_role(p.name) == "other"]
    video_sources: list[tuple[Path, bool]] = []
    if nested_program_dirs:
        for container in nested_program_dirs:
            inner = sorted((p for p in container.iterdir() if p.is_dir()), key=lambda p: p.name.casefold())
            if has_media_files(container, recursive=False):
                video_sources.append((container, False))
            video_sources.extend((p, recursive) for p in inner)
    else:
        if has_media_files(program_root, recursive=False):
            video_sources.append((program_root, False))
        video_sources.extend((p, recursive) for p in video_dirs)
        if not video_dirs and not nested_program_dirs:
            video_sources.extend((p, recursive) for p in other_dirs)
    # If this program folder itself is the chosen video directory, scan it recursively
    # only when it contains no category subfolders that must stay separate.
    if not video_sources:
        video_sources.append((program_root, False))
    deduped: list[tuple[Path, bool]] = []
    seen: set[str] = set()
    for source, recurse in video_sources:
        key = os.path.normcase(str(source.resolve()))
        if key not in seen:
            seen.add(key)
            deduped.append((source, recurse))
    primary, primary_recursive = deduped[0]
    media_folder_id = ensure_media_folder(primary, f"{flow_name} · {program_root.name}",
                                          recursive=primary_recursive, mode=mode)
    if primary.is_dir():
        folder_scan(media_folder_id)
    for source, source_recursive in deduped[1:]:
        import_source(media_folder_id, source, recursive=source_recursive)
    jingle_folder_id = create_asset_group(flow_name + " · " + program_root.name, "Vinhetas", jingle_dirs, mode)
    commercial_folder_id = create_asset_group(flow_name + " · " + program_root.name, "Comerciais", commercial_dirs, mode)
    return {"folder_id": media_folder_id, "jingle_folder_id": jingle_folder_id,
            "commercial_folder_id": commercial_folder_id, "name": program_root.name}


def is_event_active(event: dict[str, Any], now: datetime) -> bool:
    if not event["active"]:
        return False
    # Inspect today's and yesterday's start date to support overnight blocks.
    for day_offset in (0, -1):
        day = now + timedelta(days=day_offset)
        weekday = day.weekday()
        if weekday not in event["weekdays"]:
            continue
        start, end = event_datetime(event, day)
        if start <= now < end:
            return True
    return False


def local_now() -> datetime:
    return datetime.now().astimezone()


def find_overlaps(candidate: dict[str, Any], exclude_id: int | None = None) -> list[str]:
    existing = scheduled_events()
    cand_days = set(candidate["weekdays"])
    cstart, cend = as_seconds(candidate["start_time"]), as_seconds(candidate["end_time"])
    cand_overnight = cend <= cstart
    conflict = []
    for event in existing:
        if not event["active"] or event["id"] == exclude_id:
            continue
        estart, eend = as_seconds(event["start_time"]), as_seconds(event["end_time"])
        eovernight = eend <= estart
        edays = set(event["weekdays"])
        for d in cand_days:
            # Compare intervals on a 3-day axis anchored at d; account for prior overnight events.
            c0, c1 = d * 86400 + cstart, d * 86400 + cend + (86400 if cand_overnight else 0)
            for ed in range(7):
                if ed not in edays:
                    continue
                e0, e1 = ed * 86400 + estart, ed * 86400 + eend + (86400 if eovernight else 0)
                for shift in (-7 * 86400, 0, 7 * 86400):
                    if max(c0, e0 + shift) < min(c1, e1 + shift):
                        conflict.append(event["name"])
                        break
                if event["name"] in conflict:
                    break
            if event["name"] in conflict:
                break
    return sorted(set(conflict))


def pick_native(kind: str) -> list[str] | str | None:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except ImportError as exc:
        raise RuntimeError("O seletor nativo do Windows não está disponível; use o campo de caminho manual.") from exc
    root = tk.Tk()
    root.withdraw()
    try:
        root.attributes("-topmost", True)
        root.update()
        if kind == "folder":
            selected = filedialog.askdirectory(title="Escolha uma pasta de programas e mídias", parent=root)
            return selected or None
        selected = filedialog.askopenfilenames(
            title="Escolha um ou mais arquivos de áudio/vídeo",
            filetypes=[("Mídias compatíveis", "*.mp4 *.webm *.mov *.mp3 *.wav *.ogg"), ("Todos os arquivos", "*.*")],
            parent=root,
        )
        return list(selected) or None
    finally:
        root.destroy()


class FolderIn(BaseModel):
    name: str | None = None
    path: str
    recursive: bool = True
    mode: str = "sequential"
    limit_seconds: float | None = Field(default=None, ge=1)
    jingle_every: int = Field(default=0, ge=0, le=999)
    sponsor_every: int = Field(default=0, ge=0, le=999)
    commercial_every: int = Field(default=0, ge=0, le=999)


class FolderUpdate(BaseModel):
    name: str | None = None
    mode: str | None = None
    limit_seconds: float | None = None
    jingle_every: int | None = Field(default=None, ge=0, le=999)
    sponsor_every: int | None = Field(default=None, ge=0, le=999)
    commercial_every: int | None = Field(default=None, ge=0, le=999)


class FolderPathIn(BaseModel):
    path: str
    recursive: bool = True


class FlowImportIn(BaseModel):
    root_path: str
    name: str | None = None
    recursive: bool = True
    mode: str = "sequential"
    program_share: int = Field(default=90, ge=50, le=100)


class FlowUpdateIn(BaseModel):
    name: str | None = None
    mode: str | None = None
    program_share: int | None = Field(default=None, ge=50, le=100)
    recursive: bool | None = None


class FlowOrderIn(BaseModel):
    folder_ids: list[int]


class FlowProgramUpdateIn(BaseModel):
    active: bool


class FlowProgramCreateIn(BaseModel):
    name: str


class FlowFilesImportIn(BaseModel):
    category: str
    paths: list[str]


class FilesImport(BaseModel):
    folder_id: int | None = None
    paths: list[str]


class MediaUpdate(BaseModel):
    active: bool | None = None
    loop_mode: str | None = None
    repeat_count: int | None = Field(default=None, ge=1, le=999)
    category: str | None = None


class OrderIn(BaseModel):
    ids: list[int]


class ScheduleIn(BaseModel):
    name: str
    folder_id: int | None = None
    flow_id: int | None = None
    kind: str = "program"
    weekdays: list[int] = Field(min_length=1, max_length=7)
    start_time: str
    end_time: str
    transition: str = "cut"
    active: bool = True


class SettingsIn(BaseModel):
    background_path: str | None = None


class ConnectionHub:
    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()
        self.lock = asyncio.Lock()

    async def add(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self.lock:
            self.clients.add(ws)

    async def remove(self, ws: WebSocket) -> None:
        async with self.lock:
            self.clients.discard(ws)

    async def send_all(self, payload: dict[str, Any]) -> None:
        message = json.dumps(payload, ensure_ascii=False)
        async with self.lock:
            clients = list(self.clients)
        failed = []
        for ws in clients:
            try:
                await ws.send_text(message)
            except Exception:
                failed.append(ws)
        if failed:
            async with self.lock:
                self.clients.difference_update(failed)


hub = ConnectionHub()


class PlayoutEngine:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.current: dict[str, Any] | None = None
        self.queue: list[dict[str, Any]] = []
        self.queue_index = 0
        self.repeat_number = 0
        self.state = "stopped"
        self.position = 0.0
        self.last_progress_at = time.monotonic()
        self.source = "manual"
        self.session_deadline: float | None = None
        self.active_event: dict[str, Any] | None = None
        self.pending_event: dict[str, Any] | None = None
        self.flow_ad_cursor = 0
        self.triggered: set[str] = set()
        self.token = 0
        self.message = "Pronto para iniciar"
        self.random = random.Random()

    def snapshot(self) -> dict[str, Any]:
        position = self.position
        if self.state == "playing" and self.current:
            position += max(0.0, time.monotonic() - self.last_progress_at)
            position = min(position, float(self.current["duration"])) if self.current["duration"] else position
        return {
            "type": "state", "state": self.state, "current": self.current,
            "position": position, "queue_index": self.queue_index,
            "queue_length": len(self.queue), "source": self.source,
            "automation": get_setting("automation", "0") == "1",
            "active_event": self.active_event["name"] if self.active_event else None,
            "pending_event": self.pending_event["name"] if self.pending_event else None,
            "message": self.message, "token": self.token,
            "background_url": "/background" if get_setting("background_path") else "",
        }

    async def publish(self) -> None:
        await hub.send_all(self.snapshot())

    async def start_queue(self, media: list[dict[str, Any]], source: str,
                          deadline: float | None = None, event: dict[str, Any] | None = None) -> None:
        async with self.lock:
            self.queue = [m for m in media if m.get("active", True)]
            self.queue_index = 0
            self.repeat_number = 0
            self.source = source
            self.session_deadline = deadline
            self.active_event = event
            self.pending_event = None
            if not self.queue:
                await self._stop_locked("O bloco não tem arquivos ativos.")
            else:
                await self._start_current_locked()
        await self.publish()

    async def _start_current_locked(self) -> None:
        if self.queue_index >= len(self.queue):
            await self._stop_locked("Fim da programação.")
            return
        item = dict(self.queue[self.queue_index])
        self.current = item
        self.state = "playing"
        self.position = 0.0
        self.last_progress_at = time.monotonic()
        self.repeat_number = 1
        self.token += 1
        self.message = "Reproduzindo"

    async def _stop_locked(self, message: str = "Parado") -> None:
        self.current = None
        self.queue = []
        self.queue_index = 0
        self.repeat_number = 0
        self.position = 0.0
        self.state = "stopped"
        self.session_deadline = None
        self.active_event = None
        self.pending_event = None
        self.message = message
        self.token += 1

    async def stop(self, message: str = "Parado") -> None:
        async with self.lock:
            await self._stop_locked(message)
        await self.publish()

    async def pause_toggle(self, pause: bool | None = None) -> None:
        async with self.lock:
            if not self.current:
                return
            should_pause = pause if pause is not None else self.state == "playing"
            self.state = "paused" if should_pause else "playing"
            self.last_progress_at = time.monotonic()
            self.message = "Pausado" if should_pause else "Reproduzindo"
        await hub.send_all({"type": "command", "command": "pause" if should_pause else "resume"})
        await self.publish()

    async def next(self) -> None:
        async with self.lock:
            if self.pending_event:
                pending = dict(self.pending_event)
                start_at = pending.pop("_start_at")
                end_at = pending.pop("_end_at")
                media = pending.pop("_media")
                await self.start_scheduled_locked(pending, media, start_at, end_at)
            else:
                await self._advance_locked()
        await self.publish()

    async def _advance_locked(self) -> None:
        self.repeat_number = 1
        self.queue_index += 1
        if self.queue_index >= len(self.queue):
            should_cycle = (
                self.session_deadline is not None
                and time.monotonic() < self.session_deadline
                and (
                    self.source == "manual"
                    or (self.source == "schedule" and self.active_event is not None
                        and self.active_event.get("kind") in ("program", "flow"))
                )
            )
            if should_cycle:
                if self.source == "schedule" and self.active_event and self.active_event.get("kind") == "flow":
                    self.queue, self.flow_ad_cursor = build_flow_queue(
                        int(self.active_event["flow_id"]), self.flow_ad_cursor)
                    if not self.queue:
                        self.current = None
                        self.state = "stopped"
                        self.message = "O fluxo não tem arquivos ativos."
                        return
                self.queue_index = 0
                self.message = "Reiniciando a sequência até o fim do horário."
                await self._start_current_locked()
            else:
                self.current = None
                self.state = "stopped"
                self.position = 0.0
                self.message = "Fim da programação."
        else:
            await self._start_current_locked()

    async def on_ended(self, token: int) -> None:
        async with self.lock:
            # An ended event is authoritative for the current media token. The
            # browser may mark the element paused immediately before emitting it.
            if not self.current or token != self.token:
                return
            if self.pending_event:
                pending = dict(self.pending_event)
                start_at = pending.pop("_start_at")
                end_at = pending.pop("_end_at")
                media = pending.pop("_media")
                await self.start_scheduled_locked(pending, media, start_at, end_at)
            else:
                mode = self.current.get("loop_mode", "once")
                count = int(self.current.get("repeat_count", 1) or 1)
                if mode == "infinite" or (mode == "repeat" and self.repeat_number < count):
                    self.repeat_number += 1
                    self.position = 0.0
                    self.last_progress_at = time.monotonic()
                    self.token += 1
                else:
                    await self._advance_locked()
        await self.publish()

    async def report_progress(self, token: int, position: float, state: str | None = None) -> None:
        async with self.lock:
            if token != self.token or not self.current:
                return
            self.position = max(0.0, float(position))
            self.last_progress_at = time.monotonic()
            if state in ("playing", "paused"):
                self.state = state

    async def schedule_start(self, event: dict[str, Any], now: datetime,
                             start: datetime, end: datetime) -> None:
        flow_id = event.get("flow_id")
        folder = None
        if flow_id:
            media, next_ad_cursor = build_flow_queue(int(flow_id), 0)
            event = {**event, "_flow_ad_cursor": next_ad_cursor}
        else:
            folder_id = int(event["folder_id"])
            media = get_folder_media(folder_id, active_only=True)
            folder = get_folder(folder_id)
            for item in media:
                item["folder_mode"] = folder["mode"]
        if not media:
            self.message = f"{event['name']}: não há arquivos ativos."
            await self.publish()
            return
        deadline = time.monotonic() + max(0.0, (end - now).total_seconds())
        if folder and folder.get("limit_seconds"):
            deadline = min(deadline, time.monotonic() + folder["limit_seconds"])
            end = min(end, now + timedelta(seconds=folder["limit_seconds"]))
        async with self.lock:
            if event["transition"] == "wait" and self.current and self.state in ("playing", "paused"):
                self.pending_event = dict(event)
                self.pending_event.update({"_start_at": start, "_end_at": end, "_media": media})
                self.message = f"Aguardando o fim da mídia para iniciar: {event['name']}"
            else:
                await self.start_scheduled_locked(event, media, start, end)
        await self.publish()

    async def start_scheduled_locked(self, event: dict[str, Any], media: list[dict[str, Any]], start: datetime, end: datetime) -> None:
        now = local_now()
        if now >= end:
            await self._stop_locked("O horário do bloco terminou antes da transição.")
            return
        flow_id = event.get("flow_id")
        folder = get_folder(int(event["folder_id"])) if not flow_id else None
        if flow_id:
            self.queue = [dict(m, loop_mode="once", repeat_count=1) for m in media if m.get("active", True)]
            self.flow_ad_cursor = int(event.get("_flow_ad_cursor", 0))
        elif event["kind"] == "commercial":
            self.queue = sorted((dict(m) for m in media if m.get("active", True)),
                                key=lambda m: (int(m.get("sort_order", 0)), int(m.get("id", 0))))
            for item in self.queue:
                item["loop_mode"] = "once"
                item["repeat_count"] = 1
        else:
            self.queue = compose_program(media, folder)
            # The scheduled program cycles its complete sequence until the slot ends.
            # Per-file loops are disabled here so jingles and sponsors keep their place.
            for item in self.queue:
                item["loop_mode"] = "once"
                item["repeat_count"] = 1
        self.queue_index = 0
        self.repeat_number = 0
        self.source = "schedule"
        remaining = (end - now).total_seconds()
        if folder and folder.get("limit_seconds"):
            remaining = min(remaining, float(folder["limit_seconds"]))
        self.session_deadline = time.monotonic() + max(0.0, remaining)
        self.active_event = event
        self.pending_event = None
        if not self.queue:
            self.current = None
            self.state = "stopped"
            self.message = "Bloco sem mídias ativas."
        else:
            await self._start_current_locked()

    async def scheduler_tick(self) -> None:
        now = local_now()
        # Clean stale trigger keys to keep the set bounded.
        today_key = now.strftime("%Y-%m-%d")
        self.triggered = {key for key in self.triggered if key.startswith(today_key) or key.startswith((now - timedelta(days=1)).strftime("%Y-%m-%d"))}
        if get_setting("automation", "0") == "1":
            events = scheduled_events()
            for event in events:
                if not event["active"]:
                    continue
                for day_offset in (0, -1):
                    day = now + timedelta(days=day_offset)
                    if day.weekday() not in event["weekdays"]:
                        continue
                    start, end = event_datetime(event, day)
                    occurrence = f"{start.date().isoformat()}:{event['id']}:{event['start_time']}"
                    if start <= now < end and occurrence not in self.triggered:
                        self.triggered.add(occurrence)
                        await self.schedule_start(event, now, start, end)
                        break
        async with self.lock:
            if self.active_event:
                deadline_reached = self.session_deadline is not None and time.monotonic() >= self.session_deadline
                if deadline_reached and self.pending_event:
                    # A transição suave já está esperando o arquivo atual terminar.
                    # Solte o relógio do bloco anterior para que ele não corte o arquivo.
                    self.active_event = None
                    self.session_deadline = None
                elif deadline_reached:
                    await self._stop_locked(f"Fim do bloco: {self.active_event['name']}")
            elif self.session_deadline is not None and time.monotonic() >= self.session_deadline:
                await self._stop_locked("Limite de duração atingido.")
            if self.pending_event:
                start_at = self.pending_event.get("_start_at")
                end_at = self.pending_event.get("_end_at")
                if end_at and now >= end_at:
                    if self.source == "schedule":
                        await self._stop_locked("O bloco pendente expirou antes do fim da mídia.")
                    else:
                        self.pending_event = None
                        self.message = "Bloco pendente expirou antes do fim da mídia."
        await self.publish()


engine = PlayoutEngine()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    scheduler = AsyncIOScheduler(timezone=local_now().tzinfo)
    scheduler.add_job(engine.scheduler_tick, "interval", seconds=1, id="playout-clock", max_instances=1, coalesce=True)
    scheduler.start()
    app.state.scheduler = scheduler
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="Playout OBS", version="1.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", response_class=HTMLResponse)
async def home() -> HTMLResponse:
    return HTMLResponse('<meta http-equiv="refresh" content="0;url=/admin">')


@app.get("/admin", response_class=HTMLResponse)
async def admin_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "admin.html")


@app.get("/player", response_class=HTMLResponse)
async def player_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "player.html")


@app.get("/api/library")
async def api_library() -> list[dict[str, Any]]:
    return list_library()


@app.get("/api/state")
async def api_state() -> dict[str, Any]:
    return engine.snapshot()


@app.get("/api/settings")
async def api_settings() -> dict[str, Any]:
    return {"background_path": get_setting("background_path"), "background_url": "/background" if get_setting("background_path") else ""}


@app.put("/api/settings")
async def update_settings(payload: SettingsIn) -> dict[str, Any]:
    if payload.background_path is not None:
        path_text = payload.background_path.strip()
        if path_text:
            path = Path(path_text).expanduser().resolve()
            if not path.is_file() or path.suffix.lower() not in IMAGE_TYPES:
                raise HTTPException(400, "Escolha um arquivo de imagem válido (.png, .jpg, .webp ou .gif).")
            path_text = str(path)
        set_setting("background_path", path_text)
    await engine.publish()
    return await api_settings()


@app.get("/background")
async def background() -> FileResponse:
    path = get_setting("background_path")
    if not path or not Path(path).is_file():
        raise HTTPException(404, "Imagem de fundo não configurada.")
    return FileResponse(path)


@app.post("/api/picker/folder")
async def picker_folder() -> dict[str, Any]:
    try:
        selected = await asyncio.to_thread(pick_native, "folder")
    except RuntimeError as exc:
        raise HTTPException(503, str(exc)) from exc
    return {"path": selected}


@app.post("/api/picker/files")
async def picker_files() -> dict[str, Any]:
    try:
        selected = await asyncio.to_thread(pick_native, "files")
    except RuntimeError as exc:
        raise HTTPException(503, str(exc)) from exc
    return {"paths": selected or []}


@app.post("/api/folders")
async def create_folder(payload: FolderIn) -> dict[str, Any]:
    path = Path(payload.path).expanduser().resolve()
    if not path.is_dir():
        raise HTTPException(400, "Escolha uma pasta existente e acessível.")
    if payload.mode not in ("sequential", "random"):
        raise HTTPException(400, "Modo deve ser sequential ou random.")
    name = (payload.name or path.name or str(path)).strip()
    with db() as con:
        existing = con.execute("SELECT id FROM folders WHERE path=?", (str(path),)).fetchone()
        if existing:
            folder_id = int(existing["id"])
            con.execute("""UPDATE folders SET name=?,recursive=?,mode=?,limit_seconds=?,
                jingle_every=?,sponsor_every=?,commercial_every=? WHERE id=?""",
                (name, int(payload.recursive), payload.mode, payload.limit_seconds,
                 payload.jingle_every, payload.sponsor_every, payload.commercial_every, folder_id))
        else:
            cur = con.execute("""INSERT INTO folders(name,path,recursive,mode,limit_seconds,
                jingle_every,sponsor_every,commercial_every) VALUES(?,?,?,?,?,?,?,?)""",
                (name, str(path), int(payload.recursive), payload.mode, payload.limit_seconds,
                 payload.jingle_every, payload.sponsor_every, payload.commercial_every))
            folder_id = int(cur.lastrowid)
    added, found = folder_scan(folder_id)
    await engine.publish()
    return {"folder": get_folder(folder_id), "added": added, "found": found}


@app.post("/api/flows/import-root")
async def import_flow_root(payload: FlowImportIn) -> dict[str, Any]:
    root = Path(payload.root_path).expanduser().resolve()
    if not root.is_dir():
        raise HTTPException(400, "Escolha uma pasta raiz existente.")
    if payload.mode not in ("sequential", "random"):
        raise HTTPException(400, "Modo deve ser sequential ou random.")
    previous_db = DB_PATH.resolve()
    switch_workspace(root)
    if DB_PATH.resolve() != previous_db:
        engine.triggered.clear()
        await engine.stop("Pasta de dados alterada. Confira a programação antes de colocar no ar.")
        set_setting("automation", "0")
    flow_name = (payload.name or root.name or "Programação").strip()
    directories = sorted((p for p in root.iterdir() if p.is_dir() and p.name != ".playout"),
                         key=lambda p: p.name.casefold())
    global_jingles = [p for p in directories if directory_role(p.name) == "jingle"]
    global_commercials = [p for p in directories if directory_role(p.name) == "commercial"]
    program_container = next((p for p in directories if directory_role(p.name) == "programs"), None)
    if program_container:
        program_paths = sorted((p for p in program_container.iterdir() if p.is_dir()),
                               key=lambda p: p.name.casefold())
        if has_media_files(program_container, recursive=False):
            program_paths.insert(0, program_container)
    else:
        has_video_container = any(directory_role(p.name) == "videos" for p in directories)
        other_program_dirs = [p for p in directories if directory_role(p.name) not in ("jingle", "commercial", "videos")]
        if has_video_container:
            program_paths = [root, *other_program_dirs]
        else:
            program_paths = [p for p in directories if directory_role(p.name) not in ("jingle", "commercial")]
        if has_media_files(root, recursive=False) and root not in program_paths:
            program_paths.insert(0, root)
    global_jingle_id = create_asset_group(flow_name, "Vinhetas", global_jingles, payload.mode)
    global_commercial_id = create_asset_group(flow_name, "Comerciais", global_commercials, payload.mode)
    with db() as con:
        current = con.execute("SELECT id FROM flows WHERE root_path=?", (str(root),)).fetchone()
        if current:
            flow_id = int(current["id"])
            con.execute("""UPDATE flows SET name=?,mode=?,recursive=?,program_share=?,jingle_folder_id=?,commercial_folder_id=?
                WHERE id=?""", (flow_name, payload.mode, int(payload.recursive), payload.program_share, global_jingle_id,
                                 global_commercial_id, flow_id))
            old_rows = con.execute("SELECT folder_id,active FROM flow_programs WHERE flow_id=? ORDER BY sort_order",
                                   (flow_id,)).fetchall()
            old_order = [int(r["folder_id"]) for r in old_rows]
            old_active = {int(r["folder_id"]): int(r["active"]) for r in old_rows}
        else:
            cur = con.execute("""INSERT INTO flows(name,root_path,mode,recursive,program_share,jingle_folder_id,commercial_folder_id)
                VALUES(?,?,?,?,?,?,?)""", (flow_name, str(root), payload.mode, int(payload.recursive), payload.program_share,
                                           global_jingle_id, global_commercial_id))
            flow_id = int(cur.lastrowid)
            old_order = []
            old_active = {}
    discovered = [create_flow_program(flow_name, path, payload.recursive, payload.mode) for path in program_paths]
    discovered_ids = [int(item["folder_id"]) for item in discovered]
    ordered_ids = [folder_id for folder_id in old_order if folder_id in discovered_ids]
    ordered_ids.extend(folder_id for folder_id in discovered_ids if folder_id not in ordered_ids)
    by_id = {int(item["folder_id"]): item for item in discovered}
    with db() as con:
        con.execute("DELETE FROM flow_programs WHERE flow_id=?", (flow_id,))
        for order, folder_id in enumerate(ordered_ids):
            item = by_id[folder_id]
            con.execute("""INSERT INTO flow_programs(flow_id,folder_id,jingle_folder_id,commercial_folder_id,sort_order,active)
                VALUES(?,?,?,?,?,?)""", (flow_id, folder_id, item["jingle_folder_id"],
                                         item["commercial_folder_id"], order, old_active.get(folder_id, 1)))
    await engine.publish()
    return flow_snapshot(flow_id)


@app.get("/api/flows")
async def api_flows() -> list[dict[str, Any]]:
    with db() as con:
        ids = [int(r["id"]) for r in con.execute("SELECT id FROM flows ORDER BY name COLLATE NOCASE,id").fetchall()]
    return [flow_snapshot(flow_id) for flow_id in ids]


@app.post("/api/flows/{flow_id}/play")
async def play_flow(flow_id: int) -> dict[str, Any]:
    media, _ = build_flow_queue(flow_id, 0)
    if not media:
        raise HTTPException(400, "Este fluxo não tem arquivos ativos.")
    await engine.start_queue(media, "manual")
    return engine.snapshot()


@app.get("/api/flows/{flow_id}")
async def api_flow(flow_id: int) -> dict[str, Any]:
    return flow_snapshot(flow_id)


@app.put("/api/flows/{flow_id}")
async def update_flow(flow_id: int, payload: FlowUpdateIn) -> dict[str, Any]:
    flow = get_flow(flow_id)
    name = (payload.name or flow["name"]).strip()
    mode = payload.mode or flow["mode"]
    share = payload.program_share if payload.program_share is not None else flow["program_share"]
    recursive = int(payload.recursive) if payload.recursive is not None else int(flow["recursive"])
    if mode not in ("sequential", "random"):
        raise HTTPException(400, "Modo deve ser sequential ou random.")
    with db() as con:
        con.execute("UPDATE flows SET name=?,mode=?,recursive=?,program_share=? WHERE id=?",
                    (name, mode, recursive, share, flow_id))
    await engine.publish()
    return flow_snapshot(flow_id)


@app.put("/api/flows/{flow_id}/order")
async def reorder_flow_programs(flow_id: int, payload: FlowOrderIn) -> dict[str, Any]:
    get_flow(flow_id)
    current = [int(p["folder_id"]) for p in get_flow_programs(flow_id)]
    if set(payload.folder_ids) != set(current) or len(payload.folder_ids) != len(current):
        raise HTTPException(400, "A ordem deve conter cada programa do fluxo uma única vez.")
    with db() as con:
        con.executemany("UPDATE flow_programs SET sort_order=? WHERE flow_id=? AND folder_id=?",
                        [(i, flow_id, folder_id) for i, folder_id in enumerate(payload.folder_ids)])
    return flow_snapshot(flow_id)


@app.post("/api/flows/{flow_id}/programs")
async def create_flow_program_endpoint(flow_id: int, payload: FlowProgramCreateIn) -> dict[str, Any]:
    flow = get_flow(flow_id)
    name = payload.name.strip().rstrip(" .")
    if not name or name in (".", "..") or any(char in name for char in '<>:"/\\|?*'):
        raise HTTPException(400, "Informe um nome de programa válido para uma pasta do Windows.")
    if normalized_label(name).split(".")[0] in {"con", "prn", "aux", "nul", *(f"com{i}" for i in range(1, 10)), *(f"lpt{i}" for i in range(1, 10))}:
        raise HTTPException(400, "Esse nome é reservado pelo Windows.")
    root = Path(flow["root_path"]).resolve()
    if not root.is_dir():
        raise HTTPException(400, "A pasta raiz do fluxo não está acessível.")
    program_parent = next((p for p in root.iterdir() if p.is_dir() and directory_role(p.name) == "programs"), root)
    program_path = program_parent / name
    if program_path.exists():
        raise HTTPException(409, "Já existe uma pasta com esse nome na raiz escolhida.")
    (program_path / "Videos").mkdir(parents=True)
    (program_path / "Vinhetas").mkdir()
    (program_path / "Comerciais").mkdir()
    item = create_flow_program(flow["name"], program_path, bool(flow["recursive"]), flow["mode"])
    with db() as con:
        next_order = int(con.execute("SELECT COALESCE(MAX(sort_order),-1)+1 FROM flow_programs WHERE flow_id=?",
                                     (flow_id,)).fetchone()[0])
        con.execute("""INSERT INTO flow_programs(flow_id,folder_id,jingle_folder_id,commercial_folder_id,sort_order,active)
            VALUES(?,?,?,?,?,1)""", (flow_id, item["folder_id"], item["jingle_folder_id"],
                                      item["commercial_folder_id"], next_order))
    await engine.publish()
    return flow_snapshot(flow_id)


@app.post("/api/flows/{flow_id}/programs/{folder_id}/files")
async def import_flow_program_files(flow_id: int, folder_id: int,
                                    payload: FlowFilesImportIn) -> dict[str, Any]:
    flow = get_flow(flow_id)
    program = next((p for p in get_flow_programs(flow_id) if int(p["folder_id"]) == folder_id), None)
    if not program:
        raise HTTPException(404, "Programa não pertence a este fluxo.")
    category = payload.category
    if category not in ("videos", "jingles", "commercials"):
        raise HTTPException(400, "Escolha vídeos, vinhetas ou comerciais.")
    if not payload.paths:
        raise HTTPException(400, "Nenhum arquivo foi selecionado.")
    field = {"videos": "folder_id", "jingles": "jingle_folder_id", "commercials": "commercial_folder_id"}[category]
    target_id = int(program[field]) if program.get(field) else None
    if target_id is None:
        video_path = Path(program["path"]).resolve()
        program_path = video_path.parent if directory_role(video_path.name) == "videos" else video_path
        child_name = {"jingles": "Vinhetas", "commercials": "Comerciais"}[category]
        destination = program_path / child_name
        destination.mkdir(parents=True, exist_ok=True)
        target_id = ensure_media_folder(destination, f"{flow['name']} · {program['name']} · {child_name}",
                                        recursive=True, mode=flow["mode"])
        with db() as con:
            con.execute(f"UPDATE flow_programs SET {field}=? WHERE flow_id=? AND folder_id=?",
                        (target_id, flow_id, folder_id))
    target_folder = get_folder(target_id)
    destination = Path(target_folder["path"]).resolve()
    root = Path(flow["root_path"]).resolve()
    try:
        destination.relative_to(root)
    except ValueError as exc:
        raise HTTPException(400, "A pasta de destino precisa ficar dentro da raiz do fluxo.") from exc
    destination.mkdir(parents=True, exist_ok=True)
    added, copied, invalid = 0, 0, []
    for path_text in payload.paths:
        source = Path(path_text).expanduser().resolve()
        if not source.is_file() or source.suffix.lower() not in SUPPORTED:
            invalid.append(source.name)
            continue
        dest = destination / source.name
        if source != dest.resolve():
            suffix = 2
            while dest.exists():
                dest = destination / f"{source.stem} ({suffix}){source.suffix}"
                suffix += 1
            try:
                shutil.copy2(source, dest)
            except OSError:
                invalid.append(source.name)
                continue
            copied += 1
        if add_file(target_id, str(dest), source_root=destination):
            added += 1
        else:
            invalid.append(source.name)
            if dest.exists() and dest.resolve() != source:
                dest.unlink(missing_ok=True)
    await engine.publish()
    return {"added": added, "copied": copied, "invalid": invalid,
            "program": program["name"], "category": category}


@app.put("/api/flows/{flow_id}/programs/{folder_id}")
async def update_flow_program(flow_id: int, folder_id: int,
                              payload: FlowProgramUpdateIn) -> dict[str, Any]:
    get_flow(flow_id)
    with db() as con:
        cur = con.execute("UPDATE flow_programs SET active=? WHERE flow_id=? AND folder_id=?",
                          (int(payload.active), flow_id, folder_id))
    if cur.rowcount == 0:
        raise HTTPException(404, "Programa não pertence a este fluxo.")
    await engine.publish()
    return flow_snapshot(flow_id)


@app.delete("/api/flows/{flow_id}")
async def delete_flow(flow_id: int) -> dict[str, bool]:
    get_flow(flow_id)
    with db() as con:
        con.execute("DELETE FROM flows WHERE id=?", (flow_id,))
    if engine.active_event and engine.active_event.get("flow_id") == flow_id:
        await engine.stop("O fluxo atual foi removido.")
    return {"ok": True}


@app.post("/api/folders/{folder_id}/scan")
async def rescan_folder(folder_id: int) -> dict[str, Any]:
    added, found = folder_scan(folder_id)
    await engine.publish()
    return {"added": added, "found": found}


@app.post("/api/folders/{folder_id}/import-path")
async def import_folder_path(folder_id: int, payload: FolderPathIn) -> dict[str, Any]:
    get_folder(folder_id)
    root = Path(payload.path).expanduser().resolve()
    if not root.is_dir():
        raise HTTPException(400, "Escolha uma pasta existente e acessível.")
    iterator = root.rglob("*") if payload.recursive else root.iterdir()
    paths = sorted((p for p in iterator if p.is_file() and p.suffix.lower() in SUPPORTED),
                   key=lambda p: str(p).casefold())
    added = sum(1 for path in paths if add_file(folder_id, str(path), source_root=root))
    await engine.publish()
    return {"added": added, "found": len(paths), "source": str(root.name)}


@app.put("/api/folders/{folder_id}")
async def update_folder(folder_id: int, payload: FolderUpdate) -> dict[str, Any]:
    folder = get_folder(folder_id)
    name = payload.name.strip() if payload.name is not None else folder["name"]
    mode = payload.mode or folder["mode"]
    if mode not in ("sequential", "random"):
        raise HTTPException(400, "Modo de reprodução inválido.")
    limit = payload.limit_seconds if "limit_seconds" in payload.model_fields_set else folder["limit_seconds"]
    if limit is not None and limit < 1:
        raise HTTPException(400, "O limite deve ser de pelo menos 1 segundo.")
    jingle_every = (payload.jingle_every or 0) if "jingle_every" in payload.model_fields_set else folder["jingle_every"]
    sponsor_every = (payload.sponsor_every or 0) if "sponsor_every" in payload.model_fields_set else folder["sponsor_every"]
    commercial_every = (payload.commercial_every or 0) if "commercial_every" in payload.model_fields_set else folder["commercial_every"]
    with db() as con:
        con.execute("""UPDATE folders SET name=?,mode=?,limit_seconds=?,jingle_every=?,
            sponsor_every=?,commercial_every=? WHERE id=?""",
            (name, mode, limit, jingle_every, sponsor_every, commercial_every, folder_id))
    await engine.publish()
    return get_folder(folder_id)


@app.delete("/api/folders/{folder_id}")
async def delete_folder(folder_id: int) -> dict[str, bool]:
    get_folder(folder_id)
    with db() as con:
        con.execute("DELETE FROM folders WHERE id=?", (folder_id,))
    if engine.active_event and int(engine.active_event["folder_id"]) == folder_id:
        await engine.stop("O programa atual foi removido.")
    await engine.publish()
    return {"ok": True}


@app.post("/api/files/import")
async def import_files(payload: FilesImport) -> dict[str, Any]:
    if not payload.paths:
        raise HTTPException(400, "Nenhum arquivo selecionado.")
    folder_id = payload.folder_id
    if folder_id is None:
        with db() as con:
            cur = con.execute("INSERT INTO folders(name,path,recursive,mode) VALUES('Arquivos selecionados','',0,'sequential')")
            folder_id = int(cur.lastrowid)
    else:
        get_folder(folder_id)
    added = 0
    invalid: list[str] = []
    for path in payload.paths:
        if add_file(folder_id, path):
            added += 1
        else:
            invalid.append(Path(path).name)
    await engine.publish()
    return {"folder_id": folder_id, "added": added, "invalid": invalid}


@app.put("/api/folders/{folder_id}/order")
async def reorder_media(folder_id: int, payload: OrderIn) -> dict[str, bool]:
    get_folder(folder_id)
    current = {int(m["id"]) for m in get_folder_media(folder_id)}
    if set(payload.ids) != current or len(payload.ids) != len(current):
        raise HTTPException(400, "A ordem deve conter cada arquivo desta pasta uma única vez.")
    with db() as con:
        con.executemany("UPDATE media SET sort_order=? WHERE id=?", [(i, media_id) for i, media_id in enumerate(payload.ids)])
    return {"ok": True}


@app.put("/api/media/{media_id}")
async def update_media(media_id: int, payload: MediaUpdate) -> dict[str, Any]:
    media = get_media(media_id)
    active = int(payload.active) if payload.active is not None else int(media["active"])
    loop_mode = payload.loop_mode or media["loop_mode"]
    if loop_mode not in ("once", "repeat", "infinite"):
        raise HTTPException(400, "Modo de repetição inválido.")
    count = payload.repeat_count if payload.repeat_count is not None else media["repeat_count"]
    category = payload.category or media.get("category", "music")
    if category not in ("music", "jingle", "sponsor", "commercial", "other"):
        raise HTTPException(400, "Categoria de mídia inválida.")
    with db() as con:
        con.execute("UPDATE media SET active=?,loop_mode=?,repeat_count=?,category=? WHERE id=?",
                    (active, loop_mode, count, category, media_id))
    return get_media(media_id)


@app.delete("/api/media/{media_id}")
async def delete_media(media_id: int) -> dict[str, bool]:
    get_media(media_id)
    with db() as con:
        con.execute("DELETE FROM media WHERE id=?", (media_id,))
    return {"ok": True}


@app.get("/api/schedules")
async def api_schedules() -> list[dict[str, Any]]:
    return scheduled_events()


def normalize_schedule(payload: ScheduleIn) -> dict[str, Any]:
    if not payload.name.strip():
        raise HTTPException(400, "Informe o nome do bloco.")
    if payload.kind not in ("program", "commercial", "flow"):
        raise HTTPException(400, "Tipo de bloco inválido.")
    if payload.transition not in ("cut", "wait"):
        raise HTTPException(400, "Transição inválida.")
    try:
        start = clock_time.fromisoformat(payload.start_time).strftime("%H:%M:%S")
        end = clock_time.fromisoformat(payload.end_time).strftime("%H:%M:%S")
    except ValueError as exc:
        raise HTTPException(400, "Informe horários válidos.") from exc
    if start == end:
        raise HTTPException(400, "Início e fim não podem ser iguais; use horários diferentes.")
    if any(day not in range(7) for day in payload.weekdays):
        raise HTTPException(400, "Dia da semana inválido.")
    flow_id = None
    folder_id = payload.folder_id
    if payload.kind == "flow":
        if not payload.flow_id:
            raise HTTPException(400, "Selecione um fluxo de programação.")
        flow_id = payload.flow_id
        get_flow(flow_id)
        programs = get_flow_programs(flow_id)
        active_programs = [program for program in programs if program["active"]]
        if not active_programs:
            raise HTTPException(400, "Ative pelo menos um programa no fluxo.")
        folder_id = int(active_programs[0]["folder_id"])
    else:
        if folder_id is None:
            raise HTTPException(400, "Selecione um programa ou playlist.")
        get_folder(folder_id)
    return {"name": payload.name.strip(), "folder_id": folder_id, "flow_id": flow_id, "kind": payload.kind,
            "weekdays": sorted(set(payload.weekdays)), "start_time": start, "end_time": end,
            "transition": payload.transition, "active": payload.active}


@app.post("/api/schedules")
async def create_schedule(payload: ScheduleIn) -> dict[str, Any]:
    data = normalize_schedule(payload)
    conflicts = find_overlaps(data)
    if conflicts:
        raise HTTPException(409, "Conflito de horário com: " + ", ".join(conflicts))
    with db() as con:
        cur = con.execute("INSERT INTO schedules(name,folder_id,flow_id,kind,weekdays,start_time,end_time,transition,active) VALUES(?,?,?,?,?,?,?,?,?)",
                          (data["name"], data["folder_id"], data["flow_id"], data["kind"], ",".join(map(str, data["weekdays"])), data["start_time"], data["end_time"], data["transition"], int(data["active"])))
        new_id = int(cur.lastrowid)
    await engine.publish()
    return next(e for e in scheduled_events() if e["id"] == new_id)


@app.put("/api/schedules/{schedule_id}")
async def edit_schedule(schedule_id: int, payload: ScheduleIn) -> dict[str, Any]:
    existing = next((e for e in scheduled_events() if e["id"] == schedule_id), None)
    if not existing:
        raise HTTPException(404, "Evento não encontrado.")
    data = normalize_schedule(payload)
    conflicts = find_overlaps(data, schedule_id)
    if conflicts:
        raise HTTPException(409, "Conflito de horário com: " + ", ".join(conflicts))
    with db() as con:
        con.execute("UPDATE schedules SET name=?,folder_id=?,flow_id=?,kind=?,weekdays=?,start_time=?,end_time=?,transition=?,active=? WHERE id=?",
                    (data["name"], data["folder_id"], data["flow_id"], data["kind"], ",".join(map(str, data["weekdays"])), data["start_time"], data["end_time"], data["transition"], int(data["active"]), schedule_id))
    await engine.publish()
    return next(e for e in scheduled_events() if e["id"] == schedule_id)


@app.delete("/api/schedules/{schedule_id}")
async def delete_schedule(schedule_id: int) -> dict[str, bool]:
    with db() as con:
        cur = con.execute("DELETE FROM schedules WHERE id=?", (schedule_id,))
    if cur.rowcount == 0:
        raise HTTPException(404, "Evento não encontrado.")
    await engine.publish()
    return {"ok": True}


@app.post("/api/control/{action}")
async def control(action: str) -> dict[str, Any]:
    if action == "play":
        await engine.pause_toggle(False)
    elif action == "pause":
        await engine.pause_toggle(True)
    elif action == "next":
        await engine.next()
    elif action == "stop":
        await engine.stop()
    elif action == "automation-on":
        set_setting("automation", "1")
        engine.triggered.clear()
        await engine.publish()
    elif action == "automation-off":
        set_setting("automation", "0")
        engine.pending_event = None
        await engine.publish()
    else:
        raise HTTPException(404, "Comando desconhecido.")
    return engine.snapshot()


@app.post("/api/media/{media_id}/play")
async def play_media(media_id: int) -> dict[str, Any]:
    media = get_media(media_id)
    await engine.start_queue([media], "manual")
    return engine.snapshot()


@app.post("/api/folders/{folder_id}/play")
async def play_folder(folder_id: int) -> dict[str, Any]:
    folder = get_folder(folder_id)
    media = get_folder_media(folder_id, active_only=True)
    media = compose_program(media, folder)
    if not media:
        raise HTTPException(400, "Este programa não tem arquivos ativos.")
    deadline = time.monotonic() + float(folder["limit_seconds"]) if folder["limit_seconds"] else None
    if deadline is not None:
        for item in media:
            item["loop_mode"] = "once"
            item["repeat_count"] = 1
    await engine.start_queue(media, "manual", deadline=deadline)
    return engine.snapshot()


@app.get("/media/{media_id}")
async def stream_media(media_id: int) -> FileResponse:
    media = get_media(media_id)
    path = Path(media["path"])
    if not path.is_file():
        raise HTTPException(404, "Arquivo removido ou indisponível. Reimporte a pasta.")
    return FileResponse(path, media_type="video/mp4" if path.suffix.lower() == ".mp4" else None, filename=path.name)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await hub.add(ws)
    try:
        await ws.send_text(json.dumps(engine.snapshot(), ensure_ascii=False))
        while True:
            raw = await ws.receive_text()
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue
            kind = message.get("type")
            if kind == "progress":
                await engine.report_progress(int(message.get("token", -1)), float(message.get("position", 0)), message.get("state"))
            elif kind == "ended":
                await engine.on_ended(int(message.get("token", -1)))
            elif kind == "player-error":
                if int(message.get("token", -1)) == engine.token:
                    engine.message = "Falha ao reproduzir a mídia. Confira o formato e o arquivo."
                    await engine.publish()
    except WebSocketDisconnect:
        await hub.remove(ws)
    except Exception:
        await hub.remove(ws)
        log.exception("WebSocket encerrado com erro")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=False)
