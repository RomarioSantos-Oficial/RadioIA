from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
import threading
import unittest
import wave
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import app as playout


def make_wav(path: Path, seconds: int = 1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(path), "wb") as audio:
        audio.setnchannels(1)
        audio.setsampwidth(2)
        audio.setframerate(8000)
        audio.writeframes(b"\0\0" * 8000 * seconds)


class PlayoutRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="playout-tests-")
        self.old_paths = (playout.DATA_DIR, playout.DB_PATH, playout.WORKSPACE_POINTER)
        base = Path(self.temp.name)
        playout.DATA_DIR = base / "data"
        playout.DB_PATH = playout.DATA_DIR / "playout.sqlite3"
        playout.WORKSPACE_POINTER = playout.DATA_DIR / "workspace.json"
        playout.SCAN_JOBS.clear()
        playout.SCAN_CANCELLATIONS.clear()
        playout.SCAN_TASKS.clear()
        playout.init_db()
        await playout.engine.stop("Teste iniciado.")
        playout.engine.triggered.clear()

    async def asyncTearDown(self) -> None:
        await playout.engine.stop("Teste finalizado.")
        playout.DATA_DIR, playout.DB_PATH, playout.WORKSPACE_POINTER = self.old_paths
        self.temp.cleanup()

    def create_station_root(self, media_count: int = 15) -> Path:
        root = Path(self.temp.name) / "Emissora"
        video_dir = root / "Programas" / "Manhã" / "Vídeos"
        jingles = root / "Programas" / "Manhã" / "Vinhetas"
        commercials = root / "Programas" / "Manhã" / "Comerciais"
        for i in range(media_count):
            make_wav(video_dir / f"programa-{i:02}.wav")
        for i in range(6):
            make_wav(jingles / f"vinheta-{i:02}.wav")
            make_wav(commercials / f"comercial-{i:02}.wav")
        return root

    async def import_station(self, root: Path, *, share: int = 75, break_seconds: int = 0) -> dict:
        return await playout.import_flow_root(playout.FlowImportIn(
            root_path=str(root), name="Emissora", program_share=share,
            ad_break_seconds=break_seconds,
        ))

    def test_database_context_closes_connection_and_migrates_old_columns(self) -> None:
        playout.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        playout.DB_PATH = playout.DATA_DIR / "legacy.sqlite3"
        with closing(sqlite3.connect(playout.DB_PATH)) as con:
            con.executescript("""
                CREATE TABLE flows (id INTEGER PRIMARY KEY, name TEXT NOT NULL,
                    root_path TEXT NOT NULL UNIQUE, mode TEXT NOT NULL DEFAULT 'sequential',
                    program_share INTEGER NOT NULL DEFAULT 90,
                    jingle_folder_id INTEGER, commercial_folder_id INTEGER);
                CREATE TABLE schedules (id INTEGER PRIMARY KEY, name TEXT NOT NULL,
                    folder_id INTEGER NOT NULL, kind TEXT NOT NULL DEFAULT 'program',
                    weekdays TEXT NOT NULL, start_time TEXT NOT NULL, end_time TEXT NOT NULL,
                    transition TEXT NOT NULL DEFAULT 'cut', active INTEGER NOT NULL DEFAULT 1);
            """)
            con.commit()
        playout.init_db()
        with playout.db() as con:
            flow_columns = {r["name"] for r in con.execute("PRAGMA table_info(flows)")}
            schedule_columns = {r["name"] for r in con.execute("PRAGMA table_info(schedules)")}
            program_columns = {r["name"] for r in con.execute("PRAGMA table_info(flow_programs)")}
        self.assertTrue({"recursive", "ad_break_seconds"} <= flow_columns)
        self.assertTrue({"flow_id", "schedule_date"} <= schedule_columns)
        self.assertIn("duration_seconds", program_columns)
        with playout.db() as con:
            pass
        with self.assertRaises(sqlite3.ProgrammingError):
            con.execute("SELECT 1")

    async def test_ticker_weather_seeds_brazilian_capitals_and_one_random_city(self) -> None:
        expected_states = {state for _, state, _, _ in playout.BRAZIL_STATE_CAPITALS}
        expected_random_cities = {name for name, _, _, _ in playout.BRAZIL_RANDOM_WEATHER_CITIES}
        with playout.db() as con:
            cities = con.execute(
                "SELECT name,admin1 FROM ticker_weather_cities"
            ).fetchall()
        self.assertEqual(len(cities), 28)
        self.assertEqual({city["admin1"] for city in cities}, expected_states)
        self.assertEqual(sum(city["name"] in expected_random_cities for city in cities), 1)

        playout.init_db()
        with playout.db() as con:
            city_count = con.execute("SELECT COUNT(*) FROM ticker_weather_cities").fetchone()[0]
        self.assertEqual(city_count, 28)

    async def test_ticker_weather_allows_manual_cities_beyond_old_limit(self) -> None:
        for index in range(12):
            await playout.add_ticker_weather_city(playout.TickerWeatherCityIn(
                name=f"Cidade teste {index}",
                admin1="",
                country="Teste",
                latitude=-60 + index / 10,
                longitude=100 + index / 10,
            ))
        with self.assertRaises(playout.HTTPException) as error:
            await playout.add_ticker_weather_city(playout.TickerWeatherCityIn(
                name="Cidade excedente",
                admin1="",
                country="Teste",
                latitude=-50,
                longitude=110,
            ))
        self.assertEqual(error.exception.status_code, 400)

    def test_ticker_weather_automatically_selects_two_capitals_and_one_city(self) -> None:
        cities = playout.ticker_weather_cities_for_display()
        random_city_names = {name for name, _, _, _ in playout.BRAZIL_RANDOM_WEATHER_CITIES}
        self.assertEqual(len(cities), 3)
        self.assertEqual(sum(city["id"].startswith("capital-") for city in cities), 2)
        self.assertEqual(sum(city["name"] in random_city_names for city in cities), 1)

    def test_ticker_pruning_expires_old_news_and_enforces_configured_limit(self) -> None:
        now = datetime.now(timezone.utc)
        with playout.db() as con:
            feed_id = con.execute(
                "INSERT INTO ticker_feeds(name,url) VALUES(?,?)",
                ("Fonte de teste", "https://example.com/feed.xml"),
            ).lastrowid
            timestamps = [(now - timedelta(hours=13)).isoformat(timespec="microseconds")]
            timestamps.extend(
                (now - timedelta(hours=index)).isoformat(timespec="microseconds")
                for index in range(6)
            )
            for index, timestamp in enumerate(timestamps):
                con.execute(
                    """INSERT INTO ticker_items(feed_id,external_id,text,updated_at,published_at)
                       VALUES(?,?,?,?,?)""",
                    (feed_id, str(index), f"Manchete {index}", timestamp, timestamp),
                )
        playout.set_setting(
            "ticker_config",
            json.dumps({**playout.TICKER_DEFAULTS, "max_news_items": 5}),
        )
        playout.prune_ticker_items(now)
        with playout.db() as con:
            rows = con.execute(
                "SELECT text FROM ticker_items WHERE feed_id=? ORDER BY updated_at DESC",
                (feed_id,),
            ).fetchall()
        self.assertEqual(len(rows), 5)
        self.assertNotIn("Manchete 0", {row["text"] for row in rows})

    def test_ticker_feed_keeps_only_recently_published_news(self) -> None:
        now = datetime.now(timezone.utc)
        recent = (now - timedelta(hours=2)).isoformat()
        old = (now - timedelta(hours=13)).isoformat()
        future = (now + timedelta(hours=1)).isoformat()
        payload = f"""<rss><channel>
          <item><title>Recente</title><pubDate>{recent}</pubDate></item>
          <item><title>Antiga</title><pubDate>{old}</pubDate></item>
          <item><title>Futura</title><pubDate>{future}</pubDate></item>
        </channel></rss>""".encode()
        entries = playout.parse_ticker_payload(payload)
        fresh = playout.ticker_fresh_entries(entries, now)
        self.assertEqual([entry["text"] for entry in fresh], ["Recente"])

    def test_ticker_feed_parses_rss_and_json_publication_dates(self) -> None:
        rss = b"<rss><channel><item><title>RSS</title><pubDate>Sat, 14 Nov 2026 10:00:00 GMT</pubDate></item></channel></rss>"
        rss_entry = playout.parse_ticker_payload(rss)[0]
        self.assertTrue(rss_entry["published_at"].startswith("2026-11-14T10:00:00"))
        json_entry = playout.parse_ticker_payload(
            b'[{"title":"JSON","published_at":"2026-11-14T10:00:00Z"}]',
            "application/json",
        )[0]
        self.assertTrue(json_entry["published_at"].startswith("2026-11-14T10:00:00"))

    async def test_flow_import_reads_categories_and_inserts_ads_at_configured_interval(self) -> None:
        root = self.create_station_root()
        flow = await self.import_station(root, share=75, break_seconds=2)
        self.assertEqual(len(flow["programs"]), 1)
        program = flow["programs"][0]
        self.assertEqual(program["media_count"], 15)
        self.assertEqual(program["jingle_count"], 6)
        self.assertEqual(program["commercial_count"], 6)
        self.assertEqual(playout.directory_role("Comerciais"), "commercial")

        queue, ad_cursor, carried = playout.build_flow_queue(flow["id"])
        program_folder = program["folder_id"]
        first_break_index = next(i for i, item in enumerate(queue) if item["folder_id"] != program_folder)
        self.assertEqual(first_break_index, 6)
        self.assertNotEqual(queue[first_break_index + 1]["folder_id"], program_folder)
        self.assertGreater(ad_cursor, 0)
        self.assertGreaterEqual(carried, 0)

        await playout.update_flow(flow["id"], playout.FlowUpdateIn(ad_break_seconds=0))
        proportional_queue, _, _ = playout.build_flow_queue(flow["id"])
        proportional_break = next(i for i, item in enumerate(proportional_queue)
                                  if item["folder_id"] != program_folder)
        self.assertEqual(proportional_break, 3)

    async def test_schedule_supports_weekly_fixed_date_and_rejects_conflicts(self) -> None:
        root = self.create_station_root(media_count=2)
        make_wav(root / "Programas" / "Tarde" / "Vídeos" / "tarde-01.wav")
        make_wav(root / "Programas" / "Tarde" / "Vídeos" / "tarde-02.wav")
        flow = await self.import_station(root)
        afternoon = next(p for p in flow["programs"] if p["name"] == "Tarde")
        await playout.update_flow_program(
            flow["id"], afternoon["folder_id"], playout.FlowProgramUpdateIn(duration_seconds=2700))
        flow = playout.flow_snapshot(flow["id"])
        afternoon = next(p for p in flow["programs"] if p["name"] == "Tarde")
        self.assertEqual(afternoon["duration_seconds"], 2700)
        self.assertEqual(afternoon["estimated_block_seconds"], 2700)
        fixed_day = date.today() + timedelta(days=2)
        first = playout.ScheduleIn(
            name="Especial pontual", kind="flow", flow_id=flow["id"],
            schedule_date=fixed_day.isoformat(), start_time="09:00", end_time="10:00",
        )
        saved = await playout.create_schedule(first)
        self.assertEqual(saved["schedule_date"], fixed_day.isoformat())
        conflict = playout.ScheduleIn(
            name="Conflito", kind="flow", flow_id=flow["id"],
            schedule_date=fixed_day.isoformat(), start_time="09:30", end_time="10:30",
        )
        with self.assertRaises(playout.HTTPException) as error:
            await playout.create_schedule(conflict)
        self.assertEqual(error.exception.status_code, 409)

        individual = await playout.create_schedule(playout.ScheduleIn(
            name="Programa da tarde", kind="flow_program", flow_id=flow["id"],
            folder_id=afternoon["folder_id"], schedule_date=fixed_day.isoformat(),
            start_time="13:00", end_time="14:00",
        ))
        self.assertEqual(individual["kind"], "flow_program")
        self.assertEqual(individual["folder_name"], "Tarde")
        queue, _, _ = playout.build_flow_queue(flow["id"], program_folder_id=afternoon["folder_id"])
        self.assertTrue(queue)
        self.assertTrue(all(item["folder_id"] == afternoon["folder_id"] for item in queue))
        morning = next(p for p in flow["programs"] if p["name"] == "Manhã")
        morning_queue, _, _ = playout.build_flow_queue(flow["id"], program_folder_id=morning["folder_id"])
        allowed_ids = {morning["folder_id"], morning["jingle_folder_id"], morning["commercial_folder_id"]}
        self.assertTrue(all(item["folder_id"] in allowed_ids for item in morning_queue))
        self.assertTrue(any(item["folder_id"] != morning["folder_id"] for item in morning_queue))
        now = playout.local_now()
        await playout.engine.schedule_start(individual, now, now, now + timedelta(minutes=20))
        self.assertEqual(playout.engine.active_event["kind"], "flow_program")
        self.assertTrue(all(item["folder_id"] == afternoon["folder_id"] for item in playout.engine.queue))
        morning_event = await playout.create_schedule(playout.ScheduleIn(
            name="Programa da manhã", kind="flow_program", flow_id=flow["id"],
            folder_id=morning["folder_id"], schedule_date=fixed_day.isoformat(),
            start_time="14:00", end_time="15:00",
        ))
        next_start = now + timedelta(minutes=20)
        await playout.engine.schedule_start(morning_event, next_start, next_start,
                                            next_start + timedelta(minutes=20))
        self.assertEqual(playout.engine.active_event["folder_id"], morning["folder_id"])
        self.assertTrue(any(item["folder_id"] == morning["jingle_folder_id"]
                            for item in playout.engine.queue))
        weekly = await playout.create_schedule(playout.ScheduleIn(
            name="Grade semanal", kind="flow", flow_id=flow["id"],
            weekdays=[0, 2, 4], start_time="11:00", end_time="12:00",
        ))
        self.assertIsNone(weekly["schedule_date"])
        self.assertEqual(weekly["weekdays"], [0, 2, 4])

    def test_schedule_clock_uses_brasilia_time(self) -> None:
        self.assertEqual(playout.BRAZIL_TZ.key, "America/Sao_Paulo")
        event = {"schedule_date": "2026-11-14", "start_time": "08:00:00", "end_time": "09:00:00"}
        start, end = playout.event_datetime(event, datetime(2026, 11, 14))
        self.assertEqual(start.hour, 8)
        self.assertEqual(start.utcoffset(), timedelta(hours=-3))
        self.assertEqual(end.hour, 9)

    async def test_scheduler_starts_a_fixed_date_block(self) -> None:
        root = self.create_station_root(media_count=2)
        flow = await self.import_station(root)
        now = playout.local_now()
        start = now - timedelta(seconds=20)
        end = now + timedelta(minutes=2)
        event = await playout.create_schedule(playout.ScheduleIn(
            name="Ao vivo agora", kind="flow", flow_id=flow["id"],
            schedule_date=now.date().isoformat(), start_time=start.strftime("%H:%M:%S"),
            end_time=end.strftime("%H:%M:%S"),
        ))
        playout.set_setting("automation", "1")
        await playout.engine.scheduler_tick()
        self.assertEqual(playout.engine.active_event["id"], event["id"])
        self.assertIsNotNone(playout.engine.current)

    async def test_player_error_and_ended_after_pause_advance_once(self) -> None:
        media = [
            {"id": 1, "name": "falha.wav", "kind": "audio", "duration": 10, "active": True},
            {"id": 2, "name": "proximo.wav", "kind": "audio", "duration": 10, "active": True},
            {"id": 3, "name": "terceiro.wav", "kind": "audio", "duration": 10, "active": True},
        ]
        await playout.engine.start_queue(media, "manual")
        failed_token = playout.engine.token
        await playout.engine.on_player_error(failed_token)
        self.assertEqual(playout.engine.current["name"], "proximo.wav")
        self.assertIn("pulando", playout.engine.message)
        await playout.engine.on_player_error(failed_token)
        self.assertEqual(playout.engine.current["name"], "proximo.wav")
        paused_token = playout.engine.token
        await playout.engine.pause_toggle(True)
        await playout.engine.on_ended(paused_token)
        self.assertEqual(playout.engine.current["name"], "terceiro.wav")

    async def test_background_scan_reports_invalid_media_and_backup_is_valid(self) -> None:
        root = self.create_station_root(media_count=1)
        flow = await self.import_station(root)
        program_dir = root / "Programas" / "Manhã" / "Vídeos"
        (program_dir / "corrompido.wav").write_bytes(b"not a wav")
        folder_id = flow["programs"][0]["folder_id"]
        started = await playout.rescan_folder(folder_id)
        job_id = started["job_id"]
        for _ in range(100):
            status = await playout.scan_status(job_id)
            if status["status"] != "running":
                break
            await asyncio.sleep(0.03)
        self.assertEqual(status["status"], "completed")
        self.assertEqual(len(status["invalid"]), 1)

        backup = playout.create_database_backup()
        self.assertIsNotNone(backup)
        with closing(sqlite3.connect(backup)) as con:
            self.assertEqual(con.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    async def test_cancelled_folder_scan_preserves_existing_media_and_reports_progress(self) -> None:
        folder_path = Path(self.temp.name) / "playlist"
        media_path = folder_path / "track.wav"
        make_wav(media_path)
        with playout.db() as con:
            folder_id = int(con.execute("INSERT INTO folders(name,path) VALUES(?,?)",
                                        ("Playlist", str(folder_path))).lastrowid)
        self.assertTrue(playout.add_file(folder_id, str(media_path), source_root=folder_path))
        cancel = threading.Event()
        cancel.set()
        progress = []
        result = playout.folder_scan_report(
            folder_id,
            lambda *values: progress.append(values),
            cancel,
        )
        self.assertTrue(result["cancelled"])
        self.assertEqual(result["scanned"], 0)
        self.assertEqual(progress, [])
        self.assertEqual(len(playout.get_folder_media(folder_id)), 1)


if __name__ == "__main__":
    unittest.main()
