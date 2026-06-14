"""
Standup Mood Check-in Server
Single-port aiohttp server with SQLite persistence and session cookies.
"""

import asyncio
import json
import os
import uuid
import logging
import aiosqlite
from aiohttp import web
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.environ.get("DB_PATH", "standup.db")
MOODS = {"blazing", "great", "okay", "meh", "rough"}
COOKIE = "sm_session"

# Active WebSocket connections: session_id -> ws
active: dict[str, web.WebSocketResponse] = {}


# ── Database ─────────────────────────────────────────────

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS rooms (
                id         TEXT PRIMARY KEY,
                creator_id TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS users (
                id         TEXT PRIMARY KEY,
                session_id TEXT NOT NULL UNIQUE,
                name       TEXT NOT NULL,
                room_id    TEXT REFERENCES rooms(id),
                mood       TEXT,
                is_online  INTEGER DEFAULT 0,
                joined_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS reset_log (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                room_id  TEXT NOT NULL,
                admin_id TEXT NOT NULL,
                reset_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await db.commit()


async def db_get_user(session_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM users WHERE session_id = ?", (session_id,)
        ) as cur:
            return await cur.fetchone()


async def db_get_room(room_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM rooms WHERE id = ?", (room_id,)
        ) as cur:
            return await cur.fetchone()


async def db_room_members(room_id: str) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM users WHERE room_id = ? ORDER BY joined_at", (room_id,)
        ) as cur:
            return await cur.fetchall()


async def db_create_room(room_id: str, creator_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO rooms (id, creator_id) VALUES (?, ?)",
            (room_id, creator_id),
        )
        await db.commit()


async def db_upsert_user(user_id: str, session_id: str, name: str, room_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users (id, session_id, name, room_id, mood, is_online)
            VALUES (?, ?, ?, ?, NULL, 1)
            ON CONFLICT(session_id) DO UPDATE SET
                name=excluded.name, room_id=excluded.room_id,
                mood=NULL, is_online=1
        """, (user_id, session_id, name, room_id))
        await db.commit()


async def db_set_mood(session_id: str, mood):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET mood=? WHERE session_id=?", (mood, session_id)
        )
        await db.commit()


async def db_set_online(session_id: str, online: bool):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET is_online=? WHERE session_id=?",
            (1 if online else 0, session_id),
        )
        await db.commit()


async def db_reset_room(room_id: str, admin_id: str):
    """Remove all members from the room and log the reset."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET room_id=NULL, mood=NULL, is_online=0 WHERE room_id=?",
            (room_id,),
        )
        await db.execute(
            "INSERT INTO reset_log (room_id, admin_id) VALUES (?, ?)",
            (room_id, admin_id),
        )
        await db.commit()


# ── Helpers ──────────────────────────────────────────────

def members_payload(members, room_creator_id: str) -> list:
    return [
        {
            "id": m["id"],
            "name": m["name"],
            "mood": m["mood"],
            "ready": m["mood"] is not None,
            "isAdmin": m["id"] == room_creator_id,
            "isOnline": bool(m["is_online"]),
        }
        for m in members
    ]


async def broadcast(room_id: str, payload: str, exclude_session: str = None):
    members = await db_room_members(room_id)
    dead = []
    for m in members:
        sid = m["session_id"]
        if sid == exclude_session:
            continue
        ws = active.get(sid)
        if ws and not ws.closed:
            try:
                await ws.send_str(payload)
            except Exception:
                dead.append(sid)
    for sid in dead:
        active.pop(sid, None)
        await db_set_online(sid, False)


async def broadcast_update(room_id: str, exclude_session: str = None):
    room = await db_get_room(room_id)
    members = await db_room_members(room_id)
    payload = json.dumps({
        "type": "update",
        "members": members_payload(members, room["creator_id"]),
    })
    await broadcast(room_id, payload, exclude_session)


# ── WebSocket handler ─────────────────────────────────────

async def ws_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    session_id = request.cookies.get(COOKIE)
    if not session_id:
        await ws.send_str(json.dumps({"type": "error", "msg": "no_session"}))
        await ws.close()
        return ws

    # Register this WebSocket
    active[session_id] = ws
    await db_set_online(session_id, True)

    # Auto-rejoin if user already has a room in DB
    user = await db_get_user(session_id)
    if user and user["room_id"]:
        room = await db_get_room(user["room_id"])
        members = await db_room_members(user["room_id"])
        await ws.send_str(json.dumps({
            "type": "joined",
            "room": user["room_id"],
            "userId": user["id"],
            "isAdmin": user["id"] == room["creator_id"],
            "members": members_payload(members, room["creator_id"]),
        }))
        await broadcast_update(user["room_id"], exclude_session=session_id)
        log.info("Session %s rejoined room %s", session_id[:8], user["room_id"])

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                kind = data.get("type")

                if kind == "join":
                    name = (data.get("name") or "Anonymous")[:32].strip()
                    room_id = (data.get("room") or "").strip().upper() or uuid.uuid4().hex[:6].upper()

                    # Refresh user session from DB (may have changed)
                    user = await db_get_user(session_id)
                    user_id = user["id"] if user else str(uuid.uuid4())[:8]

                    room_exists = await db_get_room(room_id)
                    if not room_exists:
                        await db_create_room(room_id, user_id)

                    await db_upsert_user(user_id, session_id, name, room_id)

                    room = await db_get_room(room_id)
                    members = await db_room_members(room_id)

                    await ws.send_str(json.dumps({
                        "type": "joined",
                        "room": room_id,
                        "userId": user_id,
                        "isAdmin": user_id == room["creator_id"],
                        "members": members_payload(members, room["creator_id"]),
                    }))
                    await broadcast_update(room_id, exclude_session=session_id)
                    log.info("User %s joined room %s as %s", user_id, room_id, name)

                elif kind == "mood":
                    user = await db_get_user(session_id)
                    if not user or not user["room_id"]:
                        continue
                    mood = data.get("mood")
                    if mood not in MOODS and mood is not None:
                        continue
                    await db_set_mood(session_id, mood)
                    await broadcast_update(user["room_id"])

                elif kind == "reset":
                    user = await db_get_user(session_id)
                    if not user or not user["room_id"]:
                        continue
                    room = await db_get_room(user["room_id"])
                    if user["id"] != room["creator_id"]:
                        log.warning("Non-admin %s attempted reset of room %s", user["id"], user["room_id"])
                        continue

                    room_id = user["room_id"]
                    log.info("Admin %s reset room %s", user["id"], room_id)

                    # Notify all members BEFORE wiping (so they know which room)
                    members = await db_room_members(room_id)
                    reset_payload = json.dumps({"type": "room_reset", "room": room_id})
                    for m in members:
                        sid = m["session_id"]
                        w = active.get(sid)
                        if w and not w.closed:
                            try:
                                await w.send_str(reset_payload)
                            except Exception:
                                pass

                    await db_reset_room(room_id, user["id"])

            elif msg.type in (web.WSMsgType.ERROR, web.WSMsgType.CLOSE):
                break

    except Exception as e:
        log.info("WS error for session %s: %s", session_id[:8], e)
    finally:
        active.pop(session_id, None)
        await db_set_online(session_id, False)
        # Broadcast the departure to remaining room members
        user = await db_get_user(session_id)
        if user and user["room_id"]:
            await broadcast_update(user["room_id"])

    return ws


# ── HTTP handlers ─────────────────────────────────────────

async def index_handler(request):
    session_id = request.cookies.get(COOKIE)
    html_path = Path(__file__).parent / "public" / "index.html"
    response = web.FileResponse(html_path)
    if not session_id:
        session_id = str(uuid.uuid4())
        response.set_cookie(
            COOKIE,
            session_id,
            httponly=True,
            samesite="Lax",
            secure=request.secure,
            max_age=60 * 60 * 24 * 30,  # 30 days
        )
    return response


# ── App startup ───────────────────────────────────────────

async def main():
    await init_db()
    port = int(os.environ.get("PORT", 3000))

    app = web.Application()
    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/", index_handler)
    app.router.add_static("/", Path(__file__).parent / "public")

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    log.info("Server running on http://0.0.0.0:%d  db=%s", port, DB_PATH)
    print(f"\n  Standup Mood Check-in")
    print(f"  Open http://localhost:{port} in your browser\n")

    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
