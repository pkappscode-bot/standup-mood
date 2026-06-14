"""
Standup Mood Check-in Server
Single-port aiohttp server (HTTP + WebSocket) for Render deployment.
"""

import asyncio
import json
import os
import uuid
import logging
from aiohttp import web
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

rooms: dict[str, dict] = {}
ws_to_room: dict = {}

MOODS = {"blazing", "great", "okay", "meh", "rough"}


def room_snapshot(room_id: str) -> list:
    return [
        {"id": uid, "name": info["name"], "mood": info["mood"], "ready": info["ready"]}
        for uid, info in rooms.get(room_id, {}).items()
    ]


async def broadcast(room_id: str, exclude=None):
    if room_id not in rooms:
        return
    payload = json.dumps({"type": "update", "members": room_snapshot(room_id)})
    dead = []
    for ws, (rid, _) in list(ws_to_room.items()):
        if rid == room_id and ws is not exclude:
            try:
                await ws.send_str(payload)
            except Exception:
                dead.append(ws)
    for ws in dead:
        await remove_client(ws)


async def remove_client(ws):
    if ws not in ws_to_room:
        return
    room_id, user_id = ws_to_room.pop(ws)
    if room_id in rooms:
        rooms[room_id].pop(user_id, None)
        if not rooms[room_id]:
            del rooms[room_id]
            log.info("Room %s closed (empty)", room_id)
        else:
            await broadcast(room_id)


async def ws_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    user_id = str(uuid.uuid4())[:8]
    log.info("New connection %s", user_id)

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
                    room_id = (data.get("room") or str(uuid.uuid4())[:6]).strip().upper()

                    if room_id not in rooms:
                        rooms[room_id] = {}

                    rooms[room_id][user_id] = {"name": name, "mood": None, "ready": False}
                    ws_to_room[ws] = (room_id, user_id)

                    await ws.send_str(json.dumps({
                        "type": "joined",
                        "room": room_id,
                        "userId": user_id,
                        "members": room_snapshot(room_id),
                    }))
                    await broadcast(room_id, exclude=ws)
                    log.info("%s joined room %s as %s", user_id, room_id, name)

                elif kind == "mood":
                    if ws not in ws_to_room:
                        continue
                    room_id, uid = ws_to_room[ws]
                    mood = data.get("mood")
                    if mood not in MOODS and mood is not None:
                        continue
                    rooms[room_id][uid]["mood"] = mood
                    rooms[room_id][uid]["ready"] = mood is not None
                    await broadcast(room_id)

                elif kind == "reset":
                    if ws not in ws_to_room:
                        continue
                    room_id, _ = ws_to_room[ws]
                    for member in rooms[room_id].values():
                        member["mood"] = None
                        member["ready"] = False
                    await broadcast(room_id)

            elif msg.type in (web.WSMsgType.ERROR, web.WSMsgType.CLOSE):
                break

    except Exception as e:
        log.info("Connection error %s: %s", user_id, e)
    finally:
        await remove_client(ws)

    return ws


async def index_handler(request):
    html_path = Path(__file__).parent / "public" / "index.html"
    return web.FileResponse(html_path)


async def main():
    port = int(os.environ.get("PORT", 3000))

    app = web.Application()
    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/", index_handler)
    app.router.add_static("/", Path(__file__).parent / "public")

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    log.info("Server running on http://0.0.0.0:%d", port)
    print(f"\n  Standup Mood Check-in")
    print(f"  Open http://localhost:{port} in your browser\n")

    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
