"""
Telegram Relay — WEB edition. One file, only `requests` (stdlib sqlite3 for storage).
Web panel + hidden Telegram /samx panel.

* You (ADMIN_CHAT_ID) are the one owner/admin.
* Everything lives behind the hidden commands /samx (operators) and /panel (owner),
  nothing in /start or the bot menu.
* Anyone else who sends /samx becomes a *pending request*; you get a message with their ID
  and ✅ Allow / 🚫 Ban buttons. Allowed people get the operator panel; you can revoke/ban any time.
* Users messaging the bot are relayed to you (and to allowed operators) with a Reply button.

.env:
    BOT_TOKEN=123:abc
    ADMIN_CHAT_ID=123456789
    WEB_PASSWORD=choose-one   # web login (random one is printed if empty)
    WEB_HOST=127.0.0.1        # use 0.0.0.0 only behind HTTPS reverse proxy
    WEB_PORT=8080
    DATA_DIR=/data      # persistent storage (Railway volume); defaults to the app folder
    SAVE_HISTORY=1      # optional, 0 = keep chats in RAM only (default: SQLite relay.db)

Run:   pip install requests python-dotenv   &&   python relay_web.py
systemd: ExecStart=/usr/bin/python3 /opt/relay/relay_web.py   Restart=always
"""
import heapq
import html
import itertools
import json
import os
import queue
import re
import shutil
import signal
import sqlite3
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

try:  # Windows consoles default to a legacy code page; emoji logs must never crash the loop
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:  # noqa: BLE001
    pass

try:
    from dotenv import load_dotenv
except ImportError:  # python-dotenv is optional
    def load_dotenv(*_a, **_k):
        return False

BASE = Path(__file__).parent
load_dotenv(BASE / ".env")
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0") or 0)
SAVE_HISTORY = os.getenv("SAVE_HISTORY", "1").strip() in ("1", "true", "yes")

DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE))).expanduser()
DATA_DIR.mkdir(parents=True, exist_ok=True)
if DATA_DIR.resolve() != BASE.resolve():
    for suffix in ("", "-wal", "-shm"):
        src = BASE / ("relay.db" + suffix)
        dst = DATA_DIR / ("relay.db" + suffix)
        if src.exists() and not dst.exists():
            shutil.copy2(src, dst)
    for name in ("settings.json", "history.json", "access.json", "schedules.json"):
        src = BASE / name
        dst = DATA_DIR / name
        if src.exists() and not dst.exists():
            shutil.copy2(src, dst)
SETTINGS_FILE = DATA_DIR / "settings.json"
HISTORY_FILE = DATA_DIR / "history.json"
ACCESS_FILE = DATA_DIR / "access.json"
SCHEDULES_FILE = DATA_DIR / "schedules.json"
DB_FILE = DATA_DIR / "relay.db"
MAX_MSGS = 200  # per chat, keeps RAM tiny
MAX_SCHEDULES = 200
DEFAULT_SETTINGS = {
    "notify_admin": True,
    "log_usage": True,
    "panel_clean": True,
    "auto_reply": {"enabled": False, "text": "سلام 👋 پیامت رسید، به‌زودی جواب می‌دم 🙏", "cooldown_min": 60},
}
KIND_ICON = {
    "photo": "🖼", "sticker": "😀", "animation": "🎞", "video": "🎬", "video_note": "⭕",
    "voice": "🎤", "audio": "🎵", "document": "📄",
}
SERVICE_KEYS = ("new_chat_members", "left_chat_member", "new_chat_title", "new_chat_photo", "delete_chat_photo",
                "group_chat_created", "supergroup_chat_created", "channel_chat_created", "pinned_message",
                "message_auto_delete_timer_changed", "forum_topic_created", "forum_topic_closed",
                "forum_topic_reopened", "video_chat_started", "video_chat_ended", "video_chat_scheduled",
                "migrate_to_chat_id", "migrate_from_chat_id")


def log(*a):
    line = " ".join(str(x) for x in a)
    try:
        print(time.strftime("%H:%M:%S"), line, flush=True)
    except Exception:  # noqa: BLE001  last-resort fallback for exotic consoles
        try:
            print(time.strftime("%H:%M:%S"), line.encode("ascii", "replace").decode("ascii"), flush=True)
        except Exception:
            pass


# ───────────────────────────── Telegram API ─────────────────────────────
class TG:
    def __init__(self, token):
        self.base = f"https://api.telegram.org/bot{token}"
        self.fbase = f"https://api.telegram.org/file/bot{token}"

    def call(self, method, data=None, files=None, timeout=60):
        r = requests.post(f"{self.base}/{method}", data=data, files=files, timeout=timeout)
        try:
            j = r.json()
        except ValueError:
            raise RuntimeError(f"HTTP {r.status_code}")
        if not j.get("ok"):
            raise RuntimeError(j.get("description", "unknown error"))
        return j["result"]

    def get_file(self, file_id):
        info = self.call("getFile", {"file_id": file_id})
        r = requests.get(f"{self.fbase}/{info['file_path']}", timeout=120)
        r.raise_for_status()
        return r.content, info["file_path"]


class Poller(threading.Thread):
    def __init__(self, token, q):
        super().__init__(daemon=True)
        self.tg = TG(token)
        self.q = q
        self.stop = threading.Event()

    def run(self):
        offset = None
        fails = 0
        while not self.stop.is_set():
            try:
                params = {"timeout": 25,
                          "allowed_updates": json.dumps(["message", "edited_message", "callback_query", "my_chat_member"])}
                if offset:
                    params["offset"] = offset
                for u in self.tg.call("getUpdates", params, timeout=40):
                    offset = u["update_id"] + 1
                    if "message" in u:
                        self.q.put(("msg", u["message"]))
                    elif "edited_message" in u:
                        self.q.put(("edit_in", u["edited_message"]))
                    elif "callback_query" in u:
                        self.q.put(("cb", u["callback_query"]))
                    elif "my_chat_member" in u:
                        self.q.put(("my_chat", u["my_chat_member"]))
                fails = 0
                self.q.put(("online", True, ""))
            except Exception as e:  # noqa: BLE001
                fails += 1
                self.q.put(("online", False, str(e)))
                self.stop.wait(min(3 * fails, 15))


# ───────────────────────────── Helpers ─────────────────────────────
def load_json(path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return default


def save_json(path, obj):
    try:
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)
    except Exception:  # noqa: BLE001
        pass


class DB:
    """SQLite store: chats, messages, access, settings and schedules.

    Writes are incremental (one message at a time) instead of rewriting a giant JSON file,
    and startup only loads the last MAX_MSGS messages per chat. Old JSON files are imported once.
    """

    CHAT_COLS = ("id", "name", "username", "type", "in_bot", "members", "unread", "last_ts",
                 "pinned", "muted", "blocked", "note", "draft", "last_auto")

    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        self.con = sqlite3.connect(str(path), check_same_thread=False)
        self.con.row_factory = sqlite3.Row
        self.con.execute("PRAGMA busy_timeout=5000")
        self.con.execute("PRAGMA synchronous=NORMAL")
        if str(path) != ":memory:":
            try:
                self.con.execute("PRAGMA journal_mode=WAL")
            except sqlite3.DatabaseError:
                pass
        self._init()

    def _init(self):
        with self.lock, self.con:
            self.con.executescript("""
CREATE TABLE IF NOT EXISTS chats(
    id INTEGER PRIMARY KEY, name TEXT NOT NULL DEFAULT '', username TEXT NOT NULL DEFAULT '',
    type TEXT NOT NULL DEFAULT 'private', in_bot INTEGER NOT NULL DEFAULT 1, members INTEGER NOT NULL DEFAULT 0,
    unread INTEGER NOT NULL DEFAULT 0, last_ts INTEGER NOT NULL DEFAULT 0,
    pinned INTEGER NOT NULL DEFAULT 0, muted INTEGER NOT NULL DEFAULT 0, blocked INTEGER NOT NULL DEFAULT 0,
    note TEXT NOT NULL DEFAULT '', draft TEXT NOT NULL DEFAULT '', last_auto INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS messages(
    chat_id INTEGER NOT NULL, msg_id INTEGER NOT NULL, dir TEXT NOT NULL, ts INTEGER NOT NULL,
    kind TEXT NOT NULL DEFAULT 'text', text TEXT NOT NULL DEFAULT '', data TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY(chat_id, msg_id, dir));
CREATE INDEX IF NOT EXISTS idx_msg_chat_ts ON messages(chat_id, ts);
CREATE TABLE IF NOT EXISTS access(
    uid TEXT PRIMARY KEY, name TEXT, username TEXT, status TEXT,
    hits INTEGER, first INTEGER, last INTEGER, notify INTEGER);
CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS schedules(
    id TEXT PRIMARY KEY, chat_id TEXT NOT NULL, text TEXT NOT NULL, send_at INTEGER NOT NULL,
    repeat TEXT NOT NULL, created INTEGER NOT NULL, last_sent INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL, result TEXT NOT NULL DEFAULT '');
CREATE INDEX IF NOT EXISTS idx_sched_due ON schedules(status, send_at);
""")

    def _count(self, table):
        with self.lock:
            return self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    # ── chats + messages
    @staticmethod
    def _chat_values(c):
        return (int(c["id"]), c.get("name") or "", c.get("username") or "",
                c.get("type") or ("private" if int(c["id"]) > 0 else "group"),
                int(bool(c.get("in_bot", True))), int(c.get("members", 0) or 0),
                int(c.get("unread", 0) or 0), int(c.get("last_ts", 0) or 0),
                int(bool(c.get("pinned"))), int(bool(c.get("muted"))), int(bool(c.get("blocked"))),
                c.get("note") or "", c.get("draft") or "", int(c.get("last_auto", 0) or 0))

    def save_chat_meta(self, c):
        q = f"INSERT OR REPLACE INTO chats({','.join(self.CHAT_COLS)}) VALUES({','.join('?' * len(self.CHAT_COLS))})"
        with self.lock, self.con:
            self.con.execute(q, self._chat_values(c))

    @staticmethod
    def _msg_values(cid, m):
        extra = {k: v for k, v in m.items() if k not in ("id", "dir", "ts", "kind", "text")}
        return (int(cid), int(m["id"]), m.get("dir", "in"), int(m.get("ts", 0) or 0),
                m.get("kind", "text"), m.get("text") or "", json.dumps(extra, ensure_ascii=False))

    def save_messages(self, cid, msgs):
        rows = [self._msg_values(cid, m) for m in msgs]
        if not rows:
            return
        with self.lock, self.con:
            self.con.executemany(
                "INSERT OR REPLACE INTO messages(chat_id,msg_id,dir,ts,kind,text,data) VALUES(?,?,?,?,?,?,?)", rows)

    def save_chat(self, c):
        self.save_chat_meta(c)
        self.save_messages(c["id"], c.get("messages") or [])

    def add_message(self, cid, m):
        with self.lock, self.con:
            self.con.execute(
                "INSERT OR REPLACE INTO messages(chat_id,msg_id,dir,ts,kind,text,data) VALUES(?,?,?,?,?,?,?)",
                self._msg_values(cid, m))

    @staticmethod
    def _msg_from_row(r):
        try:
            d = json.loads(r["data"] or "{}")
        except ValueError:
            d = {}
        d.update(id=r["msg_id"], dir=r["dir"], ts=r["ts"], kind=r["kind"], text=r["text"])
        return d

    def load_chats(self, max_msgs=MAX_MSGS):
        chats, rows = {}, []
        with self.lock:
            rows = self.con.execute("SELECT * FROM chats").fetchall()
            for r in rows:
                chats[r["id"]] = {"id": r["id"], "name": r["name"], "username": r["username"], "type": r["type"],
                                  "in_bot": bool(r["in_bot"]), "members": r["members"], "unread": r["unread"],
                                  "last_ts": r["last_ts"], "pinned": bool(r["pinned"]), "muted": bool(r["muted"]),
                                  "blocked": bool(r["blocked"]), "note": r["note"], "draft": r["draft"],
                                  "last_auto": r["last_auto"], "messages": []}
            for cid, c in chats.items():
                mrows = self.con.execute(
                    "SELECT * FROM messages WHERE chat_id=? ORDER BY ts DESC, rowid DESC LIMIT ?",
                    (cid, max_msgs)).fetchall()
                c["messages"] = [self._msg_from_row(r) for r in reversed(mrows)]
        return chats

    def delete_chat(self, cid):
        with self.lock, self.con:
            self.con.execute("DELETE FROM messages WHERE chat_id=?", (int(cid),))
            self.con.execute("DELETE FROM chats WHERE id=?", (int(cid),))

    # ── access
    def load_access(self):
        with self.lock:
            return {r["uid"]: {"name": r["name"] or "", "username": r["username"] or "",
                               "status": r["status"] or "none", "hits": r["hits"] or 0,
                               "first": r["first"] or 0, "last": r["last"] or 0,
                               "notify": bool(r["notify"])}
                    for r in self.con.execute("SELECT * FROM access")}

    def save_access(self, users):
        rows = [(str(k), v.get("name") or "", v.get("username") or "", v.get("status") or "none",
                 int(v.get("hits", 0) or 0), int(v.get("first", 0) or 0), int(v.get("last", 0) or 0),
                 int(bool(v.get("notify")))) for k, v in users.items()]
        with self.lock, self.con:
            self.con.execute("DELETE FROM access")
            self.con.executemany("INSERT INTO access(uid,name,username,status,hits,first,last,notify) VALUES(?,?,?,?,?,?,?,?)", rows)

    # ── settings
    def load_settings(self):
        with self.lock:
            out = {}
            for r in self.con.execute("SELECT k, v FROM settings"):
                try:
                    out[r["k"]] = json.loads(r["v"])
                except ValueError:
                    pass
            return out

    def save_settings(self, s):
        rows = [(k, json.dumps(v, ensure_ascii=False)) for k, v in s.items()]
        with self.lock, self.con:
            self.con.execute("DELETE FROM settings")
            self.con.executemany("INSERT INTO settings(k,v) VALUES(?,?)", rows)

    # ── schedules
    @staticmethod
    def _sched_from_row(r):
        d = {k: r[k] for k in r.keys()}
        d["chat_id"] = "all" if str(d.get("chat_id")) == "all" else int(d.get("chat_id") or 0)
        return d

    def load_schedules(self):
        with self.lock:
            return [self._sched_from_row(r) for r in self.con.execute("SELECT * FROM schedules ORDER BY send_at")]

    def save_schedule(self, j):
        with self.lock, self.con:
            self.con.execute(
                "INSERT OR REPLACE INTO schedules(id,chat_id,text,send_at,repeat,created,last_sent,status,result) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (str(j["id"]), str(j.get("chat_id", "all")), j.get("text") or "", int(j.get("send_at", 0) or 0),
                 j.get("repeat", "once"), int(j.get("created", 0) or 0), int(j.get("last_sent", 0) or 0),
                 j.get("status", "active"), j.get("result", "")))

    def delete_schedule(self, sid):
        with self.lock, self.con:
            self.con.execute("DELETE FROM schedules WHERE id=?", (str(sid),))

    # ── one-time JSON import
    def _meta(self, key):
        with self.lock:
            return bool(self.con.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone())

    def _set_meta(self, key):
        with self.lock, self.con:
            self.con.execute("INSERT OR REPLACE INTO meta(k,v) VALUES(?,'1')", (key,))

    def migrate_json(self, history_file, access_file, settings_file, schedules_file, include_history):
        if not self._meta("migrated"):
            if not self._count("settings") and settings_file.exists():
                data = load_json(settings_file, {})
                if isinstance(data, dict) and data:
                    self.save_settings(data)
            if not self._count("access") and access_file.exists():
                data = load_json(access_file, {})
                if isinstance(data, dict):
                    self.save_access(data)
            if not self._count("schedules") and schedules_file.exists():
                data = load_json(schedules_file, [])
                if isinstance(data, list):
                    for j in data:
                        if isinstance(j, dict) and j.get("id"):
                            self.save_schedule(j)
            self._set_meta("migrated")
        if include_history and not self._meta("history"):
            if history_file.exists():
                data = load_json(history_file, {})
                if isinstance(data, dict):
                    for k, c in data.items():
                        if not isinstance(c, dict):
                            continue
                        try:
                            c["id"] = int(c.get("id", k))
                        except (TypeError, ValueError):
                            continue
                        self.save_chat(c)
            self._set_meta("history")

    def close(self):
        with self.lock:
            try:
                self.con.commit()
                self.con.close()
            except sqlite3.Error:
                pass


def chat_info(c):
    name = c.get("title") or " ".join(filter(None, [c.get("first_name"), c.get("last_name")])) or str(c["id"])
    cid = int(c.get("id", 0) or 0)
    ctype = c.get("type") or ("private" if cid > 0 else "group")
    return name, c.get("username"), ctype


def parse_message(m, direction):
    d = {
        "id": m["message_id"], "dir": direction, "ts": m.get("date", int(time.time())),
        "kind": "text", "text": m.get("text") or m.get("caption") or "",
    }
    rt = m.get("reply_to_message")
    if rt:
        d["reply_id"] = rt.get("message_id")
        d["reply"] = (rt.get("text") or rt.get("caption") or "📎 media")[:80]
        frm = rt.get("from") or {}
        d["reply_who"] = "You" if frm.get("is_bot") else (frm.get("first_name") or "")
    thumb = lambda o: (o.get("thumbnail") or o.get("thumb") or {}).get("file_id")  # noqa: E731

    if m.get("photo"):
        ph = m["photo"]
        d.update(kind="photo", file_id=ph[-1]["file_id"], disp=(ph[1] if len(ph) > 1 else ph[0])["file_id"])
    elif m.get("sticker"):
        s = m["sticker"]
        flat = s.get("is_animated") or s.get("is_video")
        d.update(kind="sticker", file_id=s["file_id"], uid=s.get("file_unique_id"),
                 emoji=s.get("emoji", ""), disp=thumb(s) if flat else s["file_id"])
    elif m.get("animation"):
        a = m["animation"]
        d.update(kind="animation", file_id=a["file_id"], uid=a.get("file_unique_id"),
                 disp=thumb(a), name=a.get("file_name"))
    elif m.get("video"):
        v = m["video"]
        d.update(kind="video", file_id=v["file_id"], disp=thumb(v), name=v.get("file_name"))
    elif m.get("video_note"):
        v = m["video_note"]
        d.update(kind="video_note", file_id=v["file_id"], disp=thumb(v))
    elif m.get("voice"):
        v = m["voice"]
        d.update(kind="voice", file_id=v["file_id"], dur=v.get("duration", 0))
    elif m.get("audio"):
        a = m["audio"]
        d.update(kind="audio", file_id=a["file_id"], name=a.get("title") or a.get("file_name") or "audio",
                 dur=a.get("duration", 0))
    elif m.get("document"):
        dc = m["document"]
        d.update(kind="document", file_id=dc["file_id"], name=dc.get("file_name") or "file", disp=thumb(dc))
    elif m.get("contact"):
        c = m["contact"]
        d["text"] = f"👤 {c.get('first_name', '')} {c.get('last_name', '')}\n📞 {c.get('phone_number', '')}"
    elif m.get("location"):
        lo = m["location"]
        d["text"] = f"📍 {lo['latitude']}, {lo['longitude']}"
    elif m.get("poll"):
        d["text"] = "📊 " + m["poll"].get("question", "")
    elif not d["text"]:
        d["text"] = "[unsupported message]"
    return d


def preview_of(m):
    if not m:
        return ""
    icon = KIND_ICON.get(m["kind"], "")
    body = m["text"] or m.get("name") or m.get("emoji") or m["kind"]
    s = f"{icon} {body}".strip().replace("\n", " ")
    return ("You: " if m["dir"] == "out" else "") + s


def time_text(m):
    t = time.strftime("%H:%M", time.localtime(m["ts"]))
    if m.get("edited"):
        t = "edited  " + t
    if m.get("auto"):
        t = "🤖 auto  " + t
    if m.get("via"):
        t = f"via {m['via']}  " + t
    if m["dir"] == "out":
        t += "  ✓"
    return t


def guess_kind(path):
    ext = Path(path).suffix.lower()
    if ext in (".jpg", ".jpeg", ".png"):
        return "photo"
    if ext == ".gif":
        return "animation"
    if ext in (".mp4", ".mov", ".mkv", ".webm"):
        return "video"
    if ext in (".mp3", ".m4a", ".flac", ".wav"):
        return "audio"
    if ext in (".ogg", ".oga"):
        return "voice"
    if ext == ".webp":
        return "sticker"
    return "document"



# ───────────────────────────── Access store + hidden Telegram panel (/samx) ─────────────────────────────
SPARK = "▁▂▃▄▅▆▇█"
SECRET_RE = re.compile(r"^/samx(@\w+)?(\s|$)", re.I)
PANEL_RE = re.compile(r"^/panel(@\w+)?(\s|$)", re.I)
esc = html.escape


def spark(vals):
    mx = max(vals) or 1
    return "".join(SPARK[round(v / mx * 7)] if v else "·" for v in vals)


def compute_stats(chats):
    now = time.time()
    st = dict(chats=len(chats), unread=0, blocked=0, pinned=0, m_in=0, m_out=0, d_in=0, d_out=0,
              hours=[0] * 12, top=[])
    per = []
    for c in chats.values():
        st["unread"] += c.get("unread", 0)
        st["blocked"] += 1 if c.get("blocked") else 0
        st["pinned"] += 1 if c.get("pinned") else 0
        n24 = 0
        for m in c["messages"]:
            age = now - m["ts"]
            inc = m["dir"] == "in"
            st["m_in" if inc else "m_out"] += 1
            if age < 86400:
                n24 += 1
                st["d_in" if inc else "d_out"] += 1
            h = int(age // 3600)
            if 0 <= h < 12:
                st["hours"][11 - h] += 1
        if n24:
            per.append((n24, c["name"], c["id"]))
    per.sort(reverse=True)
    st["top"] = per[:5]
    return st


class AccessStore:
    """Who may use the hidden /samx panel. The owner (ADMIN_CHAT_ID) is always allowed."""

    def __init__(self, db, owner):
        self.db, self.owner = db, owner
        self.users = db.load_access()

    def save(self):
        self.db.save_access(self.users)

    def role(self, uid):
        if uid and uid == self.owner:
            return "owner"
        return self.users.get(str(uid), {}).get("status", "none")

    def can(self, uid):
        return self.role(uid) in ("owner", "allowed")

    def touch(self, frm):
        uid = frm["id"]
        name = " ".join(filter(None, [frm.get("first_name"), frm.get("last_name")])) or str(uid)
        now = int(time.time())
        r = self.users.setdefault(str(uid), {"status": "pending", "first": now, "hits": 0, "notify": True})
        r.update(name=name, username=frm.get("username") or "", last=now)
        r["hits"] = r.get("hits", 0) + 1
        self.save()
        return r

    def set(self, uid, status):
        now = int(time.time())
        r = self.users.setdefault(str(uid), {"first": now, "last": now, "hits": 0, "name": str(uid),
                                             "username": "", "notify": False})
        r["status"] = status
        self.save()

    def forget(self, uid):
        self.users.pop(str(uid), None)
        self.save()

    def pending(self):
        return sum(1 for r in self.users.values() if r.get("status") == "pending")

    def operators(self):
        return sum(1 for r in self.users.values() if r.get("status") == "allowed")

    def sorted(self):
        order = {"pending": 0, "allowed": 1, "banned": 2, "none": 3}
        items = [(int(k), v) for k, v in self.users.items()]
        items.sort(key=lambda kv: (order.get(kv[1].get("status"), 9), -kv[1].get("last", 0)))
        return items


def btn(text, data, url=None):
    b = {"text": text}
    if url:
        b["url"] = url
    else:
        b["callback_data"] = data[:64]
    return b


def kb(rows):
    return json.dumps({"inline_keyboard": rows}, ensure_ascii=False)


class Panel:
    """Inline-keyboard control panel inside Telegram. Invisible to normal users: only /samx opens it,
    nothing about it appears in /start or the bot menu. Everything here runs on the Tk thread."""

    PAGE = 6

    def __init__(self, app):
        self.app = app
        self.sessions = {}
        self._notified = {}
        self._jobs = {}

    @property
    def access(self):
        return self.app.access

    # ───── plumbing ─────
    def sess(self, uid, chat_id=None):
        s = self.sessions.get(uid)
        if s is None:
            s = self.sessions[uid] = dict(chat_id=chat_id or uid, mid=None, view="home", prev="home", open=None,
                                          mode=None, page=0, flt="all", q="", n=8, au=None, prompt="", pending=None,
                                          edit_mid=None, sched=None)
        if chat_id:
            s["chat_id"] = chat_id
        return s

    @staticmethod
    def who(frm):
        name = " ".join(filter(None, [frm.get("first_name"), frm.get("last_name")])) or str(frm.get("id"))
        return name + (f" (@{frm['username']})" if frm.get("username") else "")

    @staticmethod
    def stamp():
        return time.strftime("%H:%M:%S")

    def head(self, uid, title):
        tag = "👑 owner" if self.access.role(uid) == "owner" else "🟢 operator"
        return f"<b>🛰 {title}</b>\n<code>{tag} · ID {uid}</code>\n━━━━━━━━━━━━━━\n"

    def tell_owner(self, text, rows=None, silent=False):
        o = self.access.owner
        if not o:
            return
        d = {"chat_id": o, "text": text[:4000], "parse_mode": "HTML",
             "disable_notification": "true" if silent else "false"}
        if rows:
            d["reply_markup"] = kb(rows)
        self.app.api("sendMessage", d)

    def clean(self, m):
        if self.app.settings.get("panel_clean", True):
            self.app.api("deleteMessage", {"chat_id": m["chat"]["id"], "message_id": m["message_id"]})

    def show(self, uid, new=False):
        s = self.sessions.get(uid)
        if not s:
            return
        text, rows = getattr(self, "v_" + s["view"])(uid, s)
        data = {"chat_id": s["chat_id"], "text": text[:4000], "parse_mode": "HTML",
                "disable_web_page_preview": "true", "reply_markup": kb(rows)}
        mid = None if new else s.get("mid")
        if mid:
            data["message_id"] = mid

            def failed(e):
                if "not modified" not in e:
                    s["mid"] = None
                    self.show(uid, new=True)
            self.app.api("editMessageText", data, err=failed)
        else:
            def sent(res):
                s["mid"] = res["message_id"]
            self.app.api("sendMessage", data, cb=sent)

    # ───── entry points ─────
    def intercept(self, m):
        """Return True when the message belongs to the panel (never shows up as a normal chat)."""
        if m["chat"].get("type") != "private":
            return False
        frm = m.get("from") or {}
        uid = frm.get("id")
        if not uid:
            return False
        text = (m.get("text") or "").strip()
        if PANEL_RE.match(text):
            if self.access.role(uid) != "owner":
                self.app.api("sendMessage", {"chat_id": uid, "text": "⛔ This command is owner-only."})
                return True
            self.on_command(m)
            return True
        if SECRET_RE.match(text):
            self.on_command(m)
            return True
        if not self.access.can(uid):
            return False
        s = self.sessions.get(uid)
        if not s:
            return False
        if s.get("mode"):
            self.on_mode_input(uid, s, m)
            return True
        if s.get("view") == "chat" and s.get("open") is not None and not text.startswith("/"):
            self.relay(uid, m, s["open"])
            return True
        return False

    def on_command(self, m):
        app = self.app
        frm = m["from"]
        uid = frm["id"]
        role = self.access.role(uid)
        if role == "banned":
            self.access.touch(frm)
            app.log_event(uid, "🚫 banned user tried /samx", frm)
            return
        if role in ("owner", "allowed"):
            if role == "allowed":
                self.access.touch(frm)
                app.log_event(uid, "🟢 opened the panel", frm)
                if app.settings.get("log_usage", True):
                    self.tell_owner(f"🛰 <b>Panel in use</b>\n👤 {esc(self.who(frm))}\n🆔 <code>{uid}</code>", silent=True)
            s = self.sess(uid, m["chat"]["id"])
            if s.get("mid"):
                app.api("deleteMessage", {"chat_id": s["chat_id"], "message_id": s["mid"]})
                s["mid"] = None
            s.update(view="home", mode=None, open=None, sched=None)
            self.clean(m)
            self.show(uid, new=True)
            return
        rec = self.access.touch(frm)
        if rec.get("status") == "none":
            rec["status"] = "pending"
            self.access.save()
        app.log_event(uid, "⏳ requested access (/samx)", frm)
        app.access_changed()
        if time.time() - self._notified.get(uid, 0) > 60:
            self._notified[uid] = time.time()
            self.tell_owner(
                f"🔔 <b>/samx access request</b>\n👤 {esc(self.who(frm))}\n🆔 <code>{uid}</code>\n"
                f"🔁 attempts: {rec.get('hits', 1)}",
                [[btn("✅ Allow", f"a:{uid}:allowed"), btn("🚫 Ban", f"a:{uid}:banned")],
                 [btn("🙈 Ignore", "a:ign")]])

    def on_mode_input(self, uid, s, m):
        app = self.app
        text = (m.get("text") or "").strip()
        mode = s["mode"]
        owner = self.access.role(uid) == "owner"
        keep_src = False
        if mode == "search":
            s.update(q=text, flt="all", page=0, view="chats")
        elif mode == "note":
            c = app.chats.get(s["open"])
            if c:
                c["note"] = "" if text in ("", "-") else text[:300]
                app.mark_dirty(c["id"])
                app.bump()
                if app.current == c["id"]:
                    app.update_header()
            s["view"] = "chat"
        elif mode == "edit" and text:
            cid, mid = s.get("open"), s.get("edit_mid")

            def work():
                try:
                    app.tg.call("editMessageText", {"chat_id": cid, "message_id": mid, "text": text[:4096]})
                    app.q.put(("edit_ok", cid, mid, text))
                except Exception as e:  # noqa: BLE001
                    app.q.put(("toast", f"✏️ Edit failed: {e}"))
            if cid and mid:
                app.pool.submit(work)
            s.update(view="chat", mode=None, edit_mid=None)
        elif mode == "schedtext" and owner and text:
            s.setdefault("sched", {})["text"] = text[:4096]
            self.prompt(s, "schedtime", "⏰ When should it send?\n"
                                        "Send <code>YYYY-MM-DD HH:MM</code>, <code>HH:MM</code>, or <code>+30</code> (minutes).")
            if not keep_src:
                self.clean(m)
            self.show(uid)
            return
        elif mode == "schedtime" and owner and text:
            ts = self.parse_when(text)
            if not ts:
                self.prompt(s, "schedtime", "⚠️ Could not read that time.\n"
                                            "Try <code>2026-11-10 14:30</code>, <code>14:30</code>, or <code>+30</code>.")
                if not keep_src:
                    self.clean(m)
                self.show(uid)
                return
            s.setdefault("sched", {})["send_at"] = ts
            s["view"] = "schedrep"
        elif mode == "addchat" and owner and text:
            q = text
            if "t.me/" in q:
                q = q.split("t.me/")[-1].strip("/").split("?")[0]
            if q.startswith("+") or q.startswith("joinchat/"):
                self.prompt(s, "addchat", "⚠️ Private invite links cannot be resolved.\n"
                                          "Add the bot to that group, or send its numeric ID / @username.")
                if not keep_src:
                    self.clean(m)
                self.show(uid)
                return
            if q.startswith("@"):
                target = q
            elif q.lstrip("-").isdigit():
                target = int(q)
            else:
                target = "@" + q.lstrip("@")

            def work():
                try:
                    info = app.tg.call("getChat", {"chat_id": target}, timeout=30)
                    members = 0
                    if info.get("type") in ("group", "supergroup", "channel"):
                        try:
                            members = int(app.tg.call("getChatMemberCount", {"chat_id": info["id"]}, timeout=20))
                        except Exception:  # noqa: BLE001
                            pass
                    app.q.put(("add_chat", info, members, uid))
                except Exception as e:  # noqa: BLE001
                    app.api("sendMessage", {"chat_id": s["chat_id"], "parse_mode": "HTML",
                                            "text": f"⚠️ Add chat failed: <i>{esc(str(e))}</i>"})
            app.pool.submit(work)
            s["view"] = "tools"
        elif mode == "autotext" and owner and text:
            app.set_ar(text=text[:1000])
            s["view"] = "auto"
        elif mode == "bc" and owner:
            s.update(pending=m, view="bcc")
            keep_src = True
        else:
            s["view"] = "home"
        s["mode"] = None
        if not keep_src:
            self.clean(m)
        self.show(uid)

    def relay(self, uid, m, cid):
        app = self.app
        if cid not in app.chats:
            return
        opname = self.who(m.get("from") or {"id": uid}).split(" (@")[0]

        def work():
            res = app.tg.call("copyMessage", {"chat_id": cid, "from_chat_id": m["chat"]["id"], "message_id": m["message_id"]})
            rec = parse_message(m, "out")
            for k in ("reply", "reply_id", "reply_who"):
                rec.pop(k, None)
            rec.update(id=res["message_id"], ts=int(time.time()), via=opname)
            app.q.put(("sent_local", cid, rec))
            if app.settings.get("panel_clean", True):
                try:
                    app.tg.call("deleteMessage", {"chat_id": m["chat"]["id"], "message_id": m["message_id"]})
                except Exception:  # noqa: BLE001
                    pass
            app.q.put(("panel_refresh", uid))
        app.bg(work)

    # ───── live updates ─────
    def on_chat_update(self, cid):
        for uid, s in list(self.sessions.items()):
            if s.get("view") == "chat" and s.get("open") == cid and s.get("mid") and uid not in self._jobs:
                self._jobs[uid] = self.app.after(1200, lambda u=uid: self._live(u))

    def _live(self, uid):
        self._jobs.pop(uid, None)
        self.show(uid)

    def notify_new(self, c, msg):
        cid = c["id"]
        if c.get("muted"):
            return
        targets = []
        owner = self.access.owner
        if owner and cid != owner and self.app.settings["notify_admin"]:
            targets.append(owner)
        for k, r in self.access.users.items():
            if r.get("status") == "allowed" and r.get("notify") and int(k) != cid:
                targets.append(int(k))
        for uid in targets:
            s = self.sessions.get(uid)
            if s and s.get("view") == "chat" and s.get("open") == cid:
                continue
            text = f"📩 <b>{esc(c['name'])}</b> · <code>{cid}</code>\n{esc(preview_of(msg)[:300])}"
            self.app.api("sendMessage", {"chat_id": uid, "text": text, "parse_mode": "HTML",
                                         "reply_markup": kb([[btn("↩ Reply", f"n:{cid}"), btn("👁 Read", f"nr:{cid}")]])})

    # ───── callbacks ─────
    def on_callback(self, cq):
        app = self.app
        uid = cq["from"]["id"]
        data = cq.get("data") or ""
        msg = cq.get("message") or {}
        mid = msg.get("message_id")
        ch = msg.get("chat") or {}
        note = None
        if self.access.can(uid) and ch.get("type") == "private" and mid:
            try:
                note = self.dispatch(uid, self.sess(uid, ch["id"]), data, mid)
            except Exception as e:  # noqa: BLE001
                note = f"⚠ {e}"[:150]
        app.api("answerCallbackQuery", {"callback_query_id": cq["id"], "text": note or ""})

    def dispatch(self, uid, s, data, mid):
        app = self.app
        owner = self.access.role(uid) == "owner"
        kind, _, rest = data.partition(":")
        adopt, do_show = True, True

        def need_owner():
            return None if owner else "⛔ Owner only"

        if kind == "m":
            if rest in ("home", "chats", "stats"):
                s.update(view=rest, mode=None, open=None)
            elif rest in ("access", "auto", "sched", "tools"):
                if need_owner():
                    return need_owner()
                s.update(view=rest, mode=None, open=None, page=0)
            elif rest == "set":
                s.update(view="set", mode=None, open=None)
            elif rest == "ref":
                pass
            elif rest == "noop":
                do_show = False
            elif rest == "clr":
                s.update(q="", view="chats", page=0)
            elif rest == "cancel":
                s.update(mode=None, view=s.get("prev", "home"))
            elif rest == "search":
                self.prompt(s, "search", "🔍 Send a name, @username, ID or keyword to search.")
            elif rest == "bc":
                if need_owner():
                    return need_owner()
                self.prompt(s, "bc", "📣 Send the message to broadcast — text, photo, file, voice… anything.")
            elif rest == "bcno":
                s.update(pending=None, view="home")
            elif rest == "bcgo":
                if need_owner():
                    return need_owner()
                self.do_broadcast(uid, s)
                s.update(pending=None, view="home")
                return_note = "📣 Sending…"
                s["mid"] = mid
                self.show(uid)
                return return_note
            elif rest == "close":
                app.api("deleteMessage", {"chat_id": s["chat_id"], "message_id": mid})
                self.sessions.pop(uid, None)
                return None
        elif kind == "t":
            if need_owner():
                return need_owner()
            if rest == "add":
                self.prompt(s, "addchat", "➕ Send an @username, t.me link, or numeric chat ID.\n"
                                          "<i>Private invite links cannot be resolved; add the bot to that group instead.</i>")
            elif rest == "sched":
                s.update(sched={"chat_id": "all"})
                self.prompt(s, "schedtext", "⏰ Send the message to schedule for <b>all chats</b>.")
        elif kind == "sc":
            if need_owner():
                return need_owner()
            if rest == "new":
                s.update(sched={"chat_id": "all"})
                self.prompt(s, "schedtext", "⏰ Send the message to schedule for <b>all chats</b>.")
            elif rest == "cancel":
                s["sched"], s["view"] = None, "sched"
            elif rest.startswith("rep:"):
                rep = rest[4:]
                d = s.get("sched") or {}
                if rep not in ("once", "daily", "weekly") or not d.get("text") or not d.get("send_at"):
                    return "⚠️ Schedule expired — start again"
                app.schedule_add(d.get("chat_id", "all"), d["text"], int(d["send_at"]), rep)
                s["sched"], s["view"] = None, "sched"
                return "⏰ Scheduled"
        elif kind == "f":
            s.update(flt=rest, page=0, view="chats", open=None)
        elif kind == "p":
            s["page"] = int(rest)
        elif kind == "c":
            cid = int(rest)
            c = app.chats.get(cid)
            if not c:
                return "Chat no longer exists"
            s.update(open=cid, view="chat", n=8, mode=None)
            c["unread"] = 0
            app.mark_dirty(cid)
            app.bump()
            app.refresh_list()
            app.update_title()
        elif kind == "d":
            if need_owner():
                return need_owner()
            app.schedule_del(rest)
            s["view"] = "sched"
        elif kind == "x":
            c = app.chats.get(s["open"])
            act, _, _arg = rest.partition(":")
            if not c:
                s.update(view="chats", open=None)
            elif act == "read":
                c["unread"] = 0
                app.mark_dirty(c["id"])
                app.bump()
                app.refresh_list()
                app.update_title()
            elif act == "pin":
                app.set_flag(c["id"], "pinned", not c["pinned"])
            elif act == "mute":
                app.set_flag(c["id"], "muted", not c["muted"])
            elif act == "blk":
                if need_owner():
                    return need_owner()
                app.set_flag(c["id"], "blocked", not c["blocked"])
            elif act == "old":
                s["n"] = min(s["n"] + 8, 40)
            elif act == "edit":
                mid = next((m["id"] for m in reversed(c["messages"])
                            if m["dir"] == "out" and m["kind"] == "text"), None)
                if mid is None:
                    return "✏️ No sent text messages to edit yet"
                s["edit_mid"] = mid
                self.prompt(s, "edit", "✏️ Send the new text for your last message  (or send /samx to cancel).")
            elif act == "note":
                self.prompt(s, "note", f"📝 Send the note for <b>{esc(c['name'])}</b>  (send <code>-</code> to clear).")
            elif act == "sched":
                if need_owner():
                    return need_owner()
                s["sched"] = {"chat_id": c["id"]}
                self.prompt(s, "schedtext", f"⏰ Send the message to schedule for <b>{esc(c['name'])}</b>.")
            elif act == "del":
                if need_owner():
                    return need_owner()
                s["view"] = "delchat"
            elif act == "delgo":
                if need_owner():
                    return need_owner()
                app.chats.pop(c["id"], None)
                app.db.delete_chat(c["id"])
                app.bump()
                s.update(view="chats", open=None)
                return "🗑 Chat removed"
            elif act == "back":
                s.update(view="chats", open=None)
        elif kind == "r":
            if need_owner():
                return need_owner()
            s["view"] = "auto"
            ar = app.ar()
            if rest == "t":
                app.set_ar(enabled=not ar["enabled"])
            elif rest == "txt":
                self.prompt(s, "autotext", "✏️ Send the new auto-reply text.")
            elif rest.startswith("cd:"):
                app.set_ar(cooldown_min=int(rest[3:]))
        elif kind == "s":
            s["view"] = "set"
            if rest == "n":
                if owner:
                    app.settings["notify_admin"] = not app.settings["notify_admin"]
                    app.save_settings()
                else:
                    r = self.access.users.get(str(uid))
                    if r is not None:
                        r["notify"] = not r.get("notify", False)
                        self.access.save()
            elif rest in ("u", "cl"):
                if need_owner():
                    return need_owner()
                key = "log_usage" if rest == "u" else "panel_clean"
                app.settings[key] = not app.settings.get(key, True)
                app.save_settings()
        elif kind == "a":
            if need_owner():
                return need_owner()
            if rest == "ign":
                app.api("deleteMessage", {"chat_id": s["chat_id"], "message_id": mid})
                return None
            parts = rest.split(":")
            if parts[0] == "v":
                s.update(view="auser", au=int(parts[1]))
            elif parts[0] == "pg":
                s["page"] = int(parts[1])
            elif parts[0] == "f":
                app.forget_access(int(parts[1]))
                s.update(view="access", au=None)
            else:
                target, st = int(parts[0]), parts[1]
                app.set_access(target, st)
                if s.get("view") in ("access", "auser") and mid == s.get("mid"):
                    pass
                else:  # tapped on the owner notification → edit it in place
                    r = self.access.users.get(str(target), {})
                    label = {"allowed": "✅ Allowed", "banned": "🚫 Banned", "none": "↩ Revoked"}[st]
                    rows = {"allowed": [[btn("↩ Revoke", f"a:{target}:none"), btn("🚫 Ban", f"a:{target}:banned")]],
                            "banned": [[btn("♻ Unban", f"a:{target}:none"), btn("✅ Allow", f"a:{target}:allowed")]],
                            "none": [[btn("✅ Allow", f"a:{target}:allowed"), btn("🚫 Ban", f"a:{target}:banned")]]}[st]
                    app.api("editMessageText", {"chat_id": s["chat_id"], "message_id": mid, "parse_mode": "HTML",
                                                "text": f"{label} — {esc(r.get('name', str(target)))} <code>{target}</code>",
                                                "reply_markup": kb(rows)})
                    return label
        elif kind == "n":
            cid = int(rest)
            c = app.chats.get(cid)
            if not c:
                return "Chat no longer exists"
            s.update(open=cid, view="chat", n=8, mode=None)
            c["unread"] = 0
            app.mark_dirty(cid)
            app.bump()
            app.refresh_list()
            app.update_title()
            self.show(uid, new=True)
            return None
        elif kind == "nr":
            c = app.chats.get(int(rest))
            if c:
                c["unread"] = 0
                app.mark_dirty(c["id"])
                app.bump()
                app.refresh_list()
                app.update_title()
            return "✅ Marked read"
        if do_show:
            if adopt:
                s["mid"] = mid
            self.show(uid)
        return None

    @staticmethod
    def parse_when(text):
        now = time.time()
        t = text.strip()
        m = re.match(r"^\+\s*(\d+)\s*(m|min|mins|minute|minutes|h|hr|hour|hours)?$", t, re.I)
        if m:
            n = int(m.group(1))
            if (m.group(2) or "m").lower().startswith("h"):
                n *= 60
            return int(now + n * 60) if n > 0 else None
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%H:%M"):
            try:
                tm = time.strptime(t, fmt)
            except ValueError:
                continue
            if fmt == "%H:%M":
                lt = time.localtime(now)
                ts = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, tm.tm_hour, tm.tm_min, 0, 0, 0, -1))
                if ts <= now:
                    ts += 86400
            else:
                ts = time.mktime(tm)
            return int(ts) if ts > now else None
        return None

    def prompt(self, s, mode, text):
        s.update(prev=s["view"] if s["view"] != "prompt" else s.get("prev", "home"), mode=mode, view="prompt", prompt=text)

    def do_broadcast(self, uid, s):
        app = self.app
        src = s.get("pending")
        if not src:
            return
        targets = [c["id"] for c in app.chats.values()
                   if not c.get("blocked") and not self.access.can(c["id"]) and self.access.role(c["id"]) != "banned"]

        def work():
            ok = 0
            for t in targets:
                try:
                    res = app.tg.call("copyMessage", {"chat_id": t, "from_chat_id": src["chat"]["id"], "message_id": src["message_id"]})
                    rec = parse_message(src, "out")
                    for k in ("reply", "reply_id", "reply_who"):
                        rec.pop(k, None)
                    rec.update(id=res["message_id"], ts=int(time.time()))
                    app.q.put(("sent_local", t, rec))
                    ok += 1
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(0.06)
            try:
                self.clean(src)
            except Exception:  # noqa: BLE001
                pass
            app.q.put(("toast", f"📣 Broadcast: {ok}/{len(targets)}"))
            app.api("sendMessage", {"chat_id": s["chat_id"], "text": f"📣 Broadcast done: <b>{ok}/{len(targets)}</b>", "parse_mode": "HTML"})
        app.pool.submit(work)

    # ───── views ─────
    def v_home(self, uid, s):
        app = self.app
        st = compute_stats(app.chats)
        ar = app.ar()
        owner = self.access.role(uid) == "owner"
        notif = app.settings["notify_admin"] if owner else self.access.users.get(str(uid), {}).get("notify", False)
        t = self.head(uid, "SamX Control Center")
        t += (f"💬 <b>{st['chats']}</b> chats · 🔴 <b>{st['unread']}</b> unread\n"
              f"📈 24h: ⬇ {st['d_in']}   ⬆ {st['d_out']}\n"
              f"<code>{spark(st['hours'])}</code>  <i>last 12h</i>\n"
              f"🤖 auto-reply <b>{'ON' if ar['enabled'] else 'OFF'}</b> · 🔔 alerts <b>{'ON' if notif else 'OFF'}</b>\n"
              f"<i>🕒 {self.stamp()}</i>")
        rows = [[btn(f"💬 Chats ({st['unread']})" if st["unread"] else "💬 Chats", "m:chats"), btn("🔍 Search", "m:search")],
                [btn("📊 Stats", "m:stats"), btn("🔄 Refresh", "m:ref")]]
        if owner:
            pend = self.access.pending()
            rows.append([btn("📣 Broadcast", "m:bc"), btn("🤖 Auto-reply", "m:auto")])
            rows.append([btn(f"👥 Access ({pend})" if pend else "👥 Access", "m:access"), btn("⚙️ Settings", "m:set")])
            rows.append([btn("🛠 Tools", "m:tools"), btn(f"⏰ Schedules ({len(app.schedules)})", "m:sched")])
        else:
            rows.append([btn("⚙️ Settings", "m:set")])
        rows.append([btn("✖ Close", "m:close")])
        return t, rows

    @staticmethod
    def match(c, q):
        return (q in c["name"].lower() or q in (c.get("username") or "").lower() or q in str(c["id"])
                or q in (c.get("note") or "").lower() or any(q in (m["text"] or "").lower() for m in c["messages"][-100:]))

    def v_chats(self, uid, s):
        cs = self.app.ordered_chats()
        if s["flt"] == "unread":
            cs = [c for c in cs if c["unread"]]
        elif s["flt"] == "pinned":
            cs = [c for c in cs if c.get("pinned")]
        elif s["flt"] == "groups":
            cs = [c for c in cs if c.get("type") in ("group", "supergroup", "channel")]
        q = s["q"].lower()
        if q:
            cs = [c for c in cs if self.match(c, q)]
        pages = max(1, -(-len(cs) // self.PAGE))
        s["page"] = max(0, min(s["page"], pages - 1))
        p = s["page"]
        t = self.head(uid, "Chats") + f"💬 <b>{len(cs)}</b> chats · page {p + 1}/{pages}"
        if q:
            t += f"\n🔍 “{esc(s['q'])}”"
        if not cs:
            t += "\n\n<i>Nothing here.</i>"
        t += f"\n<i>🕒 {self.stamp()}</i>"
        rows = []
        for c in cs[p * self.PAGE:(p + 1) * self.PAGE]:
            lab = ("🔴 " if c["unread"] else "⚪️ ") + ("📌" if c.get("pinned") else "") + ("🔕" if c.get("muted") else "") \
                + ("🚫" if c.get("blocked") else "") + c["name"][:18] + (f" ({c['unread']})" if c["unread"] else "")
            rows.append([btn(lab, f"c:{c['id']}")])
        nav = []
        if p > 0:
            nav.append(btn("◀", f"p:{p - 1}"))
        nav.append(btn(f"{p + 1}/{pages}", "m:noop"))
        if p < pages - 1:
            nav.append(btn("▶", f"p:{p + 1}"))
        rows.append(nav)
        rows.append([btn(("● " if s["flt"] == k else "") + lab, f"f:{k}")
                     for k, lab in (("all", "All"), ("unread", "Unread"), ("groups", "Groups"), ("pinned", "Pinned"))])
        if q:
            rows.append([btn("✖ Clear search", "m:clr")])
        rows.append([btn("🔄", "m:ref"), btn("🏠 Home", "m:home")])
        return t, rows

    def fmt_msgs(self, c, n):
        lines, size = [], 0
        for m in reversed(c["messages"][-n:]):
            body = m["text"] or ""
            if m["kind"] != "text":
                body = f"{KIND_ICON.get(m['kind'], '📎')} {m.get('name') or m['kind']} {body}".strip()
            who = "🧑‍💻" if m["dir"] == "out" else "👤"
            line = f"<code>{time.strftime('%H:%M', time.localtime(m['ts']))}</code> {who} {esc(body[:280])}"
            size += len(line) + 1
            if size > 2900:
                break
            lines.append(line)
        return "\n".join(reversed(lines)) or "<i>No messages yet.</i>"

    def v_chat(self, uid, s):
        c = self.app.chats.get(s["open"])
        if not c:
            s.update(view="chats", open=None)
            return self.v_chats(uid, s)
        owner = self.access.role(uid) == "owner"
        t = self.head(uid, "Chat")
        t += f"👤 <b>{esc(c['name'])}</b>" + (f" · @{esc(c['username'])}" if c.get("username") else "")
        t += f"\n🆔 <code>{c['id']}</code>\n"
        if c.get("note"):
            t += f"📝 <i>{esc(c['note'][:200])}</i>\n"
        t += "━━━━━━━━━━━━━━\n" + self.fmt_msgs(c, s["n"])
        t += f"\n━━━━━━━━━━━━━━\n✍️ <i>Type here to reply — I relay it.</i>  <i>🕒 {self.stamp()}</i>"
        rows = [[btn("🔄 Refresh", "x:ref"), btn("✅ Read", "x:read"), btn("📝 Note", "x:note")],
                [btn("✏️ Edit last", "x:edit"), btn("📌 Unpin" if c["pinned"] else "📌 Pin", "x:pin"),
                 btn("🔔 Unmute" if c["muted"] else "🔕 Mute", "x:mute")]]
        if owner:
            rows[1].append(btn("✅ Unblock" if c["blocked"] else "🚫 Block", "x:blk"))
            rows.append([btn("⏰ Schedule", "x:sched"), btn("🗑 Delete chat", "x:del")])
        rows.append([btn("🕘 Older", "x:old")])
        rows.append([btn("⬅ Chats", "x:back"), btn("🏠 Home", "m:home")])
        return t, rows

    def v_sched(self, uid, s):
        if self.access.role(uid) != "owner":
            s["view"] = "home"
            return self.v_home(uid, s)
        app = self.app
        jobs = list(app.schedules)
        active = sum(1 for j in jobs if j.get("status") == "active")
        t = self.head(uid, "Scheduled messages")
        t += f"⏰ <b>{len(jobs)}</b> total · 🟢 <b>{active}</b> active · max {MAX_SCHEDULES}"
        if not jobs:
            t += "\n\n<i>No scheduled messages yet.</i>"
        else:
            t += "\n"
            shown = 0
            for j in jobs:
                if shown >= 10:
                    break
                shown += 1
                cid = j.get("chat_id")
                target = "🌐 All chats" if cid == "all" else esc(app.chats.get(cid, {}).get("name", f"ID {cid}"))
                when = time.strftime("%Y-%m-%d %H:%M", time.localtime(int(j.get("send_at", 0))))
                icon = {"active": "🟢", "sending": "📤", "done": "✅"}.get(j.get("status"), "⚪️")
                t += (f"{icon} <code>{esc(str(j.get('id')))}</code> · {target}\n"
                      f"   {esc((j.get('text') or '')[:70])}\n"
                      f"   <i>{when} · {esc(str(j.get('repeat') or 'once'))}"
                      f"{' · ' + esc(str(j.get('result'))) if j.get('result') else ''}</i>\n")
            if len(jobs) > shown:
                t += f"\n<i>…and {len(jobs) - shown} more.</i>"
        t += f"\n<i>🕒 {self.stamp()}</i>"
        rows = [[btn("➕ New schedule", "sc:new"), btn("🔄 Refresh", "m:sched")],
                [btn("🛠 Tools", "m:tools"), btn("🏠 Home", "m:home")]]
        for j in jobs[:8]:
            if j.get("status") == "sending":
                continue
            label = (j.get("text") or "").replace("\n", " ")[:24] or str(j.get("id"))
            rows.append([btn(f"🗑 {label}", f"d:{j.get('id')}")])
        return t, rows

    def v_schedrep(self, uid, s):
        if self.access.role(uid) != "owner":
            s["view"] = "home"
            return self.v_home(uid, s)
        d = s.get("sched") or {}
        if not d.get("text") or not d.get("send_at"):
            s["view"] = "sched"
            return self.v_sched(uid, s)
        cid = d.get("chat_id", "all")
        target = "🌐 All chats" if cid == "all" else esc(self.app.chats.get(cid, {}).get("name", f"ID {cid}"))
        when = time.strftime("%Y-%m-%d %H:%M", time.localtime(int(d["send_at"])))
        t = self.head(uid, "New schedule")
        t += f"📤 To: <b>{target}</b>\n🕒 <b>{when}</b>\n\n<i>{esc(d['text'][:400])}</i>\n\nChoose repeat:"
        return t, [[btn("1️⃣ Once", "sc:rep:once")],
                   [btn("🔁 Daily", "sc:rep:daily"), btn("📅 Weekly", "sc:rep:weekly")],
                   [btn("✖ Cancel", "sc:cancel")]]

    def v_tools(self, uid, s):
        if self.access.role(uid) != "owner":
            s["view"] = "home"
            return self.v_home(uid, s)
        app = self.app
        t = self.head(uid, "Tools")
        t += f"💬 <b>{len(app.chats)}</b> chats · ⏰ <b>{len(app.schedules)}</b> schedules\n"
        t += "<i>Add chats, schedule messages, and manage everything from here.</i>"
        return t, [[btn("➕ Add chat / group", "t:add")],
                   [btn("⏰ New schedule", "sc:new"), btn("⏰ Schedules", "m:sched")],
                   [btn("💬 Chats", "m:chats"), btn("📊 Stats", "m:stats")],
                   [btn("🏠 Home", "m:home")]]

    def v_delchat(self, uid, s):
        if self.access.role(uid) != "owner":
            s["view"] = "home"
            return self.v_home(uid, s)
        c = self.app.chats.get(s["open"])
        if not c:
            s["view"] = "chats"
            return self.v_chats(uid, s)
        t = self.head(uid, "Delete chat")
        t += f"🗑 Remove <b>{esc(c['name'])}</b> from the console?\n\n"
        t += "<i>This deletes the local history in relay.db. Nothing is deleted on Telegram.</i>"
        return t, [[btn("🗑 Delete", "x:delgo"), btn("✖ Cancel", "x:back")]]

    def v_stats(self, uid, s):
        st = compute_stats(self.app.chats)
        t = self.head(uid, "Stats")
        t += (f"💬 Chats <b>{st['chats']}</b>  (📌 {st['pinned']} · 🚫 {st['blocked']})\n"
              f"🔴 Unread <b>{st['unread']}</b>\n"
              f"✉️ Messages ⬇ {st['m_in']}  ⬆ {st['m_out']}\n"
              f"📈 24h  ⬇ {st['d_in']}  ⬆ {st['d_out']}\n"
              f"<code>{spark(st['hours'])}</code>  <i>last 12h</i>\n")
        if st["top"]:
            t += "\n🏆 <b>Most active (24h)</b>\n" + "\n".join(
                f"{i + 1}. {esc(n[:22])} — {k}" for i, (k, n, _id) in enumerate(st["top"])) + "\n"
        t += f"\n👥 operators <b>{self.access.operators()}</b> · ⏳ pending <b>{self.access.pending()}</b>\n<i>🕒 {self.stamp()}</i>"
        return t, [[btn("🔄 Refresh", "m:ref"), btn("🏠 Home", "m:home")]]

    ICON = {"pending": "⏳", "allowed": "🟢", "banned": "🚫", "none": "⚪️"}

    def v_access(self, uid, s):
        items = self.access.sorted()
        pages = max(1, -(-len(items) // 5))
        s["page"] = max(0, min(s["page"], pages - 1))
        p = s["page"]
        t = self.head(uid, "Access") + f"👥 <b>{len(items)}</b> users · ⏳ {self.access.pending()} pending"
        t += "\n<i>Only people who typed /samx show up here.</i>"
        if not items:
            t += "\n\n<i>Nobody yet.</i>"
        rows = []
        for k, r in items[p * 5:(p + 1) * 5]:
            rows.append([btn(f"{self.ICON.get(r.get('status'), '⚪️')} {r.get('name', k)[:16]} · {k}", f"a:v:{k}")])
        nav = []
        if p > 0:
            nav.append(btn("◀", f"a:pg:{p - 1}"))
        nav.append(btn(f"{p + 1}/{pages}", "m:noop"))
        if p < pages - 1:
            nav.append(btn("▶", f"a:pg:{p + 1}"))
        rows.append(nav)
        rows.append([btn("🔄", "m:ref"), btn("🏠 Home", "m:home")])
        return t, rows

    def v_auser(self, uid, s):
        k = s["au"]
        r = self.access.users.get(str(k))
        if not r:
            s["view"] = "access"
            return self.v_access(uid, s)
        st = r.get("status", "none")
        ft = lambda x: time.strftime("%Y-%m-%d %H:%M", time.localtime(x)) if x else "—"  # noqa: E731
        t = self.head(uid, "User")
        t += (f"👤 <b>{esc(r.get('name', str(k)))}</b>" + (f" · @{esc(r['username'])}" if r.get("username") else "") +
              f"\n🆔 <code>{k}</code>\n{self.ICON.get(st, '')} status: <b>{st}</b>\n"
              f"🔁 /samx uses: {r.get('hits', 0)}\n🕐 first: {ft(r.get('first'))}\n🕑 last: {ft(r.get('last'))}")
        if st == "allowed":
            row = [btn("↩ Revoke", f"a:{k}:none"), btn("🚫 Ban", f"a:{k}:banned")]
        elif st == "banned":
            row = [btn("♻ Unban", f"a:{k}:none"), btn("✅ Allow", f"a:{k}:allowed")]
        else:
            row = [btn("✅ Allow", f"a:{k}:allowed"), btn("🚫 Ban", f"a:{k}:banned")]
        return t, [row, [btn("🗑 Forget", f"a:f:{k}")], [btn("⬅ Access", "m:access"), btn("🏠 Home", "m:home")]]

    def v_auto(self, uid, s):
        ar = self.app.ar()
        cd = ar["cooldown_min"]
        t = self.head(uid, "Auto-reply")
        t += (f"Status: <b>{'🟢 ON' if ar['enabled'] else '⚪️ OFF'}</b>\n⏱ Once per chat every <b>{cd}</b> min — and only if you "
              f"haven't replied in that window.\n\n<i>{esc(ar['text'][:600])}</i>")
        opts = [(15, "15m"), (60, "1h"), (360, "6h"), (1440, "24h")]
        return t, [[btn("⏸ Turn OFF" if ar["enabled"] else "▶️ Turn ON", "r:t"), btn("✏️ Edit text", "r:txt")],
                   [btn(("● " if cd == v else "") + lab, f"r:cd:{v}") for v, lab in opts],
                   [btn("⬅ Home", "m:home")]]

    def v_set(self, uid, s):
        owner = self.access.role(uid) == "owner"
        notif = self.app.settings["notify_admin"] if owner else self.access.users.get(str(uid), {}).get("notify", False)
        t = self.head(uid, "Settings") + "Tune how the panel behaves."
        rows = [[btn(f"🔔 New-message alerts: {'ON' if notif else 'OFF'}", "s:n")]]
        if owner:
            rows.append([btn(f"🛰 Usage alerts: {'ON' if self.app.settings.get('log_usage', True) else 'OFF'}", "s:u")])
            rows.append([btn(f"🧹 Clean mode: {'ON' if self.app.settings.get('panel_clean', True) else 'OFF'}", "s:cl")])
            t += "\n<i>Usage alerts = I tell you (with their ID) whenever an operator opens the panel.\nClean mode = relayed messages are deleted from the operator's chat.</i>"
        rows.append([btn("⬅ Home", "m:home")])
        return t, rows

    def v_prompt(self, uid, s):
        return self.head(uid, "Waiting for input") + s["prompt"], [[btn("✖ Cancel", "m:cancel")]]

    def v_bcc(self, uid, s):
        src = s.get("pending")
        n = sum(1 for c in self.app.chats.values()
                if not c.get("blocked") and not self.access.can(c["id"]) and self.access.role(c["id"]) != "banned")
        if not src:
            s["view"] = "home"
            return self.v_home(uid, s)
        prev = src.get("text") or src.get("caption") or "📎 media"
        t = self.head(uid, "Broadcast") + f"Send this to <b>{n}</b> chats?\n\n<i>{esc(prev[:300])}</i>"
        return t, [[btn("✅ Send now", "m:bcgo"), btn("✖ Cancel", "m:bcno")]]




# ───────────────────────────── Headless core ─────────────────────────────
class Core:
    """Provides the small 'app' surface the Panel expects, minus any GUI."""

    def __init__(self):
        self.db = DB(DB_FILE)
        self.db.migrate_json(HISTORY_FILE, ACCESS_FILE, SETTINGS_FILE, SCHEDULES_FILE, include_history=SAVE_HISTORY)
        self.settings = {**DEFAULT_SETTINGS, **self.db.load_settings()}
        self.tg = TG(BOT_TOKEN)
        self.q = queue.Queue()
        self.pool = ThreadPoolExecutor(max_workers=4)
        self.panel_pool = ThreadPoolExecutor(max_workers=1)  # keeps panel edits ordered
        self.access = AccessStore(self.db, ADMIN_CHAT_ID)
        self.panel = Panel(self)
        self.chats = self.db.load_chats(MAX_MSGS) if SAVE_HISTORY else {}
        self.current = None  # (no GUI, kept for Panel compatibility)
        self.events = deque(maxlen=300)
        self._timers, self._seq = [], itertools.count()
        self._dirty, self._dirty_ids, self._dirty_all = False, set(), False
        self._online = None
        self.poller = Poller(BOT_TOKEN, self.q)
        self.schedules = self.db.load_schedules()
        for job in self.schedules:
            if job.get("status") == "sending":  # interrupted by a restart → retry safely
                job["status"] = "active"
                self.db.save_schedule(job)
        self._next_sched = 0

    # ── scheduler / threads
    def after(self, ms, fn):
        heapq.heappush(self._timers, (time.time() + ms / 1000, next(self._seq), fn))

    def bg(self, fn):
        def work():
            try:
                fn()
            except Exception as e:  # noqa: BLE001
                log("❌", e)
        self.pool.submit(work)

    def api(self, method, data=None, cb=None, err=None):
        def work():
            try:
                res = self.tg.call(method, data, timeout=30)
            except Exception as e:  # noqa: BLE001
                if err:
                    self.q.put(("call", err, (str(e),)))
                return
            if cb:
                self.q.put(("call", cb, (res,)))
        self.panel_pool.submit(work)

    # ── settings
    def ar(self):
        return {"enabled": False, "text": "", "cooldown_min": 60, **self.settings.get("auto_reply", {})}

    def bump(self):
        pass

    def save_settings(self):
        self.db.save_settings(self.settings)
        self.bump()

    def set_ar(self, **kw):
        self.settings["auto_reply"] = {**self.ar(), **kw}
        self.save_settings()

    # ── no-op GUI hooks the Panel calls
    def refresh_list(self): pass
    def update_title(self): pass
    def update_header(self): pass
    def access_changed(self): pass

    def mark_dirty(self, cid=None):
        if not SAVE_HISTORY:
            return
        if cid is None:
            self._dirty_all = True
        else:
            self._dirty_ids.add(cid)
        if not self._dirty:
            self._dirty = True
            try:
                self.after(800, self._save_now)
            except Exception:  # noqa: BLE001
                self.flush()
    def _save_now(self):
        if self._dirty:
            self.flush()
    def flush(self):
        if SAVE_HISTORY:
            if self._dirty_all:
                for c in self.chats.values():
                    self.db.save_chat_meta(c)
            else:
                for cid in list(self._dirty_ids):
                    c = self.chats.get(cid)
                    if c:
                        self.db.save_chat_meta(c)
        self._dirty, self._dirty_ids, self._dirty_all = False, set(), False

    # ── data
    def ordered_chats(self):
        return sorted(self.chats.values(), key=lambda c: (not c.get("pinned"), -c["last_ts"]))

    def ensure_chat(self, cid, name, username, ctype=None):
        c = self.chats.get(cid)
        if not c:
            c = self.chats[cid] = {"id": cid, "name": name, "username": username,
                                   "type": ctype or ("private" if cid > 0 else "group"),
                                   "in_bot": True, "members": 0,
                                   "unread": 0, "last_ts": 0,
                                   "messages": [], "pinned": False, "muted": False, "blocked": False, "draft": "",
                                   "note": "", "last_auto": 0}
        else:
            c["name"], c["username"] = name or c["name"], username or c.get("username")
            if ctype:
                c["type"] = ctype
            for k, v in (("unread", 0), ("last_ts", 0), ("messages", []), ("pinned", False), ("muted", False),
                         ("blocked", False), ("note", ""), ("last_auto", 0),
                         ("type", "private" if cid > 0 else "group"), ("in_bot", True), ("members", 0)):
                c.setdefault(k, v)
        self.mark_dirty(cid)
        return c

    def add_message(self, cid, msg):
        c = self.chats[cid]
        if any(x["id"] == msg["id"] and x["dir"] == msg["dir"] for x in c["messages"][-80:]):
            return False
        c["messages"].append(msg)
        del c["messages"][:-MAX_MSGS]
        c["last_ts"] = msg["ts"]
        if SAVE_HISTORY:
            self.db.add_message(cid, msg)
        self.mark_dirty(cid)
        return True

    def set_flag(self, cid, key, value):
        self.chats[cid][key] = value
        self.mark_dirty(cid)
        self.bump()

    def get_message(self, cid, mid):
        c = self.chats.get(cid)
        if not c:
            return None
        m = next((x for x in c["messages"] if x.get("id") == mid), None)
        return dict(m) if m else None

    def apply_edit(self, cid, mid, text):
        c = self.chats.get(cid)
        if not c:
            return False
        for m in c["messages"]:
            if m.get("id") == mid:
                m["text"], m["edited"] = text, True
                if SAVE_HISTORY:
                    self.db.add_message(cid, m)
                self.mark_dirty(cid)
                self.panel.on_chat_update(cid)
                return True
        return False

    # ── access (owner approves others who typed /samx)
    def log_event(self, uid, text, frm=None):
        name = Panel.who(frm) if frm else self.access.users.get(str(uid), {}).get("name", "")
        self.events.appendleft((time.time(), uid, name, text))
        log(f"🛰 {uid} {name} — {text}")

    def set_access(self, uid, status):
        try:
            uid = int(uid)
        except (TypeError, ValueError):
            pass
        prev = self.access.role(uid)
        self.access.set(uid, status)
        c = self.chats.get(uid)
        if c is not None:
            if status == "banned":
                c["blocked"] = True
            elif prev == "banned" and c.get("blocked"):
                c["blocked"] = False
            self.mark_dirty(uid)
        if status == "allowed":
            self.api("sendMessage", {"chat_id": uid, "text": "✅ Access granted.\nSend /samx to open the panel."})
        if status != "allowed":
            self.panel.sessions.pop(uid, None)
        self.log_event(uid, {"allowed": "✅ access granted", "banned": "🚫 banned", "none": "↩ access revoked"}.get(status, status))
        self.bump()

    def forget_access(self, uid):
        self.access.forget(uid)
        self.panel.sessions.pop(uid, None)
        self.log_event(uid, "🗑 removed from access list")
        self.bump()

    # ── incoming
    def on_incoming(self, m):
        frm = m.get("from") or {}
        if m["chat"].get("type") == "private":
            if self.access.role(frm.get("id")) == "banned":
                return
            if self.panel.intercept(m):
                return
        cid = m["chat"]["id"]
        name, user, ctype = chat_info(m["chat"])
        c = self.ensure_chat(cid, name, user, ctype)
        if c["blocked"] or any(k in m for k in SERVICE_KEYS):
            return
        msg = parse_message(m, "in")
        if not self.add_message(cid, msg):
            return
        c["unread"] += 1
        self.mark_dirty(cid)
        self.panel.notify_new(c, msg)
        self.panel.on_chat_update(cid)
        self.maybe_auto_reply(c, msg)

    def on_incoming_edit(self, m):
        cid = m["chat"]["id"]
        c = self.chats.get(cid)
        if not c:
            return
        for x in reversed(c["messages"]):
            if x["id"] == m["message_id"]:
                x["text"], x["edited"] = parse_message(m, "in")["text"], True
                if SAVE_HISTORY:
                    self.db.add_message(cid, x)
                self.mark_dirty(cid)
                self.panel.on_chat_update(cid)
                return

    def on_sent(self, res, flags=None):
        cid = res["chat"]["id"]
        name, user, ctype = chat_info(res["chat"])
        self.ensure_chat(cid, name, user, ctype)
        msg = parse_message(res, "out")
        if flags:
            msg.update(flags)
        self.add_message(cid, msg)
        self.panel.on_chat_update(cid)

    def on_sent_local(self, cid, rec):
        if cid in self.chats:
            self.add_message(cid, rec)
            self.panel.on_chat_update(cid)

    def on_my_chat(self, m):
        """Bot added to / removed from a group or channel (works even with zero messages)."""
        chat = m.get("chat") or {}
        try:
            cid = int(chat.get("id"))
        except (TypeError, ValueError):
            return
        name, user, ctype = chat_info(chat)
        c = self.ensure_chat(cid, name, user, ctype)
        new = m.get("new_chat_member") or {}
        status = new.get("status")
        in_bot = status in ("creator", "administrator", "member") or bool(new.get("is_member"))
        old = c.get("in_bot", True)
        c["in_bot"] = in_bot
        if in_bot != old:
            self.log_event(cid, "➕ bot added to chat" if in_bot else "➖ bot removed from chat", m.get("from"))
        self.mark_dirty(cid)

    def add_chat(self, info, members=0, uid=None):
        try:
            cid = int(info["id"])
        except (KeyError, TypeError, ValueError):
            return None
        name, user, ctype = chat_info(info)
        c = self.ensure_chat(cid, name, user, ctype)
        c["in_bot"] = True
        if members:
            c["members"] = int(members)
        self.mark_dirty(cid)
        self.bump()
        if uid:
            self.q.put(("panel_refresh", uid))
        return cid

    # ── scheduled messages (server-side, persisted in SQLite)
    def _prune_schedules(self):
        extra = len(self.schedules) - MAX_SCHEDULES
        if extra <= 0:
            return
        done = sorted((j for j in self.schedules if j.get("status") == "done"), key=lambda j: j.get("created", 0))
        for j in done[:extra]:
            self.schedules.remove(j)
            self.db.delete_schedule(j["id"])

    def schedule_add(self, chat_id, text, send_at, repeat):
        job = {"id": f"{int(time.time() * 1000):x}{next(self._seq):x}",
               "chat_id": chat_id, "text": text[:4096], "send_at": int(send_at),
               "repeat": repeat, "created": int(time.time()), "last_sent": 0,
               "status": "active", "result": ""}
        self.schedules.append(job)
        self.schedules.sort(key=lambda j: int(j.get("send_at", 0)))
        self._prune_schedules()
        self.db.save_schedule(job)
        self.bump()
        return job

    def schedule_del(self, sid):
        self.schedules = [j for j in self.schedules if str(j.get("id")) != str(sid)]
        self.db.delete_schedule(sid)
        self.bump()

    def schedule_targets(self, chat_id):
        if chat_id == "all":
            return [c["id"] for c in self.chats.values()
                    if c.get("in_bot", True) and not c.get("blocked")
                    and self.access.role(c["id"]) != "banned" and not self.access.can(c["id"])]
        try:
            return [int(chat_id)]
        except (TypeError, ValueError):
            return []

    def _send_schedule(self, job, targets):
        ok = 0
        for t in targets:
            try:
                res = self.tg.call("sendMessage", {"chat_id": t, "text": job["text"]})
                self.q.put(("sent", res))
                ok += 1
            except Exception as e:  # noqa: BLE001
                log("schedule error:", t, e)
            time.sleep(0.06)
        self.q.put(("call", self._finish_schedule, (job.get("id"), ok, len(targets))))

    def _finish_schedule(self, sid, ok, total):
        for j in self.schedules:
            if str(j.get("id")) == str(sid):
                j["status"] = "active" if j.get("repeat", "once") != "once" else "done"
                j["result"] = f"{ok}/{total}"
                self.db.save_schedule(j)
                break
        self._prune_schedules()
        if hasattr(self, "bump"):
            self.bump()
        self.q.put(("toast", f"⏰ Scheduled message: {ok}/{total} sent"))

    def run_schedules(self, now):
        due = [j for j in self.schedules if j.get("status") == "active" and int(j.get("send_at", 0)) <= now]
        for job in due:
            rep = job.get("repeat", "once")
            if rep == "daily":
                job["send_at"] = int(job.get("send_at", now) + 86400)
                while job["send_at"] <= now:
                    job["send_at"] += 86400
            elif rep == "weekly":
                job["send_at"] = int(job.get("send_at", now) + 604800)
                while job["send_at"] <= now:
                    job["send_at"] += 604800
            job["status"] = "sending"
            targets = self.schedule_targets(job.get("chat_id"))
            self.db.save_schedule(job)
            self.bg(lambda j=dict(job), t=list(targets): self._send_schedule(j, t))

    def maybe_auto_reply(self, c, msg):
        ar = self.ar()
        cid = c["id"]
        if not ar["enabled"] or not ar["text"].strip() or c.get("muted") or c.get("blocked"):
            return
        if self.access.can(cid) or (msg["kind"] == "text" and msg["text"].startswith("/")):
            return
        now = time.time()
        gap = max(1, int(ar["cooldown_min"])) * 60
        last_out = next((x["ts"] for x in reversed(c["messages"]) if x["dir"] == "out"), 0)
        if now - c.get("last_auto", 0) < gap or now - last_out < gap:
            return
        c["last_auto"] = now
        text = ar["text"][:4000]
        self.bg(lambda: self.q.put(("sent", self.tg.call("sendMessage", {"chat_id": cid, "text": text}), {"auto": True})))

    # ── event loop (single thread → no locking anywhere)
    def handle(self, ev):
        k = ev[0]
        if k == "msg":
            self.on_incoming(ev[1])
        elif k == "edit_in":
            self.on_incoming_edit(ev[1])
        elif k == "cb":
            self.panel.on_callback(ev[1])
        elif k == "my_chat":
            self.on_my_chat(ev[1])
        elif k == "call":
            ev[1](*ev[2])
        elif k == "sent":
            self.on_sent(ev[1], ev[2] if len(ev) > 2 else None)
        elif k == "sent_local":
            self.on_sent_local(ev[1], ev[2])
        elif k == "edit_ok":
            self.apply_edit(ev[1], ev[2], ev[3])
        elif k == "add_chat":
            self.add_chat(ev[1], ev[2] if len(ev) > 2 else 0, ev[3] if len(ev) > 3 else None)
        elif k == "panel_refresh":
            self.panel.show(ev[1])
        elif k == "toast":
            log(ev[1])
        elif k == "online":
            if ev[1] != self._online:
                self._online = ev[1]
                log("🟢 online" if ev[1] else f"🔴 {ev[2][:80]}")

    def run(self):
        me = self.tg.call("getMe")
        log(f"✈ @{me.get('username')} up · owner {ADMIN_CHAT_ID} · hidden command /samx")
        self.poller.start()
        while True:
            now = time.time()
            while self._timers and self._timers[0][0] <= now:
                _, _, fn = heapq.heappop(self._timers)
                try:
                    fn()
                except Exception as e:  # noqa: BLE001
                    log("timer error:", repr(e))
            if now >= self._next_sched:
                self._next_sched = now + 1
                try:
                    self.run_schedules(now)
                except Exception as e:  # noqa: BLE001
                    log("schedule loop error:", repr(e))
            try:
                ev = self.q.get(timeout=0.25)
            except queue.Empty:
                continue
            try:
                self.handle(ev)
            except Exception as e:  # noqa: BLE001  one bad event never kills the server
                log("event error:", repr(e))

    def shutdown(self, *_):
        self.poller.stop.set()
        try:
            self.flush()
        finally:
            self.db.close()
        log("bye")
        os._exit(0)


# ───────────────────────────── Web layer ─────────────────────────────
import mimetypes
import secrets
from concurrent.futures import Future
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
try:
    WEB_PORT = int(os.getenv("PORT", "") or os.getenv("WEB_PORT", "8080"))
except ValueError:
    WEB_PORT = 8080
WEB_PASSWORD = os.getenv("WEB_PASSWORD", "").strip()
GENERATED = not WEB_PASSWORD
if GENERATED:
    WEB_PASSWORD = secrets.token_urlsafe(9)
SESSIONS = set()


class WebCore(Core):
    def __init__(self):
        super().__init__()
        self.ver, self.cv, self.bot, self.fcache = 0, threading.Condition(), "", {}
        self._flock = threading.Lock()

    def get_file_cached(self, fid):
        with self._flock:
            hit = self.fcache.get(fid)
            if hit:
                return hit
        res = self.tg.get_file(fid)
        with self._flock:
            if len(self.fcache) >= 60:
                self.fcache.clear()
            self.fcache[fid] = res
        return res

    def find_msg(self, cid, mid):
        with self._flock:
            c = self.chats.get(cid)
            if not c:
                return None
            return next((dict(m) for m in c["messages"] if m["id"] == mid), None)

    def bump(self):
        """Tell every open browser tab that something changed (never called for read-only calls)."""
        with self.cv:
            self.ver += 1
            self.cv.notify_all()

    def handle(self, ev):
        prev_online = self._online
        super().handle(ev)
        if ev[0] in ("msg", "edit_in", "my_chat", "edit_ok", "sent", "sent_local", "add_chat") or (ev[0] == "online" and ev[1] != prev_online):
            self.bump()

    def on_sent(self, res, flags=None):
        try:
            super().on_sent(res, flags)
        finally:
            self.bump()

    def on_sent_local(self, cid, rec):
        try:
            super().on_sent_local(cid, rec)
        finally:
            self.bump()

    def inloop(self, fn, timeout=20):
        """Run fn on the single event-loop thread (all state lives there) and return its result."""
        f = Future()

        def w():
            try:
                f.set_result(fn())
            except Exception as e:  # noqa: BLE001
                f.set_exception(e)
        self.q.put(("call", w, ()))
        try:
            return f.result(timeout)
        except Exception as e:  # noqa: BLE001
            if isinstance(e, TimeoutError):
                raise RuntimeError("loop timeout")
            raise

    def state(self):
        chats = [{"id": c["id"], "name": c["name"], "user": c.get("username") or "", "unread": c["unread"], "ts": c["last_ts"],
                  "type": c.get("type") or ("private" if c["id"] > 0 else "group"),
                  "in_bot": c.get("in_bot", True), "members": c.get("members", 0),
                  "pin": c["pinned"], "mute": c["muted"], "blk": c["blocked"], "note": c.get("note", ""),
                  "count": len(c["messages"]),
                  "last": preview_of(c["messages"][-1] if c["messages"] else None)[:90]} for c in self.ordered_chats()]
        acc = [{"id": k, **{x: r.get(x) for x in ("name", "username", "status", "hits", "first", "last")}} for k, r in self.access.sorted()]
        sched = [{k: j.get(k) for k in ("id", "chat_id", "text", "send_at", "repeat", "status", "created", "last_sent", "result")}
                 for j in self.schedules]
        return {"chats": chats, "stats": compute_stats(self.chats), "access": acc, "auto": self.ar(), "ver": self.ver,
                "online": bool(self._online),
                "set": {k: self.settings.get(k, True) for k in ("notify_admin", "log_usage", "panel_clean")},
                "schedules": sched,
                "log": [[t, u, n, x] for t, u, n, x in list(self.events)[:60]], "bot": self.bot, "owner": ADMIN_CHAT_ID}

    def targets(self):
        def f():
            return [c["id"] for c in self.chats.values() if not c["blocked"] and not self.access.can(c["id"])
                    and self.access.role(c["id"]) != "banned"]
        try:
            return self.inloop(f, timeout=30)
        except Exception:  # noqa: BLE001
            return []

    def chat_snap(self, cid):
        def f():
            c = self.chats.get(cid)
            if not c:
                return {"msgs": [], "note": ""}
            c["unread"] = 0
            self.mark_dirty(cid)
            return {"msgs": [dict(m) for m in c["messages"]], "note": c.get("note", "")}
        return self.inloop(f)


def make_handler(core):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def out(self, code, body, ctype="application/json", extra=()):
            b = body if isinstance(body, bytes) else json.dumps(body, ensure_ascii=False).encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype + ("; charset=utf-8" if "text" in ctype or "json" in ctype else ""))
            self.send_header("Content-Length", str(len(b)))
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("X-Frame-Options", "SAMEORIGIN")
            if ctype.startswith("application/json") and not any(k.lower() == "cache-control" for k, _ in extra):
                self.send_header("Cache-Control", "no-store")
            for k, v in extra:
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(b)

        def authed(self):
            c = self.headers.get("Cookie", "")
            m = re.search(r"(?:^|;\s*)t=([\w-]+)", c)
            return bool(m and m.group(1) in SESSIONS)

        def body(self):
            try:
                n = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                n = 0
            n = max(0, min(n, 20 * 1024 * 1024))
            return self.rfile.read(n) if n else b""

        def do_GET(self):
            u = urlparse(self.path)
            qs = {k: v[0] for k, v in parse_qs(u.query).items()}
            try:
                if u.path == "/":
                    return self.out(200, PAGE.encode(), "text/html")
                if not self.authed():
                    return self.out(401, {"error": "auth"})
                if u.path == "/api/state":
                    return self.out(200, core.inloop(core.state))
                if u.path == "/api/chat":
                    try:
                        cid = int(qs["id"])
                    except (KeyError, TypeError, ValueError):
                        return self.out(400, {"error": "bad chat"})
                    return self.out(200, core.chat_snap(cid))
                if u.path == "/api/file":
                    return self.file(qs)
                if u.path == "/api/events":
                    return self.sse()
                self.out(404, {"error": "nf"})
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as e:  # noqa: BLE001
                self.out(500, {"error": str(e)})

        def file(self, qs):
            try:
                cid, mid = int(qs["c"]), int(qs["m"])
            except (KeyError, TypeError, ValueError):
                return self.out(400, {"error": "bad request"})
            m = core.find_msg(cid, mid)
            if not m:
                return self.out(404, {"error": "nf"})
            fid = m.get("file_id") if qs.get("full") else (m.get("disp") or m.get("file_id"))
            if not fid:
                return self.out(404, {"error": "nf"})
            try:
                data, path = core.get_file_cached(fid)
            except Exception as e:  # noqa: BLE001
                log("file error:", e)
                return self.out(404, {"error": "unavailable"})
            ct = mimetypes.guess_type(path)[0] or "application/octet-stream"
            extra = [("Cache-Control", "private, max-age=86400")]
            if qs.get("dl"):
                fname = requests.utils.quote(m.get("name") or Path(path).name, safe="")
                extra.append(("Content-Disposition", f"attachment; filename*=UTF-8''{fname}"))
            self.out(200, data, ct, extra)

        def sse(self):
            self.send_response(200)
            for k, v in (("Content-Type", "text/event-stream"), ("Cache-Control", "no-cache"), ("X-Accel-Buffering", "no"), ("Connection", "keep-alive")):
                self.send_header(k, v)
            self.end_headers()
            try:
                self.wfile.write(f"data: {core.ver}\n\n".encode())
                self.wfile.flush()
            except OSError:
                return
            last = -1
            try:
                while True:
                    with core.cv:
                        core.cv.wait_for(lambda: core.ver != last, timeout=15)
                        v = core.ver
                    self.wfile.write((f"data: {v}\n\n" if v != last else ": ka\n\n").encode())
                    self.wfile.flush()
                    if v != last:
                        last = v
            except OSError:
                pass

        def do_POST(self):
            path = urlparse(self.path).path
            d = None
            try:
                raw = self.body()
                def parse():
                    nonlocal d
                    if d is None:
                        d = json.loads(raw or b"{}")
                        if not isinstance(d, dict):
                            raise ValueError("bad json")
                    return d
                if path == "/api/login":
                    pw = parse().get("pw", "")
                    if not isinstance(pw, str):
                        pw = ""
                    ok = secrets.compare_digest(pw, WEB_PASSWORD)
                    if not ok:
                        time.sleep(1.2)
                        return self.out(403, {"error": "bad password"})
                    t = secrets.token_urlsafe(24)
                    SESSIONS.add(t)
                    sec = "; Secure" if self.headers.get("X-Forwarded-Proto") == "https" else ""
                    return self.out(200, {"ok": 1}, extra=[("Set-Cookie", f"t={t}; HttpOnly; SameSite=Strict; Path=/; Max-Age=2592000{sec}")])
                if not self.authed():
                    return self.out(401, {"error": "auth"})
                if path == "/api/logout":
                    m = re.search(r"(?:^|;\s*)t=([\w-]+)", self.headers.get("Cookie", ""))
                    if m:
                        SESSIONS.discard(m.group(1))
                    return self.out(200, {"ok": 1}, extra=[("Set-Cookie", "t=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0")])
                if path == "/api/upload":
                    return self.upload(raw)
                parse()
                if path == "/api/send":
                    text = str(d.get("text", ""))[:4096]
                    if not text.strip():
                        return self.out(400, {"error": "empty"})
                    try:
                        cid = int(d["id"])
                    except (KeyError, TypeError, ValueError):
                        return self.out(400, {"error": "bad chat"})
                    try:
                        res = core.tg.call("sendMessage", {"chat_id": cid, "text": text}, timeout=30)
                        core.inloop(lambda: core.on_sent(res))
                    except Exception as e:  # noqa: BLE001
                        log("send error:", e)
                        return self.out(400, {"error": str(e)})
                    return self.out(200, {"ok": 1})
                if path == "/api/edit":
                    try:
                        cid, mid = int(d["id"]), int(d["mid"])
                    except (KeyError, TypeError, ValueError):
                        return self.out(400, {"error": "bad message"})
                    text = str(d.get("text", ""))[:4096]
                    if not text.strip():
                        return self.out(400, {"error": "empty"})
                    m = core.inloop(lambda: core.get_message(cid, mid))
                    if not m or m.get("dir") != "out":
                        return self.out(400, {"error": "only your sent messages can be edited"})
                    if m.get("kind") != "text":
                        return self.out(400, {"error": "only text messages can be edited"})
                    try:
                        core.tg.call("editMessageText", {"chat_id": cid, "message_id": mid, "text": text}, timeout=30)
                    except Exception as e:  # noqa: BLE001
                        return self.out(400, {"error": str(e)})
                    core.inloop(lambda: core.apply_edit(cid, mid, text))
                    core.bump()
                    return self.out(200, {"ok": 1})
                if path == "/api/broadcast":
                    text = str(d.get("text", ""))[:4096]
                    if not text.strip():
                        return self.out(400, {"error": "empty"})
                    tg_ = core.targets()

                    def work():
                        ok = 0
                        for t in tg_:
                            try:
                                res = core.tg.call("sendMessage", {"chat_id": t, "text": text})
                                core.q.put(("sent", res))
                                ok += 1
                            except Exception:  # noqa: BLE001
                                pass
                            time.sleep(0.06)
                        core.q.put(("toast", f"Broadcast: {ok}/{len(tg_)}"))
                    core.bg(work)
                    return self.out(200, {"n": len(tg_)})
                if path == "/api/targets":
                    return self.out(200, {"n": len(core.targets())})
                if path == "/api/addchat":
                    q = str(d.get("q", "")).strip()
                    if not q:
                        return self.out(400, {"error": "empty"})
                    if "t.me/" in q:
                        q = q.split("t.me/")[-1].strip("/").split("?")[0]
                    if q.startswith("+") or q.startswith("joinchat/"):
                        return self.out(400, {"error": "Private invite links cannot be resolved. Add the bot to the group, or use its numeric ID."})
                    if q.startswith("@"):
                        target = q
                    elif q.lstrip("-").isdigit():
                        target = int(q)
                    else:
                        target = "@" + q.lstrip("@")
                    try:
                        info = core.tg.call("getChat", {"chat_id": target}, timeout=30)
                    except Exception as e:  # noqa: BLE001
                        return self.out(400, {"error": str(e)})
                    try:
                        cid = int(info["id"])
                    except (KeyError, TypeError, ValueError):
                        return self.out(400, {"error": "bad chat"})
                    name, user, ctype = chat_info(info)
                    members = 0
                    if info.get("type") in ("group", "supergroup", "channel"):
                        try:
                            members = int(core.tg.call("getChatMemberCount", {"chat_id": cid}, timeout=20))
                        except Exception:  # noqa: BLE001
                            pass

                    def add():
                        c = core.ensure_chat(cid, name, user, ctype)
                        c["in_bot"] = True
                        if members:
                            c["members"] = members
                        core.mark_dirty(cid)
                        return cid
                    cid = core.inloop(add)
                    core.bump()
                    return self.out(200, {"ok": 1, "id": cid})
                if path == "/api/schedule":
                    text = str(d.get("text", ""))[:4096]
                    if not text.strip():
                        return self.out(400, {"error": "empty"})
                    chat = d.get("chat", "all")
                    if chat != "all":
                        try:
                            chat = int(chat)
                        except (TypeError, ValueError):
                            return self.out(400, {"error": "bad chat"})
                    try:
                        send_at = int(float(d.get("send_at", 0)))
                    except (TypeError, ValueError):
                        return self.out(400, {"error": "bad time"})
                    if send_at <= 0:
                        return self.out(400, {"error": "bad time"})
                    rep = d.get("repeat", "once")
                    if rep not in ("once", "daily", "weekly"):
                        return self.out(400, {"error": "bad repeat"})
                    job = core.inloop(lambda: core.schedule_add(chat, text, send_at, rep))
                    core.bump()
                    return self.out(200, {"ok": 1, "job": job})
                if path == "/api/schedule/del":
                    sid = str(d.get("id", ""))
                    core.inloop(lambda: core.schedule_del(sid))
                    core.bump()
                    return self.out(200, {"ok": 1})

                def act():
                    cid = d.get("id")
                    if path == "/api/flag":
                        key = d.get("key")
                        if key not in ("pinned", "muted", "blocked"):
                            raise ValueError("bad flag")
                        core.set_flag(cid, key, bool(d.get("val")))
                    elif path == "/api/read":
                        if cid in core.chats:
                            core.chats[cid]["unread"] = 0
                            core.mark_dirty(cid)
                    elif path == "/api/note":
                        if cid in core.chats:
                            core.chats[cid]["note"] = str(d.get("text", ""))[:300]
                            core.mark_dirty(cid)
                    elif path == "/api/del":
                        core.chats.pop(cid, None)
                        core.db.delete_chat(cid)
                        core.mark_dirty(cid)
                    elif path == "/api/access":
                        st = d.get("status")
                        if st not in ("allowed", "banned", "none", "forget"):
                            raise ValueError("bad status")
                        core.forget_access(cid) if st == "forget" else core.set_access(cid, st)
                    elif path == "/api/auto":
                        core.set_ar(**{k: d[k] for k in ("enabled", "text", "cooldown_min") if k in d})
                    elif path == "/api/set":
                        k = d.get("key")
                        if k not in ("notify_admin", "log_usage", "panel_clean"):
                            raise ValueError("bad setting")
                        core.settings[k] = bool(d.get("val"))
                        core.save_settings()
                    else:
                        raise KeyError(path)
                    return True
                core.inloop(act)
                core.bump()
                self.out(200, {"ok": 1})
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as e:  # noqa: BLE001
                try:
                    self.out(400, {"error": str(e)})
                except Exception:  # noqa: BLE001
                    pass

        def upload(self, raw):
            try:
                cid = int(self.headers.get("X-Chat", ""))
            except (TypeError, ValueError):
                return self.out(400, {"error": "bad chat"})
            name = requests.utils.unquote(self.headers.get("X-Name", "file"))[:200]
            if name in ("", "file", ".", ".."):
                name = "file"
            if not raw:
                return self.out(400, {"error": "empty file"})
            ext = Path(name).suffix.lower()
            if ext in (".jpg", ".jpeg", ".png", ".webp"):
                method, field = "sendPhoto", "photo"
            elif ext in (".mp4", ".mov", ".mkv", ".webm"):
                method, field = "sendVideo", "video"
            elif ext in (".mp3", ".m4a", ".flac", ".wav", ".aac", ".ogg", ".oga"):
                method, field = "sendAudio", "audio"
            else:
                method, field = "sendDocument", "document"
            try:
                res = core.tg.call(method, {"chat_id": cid}, files={field: (name, raw)}, timeout=120)
                core.inloop(lambda: core.on_sent(res))
            except Exception as e:  # noqa: BLE001
                log("upload error:", e)
                return self.out(400, {"error": str(e)})
            self.out(200, {"ok": 1})
    return H


PAGE = r"""<!doctype html>
<html lang="en" data-theme="dark">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="color-scheme" content="dark light">
<title>SamX Relay</title>
<style>
:root{--bg:#111318;--bg-2:#15181d;--bg-3:#1b1f26;--surface:#171a20;--surface-solid:#1b1f26;--surface-2:#232832;--surface-3:#2c323d;--line:#2a303a;--line-2:#3a4250;--text:#e8eaed;--text-2:#aab1bd;--text-3:#7d8592;--accent:#3b82f6;--accent-2:#2563eb;--accent-soft:rgba(59,130,246,.14);--accent-line:rgba(59,130,246,.4);--ok:#22c55e;--warn:#eab308;--bad:#ef4444;--ok-soft:rgba(34,197,94,.12);--warn-soft:rgba(234,179,8,.12);--bad-soft:rgba(239,68,68,.12);--shadow-lg:0 12px 32px rgba(0,0,0,.4);--shadow-md:0 4px 14px rgba(0,0,0,.22);--r-xl:8px;--r-lg:8px;--r-md:6px;--r-sm:4px;--font:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif}
[data-theme=light]{--bg:#f4f5f7;--bg-2:#eef0f3;--bg-3:#fff;--surface:#fff;--surface-solid:#fff;--surface-2:#f1f3f6;--surface-3:#e5e8ed;--line:#d8dce3;--line-2:#c3c9d3;--text:#1b1f24;--text-2:#4b5563;--text-3:#7b8494;--accent:#2563eb;--accent-2:#1d4ed8;--accent-soft:rgba(37,99,235,.1);--accent-line:rgba(37,99,235,.35);--ok:#16a34a;--warn:#ca8a04;--bad:#dc2626;--shadow-lg:0 12px 32px rgba(20,30,50,.18);--shadow-md:0 4px 14px rgba(20,30,50,.1)}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%}
body{font-family:var(--font);background:var(--bg);color:var(--text);overflow:hidden;font-size:14px;-webkit-font-smoothing:antialiased;text-rendering:optimizeLegibility}
::selection{background:var(--accent);color:#fff}
button,input,textarea,select{font:inherit;color:inherit}
button{cursor:pointer;border:0;background:none}
a{color:var(--accent);text-decoration:none}
a:hover{text-decoration:underline}
::-webkit-scrollbar{width:10px;height:10px}
::-webkit-scrollbar-thumb{background:var(--line-2);border-radius:8px;border:2px solid transparent;background-clip:padding-box}
::-webkit-scrollbar-thumb:hover{background:var(--text-3);background-clip:padding-box}
::-webkit-scrollbar-track{background:transparent}
.app{display:grid;grid-template-columns:224px 340px minmax(0,1fr);height:100dvh;background:var(--bg)}
.app.no-side{grid-template-columns:224px minmax(0,1fr)}
.app.no-side .side{display:none}
.nav{display:flex;flex-direction:column;gap:2px;padding:16px 10px;background:var(--bg-2);border-right:1px solid var(--line);overflow:auto}
.brand{display:flex;align-items:center;gap:10px;padding:2px 8px 16px;margin-bottom:8px;border-bottom:1px solid var(--line)}
.brand-logo{width:36px;height:36px;border-radius:6px;display:grid;place-items:center;font-size:17px;font-weight:700;background:var(--accent);color:#fff;flex:none}
.brand-copy b{display:block;font-size:15px;font-weight:650;letter-spacing:-.01em}
.brand-copy small{display:block;font-size:11px;color:var(--text-3);margin-top:1px}
.nav-section{padding:14px 10px 5px;font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--text-3)}
.nav-link{position:relative;display:flex;align-items:center;gap:10px;width:100%;padding:9px 10px;border-radius:6px;color:var(--text-2);font-size:13px;font-weight:500;text-align:left;border:1px solid transparent}
.nav-link:hover{background:var(--surface-2);color:var(--text)}
.nav-link.on{color:var(--accent);background:var(--accent-soft);border-color:var(--accent-line)}
.nav-ico{width:20px;text-align:center;font-size:15px;flex:none}
.nav-txt{flex:1}
.nav-badge{display:inline-block;min-width:18px;padding:1px 6px;border-radius:4px;background:var(--accent);color:#fff;font-size:10px;font-weight:700;text-align:center}
.nav-badge[hidden]{display:none}
.nav-badge.amber{background:var(--warn);color:#1a1206}
.nav-spacer{flex:1}
.nav-theme{margin-top:4px}
.nav-status{display:flex;align-items:center;gap:8px;padding:9px 10px;margin-top:6px;border:1px solid var(--line);border-radius:6px;background:var(--surface)}
.status-dot{width:8px;height:8px;border-radius:50%;background:var(--text-3);flex:none}
.status-dot.ok{background:var(--ok)}
.status-dot.bad{background:var(--bad)}
.status-copy b{display:block;font-size:12px;font-weight:600;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.status-copy small{display:block;font-size:10.5px;color:var(--text-3)}
.side{display:flex;flex-direction:column;min-height:0;background:var(--bg);border-right:1px solid var(--line)}
.side-head{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;padding:18px 14px 10px}
.side-head h2{font-size:18px;font-weight:650;letter-spacing:-.01em}
.side-head p{font-size:12px;color:var(--text-3);margin-top:2px}
.icon-btn{width:32px;height:32px;border-radius:6px;display:grid;place-items:center;background:var(--surface);border:1px solid var(--line);color:var(--text-2);font-size:14px;flex:none}
.icon-btn:hover{color:var(--text);border-color:var(--line-2)}
.search-wrap{position:relative;margin:0 14px}
.search-wrap input{width:100%;padding:9px 12px 9px 34px;border-radius:6px;background:var(--surface);border:1px solid var(--line);font-size:13px;outline:none}
.search-wrap input:focus{border-color:var(--accent)}
.search-ico{position:absolute;left:11px;top:50%;transform:translateY(-50%);color:var(--text-3);font-size:15px;pointer-events:none}
.chips{display:flex;gap:6px;padding:10px 14px 8px;flex-wrap:wrap}
.chip{padding:4px 10px;border-radius:6px;background:transparent;border:1px solid var(--line);color:var(--text-2);font-size:11px;font-weight:600}
.chip:hover{color:var(--text);border-color:var(--line-2)}
.chip.on{background:var(--accent);border-color:var(--accent);color:#fff}
.chat-list{flex:1;overflow:auto;padding:2px 6px 14px}
.chat-row{display:flex;gap:10px;width:100%;padding:9px 8px;border-radius:6px;align-items:center;text-align:left;border:1px solid transparent}
.chat-row:hover{background:var(--surface-2)}
.chat-row.on{background:var(--accent-soft);border-color:var(--accent-line)}
.avatar{position:relative;width:42px;height:42px;border-radius:50%;display:grid;place-items:center;color:#fff;font-weight:600;font-size:15px;flex:none;letter-spacing:-.01em}
.avatar.sm{width:30px;height:30px;font-size:11px}
.avatar.lg{width:46px;height:46px;font-size:16px}
.chat-main{flex:1;min-width:0}
.chat-line{display:flex;align-items:baseline;gap:8px}
.chat-name{flex:1;font-size:13px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.chat-time{font-size:10.5px;color:var(--text-3);flex:none}
.chat-preview{display:flex;align-items:center;gap:5px;margin-top:2px;font-size:12px;color:var(--text-3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.chat-preview .note{color:var(--warn);flex:none}
.chat-flags{display:flex;align-items:center;gap:4px;flex:none}
.flag{font-size:10px;opacity:.7}
.unread{min-width:18px;padding:1px 5px;border-radius:4px;background:var(--accent);color:#fff;font-size:10px;font-weight:700;text-align:center;flex:none}
.main{display:flex;flex-direction:column;min-width:0;min-height:0;position:relative;background:var(--bg)}
.chat-head{display:flex;align-items:center;gap:10px;padding:10px 16px;background:var(--bg-2);border-bottom:1px solid var(--line);position:relative;z-index:3}
.chat-id{flex:1;min-width:0}
.chat-id b{display:block;font-size:14px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.chat-id small{display:block;font-size:11.5px;color:var(--text-3);margin-top:1px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.head-actions{display:flex;gap:4px;flex-wrap:wrap;justify-content:flex-end}
.act{padding:6px 9px;border-radius:6px;background:var(--surface-2);border:1px solid var(--line);color:var(--text-2);font-size:12px;font-weight:500;display:inline-flex;align-items:center;gap:5px}
.act:hover{color:var(--text);border-color:var(--line-2)}
.act.on{color:var(--accent);border-color:var(--accent-line);background:var(--accent-soft)}
.act.danger:hover{color:#fff;background:var(--bad);border-color:var(--bad)}
.back-btn{display:none}
.messages{flex:1;overflow:auto;padding:18px 18px 8px;display:flex;flex-direction:column;gap:2px;position:relative;z-index:1}
.msg{display:flex;gap:8px;align-items:flex-end;max-width:100%;animation:rise .16s ease both}
.msg.in{justify-content:flex-start}
.msg.out{justify-content:flex-end}
.msg.grouped{margin-top:1px}
.msg.first{margin-top:10px}
.msg-first{width:30px;flex:none}
.bubble{max-width:min(620px,74%);padding:8px 12px;border-radius:8px;background:var(--surface-solid);border:1px solid var(--line);font-size:13.5px;line-height:1.5;word-break:break-word;position:relative}
.msg.in .bubble{border-bottom-left-radius:2px}
.msg.out .bubble{background:var(--accent);border-color:var(--accent);color:#fff;border-bottom-right-radius:2px}
.bubble .text{white-space:pre-wrap}
.bubble a{color:inherit;text-decoration:underline;text-underline-offset:2px}
.msg.out .bubble a{color:#fff}
.msg-meta{display:flex;align-items:center;gap:6px;justify-content:flex-end;flex-wrap:wrap;margin-top:4px;font-size:10px;color:var(--text-3);opacity:.9}
.msg.out .msg-meta{color:rgba(255,255,255,.78)}
.msg-meta .ticks{color:inherit;opacity:.8}
.msg-edit{display:inline-flex;align-items:center;gap:4px;margin-top:5px;padding:2px 7px;border-radius:4px;background:rgba(255,255,255,.16);color:inherit;font-size:10.5px;font-weight:600;opacity:.75}
.msg-edit:hover{opacity:1;background:rgba(255,255,255,.26)}
.bubble.sticker{background:transparent;border:0;padding:0}
.bubble.sticker .media-img{max-width:150px;max-height:150px;border-radius:0;margin:0}
.reply-quote{padding:5px 8px;margin-bottom:6px;border-left:3px solid rgba(255,255,255,.5);border-radius:4px;background:rgba(255,255,255,.08);font-size:11.5px;opacity:.9;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.msg.in .reply-quote{border-left-color:var(--accent);background:var(--accent-soft)}
.media-img{display:block;max-width:100%;max-height:320px;border-radius:6px;margin-bottom:5px;cursor:zoom-in;background:var(--surface-2)}
.media-video{position:relative;display:inline-block;margin-bottom:5px;border-radius:6px;overflow:hidden;cursor:pointer;line-height:0}
.media-video img{max-width:100%;max-height:320px;display:block}
.media-video .play{position:absolute;inset:0;display:grid;place-items:center;background:rgba(0,0,0,.28);color:#fff;font-size:20px}
.media-video:hover .play{background:rgba(0,0,0,.42)}
.audio-card{display:flex;align-items:center;gap:8px;background:rgba(255,255,255,.09);border-radius:6px;padding:7px 9px;margin-bottom:5px;min-width:230px}
.audio-card audio{height:32px;max-width:220px}
.audio-ic{font-size:16px;flex:none}
.msg.in .audio-card{background:var(--surface-2)}
.file-card{display:flex;align-items:center;gap:8px;padding:8px 10px;border-radius:6px;background:rgba(255,255,255,.09);text-decoration:none;margin-bottom:5px;min-width:220px;color:inherit}
.msg.in .file-card{background:var(--surface-2)}
.file-card:hover{text-decoration:none;opacity:.85}
.file-ic{font-size:17px;flex:none}
.file-info{flex:1;min-width:0;text-align:left}
.file-info b{display:block;font-size:12.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.file-info small{font-size:10.5px;opacity:.7}
.file-card .dl{font-size:15px;flex:none;opacity:.8}
.day{display:flex;align-items:center;gap:10px;align-self:stretch;margin:14px 0 6px;color:var(--text-3);font-size:10.5px;font-weight:600;letter-spacing:.03em;text-transform:uppercase;justify-content:center}
.day::before,.day::after{content:"";height:1px;flex:1;max-width:90px;background:var(--line)}
.to-bottom{position:absolute;right:20px;bottom:96px;width:36px;height:36px;border-radius:50%;background:var(--surface-solid);border:1px solid var(--line);color:var(--text);display:none;place-items:center;font-size:14px;box-shadow:var(--shadow-md);z-index:4}
.to-bottom.show{display:grid}
.composer-wrap{padding:10px 14px 12px;background:var(--bg-2);border-top:1px solid var(--line);position:relative;z-index:3}
.composer{display:flex;align-items:flex-end;gap:6px;background:var(--surface-solid);border:1px solid var(--line);border-radius:8px;padding:5px 5px 5px 8px}
.composer:focus-within{border-color:var(--accent)}
.attach{width:34px;height:34px;border-radius:6px;display:grid;place-items:center;background:var(--surface-2);color:var(--text-2);font-size:17px;flex:none}
.attach:hover{color:var(--text);background:var(--surface-3)}
.composer textarea{flex:1;background:none;border:0;outline:none;resize:none;font-size:13.5px;line-height:1.5;padding:7px 3px;max-height:140px;min-height:34px}
.composer textarea::placeholder{color:var(--text-3)}
.send-btn{width:36px;height:36px;border-radius:6px;display:grid;place-items:center;background:var(--accent);color:#fff;font-size:15px;flex:none}
.send-btn:hover{background:var(--accent-2)}
.send-btn:active{opacity:.85}
.composer-hint{text-align:center;font-size:10.5px;color:var(--text-3);margin-top:6px}
.empty{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:6px;text-align:center;padding:32px;color:var(--text-3)}
.empty .big{width:52px;height:52px;border-radius:8px;display:grid;place-items:center;font-size:24px;background:var(--surface-2);border:1px solid var(--line);margin-bottom:6px;color:var(--text-2)}
.empty h3{font-size:15px;color:var(--text);font-weight:600}
.empty p{font-size:12.5px;max-width:320px;line-height:1.55}
.page{flex:1;overflow:auto;padding:28px 30px 40px;position:relative;z-index:1;background:var(--bg)}
.page-head{display:flex;align-items:flex-end;justify-content:space-between;gap:20px;margin-bottom:22px}
.page-head h1{font-size:24px;font-weight:650;letter-spacing:-.01em}
.page-head p{color:var(--text-3);font-size:13px;margin-top:4px;line-height:1.5}
.page-actions{display:flex;gap:6px;flex-wrap:wrap}
.stats{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}
.stat-card{position:relative;padding:16px 18px;border-radius:8px;background:var(--surface);border:1px solid var(--line)}
.stat-ico{display:none}
.stat-card small{display:block;font-size:10.5px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;color:var(--text-3)}
.stat-card b{display:block;font-size:26px;font-weight:650;letter-spacing:-.01em;margin-top:4px}
.stat-card span{display:block;font-size:11.5px;color:var(--text-3);margin-top:2px}
.panel{padding:18px 20px;border-radius:8px;background:var(--surface);border:1px solid var(--line)}
.panel-head{display:flex;align-items:center;justify-content:space-between;gap:14px;margin-bottom:14px}
.panel-head h2{font-size:15px;font-weight:600}
.panel-head .hint{font-size:11.5px;color:var(--text-3)}
.chart{display:flex;align-items:flex-end;gap:6px;height:140px;padding-top:8px}
.chart .bar{flex:1;min-width:0;display:flex;flex-direction:column;justify-content:flex-end;align-items:center;gap:6px;height:100%}
.chart .bar i{width:100%;min-height:4px;border-radius:3px 3px 0 0;background:var(--accent);position:relative;display:block}
.chart .bar:hover i{opacity:.8}
.chart .bar i::after{content:attr(data-v);position:absolute;bottom:calc(100% + 5px);left:50%;transform:translateX(-50%);background:var(--surface-3);color:var(--text);font-size:10.5px;padding:2px 6px;border-radius:4px;opacity:0;pointer-events:none;white-space:nowrap}
.chart .bar:hover i::after{opacity:1}
.chart .bar span{font-size:10px;color:var(--text-3);white-space:nowrap}
.cols{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:14px}
.list{display:flex;flex-direction:column;gap:6px}
.list-item{display:flex;align-items:center;gap:10px;padding:9px 10px;border-radius:6px;background:var(--surface-2);border:1px solid var(--line)}
.rank{width:24px;height:24px;border-radius:4px;display:grid;place-items:center;background:var(--accent-soft);color:var(--accent);font-size:11px;font-weight:700;flex:none}
.list-item .name{flex:1;font-size:12.5px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.list-item .val{font-size:11.5px;color:var(--text-3);flex:none}
.log{display:flex;flex-direction:column;gap:0}
.log-item{display:flex;gap:10px;padding:9px 0;border-bottom:1px solid var(--line)}
.log-item:last-child{border-bottom:0}
.log-dot{width:6px;height:6px;border-radius:50%;background:var(--accent);margin-top:7px;flex:none}
.log-body{flex:1;min-width:0}
.log-body b{font-size:12.5px;font-weight:600}
.log-body p{font-size:11.5px;color:var(--text-3);margin-top:2px}
.log-time{font-size:10.5px;color:var(--text-3);flex:none}
.field{position:relative;display:flex;align-items:center}
.field input,.input{width:100%;padding:10px 12px;border-radius:6px;background:var(--surface-solid);border:1px solid var(--line);font-size:13px;outline:none}
.field input:focus,.input:focus{border-color:var(--accent)}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:6px;padding:9px 14px;border-radius:6px;font-size:13px;font-weight:600;white-space:nowrap}
.btn.lg{padding:11px 16px;font-size:14px}
.btn.primary{background:var(--accent);color:#fff}
.btn.primary:hover{background:var(--accent-2)}
.btn.ghost{background:var(--surface-2);border:1px solid var(--line);color:var(--text)}
.btn.ghost:hover{border-color:var(--line-2)}
.btn.ok{background:var(--ok-soft);color:var(--ok);border:1px solid rgba(34,197,94,.3)}
.btn.ok:hover{background:var(--ok);color:#fff}
.btn.bad{background:var(--bad-soft);color:var(--bad);border:1px solid rgba(239,68,68,.3)}
.btn.bad:hover{background:var(--bad);color:#fff}
.btn.sm{padding:6px 10px;border-radius:4px;font-size:11.5px}
.btn:disabled{opacity:.6;cursor:not-allowed}
.access-tools{display:grid;gap:12px;margin-bottom:18px}
.pre-auth{display:flex;gap:6px;flex-wrap:wrap}
.seg{display:flex;gap:4px;flex-wrap:wrap}
.user-list{display:flex;flex-direction:column;gap:8px}
.user-card{display:flex;align-items:center;gap:12px;padding:12px 14px;border-radius:8px;background:var(--surface);border:1px solid var(--line);flex-wrap:wrap}
.user-card:hover{border-color:var(--line-2)}
.user-main{flex:1;min-width:170px}
.user-main b{display:block;font-size:13px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.user-main small{display:block;font-size:11.5px;color:var(--text-3);margin-top:2px}
.user-actions{display:flex;gap:4px;flex-wrap:wrap;justify-content:flex-end}
.pill{display:inline-flex;align-items:center;gap:4px;padding:3px 8px;border-radius:4px;font-size:10px;font-weight:700;letter-spacing:.03em;text-transform:uppercase;flex:none}
.pill.pending{background:var(--warn-soft);color:var(--warn);border:1px solid rgba(245,158,11,.28)}
.pill.allowed{background:var(--ok-soft);color:var(--ok);border:1px solid rgba(34,197,94,.28)}
.pill.banned{background:var(--bad-soft);color:var(--bad);border:1px solid rgba(239,68,68,.28)}
.pill.none{background:var(--surface-2);color:var(--text-3);border:1px solid var(--line)}
.status-pill{display:inline-flex;align-items:center;justify-content:center;padding:3px 7px;border-radius:4px;font-size:9.5px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;flex:none;background:var(--surface-3);color:var(--text-3)}
.status-pill.active{background:var(--accent-soft);color:var(--accent);border:1px solid var(--accent-line)}
.status-pill.sending{background:var(--warn-soft);color:var(--warn)}
.status-pill.done{background:var(--ok-soft);color:var(--ok)}
.sched-item{align-items:flex-start}
.sched-item .name{display:flex;flex-direction:column;gap:2px;white-space:normal;min-width:0}
.sched-item .name small{display:block;font-size:11px;font-weight:400;color:var(--text-3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:420px}
.sched-item .val{display:flex;flex-direction:column;align-items:flex-end;gap:2px;text-align:right}
.sched-item .val small{font-size:10.5px;color:var(--text-3)}
.tools-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.card{padding:18px 20px;border-radius:8px;background:var(--surface);border:1px solid var(--line)}
.card h2{font-size:15px;font-weight:600;display:flex;align-items:center;gap:6px}
.card .desc{font-size:12px;color:var(--text-3);margin-top:4px;line-height:1.5}
.card .body{margin-top:14px;display:flex;flex-direction:column;gap:12px}
.row-between{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:10px 0;border-bottom:1px solid var(--line)}
.row-between:last-child{border-bottom:0}
.row-between .t b{display:block;font-size:12.5px;font-weight:600}
.row-between .t small{display:block;font-size:11.5px;color:var(--text-3);margin-top:2px;line-height:1.4}
.switch{position:relative;width:40px;height:22px;flex:none;display:inline-block;cursor:pointer}
.switch input{position:absolute;opacity:0;width:0;height:0}
.switch span{position:absolute;inset:0;border-radius:11px;background:var(--surface-3);border:1px solid var(--line)}
.switch span::after{content:"";position:absolute;top:2px;left:2px;width:16px;height:16px;border-radius:50%;background:#fff;transition:transform .15s}
.switch input:checked+span{background:var(--accent);border-color:var(--accent)}
.switch input:checked+span::after{transform:translateX(18px)}
textarea.input{resize:vertical;min-height:90px;line-height:1.55}
select.input{appearance:auto;padding-right:12px}
.preview-bubble{max-width:100%;padding:8px 12px;border-radius:8px 8px 8px 2px;background:var(--surface-2);border:1px solid var(--line);font-size:12.5px;line-height:1.55;white-space:pre-wrap;color:var(--text-2)}
.target-count{font-size:11.5px;color:var(--text-3)}
.target-count b{color:var(--text)}
.section-title{display:flex;align-items:center;gap:8px;margin:22px 0 12px;font-size:13px;font-weight:600}
.section-title span{font-size:11px;font-weight:500;color:var(--text-3)}
.kv{display:flex;justify-content:space-between;gap:12px;padding:10px 0;border-bottom:1px solid var(--line);font-size:12.5px}
.kv:last-child{border-bottom:0}
.kv span{color:var(--text-3)}
.kv b{font-weight:600;text-align:right}
.login{position:fixed;inset:0;z-index:1000;display:none;place-items:center;padding:20px;background:var(--bg)}
.login.show{display:grid;animation:fadeIn .2s}
.login-card{width:min(380px,100%);padding:30px 28px;border-radius:8px;background:var(--surface);border:1px solid var(--line);box-shadow:var(--shadow-lg);text-align:center;animation:rise .25s ease}
.login-logo{width:48px;height:48px;margin:0 auto 16px;border-radius:8px;display:grid;place-items:center;font-size:20px;font-weight:700;background:var(--accent);color:#fff}
.login-card h1{font-size:22px;font-weight:650;letter-spacing:-.01em}
.login-card p{font-size:13px;color:var(--text-3);margin:6px 0 20px;line-height:1.5}
.login-card .field{margin-bottom:8px}
.login-card .field input{text-align:left;padding-right:58px}
.pw-toggle{position:absolute;right:4px;width:50px;height:32px;border-radius:4px;display:grid;place-items:center;color:var(--text-3);font-size:11px;font-weight:600}
.pw-toggle:hover{color:var(--text);background:var(--surface-2)}
.login-error{min-height:18px;font-size:12px;color:var(--bad);margin-bottom:6px}
.login-foot{margin-top:16px;font-size:10.5px;color:var(--text-3)}
.toast{position:fixed;left:50%;bottom:24px;transform:translate(-50%,12px);z-index:1200;padding:9px 15px;border-radius:6px;background:var(--surface-solid);border:1px solid var(--line-2);box-shadow:var(--shadow-lg);font-size:13px;font-weight:500;opacity:0;pointer-events:none;transition:opacity .18s,transform .18s;display:flex;align-items:center;gap:8px;max-width:90vw}
.toast.show{opacity:1;transform:translate(-50%,0)}
.toast.ok{border-color:rgba(34,197,94,.4)}
.toast.err{border-color:rgba(239,68,68,.4)}
.modal{position:fixed;inset:0;z-index:1100;display:none;place-items:center;padding:20px;background:rgba(0,0,0,.55)}
.modal.show{display:grid;animation:fadeIn .15s}
.modal-card{width:min(420px,100%);padding:20px;border-radius:8px;background:var(--surface-solid);border:1px solid var(--line-2);box-shadow:var(--shadow-lg);animation:rise .18s ease}
.modal-card h3{font-size:16px;font-weight:600}
.modal-card p{font-size:12.5px;color:var(--text-3);margin-top:6px;line-height:1.55;white-space:pre-wrap}
.modal-card .input{margin-top:14px}
.modal-actions{display:flex;gap:6px;justify-content:flex-end;margin-top:18px}
.lightbox{position:fixed;inset:0;z-index:1150;display:none;place-items:center;padding:24px;background:rgba(0,0,0,.92);cursor:zoom-out}
.lightbox.show{display:grid;animation:fadeIn .15s}
.lightbox img{max-width:94vw;max-height:90vh;border-radius:6px}
.lightbox video{max-width:94vw;max-height:90vh;border-radius:6px;background:#000}
@keyframes rise{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}
@keyframes fadeIn{from{opacity:0}to{opacity:1}}
@media(max-width:1200px){.app{grid-template-columns:210px 320px minmax(0,1fr)}.app.no-side{grid-template-columns:210px minmax(0,1fr)}.stats{grid-template-columns:repeat(2,minmax(0,1fr))}.tools-grid,.cols{grid-template-columns:1fr}}
@media(max-width:900px){
.app,.app.no-side{grid-template-columns:1fr;height:100dvh}
.nav{position:fixed;left:0;right:0;bottom:0;top:auto;height:calc(56px + env(safe-area-inset-bottom));flex-direction:row;align-items:stretch;justify-content:space-around;padding:4px 4px env(safe-area-inset-bottom);border-right:0;border-top:1px solid var(--line);z-index:60;overflow:visible;background:var(--bg-2)}
.brand,.nav-section,.nav-spacer,.nav-theme,.nav-status{display:none}
.nav-link{flex:1;flex-direction:column;justify-content:center;gap:2px;padding:4px 2px;font-size:10px;color:var(--text-3);border:0;border-radius:4px;text-align:center}
.nav-link .nav-ico{font-size:16px;width:auto;line-height:1}
.nav-link .nav-txt{flex:none;font-size:10px;line-height:1}
.nav-link.on{background:var(--accent-soft);color:var(--accent)}
.nav-link.on .nav-ico{color:var(--accent)}
.nav-badge{position:absolute;top:2px;right:calc(50% - 18px);min-width:15px;padding:0 4px;font-size:9px;line-height:15px}
.side{border-right:0;padding-bottom:calc(56px + env(safe-area-inset-bottom));display:flex}
.main{display:none;padding-bottom:calc(56px + env(safe-area-inset-bottom))}
body.chat .side{display:none}
body.chat .main{display:flex}
body.page-mode .side{display:none}
body.page-mode .main{display:flex}
.page{padding:18px 12px calc(72px + env(safe-area-inset-bottom))}
.chat-head{padding:8px 10px}
.messages{padding:12px 10px 4px}
.bubble{max-width:86%}
.back-btn{display:grid}
.head-actions{flex-wrap:nowrap;overflow-x:auto;max-width:52%;padding-bottom:1px}
.head-actions::-webkit-scrollbar{display:none}
.head-actions .act span{display:none}
.head-actions .act{padding:7px 8px;flex:none}
.stats{grid-template-columns:repeat(2,minmax(0,1fr))}
.to-bottom{bottom:86px;right:12px}
.composer-wrap{padding:8px 8px 10px}
.composer{padding:5px 5px 5px 8px}
.composer textarea{min-height:36px;padding:7px 2px;font-size:16px}
.attach,.send-btn{width:38px;height:38px}
.composer-hint{display:none}
.side-head{padding-top:14px}
.toast{bottom:calc(66px + env(safe-area-inset-bottom))}
.modal{align-items:end;padding:0}
.modal-card{width:100%;max-height:90dvh;overflow:auto;border-radius:8px 8px 0 0;padding-bottom:calc(20px + env(safe-area-inset-bottom))}
.login-card{padding:26px 20px}
.search-wrap input,.field input,.input,.composer textarea{font-size:16px}
}
@media(max-width:520px){
.stats{grid-template-columns:1fr 1fr;gap:8px}
.stat-card{padding:12px}
.stat-card b{font-size:22px}
.page-head{flex-direction:column;align-items:flex-start;gap:10px}
.page-head h1{font-size:21px}
.pre-auth{flex-direction:column}
.user-actions{width:100%;justify-content:flex-start}
.panel,.card{padding:14px}
.user-card{padding:12px}
.bubble{max-width:90%}
}
@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style>
</head>
<body>
<div class="app" id="app">
<nav class="nav" id="nav">
  <div class="brand"><div class="brand-logo">S</div><div class="brand-copy"><b>SamX Relay</b><small>Admin Console</small></div></div>
  <div class="nav-section">Workspace</div>
  <button class="nav-link on" data-view="chats"><span class="nav-ico">✉</span><span class="nav-txt">Inbox</span><span class="nav-badge" id="badge-unread" hidden></span></button>
  <button class="nav-link" data-view="dash"><span class="nav-ico">◧</span><span class="nav-txt">Dashboard</span></button>
  <div class="nav-section">Manage</div>
  <button class="nav-link" data-view="acc"><span class="nav-ico">⛨</span><span class="nav-txt">Access</span><span class="nav-badge amber" id="badge-pending" hidden></span></button>
  <button class="nav-link" data-view="tools"><span class="nav-ico">⚒</span><span class="nav-txt">Tools</span></button>
  <button class="nav-link" data-view="set"><span class="nav-ico">⚙</span><span class="nav-txt">Settings</span></button>
  <div class="nav-spacer"></div>
  <button class="nav-link nav-theme" id="themeBtn"><span class="nav-ico" id="themeIco">☾</span><span class="nav-txt" id="themeTxt">Dark</span></button>
  <div class="nav-status"><span class="status-dot" id="botDot"></span><div class="status-copy"><b id="botName">@bot</b><small id="botState">Connecting…</small></div></div>
</nav>
<section id="side" class="side">
  <div class="side-head"><div><h2>Inbox</h2><p id="sideSub">Loading…</p></div><div style="display:flex;gap:6px"><button class="icon-btn" id="addChatBtn" title="Add group or chat">＋</button><button class="icon-btn" id="refreshBtn" title="Refresh">⟳</button></div></div>
  <div class="search-wrap"><span class="search-ico">⌕</span><input id="q" type="search" placeholder="Search chats…" autocomplete="off"></div>
  <div class="chips" id="chips"><button class="chip on" data-filter="all">All</button><button class="chip" data-filter="unread">Unread</button><button class="chip" data-filter="groups">Groups</button><button class="chip" data-filter="pinned">Pinned</button></div>
  <div class="chat-list" id="list"></div>
</section>
<main id="main" class="main"></main>
</div>
<div id="toast" class="toast"></div>
<div id="modal" class="modal"></div>
<div id="lightbox" class="lightbox"></div>
<div id="login" class="login">
  <form id="loginForm" class="login-card" autocomplete="on">
    <div class="login-logo">S</div>
    <h1>SamX Relay</h1>
    <p>Sign in to your private relay console.</p>
    <label class="field"><input id="pw" type="password" placeholder="Password" autocomplete="current-password" required autofocus><button type="button" id="togglePw" class="pw-toggle" aria-label="Show password">Show</button></label>
    <div id="loginError" class="login-error"></div>
    <button id="loginBtn" class="btn primary lg" type="submit" style="width:100%">Sign in</button>
    <div class="login-foot">Protected session · HttpOnly cookie</div>
  </form>
</div>
<script>
"use strict";
const $=(s,r=document)=>r.querySelector(s);
const $$=(s,r=document)=>Array.from(r.querySelectorAll(s));
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const appEl=$("#app"),mainEl=$("#main"),listEl=$("#list");
let S=null,cur=null,view="chats",flt="all",q="",M=[],msgKey="",busy=false,queued=false;
let sound=localStorage.getItem("snd")!=="0";
let theme=localStorage.getItem("theme")||"dark";
let lastUnread=0,sse=null,sseRetry=0,connected=false,authed=false,accFilter="all",accQ="";
let toastTimer=0,loadTimer=0;

function setTheme(t){
  theme=t;
  document.documentElement.dataset.theme=t;
  localStorage.setItem("theme",t);
  const i=$("#themeIco"),x=$("#themeTxt");
  if(i)i.textContent=t==="dark"?"☾":"☀";
  if(x)x.textContent=t==="dark"?"Dark":"Light";
}
function hash(s){let h=0;for(let i=0;i<s.length;i++)h=(h*31+s.charCodeAt(i))|0;return h}
function initials(n){const s=String(n||"?").trim();return (Array.from(s)[0]||"?").toUpperCase()}
function avatarHTML(name,id,cls){
  const h=Math.abs(Number(id)||0)%360;
  const style=`background:hsl(${h} 36% 42%)`;
  return `<span class="avatar${cls?" "+cls:""}" style="${style}">${esc(initials(name))}</span>`;
}
function tm(t){return new Date(t*1000).toLocaleTimeString([],{hour:"2-digit",minute:"2-digit"})}
function fmtDay(d){
  const now=new Date(),a=new Date(d.getFullYear(),d.getMonth(),d.getDate()),b=new Date(now.getFullYear(),now.getMonth(),now.getDate());
  const diff=Math.round((b-a)/86400000);
  if(diff===0)return "Today";
  if(diff===1)return "Yesterday";
  return d.toLocaleDateString([],{weekday:"short",month:"short",day:"numeric"});
}
function ago(t){
  const d=Date.now()/1000-t;
  if(d<60)return "now";
  if(d<3600)return Math.floor(d/60)+"m";
  if(d<86400)return Math.floor(d/3600)+"h";
  if(d<604800)return Math.floor(d/86400)+"d";
  return new Date(t*1000).toLocaleDateString([],{month:"short",day:"numeric"});
}
function fmtDur(s){s=Number(s)||0;const m=Math.floor(s/60),r=s%60;return m?`${m}:${String(r).padStart(2,"0")}`:`0:${String(r).padStart(2,"0")}`}
function linkify(t){return esc(t).replace(/(https?:\/\/[^\s<]+)/g,u=>`<a href="${u}" target="_blank" rel="noopener">${u}</a>`)}
function findChat(id){return (S?.chats||[]).find(c=>c.id===id)}

function toast(msg,type){
  const el=$("#toast");
  el.innerHTML=`<span>${type==="error"?"⚠":type==="ok"?"✓":"•"}</span><span>${esc(msg)}</span>`;
  el.className="toast show"+(type==="error"?" err":type==="ok"?" ok":"");
  clearTimeout(toastTimer);
  toastTimer=setTimeout(()=>el.classList.remove("show"),2800);
}
function beep(){
  try{
    const C=window.AudioContext||window.webkitAudioContext; if(!C)return;
    const a=new C(),o=a.createOscillator(),g=a.createGain();
    o.connect(g);g.connect(a.destination);o.frequency.value=880;g.gain.value=.05;
    o.start();o.stop(a.currentTime+.12);
  }catch(e){}
}
async function api(path,body){
  const r=await fetch("/api/"+path,body?{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)}:{});
  if(r.status===401){showLogin();throw new Error("auth")}
  let j={};try{j=await r.json()}catch(e){}
  if(!r.ok)throw new Error(j.error||("HTTP "+r.status));
  return j;
}

function modal(opts){
  return new Promise(resolve=>{
    const el=$("#modal");
    const hasInput=opts.input!==undefined;
    el.innerHTML=`<div class="modal-card" role="dialog" aria-modal="true">
      <h3>${esc(opts.title||"Confirm")}</h3>
      ${opts.text?`<p>${esc(opts.text)}</p>`:""}
      ${hasInput?(opts.multiline?`<textarea id="modalInput" class="input" rows="5" placeholder="${esc(opts.placeholder||"")}">${esc(opts.input)}</textarea>`:`<input id="modalInput" class="input" value="${esc(opts.input)}" placeholder="${esc(opts.placeholder||"")}">`):""}
      <div class="modal-actions">
        <button class="btn ghost" id="modalCancel">${esc(opts.cancel||"Cancel")}</button>
        <button class="btn ${opts.danger?"bad":"primary"}" id="modalOk">${esc(opts.confirm||"Confirm")}</button>
      </div></div>`;
    el.classList.add("show");
    const inp=$("#modalInput");
    if(inp){inp.focus();inp.select()}
    const key=e=>{if(e.key==="Escape"){e.preventDefault();finish(null)}else if(e.key==="Enter"&&(!opts.multiline||e.ctrlKey||e.metaKey)){e.preventDefault();ok()}};
    const finish=v=>{el.classList.remove("show");el.innerHTML="";document.removeEventListener("keydown",key);resolve(v)};
    const ok=()=>finish(hasInput?(inp?inp.value:""):true);
    document.addEventListener("keydown",key);
    $("#modalOk").onclick=ok;
    $("#modalCancel").onclick=()=>finish(null);
    el.onclick=e=>{if(e.target===el)finish(null)};
  });
}
function openMedia(src,kind){
  const el=$("#lightbox");
  el.innerHTML=kind==="video"?`<video src="${src}" controls autoplay playsinline></video>`:`<img src="${src}" alt="">`;
  el.classList.add("show");
  el.onclick=e=>{
    if(e.target===el||e.target.tagName==="IMG"){
      el.classList.remove("show");el.innerHTML="";
    }
  };
}
function showLogin(){
  authed=false;
  $("#login").classList.add("show");
  if(sse){try{sse.close()}catch(e){}sse=null}
  connected=false;S=null;updateBot();
  setTimeout(()=>{const p=$("#pw");if(p)p.focus()},60);
}
function hideLogin(){$("#login").classList.remove("show")}
function updateBot(){
  const name=$("#botName"),state=$("#botState"),dot=$("#botDot");
  if(!name||!state||!dot)return;
  name.textContent="@"+((S&&S.bot)||"bot");
  let ok=!!(S&&S.online),txt=ok?"Telegram online":"Telegram offline";
  if(!connected){ok=false;txt=S?"Reconnecting…":"Connecting…"}
  dot.className="status-dot "+(ok?"ok":"bad");
  state.textContent=txt;
}
function updateNav(){
  $$(".nav-link[data-view]").forEach(b=>b.classList.toggle("on",b.dataset.view===view));
  const un=((S&&S.chats)||[]).reduce((a,c)=>a+(c.unread||0),0);
  const bu=$("#badge-unread");if(bu){bu.hidden=!un;bu.textContent=un>99?"99+":un}
  const p=((S&&S.access)||[]).filter(a=>a.status==="pending").length;
  const bp=$("#badge-pending");if(bp){bp.hidden=!p;bp.textContent=p}
  updateBot();
}
function scheduleLoad(){clearTimeout(loadTimer);loadTimer=setTimeout(load,150)}

async function load(){
  if(busy){queued=true;return}
  busy=true;
  try{
    const st=await api("state");
    S=st;
    const un=(S.stats&&S.stats.unread)||0;
    if(un>lastUnread&&sound)beep();
    lastUnread=un;
    document.title=(un?`(${un}) `:"")+"SamX Relay";
    updateNav();
    if(view==="chats"){
      renderList();
      if(cur)await refreshChat(false);
      else renderEmptyChat();
    }else if(view==="dash"||view==="acc"){
      renderPage();
    }else if(view==="tools"){
      updateSchedList();
    }
  }catch(e){
    if(e.message!=="auth"){connected=false;updateBot();toast("Connection lost — retrying…","error")}
  }finally{
    busy=false;
    if(queued){queued=false;setTimeout(load,120)}
  }
}
function go(v){
  if(v===view&&v!=="chats")return;
  if(v==="chats"&&view!=="chats"){cur=null;msgKey=""}
  view=v;
  document.body.classList.toggle("page-mode",v!=="chats");
  document.body.classList.toggle("chat",v==="chats"&&!!cur);
  appEl.classList.toggle("no-side",v!=="chats");
  updateNav();
  if(v==="chats"){renderList();cur?refreshChat(true):renderEmptyChat()}
  else renderPage();
}

function renderList(){
  if(!S)return;
  const all=S.chats||[];
  const sq=q.trim().toLowerCase();
  const rows=all.filter(c=>{
    if(flt==="unread"&&!c.unread)return false;
    if(flt==="groups"&&!(c.type&&c.type!=="private"))return false;
    if(flt==="pinned"&&!c.pin)return false;
    if(!sq)return true;
    return (c.name+" "+(c.user||"")+" "+c.id+" "+(c.note||"")+" "+(c.last||"")).toLowerCase().includes(sq);
  });
  const sub=$("#sideSub");
  const unreadAll=all.reduce((a,c)=>a+(c.unread||0),0);
  if(sub)sub.textContent=`${all.length} conversation${all.length===1?"":"s"} · ${unreadAll} unread`;
  const sc=listEl.scrollTop;
  listEl.innerHTML=rows.map(c=>{
    const flags=[c.pin?"📌":"",c.type&&c.type!=="private"?"👥":"",c.in_bot===false?"⚠":"",c.mute?"🔕":"",c.blk?"🚫":""].filter(Boolean).map(x=>`<span class="flag">${x}</span>`).join("");
    const preview=c.note?`<span class="note">📝</span><span style="overflow:hidden;text-overflow:ellipsis">${esc(c.note)}</span>`:`<span style="overflow:hidden;text-overflow:ellipsis">${esc(c.last||"No messages yet")}</span>`;
    return `<button class="chat-row ${c.id===cur?"on":""}" data-chat="${c.id}">
      ${avatarHTML(c.name,c.id)}
      <span class="chat-main"><span class="chat-line"><span class="chat-name">${esc(c.name)}</span><span class="chat-time">${c.ts?ago(c.ts):"new"}</span></span><span class="chat-preview">${preview}</span></span>
      <span class="chat-flags">${flags}${c.unread?`<span class="unread">${c.unread}</span>`:""}</span>
    </button>`;
  }).join("")||`<div class="empty" style="padding:36px 18px"><span class="big">✉</span><h3>${sq?"No matches":"No chats yet"}</h3><p>${sq?"Try a different search term.":"Open ＋ to add a group, or wait for the first message."}</p></div>`;
  listEl.scrollTop=sc;
}
function renderEmptyChat(){
  cur=null;msgKey="";
  mainEl.dataset.chat="";
  mainEl.innerHTML=`<div class="empty"><span class="big">✉</span><h3>Your inbox is ready</h3><p>Select a conversation to read and reply. Messages relay straight to Telegram.</p></div>`;
}
async function openChat(id){
  cur=id;view="chats";
  document.body.classList.add("chat");
  document.body.classList.remove("page-mode");
  appEl.classList.remove("no-side");
  updateNav();renderList();
  await refreshChat(true);
}

function ensureShell(c){
  if(mainEl.dataset.chat===String(c.id)&&$("#msgs"))return false;
  mainEl.dataset.chat=String(c.id);
  mainEl.innerHTML=`<header class="chat-head">
    <button class="icon-btn back-btn" id="backBtn" title="Back">←</button>
    ${avatarHTML(c.name,c.id,"lg")}
    <div class="chat-id" id="chatId"></div>
    <div class="head-actions">
      <button class="act" data-act="pin" id="actPin"></button>
      <button class="act" data-act="mute" id="actMute"></button>
      <button class="act" data-act="blk" id="actBlk"></button>
      <button class="act" data-act="note">✎ <span>Note</span></button>
      <button class="act danger" data-act="del" title="Delete chat">🗑</button>
    </div>
  </header>
  <div class="messages" id="msgs"></div>
  <button class="to-bottom" id="toBottom" title="Scroll to bottom">↓</button>
  <div class="composer-wrap"><div class="composer">
    <button class="attach" id="attachBtn" title="Attach a file">＋</button>
    <input id="fileIn" type="file" hidden>
    <textarea id="tx" rows="1" placeholder="Message ${esc(c.name)}…" dir="auto"></textarea>
    <button class="send-btn" id="sendBtn" title="Send (Enter)">➤</button>
  </div><div class="composer-hint">Enter to send · Shift + Enter for a new line</div></div>`;
  bindChat();
  return true;
}
function bindChat(){
  const back=$("#backBtn");if(back)back.onclick=()=>{cur=null;msgKey="";document.body.classList.remove("chat");renderEmptyChat();renderList()};
  const at=$("#attachBtn");if(at)at.onclick=()=>$("#fileIn").click();
  const fi=$("#fileIn");if(fi)fi.onchange=e=>upload(e.target.files[0],e.target);
  const sb=$("#sendBtn");if(sb)sb.onclick=send;
  const ta=$("#tx");
  if(ta){
    ta.addEventListener("input",()=>autosize(ta));
    ta.addEventListener("keydown",e=>{if(e.key==="Enter"&&!e.shiftKey){e.preventDefault();send()}});
  }
  const el=$("#msgs");
  if(el){el.addEventListener("scroll",updateBottomBtn);el.addEventListener("click",onMsgClick)}
  const tb=$("#toBottom");if(tb)tb.onclick=()=>{if(el)el.scrollTop=el.scrollHeight};
  mainEl.querySelectorAll(".head-actions .act").forEach(b=>b.onclick=()=>chatAction(b.dataset.act));
}
function autosize(ta){ta.style.height="auto";ta.style.height=Math.min(ta.scrollHeight,140)+"px"}
function updateChatHeader(c){
  const el=$("#chatId");if(!el)return;
  const typeLabel={private:"Private",group:"Group",supergroup:"Supergroup",channel:"Channel"}[c.type]||"";
  const meta=[typeLabel,c.user?"@"+c.user:null,c.members?`${c.members} members`:null,"ID "+c.id,c.count?`${c.count} messages`:null,c.in_bot===false?"⚠ bot removed":null].filter(Boolean).join(" · ");
  el.innerHTML=`<b>${esc(c.name)}</b><small>${esc(meta)}${c.note?" · 📝 "+esc(c.note):""}${c.blk?" · 🚫 blocked":""}</small>`;
  const set=(sel,icon,label,on)=>{const b=$(sel);if(b){b.innerHTML=icon+' <span>'+label+'</span>';b.title=label;b.classList.toggle("on",!!on)}};
  set("#actPin","📌",c.pin?"Unpin":"Pin",c.pin);
  set("#actMute",c.mute?"🔔":"🔕",c.mute?"Unmute":"Mute",c.mute);
  set("#actBlk",c.blk?"✅":"🚫",c.blk?"Unblock":"Block",c.blk);
}
async function refreshChat(scroll){
  const id=cur;if(!id)return;
  const c=findChat(id);
  if(!c){cur=null;renderEmptyChat();return}
  let r;
  try{r=await api("chat?id="+id)}catch(e){if(e.message!=="auth")toast(e.message,"error");return}
  if(cur!==id)return;
  M=r.msgs||[];
  ensureShell(c);
  updateChatHeader(c);
  renderMessages(scroll);
  if(c.unread){c.unread=0;renderList();updateNav()}
}
function updateBottomBtn(){
  const el=$("#msgs"),b=$("#toBottom");if(!el||!b)return;
  b.classList.toggle("show",el.scrollHeight-el.scrollTop-el.clientHeight>220);
}
function onMsgClick(e){
  const ed=e.target.closest("[data-edit]");
  if(ed){editMessage(Number(ed.dataset.edit));return}
  const v=e.target.closest("[data-media]");
  if(v)openMedia(v.dataset.full,v.dataset.media);
}
async function editMessage(mid){
  if(!cur)return;
  const m=M.find(x=>x.id===mid);
  if(!m)return;
  const v=await modal({title:"Edit message",text:"Saves here and updates the message in Telegram.",input:m.text||"",multiline:true,placeholder:"Message…",confirm:"Save"});
  if(v===null)return;
  const text=String(v).trim();
  if(!text||text===m.text)return;
  try{await api("edit",{id:cur,mid,text});toast("Message updated","ok");await refreshChat(true)}
  catch(e){toast(e.message||"Edit failed","error")}
}
function fileUrl(cid,m,full,dl){return `/api/file?c=${cid}&m=${m.id}${full?"&full=1":""}${dl?"&dl=1":""}`}
function fileCard(cid,m){
  return `<a class="file-card" href="${fileUrl(cid,m,true,true)}" target="_blank" rel="noopener"><span class="file-ic">📄</span><span class="file-info"><b>${esc(m.name||"Document")}</b><small>Download</small></span><span class="dl">↓</span></a>`;
}
function msgBody(cid,m){
  let b="";
  if(m.reply)b+=`<div class="reply-quote">↩ ${esc(m.reply)}</div>`;
  if(m.kind==="photo"||m.kind==="sticker"){
    b+=`<img class="media-img" loading="lazy" src="${fileUrl(cid,m)}" data-media="image" data-full="${fileUrl(cid,m,true)}" alt="">`;
  }else if(m.kind==="animation"||m.kind==="video"||m.kind==="video_note"){
    if(m.disp)b+=`<div class="media-video" data-media="video" data-full="${fileUrl(cid,m,true)}"><img loading="lazy" src="${fileUrl(cid,m)}" alt=""><span class="play">▶</span></div>`;
    else b+=fileCard(cid,m);
  }else if(m.kind==="voice"||m.kind==="audio"){
    b+=`<div class="audio-card"><span class="audio-ic">${m.kind==="voice"?"🎤":"🎵"}</span><audio controls preload="none" src="${fileUrl(cid,m,true)}"></audio><span style="font-size:11px;opacity:.75">${m.dur?fmtDur(m.dur):""}</span></div>`;
    if(m.name)b+=`<div style="font-size:11.5px;opacity:.8;margin-bottom:6px">${esc(m.name)}</div>`;
  }else if(m.kind==="document"){
    b+=fileCard(cid,m);
  }
  if(m.text)b+=`<div class="text">${linkify(m.text)}</div>`;
  if(m.dir==="out"&&m.kind==="text")b+=`<button class="msg-edit" data-edit="${m.id}" title="Edit this message in Telegram">✎ Edit</button>`;
  return b;
}
function renderMessages(force){
  const el=$("#msgs");if(!el)return;
  const c=findChat(cur);if(!c)return;
  const key=cur+"|"+M.map(m=>[m.id,m.dir,m.ts,m.edited?1:0,hash(m.text||""),m.kind,hash(m.name||""),m.via||""].join("~")).join(";");
  if(!force&&key===msgKey)return;
  msgKey=key;
  const near=el.scrollHeight-el.scrollTop-el.clientHeight<160;
  const prevTop=el.scrollTop,prevHeight=el.scrollHeight;
  let html="",day="",prev=null;
  for(const m of M){
    const dl=fmtDay(new Date(m.ts*1000));
    if(dl!==day){html+=`<div class="day"><span>${esc(dl)}</span></div>`;day=dl;prev=null}
    const grouped=prev&&prev.dir===m.dir&&(m.ts-prev.ts)<300;
    const first=!grouped;
    const sticker=m.kind==="sticker"&&!m.text;
    html+=`<div class="msg ${m.dir} ${first?"first":"grouped"}">${m.dir==="in"?(first?avatarHTML(c.name,c.id,"sm"):`<span class="msg-first"></span>`):""}<div class="bubble ${sticker?"sticker":""}">${msgBody(cur,m)}<div class="msg-meta">${m.via?`<span>via ${esc(m.via)}</span>`:""}${m.auto?"<span>🤖 auto</span>":""}${m.edited?"<span>edited</span>":""}<span>${tm(m.ts)}</span>${m.dir==="out"?'<span class="ticks">✓✓</span>':""}</div></div></div>`;
    prev=m;
  }
  el.innerHTML=html||`<div class="empty"><span class="big">✉</span><h3>No messages yet</h3><p>Say hello or wait for the first message.</p></div>`;
  if(force||near){
    requestAnimationFrame(()=>{el.scrollTop=el.scrollHeight;updateBottomBtn()});
  }else{
    el.scrollTop=Math.max(0,el.scrollHeight-(prevHeight-prevTop));
    updateBottomBtn();
  }
}
async function send(){
  if(!cur)return;
  const ta=$("#tx");if(!ta)return;
  const text=ta.value.trim();if(!text)return;
  const old=ta.value;
  ta.value="";autosize(ta);
  try{
    await api("send",{id:cur,text});
    await refreshChat(true);
  }catch(e){
    ta.value=old;autosize(ta);
    toast(e.message||"Send failed","error");
  }
}
async function upload(file,input){
  if(!file||!cur)return;
  if(file.size>20*1024*1024){toast("File is too large (max 20 MB)","error");input.value="";return}
  toast("Uploading "+file.name+"…");
  try{
    const r=await fetch("/api/upload",{method:"POST",headers:{"X-Chat":String(cur),"X-Name":encodeURIComponent(file.name)},body:file});
    if(r.status===401){showLogin();return}
    const j=await r.json().catch(()=>({}));
    if(!r.ok)throw new Error(j.error||"Upload failed");
    toast("File sent","ok");
    await refreshChat(true);
  }catch(e){toast(e.message||"Upload failed","error")}
  input.value="";
}
async function chatAction(act){
  if(!cur)return;
  const c=findChat(cur);if(!c)return;
  if(act==="pin"||act==="mute"||act==="blk"){
    const map={pin:"pinned",mute:"muted",blk:"blocked"};
    try{await api("flag",{id:cur,key:map[act],val:!c[map[act]]});await load()}
    catch(e){toast(e.message,"error")}
  }else if(act==="note"){
    const v=await modal({title:"Private note",text:"Only visible in this console — never sent to Telegram.",input:c.note||"",placeholder:"Write a note…",confirm:"Save"});
    if(v===null)return;
    try{await api("note",{id:cur,text:v});await load();toast("Note saved","ok")}
    catch(e){toast(e.message,"error")}
  }else if(act==="del"){
    const ok=await modal({title:"Delete conversation?",text:"This removes the chat from the web console. It does not delete anything on Telegram.",confirm:"Delete",danger:true});
    if(!ok)return;
    try{await api("del",{id:cur});cur=null;document.body.classList.remove("chat");renderEmptyChat();await load();toast("Chat removed","ok")}
    catch(e){toast(e.message,"error")}
  }
}
function renderPage(){
  mainEl.dataset.chat="";msgKey="";
  if(!S){mainEl.innerHTML=`<div class="empty"><span class="big">⏳</span><h3>Loading…</h3></div>`;return}
  if(view==="dash")renderDashboard();
  else if(view==="acc")renderAccess();
  else if(view==="tools")renderTools();
  else if(view==="set")renderSettings();
}
function statCard(icon,label,val,sub){
  return `<div class="stat-card"><span class="stat-ico">${icon}</span><small>${label}</small><b>${val}</b><span>${sub}</span></div>`;
}
function renderDashboard(){
  const st=S.stats||{};
  const pend=(S.access||[]).filter(a=>a.status==="pending").length;
  const ops=(S.access||[]).filter(a=>a.status==="allowed").length;
  const mx=Math.max.apply(null,[1].concat(st.hours||[0]));
  const bars=(st.hours||[]).map((v,i)=>`<div class="bar"><i data-v="${v}" style="height:${Math.max(4,Math.round(v/mx*100))}%"></i><span>${i===11?"now":"-"+(11-i)+"h"}</span></div>`).join("");
  const top=(st.top||[]).map((t,i)=>`<div class="list-item"><span class="rank">${i+1}</span><span class="name">${esc(t[1])}</span><span class="val">${t[0]} msgs</span></div>`).join("")||`<div class="empty" style="padding:20px"><p>No activity in the last 24 hours.</p></div>`;
  const log=(S.log||[]).map(l=>`<div class="log-item"><span class="log-dot"></span><span class="log-body"><b>${esc(l[2]||("ID "+l[1]))}</b><p>${esc(l[3])}</p></span><span class="log-time">${tm(l[0])}</span></div>`).join("")||`<div class="empty" style="padding:20px"><p>No activity yet.</p></div>`;
  mainEl.innerHTML=`<div class="page">
    <div class="page-head"><div><h1>Dashboard</h1><p>Live overview of your relay performance.</p></div><div class="page-actions"><button class="btn ghost" data-refresh>↻ Refresh</button></div></div>
    <div class="stats">
      ${statCard("💬","Total chats",st.chats||0,`${st.pinned||0} pinned · ${st.blocked||0} blocked`)}
      ${statCard("🔴","Unread",st.unread||0,"Waiting for a reply")}
      ${statCard("⬇","Inbound · 24h",st.d_in||0,`${st.m_in||0} all time`)}
      ${statCard("⬆","Outbound · 24h",st.d_out||0,`${st.m_out||0} all time`)}
    </div>
    <div class="panel" style="margin-top:16px">
      <div class="panel-head"><h2>Activity · last 12 hours</h2><span class="hint">${st.chats||0} chats · ${ops} operators</span></div>
      <div class="chart">${bars}</div>
    </div>
    <div class="cols">
      <div class="panel"><div class="panel-head"><h2>Most active · 24h</h2><span class="hint">Top chats</span></div><div class="list">${top}</div></div>
      <div class="panel"><div class="panel-head"><h2>Activity log</h2><span class="hint">${pend} pending</span></div><div class="log">${log}</div></div>
    </div>
  </div>`;
}
function renderAccess(){
  const acc=S.access||[];
  const counts={all:acc.length,pending:acc.filter(a=>a.status==="pending").length,allowed:acc.filter(a=>a.status==="allowed").length,banned:acc.filter(a=>a.status==="banned").length};
  const items=acc.map(a=>{
    const search=(a.name+" "+(a.username||"")+" "+a.id).toLowerCase();
    const last=a.last?ago(a.last):"—";
    const actions=a.status==="allowed"
      ?`<button class="btn sm ghost" data-acc="${a.id}" data-status="none">Revoke</button><button class="btn sm bad" data-acc="${a.id}" data-status="banned">Ban</button>`
      :a.status==="banned"
      ?`<button class="btn sm ok" data-acc="${a.id}" data-status="none">Unban</button><button class="btn sm ghost" data-acc="${a.id}" data-status="allowed">Allow</button>`
      :`<button class="btn sm ok" data-acc="${a.id}" data-status="allowed">Allow</button><button class="btn sm bad" data-acc="${a.id}" data-status="banned">Ban</button>`;
    return `<div class="user-card" data-status="${a.status}" data-search="${esc(search)}">${avatarHTML(a.name,a.id)}
      <div class="user-main"><b>${esc(a.name||("ID "+a.id))}</b><small>${a.username?"@"+esc(a.username)+" · ":""}ID ${a.id} · /samx × ${a.hits||0} · last ${esc(last)}</small></div>
      <span class="pill ${a.status}">${a.status}</span>
      <div class="user-actions">${actions}<button class="btn sm ghost" data-acc="${a.id}" data-status="forget">Remove</button></div></div>`;
  }).join("");
  mainEl.innerHTML=`<div class="page">
    <div class="page-head"><div><h1>Access control</h1><p>Anyone who types the hidden command <b>/samx</b> appears here. You approve who gets in.</p></div><div class="page-actions"><button class="btn ghost" data-refresh>↻ Refresh</button></div></div>
    <div class="access-tools">
      <div class="pre-auth"><input id="preId" class="input" inputmode="numeric" placeholder="Pre-authorize a Telegram ID…" style="max-width:360px"><button class="btn primary" id="preBtn">＋ Allow</button></div>
      <div class="seg" id="accFilters">${["all","pending","allowed","banned"].map(k=>`<button class="chip ${accFilter===k?"on":""}" data-afilter="${k}">${k} · ${counts[k]}</button>`).join("")}</div>
      <input id="accSearch" class="input" placeholder="Search by name, username or ID…" style="max-width:360px" value="${esc(accQ)}">
    </div>
    <div class="user-list" id="accList">${items||`<div class="empty" style="padding:40px"><span class="big">⛨</span><h3>No access requests</h3><p>Users will appear here after they type /samx.</p></div>`}</div>
  </div>`;
  bindAccess();
  applyAccFilters();
}
function bindAccess(){
  const pre=$("#preBtn");if(pre)pre.onclick=async()=>{
    const v=($("#preId")||{}).value||"";
    if(!/^\d{3,}$/.test(v.trim())){toast("Enter a valid Telegram numeric ID","error");return}
    try{await api("access",{id:Number(v.trim()),status:"allowed"});$("#preId").value="";toast("Access granted","ok");await load()}
    catch(e){toast(e.message,"error")}
  };
  const pi=$("#preId");if(pi)pi.addEventListener("keydown",e=>{if(e.key==="Enter")pre.click()});
  $$("#accFilters [data-afilter]").forEach(b=>b.onclick=()=>{accFilter=b.dataset.afilter;$$("#accFilters .chip").forEach(x=>x.classList.toggle("on",x===b));applyAccFilters()});
  const as=$("#accSearch");if(as)as.addEventListener("input",e=>{accQ=e.target.value;applyAccFilters()});
  const list=$("#accList");if(list)list.onclick=async e=>{
    const b=e.target.closest("[data-acc]");if(!b)return;
    const id=Number(b.dataset.acc),status=b.dataset.status;
    if(status==="banned"){
      const ok=await modal({title:"Ban this user?",text:"They will be denied access to /samx and their messages will be ignored.",confirm:"Ban",danger:true});
      if(!ok)return;
    }
    if(status==="forget"){
      const ok=await modal({title:"Remove from list?",text:"This only removes the record from the access list.",confirm:"Remove",danger:true});
      if(!ok)return;
    }
    try{
      await api("access",{id,status});
      toast(status==="allowed"?"Access granted":status==="banned"?"User banned":status==="forget"?"Removed":"Access revoked",status==="banned"?"error":"ok");
      await load();
    }catch(e2){toast(e2.message,"error")}
  };
}
function applyAccFilters(){
  const list=$("#accList");if(!list)return;
  const cards=$$("#accList .user-card");
  let em=$("#accEmpty");
  if(!cards.length){if(em)em.remove();return}
  const sq=(accQ||"").toLowerCase();
  let shown=0;
  cards.forEach(el=>{
    const ok=(accFilter==="all"||el.dataset.status===accFilter)&&(!sq||el.dataset.search.includes(sq));
    el.style.display=ok?"":"none";
    if(ok)shown++;
  });
  if(!shown){if(!em){em=document.createElement("div");em.id="accEmpty";em.className="empty";em.style.padding="40px";em.innerHTML="<span class='big'>⌕</span><h3>No matches</h3><p>Try a different filter or search term.</p>";list.appendChild(em)}}
  else if(em){em.remove()}
}
function schedTargetName(id){
  if(id==="all"||id===-1||id==="-1")return "All eligible chats";
  const c=findChat(Number(id));
  return c?c.name:("ID "+id);
}
function schedTime(ts){return Number(ts)?new Date(Number(ts)*1000).toLocaleString([], {month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"}):"—"}
function schedRowsHTML(){
  const sched=((S&&S.schedules)||[]).slice().sort((a,b)=>(Number(a.send_at)||0)-(Number(b.send_at)||0));
  return sched.map(j=>{
    const st=["active","sending","done"].includes(j.status)?j.status:"active";
    const rep=j.repeat==="daily"?"Daily":j.repeat==="weekly"?"Weekly":"Once";
    return `<div class="list-item sched-item">
      <span class="status-pill ${st}">${st}</span>
      <span class="name"><b>${esc(schedTargetName(j.chat_id))}</b><small>${esc((j.text||"").replace(/\s+/g," ").slice(0,110))}</small></span>
      <span class="val">${esc(schedTime(j.send_at))}<small>${rep}${j.result?" · "+esc(j.result):""}</small></span>
      <button class="btn sm ghost" data-schdel="${esc(j.id)}" title="Delete">✕</button>
    </div>`;
  }).join("")||`<div class="empty" style="padding:18px"><p>No scheduled messages yet.</p></div>`;
}
function updateSchedList(){const el=$("#schList");if(el)el.innerHTML=schedRowsHTML()}
function renderTools(){
  const a=S.auto||{enabled:false,text:"",cooldown_min:60};
  const opts=[[5,"5 min"],[15,"15 min"],[60,"1 hour"],[360,"6 hours"],[1440,"24 hours"]].map(p=>`<option value="${p[0]}" ${Number(a.cooldown_min)===p[0]?"selected":""}>${p[1]}</option>`).join("");
  const chatOpts=`<option value="all">All eligible chats</option>`+((S.chats||[]).filter(c=>c.in_bot!==false).map(c=>`<option value="${c.id}">${esc(c.name)}${c.type&&c.type!=="private"?" · "+(c.type==="supergroup"?"group":c.type):""}</option>`).join(""));
  mainEl.innerHTML=`<div class="page">
    <div class="page-head"><div><h1>Tools</h1><p>Automate greetings and reach every chat at once.</p></div></div>
    <div class="tools-grid">
      <div class="card"><h2>Auto-reply</h2><p class="desc">Send a friendly reply once per chat, only if you have not answered recently.</p>
        <div class="body">
          <div class="row-between"><div class="t"><b>Enabled</b><small>Automatically answer new chats</small></div><label class="switch"><input type="checkbox" id="arEnabled" ${a.enabled?"checked":""}><span></span></label></div>
          <textarea class="input" id="arText" rows="4" dir="auto" placeholder="Auto-reply text…">${esc(a.text)}</textarea>
          <div class="row-between"><div class="t"><b>Cooldown</b><small>Minimum gap per chat</small></div><select class="input" id="arCd" style="width:auto;min-width:130px">${opts}</select></div>
          <div><div class="section-title" style="margin:0 0 8px">Preview <span>what they see</span></div><div class="preview-bubble" id="arPreview">${esc(a.text)||"…"}</div></div>
          <button class="btn primary" id="arSave">Save auto-reply</button>
        </div>
      </div>
      <div class="card"><h2>Broadcast</h2><p class="desc">Send one message to every chat that is not blocked and not an operator.</p>
        <div class="body">
          <textarea class="input" id="bcText" rows="6" dir="auto" placeholder="Message for every chat…"></textarea>
          <div class="row-between"><div class="t"><b>Recipients</b><small class="target-count" id="bcCount">Calculating…</small></div><button class="btn primary" id="bcSend">Send broadcast</button></div>
        </div>
      </div>
      <div class="card" style="grid-column:1/-1"><h2>Scheduled messages</h2><p class="desc">Server-side queue. It sends even when this page is closed and survives restarts.</p>
        <div class="body">
          <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end">
            <label style="display:flex;flex-direction:column;gap:6px;flex:1;min-width:200px"><span style="font-size:11px;font-weight:600;color:var(--text-3)">Send to</span><select class="input" id="schChat">${chatOpts}</select></label>
            <label style="display:flex;flex-direction:column;gap:6px;flex:1;min-width:210px"><span style="font-size:11px;font-weight:600;color:var(--text-3)">When (your local time)</span><input class="input" id="schWhen" type="datetime-local"></label>
            <label style="display:flex;flex-direction:column;gap:6px;min-width:130px"><span style="font-size:11px;font-weight:600;color:var(--text-3)">Repeat</span><select class="input" id="schRepeat"><option value="once">Once</option><option value="daily">Daily</option><option value="weekly">Weekly</option></select></label>
          </div>
          <textarea class="input" id="schText" rows="3" dir="auto" placeholder="Message to send at the scheduled time…"></textarea>
          <div class="row-between"><div class="t"><b>Queue</b><small>Stored on the server; scheduled times use your browser clock.</small></div><button class="btn primary" id="schAdd">⏰ Schedule</button></div>
          <div class="list" id="schList" style="display:flex;flex-direction:column;gap:8px">${schedRowsHTML()}</div>
        </div>
      </div>
    </div>
  </div>`;
  const ta=$("#arText"),prev=$("#arPreview");
  if(ta){ta.addEventListener("input",()=>{prev.textContent=ta.value||"…"})}
  const save=$("#arSave");if(save)save.onclick=async()=>{
    try{await api("auto",{enabled:$("#arEnabled").checked,text:$("#arText").value,cooldown_min:Number($("#arCd").value)});toast("Auto-reply saved","ok");await load()}
    catch(e){toast(e.message,"error")}
  };
  api("targets").then(r=>{const el=$("#bcCount");if(el)el.innerHTML=`<b>${r.n||0}</b> chats will receive it`}).catch(()=>{});
  const bc=$("#bcSend");if(bc)bc.onclick=async()=>{
    const text=$("#bcText").value.trim();if(!text){toast("Write a message first","error");return}
    const m=($("#bcCount").textContent||"").match(/\d+/);
    const n=m?Number(m[0]):0;
    const ok=await modal({title:"Send broadcast?",text:`This will message ${n||"all eligible"} chats. Continue?`,confirm:"Send now"});
    if(!ok)return;
    try{const r=await api("broadcast",{text});toast(`Broadcasting to ${r.n||0} chats…`,"ok");$("#bcText").value=""}
    catch(e){toast(e.message,"error")}
  };
  const when=$("#schWhen");
  const pad2=n=>String(n).padStart(2,"0");
  if(when){
    const d=new Date(Date.now()+3600000);d.setSeconds(0,0);
    when.value=`${d.getFullYear()}-${pad2(d.getMonth()+1)}-${pad2(d.getDate())}T${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
    const n=new Date();
    when.min=`${n.getFullYear()}-${pad2(n.getMonth()+1)}-${pad2(n.getDate())}T${pad2(n.getHours())}:${pad2(n.getMinutes())}`;
  }
  const schAdd=$("#schAdd");
  if(schAdd)schAdd.onclick=async()=>{
    const text=$("#schText").value.trim();
    if(!text){toast("Write a message first","error");return}
    const val=$("#schWhen").value;
    if(!val){toast("Pick a date and time","error");return}
    const ts=Math.floor(new Date(val).getTime()/1000);
    if(!isFinite(ts)||ts<=0){toast("Invalid date and time","error");return}
    if(ts<Math.floor(Date.now()/1000)-30){toast("Pick a future time","error");return}
    const chat=$("#schChat").value;
    try{
      await api("schedule",{chat:chat==="all"?"all":Number(chat),text,send_at:ts,repeat:$("#schRepeat").value});
      $("#schText").value="";toast("Scheduled","ok");await load();updateSchedList();
    }catch(e){toast(e.message,"error")}
  };
  const schList=$("#schList");
  if(schList)schList.onclick=async e=>{
    const b=e.target.closest("[data-schdel]");if(!b)return;
    const ok=await modal({title:"Delete this schedule?",text:"It will not be sent.",confirm:"Delete",danger:true});
    if(!ok)return;
    try{await api("schedule/del",{id:b.dataset.schdel});toast("Schedule deleted","ok");await load();updateSchedList()}
    catch(e2){toast(e2.message,"error")}
  };
}
function renderSettings(){
  const s=S.set||{};
  const row=(key,title,desc)=>`<div class="row-between"><div class="t"><b>${title}</b><small>${desc}</small></div><label class="switch"><input type="checkbox" data-set="${key}" ${s[key]?"checked":""}><span></span></label></div>`;
  mainEl.innerHTML=`<div class="page">
    <div class="page-head"><div><h1>Settings</h1><p>Make the console behave the way you like.</p></div></div>
    <div class="tools-grid">
      <div class="card"><h2>Appearance</h2><p class="desc">Theme and browser feedback.</p>
        <div class="body">
          <div class="row-between"><div class="t"><b>Theme</b><small>Switch between dark and light</small></div><div class="seg"><button class="chip ${theme==="dark"?"on":""}" data-theme="dark">☾ Dark</button><button class="chip ${theme==="light"?"on":""}" data-theme="light">☀ Light</button></div></div>
          <div class="row-between"><div class="t"><b>Browser sound</b><small>Play a chime on new messages</small></div><label class="switch"><input type="checkbox" id="sndToggle" ${sound?"checked":""}><span></span></label></div>
        </div>
      </div>
      <div class="card"><h2>Telegram</h2><p class="desc">Alerts and panel behaviour.</p>
        <div class="body">${row("notify_admin","New-message alerts","Notify me in Telegram for every new message")}${row("log_usage","Operator usage alerts","Tell me when an operator opens /samx")}${row("panel_clean","Clean mode","Delete relayed messages from the operator chat")}</div>
      </div>
      <div class="card"><h2>Account</h2><p class="desc">Connection details for this console.</p>
        <div class="body">
          <div class="kv"><span>Bot</span><b>@${esc(S.bot||"—")}</b></div>
          <div class="kv"><span>Owner ID</span><b>${S.owner||"—"}</b></div>
          <div class="kv"><span>Telegram status</span><b>${S.online?"🟢 Online":"🔴 Offline"}</b></div>
          <div class="kv"><span>Session</span><b>🔒 HttpOnly cookie</b></div>
          <button class="btn bad" id="logoutBtn" style="margin-top:6px">Sign out</button>
        </div>
      </div>
    </div>
  </div>`;
  $$("#main [data-set]").forEach(inp=>inp.onchange=async()=>{
    const key=inp.dataset.set,val=inp.checked;
    inp.disabled=true;
    try{await api("set",{key,val});toast("Setting saved","ok")}
    catch(e){inp.checked=!val;toast(e.message,"error")}
    finally{inp.disabled=false}
  });
  $$("#main [data-theme]").forEach(b=>b.onclick=()=>{setTheme(b.dataset.theme);renderSettings()});
  const st=$("#sndToggle");if(st)st.onchange=e=>{sound=e.target.checked;localStorage.setItem("snd",sound?"1":"0")};
  const lo=$("#logoutBtn");if(lo)lo.onclick=async()=>{
    const ok=await modal({title:"Sign out?",text:"You will need the password to sign in again.",confirm:"Sign out",danger:true});
    if(!ok)return;
    try{await api("logout")}catch(e){}
    showLogin();
  };
}
function connect(){
  if(!authed)return;
  if(sse){try{sse.close()}catch(e){}sse=null}
  connected=false;updateBot();
  try{
    sse=new EventSource("/api/events");
    sse.onopen=()=>{sseRetry=0;connected=true;updateBot()};
    sse.onmessage=()=>scheduleLoad();
    sse.onerror=()=>{
      connected=false;updateBot();
      try{sse.close()}catch(e){}
      sse=null;sseRetry++;
      setTimeout(()=>{if(!sse&&authed)connect()},Math.min(8000,1200*sseRetry));
    };
  }catch(e){connected=false;updateBot()}
}
async function start(){authed=true;connect();await load();if(view!=="chats")renderPage()}
async function doLogin(e){
  e.preventDefault();
  const btn=$("#loginBtn"),err=$("#loginError");
  err.textContent="";btn.disabled=true;
  const old=btn.textContent;btn.textContent="Signing in…";
  try{
    const r=await fetch("/api/login",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({pw:$("#pw").value})});
    let j={};try{j=await r.json()}catch(e){}
    if(!r.ok){err.textContent=j.error||"Wrong password";return}
    $("#pw").value="";
    hideLogin();start();
  }catch(e){err.textContent="Network error — try again"}
  finally{btn.disabled=false;btn.textContent=old}
}

setTheme(theme);
$$(".nav-link[data-view]").forEach(b=>b.onclick=()=>go(b.dataset.view));
const themeBtn=$("#themeBtn");
if(themeBtn)themeBtn.onclick=()=>{setTheme(theme==="dark"?"light":"dark");if(view==="set")renderSettings()};
const refreshBtn=$("#refreshBtn");
if(refreshBtn)refreshBtn.onclick=()=>load();
const addChatBtn=$("#addChatBtn");
if(addChatBtn)addChatBtn.onclick=async()=>{
  const q=await modal({title:"Add group or chat",text:"Paste a @username, t.me link, or numeric chat ID. Telegram only reveals chats the bot is already a member of; groups where the bot gets added also appear automatically.",input:"",placeholder:"@mygroup or -1001234567890",confirm:"Add chat"});
  if(q===null)return;
  const v=String(q).trim();
  if(!v){toast("Enter a chat first","error");return}
  try{const r=await api("addchat",{q:v});toast("Chat added","ok");await load();if(r.id)openChat(Number(r.id))}
  catch(e){toast(e.message,"error")}
};
const qi=$("#q");
if(qi)qi.addEventListener("input",e=>{q=e.target.value;renderList()});
$$("#chips .chip").forEach(b=>b.onclick=()=>{flt=b.dataset.filter;$$("#chips .chip").forEach(x=>x.classList.toggle("on",x===b));renderList()});
listEl.addEventListener("click",e=>{const b=e.target.closest("[data-chat]");if(b)openChat(Number(b.dataset.chat))});
mainEl.addEventListener("click",e=>{const b=e.target.closest("[data-refresh]");if(b)load()});
$("#loginForm").addEventListener("submit",doLogin);
const tp=$("#togglePw");
if(tp)tp.onclick=()=>{const p=$("#pw");p.type=p.type==="password"?"text":"password";tp.textContent=p.type==="password"?"Show":"Hide"};
document.addEventListener("keydown",e=>{
  if(e.key==="Escape"){
    const lb=$("#lightbox");
    if(lb&&lb.classList.contains("show")){lb.classList.remove("show");lb.innerHTML="";return}
    const md=$("#modal");
    if(md&&md.classList.contains("show"))return;
    if(cur){cur=null;document.body.classList.remove("chat");renderEmptyChat();renderList()}
  }
  if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==="k"){
    e.preventDefault();
    if(view!=="chats")go("chats");
    const qq=$("#q");if(qq){qq.focus();qq.select()}
  }
});
(async()=>{
  try{
    const st=await api("state");
    lastUnread=(st.stats&&st.stats.unread)||0;
    hideLogin();start();
  }catch(e){
    if(e.message!=="auth"){showLogin();toast("Server unreachable — check the console","error")}
  }
})();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is missing in .env")
    if not ADMIN_CHAT_ID:
        raise SystemExit("ADMIN_CHAT_ID is missing in .env (you are the owner/admin)")
    core = WebCore()
    core.bot = core.tg.call("getMe").get("username", "")
    srv = ThreadingHTTPServer((WEB_HOST, WEB_PORT), make_handler(core))
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log(f"🌐 http://{WEB_HOST}:{WEB_PORT}   password: {WEB_PASSWORD}" + ("  (generated — set WEB_PASSWORD in .env)" if GENERATED else ""))
    log(f"db {DB_FILE}")
    signal.signal(signal.SIGTERM, core.shutdown)
    signal.signal(signal.SIGINT, core.shutdown)
    core.run()
