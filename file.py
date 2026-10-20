import os
import ssl
import json
import time
import queue
import asyncio
import base64
import http.client
from concurrent.futures import ThreadPoolExecutor

# ================= CONFIGURATION =================
BOT_TOKEN = "8837154745:AAEzy0lRzbgR3bmDCAbz4Hg24Tt7i_AQV3g"
BOT_USERNAME = "Leonfilebot"
DB_CHANNEL = -1004455742083

OWNER_ID = 8626080388
OWNER_USERNAME = "roushanog"
# =================================================

DB_FILE = "bot_data.json"

default_db = {
    "bot_active": True,
    "protect_content": False,
    "auto_delete": 0,
    "total_links": 0,
    "users": [],
    "banned_users": [],
    "fsub_channels": [],
    "user_stats": {},
    "batches": {}
}

def load_db():
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                for k, v in default_db.items():
                    if k not in data:
                        data[k] = v
                return data
        except Exception:
            pass
    return default_db.copy()

db = load_db()
_save_pending = False

async def save_db_async():
    global _save_pending
    if _save_pending:
        return
    _save_pending = True
    await asyncio.sleep(0.3)
    _save_pending = False
    await asyncio.to_thread(_sync_save_db)

def _sync_save_db():
    try:
        with open(DB_FILE, "w", encoding="utf-8") as f:
            json.dump(db, f, indent=2)
    except Exception:
        pass

def save_db():
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(save_db_async())
    except Exception:
        _sync_save_db()

# Runtime Memory & Smart Cache
user_sessions = {}
user_locks = {}
admin_state = {}
fsub_cache = {}  # {user_id: expiry_timestamp}


# ================= TURBO KEEP-ALIVE CONNECTION POOL =================
SSL_CTX = ssl.create_default_context()
CONN_POOL = queue.Queue(maxsize=30)
EXECUTOR = ThreadPoolExecutor(max_workers=32)

def _get_conn(timeout=15):
    try:
        conn = CONN_POOL.get_nowait()
        conn.timeout = timeout
        return conn
    except queue.Empty:
        return http.client.HTTPSConnection("api.telegram.org", timeout=timeout, context=SSL_CTX)

def _put_conn(conn):
    try:
        CONN_POOL.put_nowait(conn)
    except queue.Full:
        try:
            conn.close()
        except Exception:
            pass

def _fast_api_call(method: str, payload: dict, timeout: int = 15):
    body = json.dumps(payload).encode("utf-8")
    path = f"/bot{BOT_TOKEN}/{method}"
    headers = {
        "Content-Type": "application/json",
        "Connection": "keep-alive",
        "Content-Length": str(len(body))
    }

    for attempt in range(2):
        conn = _get_conn(timeout)
        try:
            conn.request("POST", path, body=body, headers=headers)
            resp = conn.getresponse()
            raw = resp.read().decode("utf-8")
            _put_conn(conn)
            return json.loads(raw)
        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass
            if attempt == 1:
                return {"ok": False, "description": str(e)}

async def api_call(method: str, payload: dict = None, timeout: int = 15):
    if payload is None:
        payload = {}
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(EXECUTOR, _fast_api_call, method, payload, timeout)

def fire_and_forget(coro):
    asyncio.create_task(coro)


# ================= BUTTON & MESSAGE HELPERS =================
def btn(text: str, callback_data: str = None, url: str = None, style: str = None):
    b = {"text": text}
    if callback_data:
        b["callback_data"] = callback_data
    if url:
        b["url"] = url
    if style:
        b["style"] = style
    return b

async def send_msg(chat_id, text, buttons=None, disable_preview=True):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": disable_preview
    }
    if buttons:
        payload["reply_markup"] = {"inline_keyboard": buttons}
    return await api_call("sendMessage", payload)

async def edit_msg(chat_id, message_id, text, buttons=None, disable_preview=True):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": disable_preview
    }
    if buttons:
        payload["reply_markup"] = {"inline_keyboard": buttons}
    return await api_call("editMessageText", payload)

async def edit_markup(chat_id, message_id, buttons):
    return await api_call("editMessageReplyMarkup", {
        "chat_id": chat_id,
        "message_id": message_id,
        "reply_markup": {"inline_keyboard": buttons}
    })

async def del_msg(chat_id, message_id):
    return await api_call("deleteMessage", {
        "chat_id": chat_id,
        "message_id": message_id
    })

async def answer_cb(cb_id, text="", alert=False):
    return await api_call("answerCallbackQuery", {
        "callback_query_id": cb_id,
        "text": text,
        "show_alert": alert
    })


# ================= HELPER FUNCTIONS =================
def encode_string(string: str) -> str:
    return base64.urlsafe_b64encode(string.encode("utf-8")).decode("utf-8").strip("=")

def decode_string(b64_str: str) -> str:
    padding = len(b64_str) % 4
    if padding:
        b64_str += "=" * (4 - padding)
    return base64.urlsafe_b64decode(b64_str.encode("utf-8")).decode("utf-8")

def register_user(user_id: int):
    uid = str(user_id)
    changed = False
    if user_id not in db["users"]:
        db["users"].append(user_id)
        changed = True
    if uid not in db["user_stats"]:
        db["user_stats"][uid] = {"files": 0, "links": 0}
        changed = True
    if changed:
        save_db()

async def _check_single_channel(idx, ch, user_id):
    res = await api_call("getChatMember", {"chat_id": ch["id"], "user_id": user_id})
    if res.get("ok"):
        status = res["result"].get("status", "")
        if status in ["left", "kicked"]:
            return (idx, ch["link"])
        return None
    return (idx, ch["link"])

async def check_fsub(user_id: int, force_refresh: bool = False):
    if not db["fsub_channels"]:
        return []

    # Smart Cache: agar user abhi verify hua hai toh 90 sec tak bar-bar API call nahi karega
    now = time.time()
    if not force_refresh and fsub_cache.get(user_id, 0) > now:
        return []

    tasks = [
        _check_single_channel(idx, ch, user_id)
        for idx, ch in enumerate(db["fsub_channels"], start=1)
    ]
    results = await asyncio.gather(*tasks)
    not_joined = [r for r in results if r is not None]

    if not not_joined:
        fsub_cache[user_id] = now + 90
    else:
        fsub_cache.pop(user_id, None)

    return not_joined

async def auto_delete_task(chat_id, msg_ids, delay):
    await asyncio.sleep(delay)
    await api_call("deleteMessages", {"chat_id": chat_id, "message_ids": msg_ids})


# ================= START & HELP UI =================
def get_start_text(user):
    uid = str(user["id"])
    u_stats = db["user_stats"].get(uid, {"files": 0, "links": 0})
    first_name = user.get("first_name", "User").replace("<", "").replace(">", "")
    mention = f"<a href='tg://user?id={user['id']}'>{first_name}</a>"

    return (
        "👽 <b>𝐋𝐞𝐨𝐧 𝐅𝐢𝐥𝐞 𝐒𝐭𝐨𝐫𝐞 𝐯𝟐</b> 👽\n\n"
        f"👋 <b>𝐍𝐚𝐦𝐚𝐬𝐭𝐞, {mention}!</b>\n\n"
        "<b>Main ek advanced File Store bot hoon.\n"
        "Photo, Video, Audio aur kisi bhi File ko store karke instant shareable link bana sakta hoon.</b>\n\n"
        f"📦 <b>𝐒𝐭𝐨𝐫𝐞𝐝 𝐅𝐢𝐥𝐞𝐬 :</b> <code>{u_stats['files']}</code>\n"
        f"🔗 <b>𝐌𝐲 𝐋𝐢𝐧𝐤𝐬 :</b> <code>{u_stats['links']}</code>\n\n"
        "✨ <b>𝐅𝐞𝐚𝐭𝐮𝐫𝐞𝐬 :</b>\n"
        "  📤 <b>ꜰɪʟᴇ ᴛᴏ ʟɪɴᴋ</b>\n"
        "  📦 <b>ᴍᴜʟᴛɪ ꜰɪʟᴇ ʙᴀᴛᴄʜ</b>\n"
        "  🔒 <b>ᴘʀᴏᴛᴇᴄᴛᴇᴅ sʜᴀʀᴇ</b>\n"
        "  ⚡ <b>ꜰᴀsᴛ ᴄʟᴏᴜᴅ sᴛᴏʀᴇ</b>\n\n"
        "👇 <b>Neeche buttons se select karo</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"👑 <b>𝐎𝐰𝐧𝐞𝐫 : @{OWNER_USERNAME}</b>\n"
        f"🤖 <b>𝐁𝐨𝐭 : @{BOT_USERNAME}</b>"
    )

def get_start_buttons():
    return [
        [
            btn("📤 UPLOAD FILE", callback_data="start_upload", style="success"),
            btn("📦 MULTI FILES", callback_data="start_batch")
        ],
        [
            btn("📊 MY STATS", callback_data="my_stats", style="success"),
            btn("📖 HELP", callback_data="help_cb")
        ],
        [
            btn("👑 OWNER", url=f"https://t.me/{OWNER_USERNAME}"),
            btn("❌ CLOSE", callback_data="close_msg", style="danger")
        ]
    ]

def get_help_text():
    return (
        "📖 <b>𝐇𝐄𝐋𝐏 & 𝐂𝐎𝐌𝐌𝐀𝐍𝐃𝐒 𝐆𝐔𝐈𝐃𝐄</b> 📖\n\n"
        "<blockquote><b>💡 BINA COMMAND KE KAISE USE KAREIN:\n"
        "1️⃣ Sidha koi bhi Photo, Video ya File bot ko bhejo (ya UPLOAD FILE button dabao).\n"
        "2️⃣ Ek sath kai files bhejni hain toh lagatar bhejte jao.\n"
        "3️⃣ Niche 🟢 GENERATE LINK button dabao — aapka link turant mil jayega!</b></blockquote>\n\n"
        "⌨️ <b>𝐀𝐥𝐥 𝐂𝐨𝐦𝐦𝐚𝐧𝐝𝐬 :</b>\n"
        "• <code>/start</code> — <b>Main Menu kholne ke liye</b>\n"
        "• <code>/genlink</code> — <b>Single ya Multiple file link banane ke liye</b>\n"
        "• <code>/batch</code> — <b>Ek sath dher saari files store karne ke liye</b>\n"
        "• <code>/help</code> — <b>Bot ki jankari aur help dekhne ke liye</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"👑 <b>𝐎𝐰𝐧𝐞𝐫 : @{OWNER_USERNAME}</b>"
    )


# ================= ADMIN PANEL UI (SECRET /ADMIN) =================
def get_admin_text():
    status = "🟢 <b>ONLINE</b>" if db["bot_active"] else "🔴 <b>OFFLINE (Maintenance)</b>"
    protect = "🟢 <b>ON (No Forward)</b>" if db["protect_content"] else "🔴 <b>OFF</b>"
    autodel = "🔴 <b>OFF</b>" if db["auto_delete"] == 0 else f"🟢 <b>{db['auto_delete'] // 60} Min</b>"

    return (
        "👑 <b>𝐋𝐄𝐎𝐍 𝐀𝐃𝐌𝐈𝐍 𝐂𝐎𝐍𝐓𝐑𝐎𝐋 𝐏𝐀𝐍𝐄𝐋</b> 👑\n\n"
        f"<blockquote><b>🤖 Bot Status :</b> {status}\n"
        f"<b>🔒 File Protection :</b> {protect}\n"
        f"<b>⏳ Auto-Delete :</b> {autodel}\n"
        f"<b>👥 Total Users :</b> <code>{len(db['users'])}</code>\n"
        f"<b>📢 FSub Channels :</b> <code>{len(db['fsub_channels'])}</code>\n"
        f"<b>🔗 Total Links :</b> <code>{db['total_links']}</code>\n"
        f"<b>🚫 Banned Users :</b> <code>{len(db['banned_users'])}</code></blockquote>\n\n"
        "👇 <b>Neeche buttons se bot control karo :</b>"
    )

def get_admin_buttons():
    bot_style = "success" if db["bot_active"] else "danger"
    bot_txt = "🟢 BOT : ON" if db["bot_active"] else "🔴 BOT : OFF"

    prot_style = "success" if db["protect_content"] else None
    prot_txt = "🔒 PROTECT : ON" if db["protect_content"] else "🔓 PROTECT : OFF"

    ad_time = db["auto_delete"]
    ad_label = "OFF" if ad_time == 0 else f"{ad_time // 60}m"

    return [
        [
            btn(bot_txt, callback_data="adm_toggle_bot", style=bot_style),
            btn(prot_txt, callback_data="adm_toggle_protect", style=prot_style)
        ],
        [
            btn("➕ ADD FSUB", callback_data="adm_add_fsub", style="success"),
            btn("➖ REMOVE FSUB", callback_data="adm_rem_fsub", style="danger")
        ],
        [
            btn(f"📋 FSUB LIST ({len(db['fsub_channels'])})", callback_data="adm_list_fsub"),
            btn(f"⏳ AUTO-DEL : {ad_label}", callback_data="adm_toggle_autodel")
        ],
        [
            btn("📢 BROADCAST", callback_data="adm_broadcast", style="success"),
            btn("📊 STATS", callback_data="adm_stats")
        ],
        [
            btn("🚫 BAN USER", callback_data="adm_ban", style="danger"),
            btn("✅ UNBAN USER", callback_data="adm_unban", style="success")
        ],
        [
            btn("❌ CLOSE PANEL", callback_data="close_msg", style="danger")
        ]
    ]


# ================= MESSAGE HANDLER =================
async def handle_message(msg):
    chat = msg.get("chat", {})
    if chat.get("type") != "private":
        return

    user = msg.get("from", {})
    user_id = user.get("id")
    if not user_id:
        return

    register_user(user_id)
    text = msg.get("text", "")

    # 1. SECRET /ADMIN COMMAND
    if text.startswith("/admin"):
        if user_id != OWNER_ID:
            return
        admin_state.pop(OWNER_ID, None)
        return await send_msg(user_id, get_admin_text(), get_admin_buttons())

    # 2. /HELP COMMAND
    if text.startswith("/help"):
        buttons = [
            [
                btn("📤 UPLOAD FILE", callback_data="start_upload", style="success"),
                btn("🏠 HOME", callback_data="back_start")
            ],
            [btn("❌ CLOSE", callback_data="close_msg", style="danger")]
        ]
        return await send_msg(user_id, get_help_text(), buttons)

    # 3. /START COMMAND
    if text.startswith("/start"):
        if user_id in db["banned_users"] and user_id != OWNER_ID:
            return await send_msg(user_id, "<blockquote><b>🚫 You are BANNED from using this bot!</b></blockquote>")

        if not db["bot_active"] and user_id != OWNER_ID:
            return await send_msg(
                user_id,
                "<b>⚠️ BOT UNDER MAINTENANCE!</b>\n\n"
                "<blockquote><b>🔴 Bot abhi maintenance ke liye OFF hai. Kripya thodi der baad try karein!</b></blockquote>"
            )

        parts = text.split(maxsplit=1)
        payload_arg = parts[1].strip() if len(parts) > 1 else ""

        # Force Subscribe Check
        not_joined = await check_fsub(user_id, force_refresh=True)
        if not_joined:
            buttons = []
            row = []
            for idx, link in not_joined:
                row.append(btn(f"📢 Join Channel {idx}", url=link, style="success"))
                if len(row) == 2:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)

            retry_url = f"https://t.me/{BOT_USERNAME}?start={payload_arg}" if payload_arg else f"https://t.me/{BOT_USERNAME}?start=ok"
            buttons.append([btn("🔄 VERIFY / TRY AGAIN", url=retry_url, style="danger")])

            first_name = user.get("first_name", "User").replace("<", "").replace(">", "")
            return await send_msg(
                user_id,
                f"👋 <b>𝐇𝐞𝐲 <a href='tg://user?id={user_id}'>{first_name}</a>!</b>\n\n"
                "<blockquote><b>⚠️ Bot use karne ke liye pehle niche diye gaye sabhi Update Channels join karein!</b></blockquote>",
                buttons
            )

        # Normal /start
        if not payload_arg or payload_arg == "ok":
            return await send_msg(user_id, get_start_text(user), get_start_buttons())

        # Super-Fast File Delivery
        try:
            decoded = decode_string(payload_arg)
            if decoded.startswith("batch_"):
                msg_ids = db["batches"].get(decoded, [])
            else:
                msg_ids = [int(x) for x in decoded.split("-")]

            if not msg_ids:
                return await send_msg(user_id, "<blockquote><b>❌ Invalid or Expired File Link!</b></blockquote>")

            sent_ids = []
            # Single API Call Batch Copy (Up to 100 files in 0.1s!)
            if len(msg_ids) > 1:
                batch_res = await api_call("copyMessages", {
                    "chat_id": user_id,
                    "from_chat_id": DB_CHANNEL,
                    "message_ids": sorted(msg_ids),
                    "protect_content": db["protect_content"]
                })
                if batch_res.get("ok"):
                    sent_ids = [m["message_id"] for m in batch_res["result"]]

            # Fallback or Single File
            if not sent_ids:
                for m_id in msg_ids:
                    cp = await api_call("copyMessage", {
                        "chat_id": user_id,
                        "from_chat_id": DB_CHANNEL,
                        "message_id": m_id,
                        "protect_content": db["protect_content"]
                    })
                    if cp.get("ok"):
                        sent_ids.append(cp["result"]["message_id"])

            if db["auto_delete"] > 0 and sent_ids:
                mins = db["auto_delete"] // 60
                warn = await send_msg(
                    user_id,
                    f"<blockquote><b>⏳ AUTO-DELETE NOTICE:\n"
                    f"Ye files {mins} minute me apne aap delete ho jayengi! Jaldi save kar lein.</b></blockquote>"
                )
                if warn.get("ok"):
                    sent_ids.append(warn["result"]["message_id"])
                fire_and_forget(auto_delete_task(user_id, sent_ids, db["auto_delete"]))

        except Exception:
            await send_msg(user_id, "<blockquote><b>❌ Invalid or Expired File Link!</b></blockquote>")
        return

    # 4. /GENLINK OR /BATCH COMMAND
    if text.startswith(("/genlink", "/batch", "/universal_link")):
        user_sessions[user_id] = {"active": True, "paused": False, "messages": [], "status_mid": None}
        return await send_msg(
            user_id,
            "📤 <b>𝐅𝐈𝐋𝐄 𝐔𝐏𝐋𝐎𝐀𝐃 𝐌𝐎𝐃𝐄</b>\n\n"
            "<blockquote><b>Ab aap jo bhi Photo, Video, Audio ya File bhejna chahte hain, yahan bhejein!</b></blockquote>",
            [[btn("❌ CANCEL", callback_data="cancel_gen", style="danger")]]
        )

    # 5. ADMIN INPUT STATES
    if user_id == OWNER_ID and user_id in admin_state:
        state = admin_state.pop(user_id)

        if state == "WAIT_FSUB":
            fwd_chat = msg.get("forward_from_chat") or msg.get("forward_origin", {}).get("chat")
            if fwd_chat:
                target_chat = fwd_chat.get("id")
            else:
                raw = text.strip()
                if "t.me/" in raw:
                    raw = "@" + raw.split("t.me/")[-1].split("/")[0]
                target_chat = int(raw) if raw.lstrip("-").isdigit() else raw

            chat_res = await api_call("getChat", {"chat_id": target_chat})
            if not chat_res.get("ok"):
                return await send_msg(
                    user_id,
                    f"<b>❌ Channel Add Nahi Hua!</b>\n\n"
                    f"<blockquote><b>Pehle @{BOT_USERNAME} ko us channel me ADMIN banayein!</b>\n"
                    f"<code>{chat_res.get('description')}</code></blockquote>",
                    [[btn("🔙 BACK TO ADMIN", callback_data="adm_home", style="success")]]
                )

            c_info = chat_res["result"]
            c_id = c_info["id"]
            c_title = c_info.get("title", "Update Channel")

            if c_info.get("username"):
                invite_link = f"https://t.me/{c_info['username']}"
            elif c_info.get("invite_link"):
                invite_link = c_info["invite_link"]
            else:
                exp = await api_call("exportChatInviteLink", {"chat_id": c_id})
                invite_link = exp.get("result", f"https://t.me/{BOT_USERNAME}")

            for existing in db["fsub_channels"]:
                if existing["id"] == c_id:
                    return await send_msg(
                        user_id,
                        "<blockquote><b>⚠️ Ye channel pehle se Force Subscribe me juda hua hai!</b></blockquote>",
                        [[btn("🔙 BACK TO ADMIN", callback_data="adm_home", style="success")]]
                    )

            db["fsub_channels"].append({"id": c_id, "title": c_title, "link": invite_link})
            fsub_cache.clear()
            save_db()

            return await send_msg(
                user_id,
                f"<b>✅ Force Subscribe Channel Added!</b>\n\n"
                f"<blockquote><b>📢 Name :</b> {c_title}\n"
                f"<b>🆔 ID :</b> <code>{c_id}</code>\n"
                f"<b>🔗 Link :</b> {invite_link}</blockquote>",
                [[btn("🔙 BACK TO ADMIN", callback_data="adm_home", style="success")]]
            )

        elif state == "WAIT_BROADCAST":
            users_list = db["users"]
            st_r = await send_msg(user_id, f"<b>📢 Broadcasting to {len(users_list)} users...</b>")
            st_mid = st_r.get("result", {}).get("message_id")
            sent, failed = 0, 0
            for uid in users_list:
                cp = await api_call("copyMessage", {
                    "chat_id": uid,
                    "from_chat_id": user_id,
                    "message_id": msg["message_id"]
                })
                if cp.get("ok"):
                    sent += 1
                else:
                    failed += 1

            return await edit_msg(
                user_id, st_mid,
                f"<b>✅ Broadcast Completed!</b>\n\n"
                f"<blockquote><b>👥 Total Users :</b> <code>{len(users_list)}</code>\n"
                f"<b>🟢 Sent :</b> <code>{sent}</code>\n"
                f"<b>🔴 Failed :</b> <code>{failed}</code></blockquote>",
                [[btn("🔙 BACK TO ADMIN", callback_data="adm_home", style="success")]]
            )

        elif state == "WAIT_BAN":
            try:
                tid = int(text.strip())
                if tid not in db["banned_users"]:
                    db["banned_users"].append(tid)
                    save_db()
                return await send_msg(
                    user_id,
                    f"<blockquote><b>🚫 User <code>{tid}</code> ko BAN kar diya gaya hai!</b></blockquote>",
                    [[btn("🔙 BACK TO ADMIN", callback_data="adm_home", style="success")]]
                )
            except Exception:
                return await send_msg(user_id, "<b>❌ Sirf numeric User ID bhejein!</b>")

        elif state == "WAIT_UNBAN":
            try:
                tid = int(text.strip())
                if tid in db["banned_users"]:
                    db["banned_users"].remove(tid)
                    save_db()
                return await send_msg(
                    user_id,
                    f"<blockquote><b>✅ User <code>{tid}</code> ko UNBAN kar diya gaya hai!</b></blockquote>",
                    [[btn("🔙 BACK TO ADMIN", callback_data="adm_home", style="success")]]
                )
            except Exception:
                return await send_msg(user_id, "<b>❌ Sirf numeric User ID bhejein!</b>")

    # 6. ULTRA-FAST FILE STORAGE
    if user_id in db["banned_users"] and user_id != OWNER_ID:
        return await send_msg(user_id, "<blockquote><b>🚫 You are BANNED from using this bot!</b></blockquote>")

    if not db["bot_active"] and user_id != OWNER_ID:
        return await send_msg(user_id, "<blockquote><b>🔴 Bot abhi maintenance me hai!</b></blockquote>")

    not_joined = await check_fsub(user_id)
    if not_joined:
        return await send_msg(user_id, "<blockquote><b>⚠️ Pehle /start dabayein aur update channels join karein!</b></blockquote>")

    # Copy file to DB Channel immediately (before lock) for maximum speed!
    cp = await api_call("copyMessage", {
        "chat_id": DB_CHANNEL,
        "from_chat_id": user_id,
        "message_id": msg["message_id"]
    })

    if not cp.get("ok"):
        return await send_msg(
            user_id,
            f"<b>❌ Database Channel Error!</b>\n\n"
            f"<blockquote><b>Kripya @{BOT_USERNAME} ko apne DB Channel (<code>{DB_CHANNEL}</code>) me ADMIN banayein!</b>\n"
            f"<code>{cp.get('description')}</code></blockquote>"
        )

    new_mid = cp["result"]["message_id"]

    if user_id not in user_locks:
        user_locks[user_id] = asyncio.Lock()

    async with user_locks[user_id]:
        if user_id not in user_sessions or not user_sessions[user_id]["active"]:
            user_sessions[user_id] = {"active": True, "paused": False, "messages": [], "status_mid": None}

        session = user_sessions[user_id]
        if session["paused"]:
            return await send_msg(
                user_id,
                "<blockquote><b>⏸️ Upload abhi PAUSE hai! Niche RESUME button dabayein.</b></blockquote>"
            )

        session["messages"].append(new_mid)
        db["user_stats"][str(user_id)]["files"] += 1
        save_db()

        old_status_mid = session["status_mid"]
        if old_status_mid:
            fire_and_forget(del_msg(user_id, old_status_mid))

        count = len(session["messages"])
        buttons = [
            [btn("🟢 GENERATE LINK", callback_data="gen_final_link", style="success")],
            [
                btn("⏸️ PAUSE", callback_data="pause_gen"),
                btn("❌ CANCEL", callback_data="cancel_gen", style="danger")
            ]
        ]

        st_res = await send_msg(
            user_id,
            f"📦 <b>𝐒𝐭𝐨𝐫𝐞𝐝 𝐌𝐞𝐬𝐬𝐚𝐠𝐞𝐬 : {count}</b>\n\n"
            f"<b>Aur files jodna chahte hain? Yahan bhejte jayein!</b>\n\n"
            f"<blockquote><b>✨ Link banane ke liye niche GENERATE LINK button dabayein.</b></blockquote>",
            buttons
        )
        if st_res.get("ok"):
            session["status_mid"] = st_res["result"]["message_id"]


# ================= CALLBACK QUERY HANDLER (INSTANT RESPONSE) =================
async def handle_callback(cb):
    cb_id = cb["id"]
    user = cb.get("from", {})
    user_id = user.get("id")
    data = cb.get("data", "")
    msg = cb.get("message", {})
    chat_id = msg.get("chat", {}).get("id")
    msg_id = msg.get("message_id")

    register_user(user_id)

    # --- CLOSE BUTTON ---
    if data == "close_msg":
        fire_and_forget(answer_cb(cb_id))
        return await del_msg(chat_id, msg_id)

    # --- MAIN MENU / BACK ---
    if data == "back_start":
        fire_and_forget(answer_cb(cb_id))
        return await edit_msg(chat_id, msg_id, get_start_text(user), get_start_buttons())

    # --- HELP MENU ---
    if data == "help_cb":
        fire_and_forget(answer_cb(cb_id))
        buttons = [
            [
                btn("📤 UPLOAD FILE", callback_data="start_upload", style="success"),
                btn("🏠 HOME", callback_data="back_start")
            ],
            [btn("❌ CLOSE", callback_data="close_msg", style="danger")]
        ]
        return await edit_msg(chat_id, msg_id, get_help_text(), buttons)

    # --- MY STATS ---
    if data == "my_stats":
        fire_and_forget(answer_cb(cb_id))
        st = db["user_stats"].get(str(user_id), {"files": 0, "links": 0})
        buttons = [
            [
                btn("📤 UPLOAD FILE", callback_data="start_upload", style="success"),
                btn("🏠 HOME", callback_data="back_start")
            ],
            [btn("❌ CLOSE", callback_data="close_msg", style="danger")]
        ]
        return await edit_msg(
            chat_id, msg_id,
            "📊 <b>𝐌𝐘 𝐀𝐂𝐂𝐎𝐔𝐍𝐓 𝐒𝐓𝐀𝐓𝐒</b> 📊\n\n"
            f"<blockquote><b>👤 Name :</b> {user.get('first_name', 'User')}\n"
            f"<b>🆔 User ID :</b> <code>{user_id}</code>\n"
            f"<b>📦 Total Files Uploaded :</b> <code>{st['files']}</code>\n"
            f"<b>🔗 Total Links Created :</b> <code>{st['links']}</code></blockquote>",
            buttons
        )

    # --- START UPLOAD / BATCH ---
    if data in ["start_upload", "start_batch"]:
        fire_and_forget(answer_cb(cb_id, "📤 Ab apni file(s) bhejein!"))
        user_sessions[user_id] = {"active": True, "paused": False, "messages": [], "status_mid": None}
        buttons = [
            [
                btn("🏠 HOME", callback_data="back_start"),
                btn("❌ CANCEL", callback_data="cancel_gen", style="danger")
            ]
        ]
        return await edit_msg(
            chat_id, msg_id,
            "📤 <b>𝐒𝐄𝐍𝐃 𝐘𝐎𝐔𝐑 𝐅𝐈𝐋𝐄𝐒 𝐍𝐎𝐖</b>\n\n"
            "<blockquote><b>✨ Ab aap ek ya ek se zyada Photo, Video, Audio ya Document yahan bhejein.\n\n"
            "File bhejne ke baad GENERATE LINK button aa jayega!</b></blockquote>",
            buttons
        )

    # --- CANCEL / PAUSE / RESUME / GENERATE LINK ---
    if data == "cancel_gen":
        user_sessions.pop(user_id, None)
        fire_and_forget(answer_cb(cb_id, "Cancelled!"))
        return await edit_msg(
            chat_id, msg_id,
            "<blockquote><b>🔴 Upload Process Cancelled!</b></blockquote>",
            [[btn("🏠 HOME", callback_data="back_start", style="success"), btn("❌ CLOSE", callback_data="close_msg", style="danger")]]
        )

    if data == "pause_gen":
        if user_id in user_sessions:
            user_sessions[user_id]["paused"] = True
            buttons = [
                [btn("🟢 GENERATE LINK", callback_data="gen_final_link", style="success")],
                [
                    btn("▶️ RESUME", callback_data="resume_gen", style="success"),
                    btn("❌ CANCEL", callback_data="cancel_gen", style="danger")
                ]
            ]
            fire_and_forget(answer_cb(cb_id, "⏸️ Paused!"))
            return await edit_markup(chat_id, msg_id, buttons)

    if data == "resume_gen":
        if user_id in user_sessions:
            user_sessions[user_id]["paused"] = False
            buttons = [
                [btn("🟢 GENERATE LINK", callback_data="gen_final_link", style="success")],
                [
                    btn("⏸️ PAUSE", callback_data="pause_gen"),
                    btn("❌ CANCEL", callback_data="cancel_gen", style="danger")
                ]
            ]
            fire_and_forget(answer_cb(cb_id, "▶️ Resumed!"))
            return await edit_markup(chat_id, msg_id, buttons)

    if data == "gen_final_link":
        if user_id not in user_sessions or not user_sessions[user_id]["messages"]:
            return await answer_cb(cb_id, "❌ Pehle kam se kam 1 file bhejein!", alert=True)

        fire_and_forget(answer_cb(cb_id, "⚡ Link Generated!"))
        msg_ids = user_sessions[user_id]["messages"]
        raw_str = "-".join(str(x) for x in msg_ids)
        encoded = encode_string(raw_str)

        if len(encoded) > 60:
            batch_key = f"batch_{db['total_links'] + 1}_{msg_ids[0]}"
            db["batches"][batch_key] = msg_ids
            encoded = encode_string(batch_key)

        final_link = f"https://t.me/{BOT_USERNAME}?start={encoded}"
        share_link = f"https://t.me/share/url?url={final_link}"

        user_sessions.pop(user_id, None)
        db["total_links"] += 1
        db["user_stats"][str(user_id)]["links"] += 1
        save_db()

        buttons = [
            [btn("➦ SHARE URL", url=share_link, style="success")],
            [
                btn("🏠 HOME", callback_data="back_start"),
                btn("❌ CLOSE", callback_data="close_msg", style="danger")
            ]
        ]

        return await edit_msg(
            chat_id, msg_id,
            f"✅ <b>𝐋𝐈𝐍𝐊 𝐆𝐄𝐍𝐄𝐑𝐀𝐓𝐄𝐃 𝐒𝐔𝐂𝐂𝐄𝐒𝐒𝐅𝐔𝐋𝐋𝐘!</b>\n\n"
            f"<blockquote><b>🔗 Here Is Your Link:</b>\n\n"
            f"{final_link}</blockquote>",
            buttons
        )

    # ---------------- ADMIN CALLBACKS ----------------
    if data.startswith("adm_"):
        if user_id != OWNER_ID:
            return await answer_cb(cb_id, "❌ Access Denied!", alert=True)

        admin_state.pop(OWNER_ID, None)

        if data == "adm_home":
            fire_and_forget(answer_cb(cb_id))
            return await edit_msg(chat_id, msg_id, get_admin_text(), get_admin_buttons())

        elif data == "adm_toggle_bot":
            db["bot_active"] = not db["bot_active"]
            save_db()
            st = "ONLINE 🟢" if db["bot_active"] else "OFFLINE 🔴"
            fire_and_forget(answer_cb(cb_id, f"Bot Status: {st}", alert=True))
            return await edit_msg(chat_id, msg_id, get_admin_text(), get_admin_buttons())

        elif data == "adm_toggle_protect":
            db["protect_content"] = not db["protect_content"]
            save_db()
            st = "ON 🔒" if db["protect_content"] else "OFF 🔓"
            fire_and_forget(answer_cb(cb_id, f"File Protection: {st}", alert=True))
            return await edit_msg(chat_id, msg_id, get_admin_text(), get_admin_buttons())

        elif data == "adm_toggle_autodel":
            options = [0, 300, 600, 1800]
            curr = db["auto_delete"]
            nxt = (options.index(curr) + 1) % len(options) if curr in options else 0
            db["auto_delete"] = options[nxt]
            save_db()
            lbl = "OFF" if db["auto_delete"] == 0 else f"{db['auto_delete'] // 60} Minutes"
            fire_and_forget(answer_cb(cb_id, f"⏳ Auto-Delete: {lbl}", alert=True))
            return await edit_msg(chat_id, msg_id, get_admin_text(), get_admin_buttons())

        elif data == "adm_add_fsub":
            admin_state[OWNER_ID] = "WAIT_FSUB"
            fire_and_forget(answer_cb(cb_id))
            return await edit_msg(
                chat_id, msg_id,
                "➕ <b>𝐀𝐃𝐃 𝐅𝐎𝐑𝐂𝐄 𝐒𝐔𝐁 𝐂𝐇𝐀𝐍𝐍𝐄𝐋</b>\n\n"
                f"<blockquote><b>1️⃣ Pehle @{BOT_USERNAME} ko apne channel me ADMIN banayein.\n"
                "2️⃣ Ab channel ka <code>@username</code>, <code>-100...</code> ID bhejein ya us channel se koi message yahan Forward karein!</b></blockquote>",
                [[btn("🔙 BACK", callback_data="adm_home", style="danger")]]
            )

        elif data == "adm_rem_fsub":
            if not db["fsub_channels"]:
                return await answer_cb(cb_id, "⚠️ Koi FSub channel juda nahi hai!", alert=True)
            fire_and_forget(answer_cb(cb_id))
            buttons = []
            for idx, ch in enumerate(db["fsub_channels"]):
                buttons.append([btn(f"🗑️ Remove: {ch['title']}", callback_data=f"adm_delch_{idx}", style="danger")])
            buttons.append([btn("🔙 BACK", callback_data="adm_home", style="success")])
            return await edit_msg(
                chat_id, msg_id,
                "➖ <b>𝐑𝐄𝐌𝐎𝐕𝐄 𝐅𝐒𝐔𝐁 𝐂𝐇𝐀𝐍𝐍𝐄𝐋</b>\n\n"
                "<blockquote><b>Jis channel ko hatana hai us par click karein:</b></blockquote>",
                buttons
            )

        elif data.startswith("adm_delch_"):
            idx = int(data.split("_")[-1])
            if 0 <= idx < len(db["fsub_channels"]):
                rem = db["fsub_channels"].pop(idx)
                fsub_cache.clear()
                save_db()
                fire_and_forget(answer_cb(cb_id, f"Removed: {rem['title']}", alert=True))
            return await edit_msg(chat_id, msg_id, get_admin_text(), get_admin_buttons())

        elif data == "adm_list_fsub":
            if not db["fsub_channels"]:
                return await answer_cb(cb_id, "📋 Koi Force Sub channel nahi hai!", alert=True)
            fire_and_forget(answer_cb(cb_id))
            txt = "📋 <b>𝐀𝐂𝐓𝐈𝐕𝐄 𝐅𝐒𝐔𝐁 𝐂𝐇𝐀𝐍𝐍𝐄𝐋𝐒 :</b>\n\n"
            for i, ch in enumerate(db["fsub_channels"], start=1):
                txt += f"<b>{i}. {ch['title']}</b>\n   🆔 <code>{ch['id']}</code>\n   🔗 {ch['link']}\n\n"
            return await edit_msg(
                chat_id, msg_id,
                f"<blockquote>{txt}</blockquote>",
                [[btn("🔙 BACK", callback_data="adm_home", style="success")]]
            )

        elif data == "adm_broadcast":
            admin_state[OWNER_ID] = "WAIT_BROADCAST"
            fire_and_forget(answer_cb(cb_id))
            return await edit_msg(
                chat_id, msg_id,
                "📢 <b>𝐁𝐑𝐎𝐀𝐃𝐂𝐀𝐒𝐓 𝐌𝐄𝐒𝐒𝐀𝐆𝐄</b>\n\n"
                f"<blockquote><b>Koi bhi Text, Photo, Video ya File bhejein — wo sabhi <code>{len(db['users'])}</code> users ko chala jayega!</b></blockquote>",
                [[btn("🔙 CANCEL", callback_data="adm_home", style="danger")]]
            )

        elif data == "adm_stats":
            return await answer_cb(
                cb_id,
                f"📊 Users: {len(db['users'])} | Links: {db['total_links']} | FSub: {len(db['fsub_channels'])}",
                alert=True
            )

        elif data == "adm_ban":
            admin_state[OWNER_ID] = "WAIT_BAN"
            fire_and_forget(answer_cb(cb_id))
            return await edit_msg(
                chat_id, msg_id,
                "🚫 <b>𝐁𝐀𝐍 𝐔𝐒𝐄𝐑</b>\n\n"
                "<blockquote><b>Jis user ko Ban karna hai uski Telegram User ID bhejein:</b></blockquote>",
                [[btn("🔙 CANCEL", callback_data="adm_home", style="danger")]]
            )

        elif data == "adm_unban":
            admin_state[OWNER_ID] = "WAIT_UNBAN"
            fire_and_forget(answer_cb(cb_id))
            b_list = ", ".join(str(x) for x in db["banned_users"]) if db["banned_users"] else "None"
            return await edit_msg(
                chat_id, msg_id,
                "✅ <b>𝐔𝐍𝐁𝐀𝐍 𝐔𝐒𝐄𝐑</b>\n\n"
                f"<blockquote><b>Banned IDs:</b> <code>{b_list}</code>\n\n"
                "<b>Jis user ko Unban karna hai uski ID bhejein:</b></blockquote>",
                [[btn("🔙 CANCEL", callback_data="adm_home", style="danger")]]
            )


# ================= BOT POLLING LOOP =================
async def setup_commands():
    await api_call("setMyCommands", {
        "commands": [
            {"command": "start", "description": "🚀 Start File Store Bot"},
            {"command": "help", "description": "📖 How to use & Commands"}
        ]
    })
    await api_call("setMyCommands", {
        "commands": [
            {"command": "start", "description": "🚀 Start File Store Bot"},
            {"command": "help", "description": "📖 How to use & Commands"},
            {"command": "admin", "description": "👑 Secret Admin Control Panel"}
        ],
        "scope": {"type": "chat", "chat_id": OWNER_ID}
    })

async def warm_up_pool():
    # Pre-warm 3 TLS connections so the very first click is instant
    conns = await asyncio.gather(*[
        asyncio.to_thread(_get_conn, 15) for _ in range(3)
    ])
    for c in conns:
        _put_conn(c)

async def main():
    await warm_up_pool()
    await api_call("deleteWebhook", {"drop_pending_updates": False})
    fire_and_forget(setup_commands())
    print("⚡ @Leonfilebot TURBO MODE is Live & Super Fast!")

    offset = 0
    while True:
        try:
            res = await api_call("getUpdates", {
                "offset": offset,
                "timeout": 20,
                "allowed_updates": ["message", "callback_query"]
            }, timeout=25)

            if res and res.get("ok"):
                for upd in res.get("result", []):
                    offset = upd["update_id"] + 1
                    if "message" in upd:
                        fire_and_forget(handle_message(upd["message"]))
                    elif "callback_query" in upd:
                        fire_and_forget(handle_callback(upd["callback_query"]))
        except Exception:
            await asyncio.sleep(0.5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        _sync_save_db()
        print("\n🔴 Bot Stopped.")
