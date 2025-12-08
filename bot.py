import os
import asyncio
import logging
import zipfile
import shutil
import re
import aiosqlite
import time
from pyrogram import Client, filters, compose
from pyrogram.errors import FloodWait, PeerIdInvalid, ChannelInvalid, UserNotParticipant
from dotenv import load_dotenv

# --- SAFE IMPORTS ---
try:
    import uvloop
    uvloop.install()
except ImportError:
    print("⚠️ UVLOOP not found. Running in standard mode.")

# --- CONFIGURATION ---
load_dotenv()
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
MAIN_BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# Parse Worker Tokens safely
WORKER_TOKENS_STR = os.getenv("WORKER_TOKENS", "")
WORKER_TOKENS = [t.strip() for t in WORKER_TOKENS_STR.split(",") if t.strip()]

# SAFETY CHECK: Prevent Token Conflict
if MAIN_BOT_TOKEN in WORKER_TOKENS:
    print("❌ FATAL ERROR: Main Bot Token found in WORKER_TOKENS!")
    print("   Remove the Main Bot Token from the worker list in .env")
    exit(1)

WORK_DIR = os.getenv("WORK_DIR", "downloads")
DB_NAME = "zipper_state.db"
SESSION_DIR = "sessions"

# Logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DebugBot")

if not os.path.exists(SESSION_DIR):
    os.makedirs(SESSION_DIR)

# --- CLIENT FACTORY ---
def create_client(name, token):
    return Client(
        name,
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=token,
        workdir=SESSION_DIR, 
        ipv6=False,
        max_concurrent_transmissions=64,
        workers=8
    )

print("🔥 Initializing Clients...")
main_app = create_client("main_bot", MAIN_BOT_TOKEN)
worker_apps = [create_client(f"worker_{i+1}", t) for i, t in enumerate(WORKER_TOKENS)]
all_apps = [main_app] + worker_apps

print(f"✅ Swarm Loaded: 1 Manager + {len(worker_apps)} Workers")

# Global State
setup_state = {}
job_progress = {} 
active_downloads = {} 

# --- DATABASE & HELPERS ---
# (Same as before, condensed for brevity)

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                chat_id INTEGER PRIMARY KEY,
                target_channel_id INTEGER,
                last_processed_id INTEGER,
                end_msg_id INTEGER,
                max_size_bytes INTEGER,
                naming_scheme TEXT,
                part_count INTEGER
            )
        """)
        await db.commit()

async def get_job(chat_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT * FROM jobs WHERE chat_id = ?", (chat_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return {
                    "chat_id": row[0], "target": row[1], "last_id": row[2],
                    "end_id": row[3], "max_size": row[4], "naming": row[5], "part": row[6]
                }
            return None

async def save_job(chat_id, target, last_id, end_id, max_size, naming):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR REPLACE INTO jobs VALUES (?, ?, ?, ?, ?, ?, ?)",
            (chat_id, target, last_id, end_id, max_size, naming, 1)
        )
        await db.commit()

async def update_checkpoint(chat_id, last_processed_id, next_part_num):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE jobs SET last_processed_id = ?, part_count = ? WHERE chat_id = ?",
            (last_processed_id, next_part_num, chat_id)
        )
        await db.commit()

async def delete_job(chat_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM jobs WHERE chat_id = ?", (chat_id,))
        await db.commit()

# --- DEBUG HANDLER ---
@main_app.on_message(group=-1)
async def log_everything(c, m):
    """Prints every message received to console to verify connection."""
    print(f"👀 RECEIVED: {m.text} from {m.chat.id}")

# --- WORKER LOGIC ---

async def progress_callback(current, total, unique_id, chat_id):
    if chat_id in active_downloads:
        active_downloads[chat_id][unique_id] = current

async def swarm_warmup(chat_id, target_channel):
    logger.info("🔥 Warming up workers...")
    success_count = 0
    
    async def check_access(bot):
        try:
            await bot.get_chat(target_channel)
            return True
        except Exception as e:
            logger.warning(f"⚠️ Worker {bot.name} cannot access channel: {e}")
            return False

    results = await asyncio.gather(*[check_access(bot) for bot in all_apps])
    success_count = sum(results)
    
    await main_app.send_message(
        chat_id, 
        f"🛡 **Swarm Warmup:** {success_count}/{len(all_apps)} bots have access.\n"
        f"If this is low, add workers to the channel!"
    )

async def fetch_worker(client, target_chat, start_id, end_id, queue, stop_event, progress_key):
    current = start_id
    batch_size = 200 
    
    while current <= end_id and not stop_event.is_set():
        batch_end = min(current + batch_size, end_id + 1)
        ids = list(range(current, batch_end))
        
        try:
            if progress_key in job_progress:
                job_progress[progress_key]['scanned'] = current

            messages = await client.get_messages(target_chat, ids)
            for msg in messages:
                if msg and (msg.document or msg.video or msg.audio or msg.photo):
                    await queue.put(msg)
            current = batch_end
            
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception as e:
            logger.error(f"Fetch Error: {e}")
            await asyncio.sleep(2)
    await queue.put(None)

async def download_worker(client_inst, queue, chat_id, user_dir, semaphore, stop_event):
    while not stop_event.is_set():
        try:
            msg = await asyncio.wait_for(queue.get(), timeout=2.0)
        except asyncio.TimeoutError:
            continue
        
        if msg is None:
            await queue.put(None)
            break

        unique_id = f"{client_inst.name}_{msg.id}"
        async with semaphore:
            try:
                active_downloads[chat_id][unique_id] = 0
                f_path = await client_inst.download_media(
                    msg, file_name=os.path.join(user_dir, ""),
                    progress=progress_callback, progress_args=(unique_id, chat_id)
                )
                if f_path:
                    f_size = os.path.getsize(f_path)
                    if chat_id in job_progress:
                        job_progress[chat_id]['finished_bytes'] += f_size
                        job_progress[chat_id]['current_chunk_size'] += f_size
                        job_progress[chat_id]['files'].append(f_path)
                        if msg.id > job_progress[chat_id]['highest_id']:
                            job_progress[chat_id]['highest_id'] = msg.id
                    if unique_id in active_downloads[chat_id]: del active_downloads[chat_id][unique_id]
            except Exception as e:
                logger.error(f"{client_inst.name} DL Fail: {e}")
                if unique_id in active_downloads[chat_id]: del active_downloads[chat_id][unique_id]
        queue.task_done()

async def status_monitor(client, chat_id, stop_event):
    msg = await client.send_message(chat_id, "⏳ **Swarm Starting...**")
    last_text = ""
    while not stop_event.is_set():
        try:
            data = job_progress.get(chat_id)
            if not data: break
            
            live_bytes = sum(active_downloads.get(chat_id, {}).values())
            total = data['finished_bytes'] + live_bytes
            
            # Simple UI update
            text = (f"🤖 **Swarm Active**\n💾 Chunk: `{total/1024/1024:.2f} MB`\n"
                    f"🔎 Scan: `{data['scanned']}` / `{data['total']}`\n"
                    f"📦 Part: `{data['part']}`")
            if text != last_text:
                await msg.edit_text(text)
                last_text = text
            await asyncio.sleep(4)
        except: pass
    await msg.delete()

async def manager_logic(chat_id):
    job = await get_job(chat_id)
    if not job: return

    await swarm_warmup(chat_id, job['target'])

    user_dir = os.path.join(WORK_DIR, str(chat_id))
    if os.path.exists(user_dir): shutil.rmtree(user_dir)
    os.makedirs(user_dir, exist_ok=True)
    
    queue = asyncio.Queue(maxsize=1000)
    stop_event = asyncio.Event()
    
    job_progress[chat_id] = {
        'scanned': job['last_id'], 'total': job['end_id'], 
        'current_chunk_size': 0, 'part': job['part'],
        'finished_bytes': 0, 'highest_id': job['last_id'],
        'files': [], 'queue_obj': queue
    }
    active_downloads[chat_id] = {}

    fetch_task = asyncio.create_task(
        fetch_worker(main_app, job['target'], job['last_id'], job['end_id'], queue, stop_event, chat_id)
    )

    semaphores = [asyncio.Semaphore(10) for _ in all_apps]
    for i, app_inst in enumerate(all_apps):
        asyncio.create_task(download_worker(app_inst, queue, chat_id, user_dir, semaphores[i], stop_event))

    asyncio.create_task(status_monitor(main_app, chat_id, stop_event))

    while not stop_event.is_set():
        await asyncio.sleep(2)
        is_full = job_progress[chat_id]['current_chunk_size'] >= job['max_size']
        is_finished = fetch_task.done() and queue.empty() and not active_downloads.get(chat_id)
        
        if is_full or (is_finished and job_progress[chat_id]['files']):
            files = list(job_progress[chat_id]['files'])
            job_progress[chat_id]['files'] = []
            job_progress[chat_id]['current_chunk_size'] = 0
            
            zip_name = job['naming'].format(job_progress[chat_id]['part']) + ".zip"
            zip_path = os.path.join(user_dir, zip_name)
            
            await main_app.send_message(chat_id, f"🤐 **Zipping {zip_name}...**")
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_STORED) as zipf:
                for f in files:
                    if os.path.exists(f): zipf.write(f, os.path.basename(f))
            
            end_id = job_progress[chat_id]['highest_id']
            await main_app.send_document(chat_id, zip_path, caption=f"🗂 **{zip_name}**\nID: {end_id}")
            
            job_progress[chat_id]['part'] += 1
            await update_checkpoint(chat_id, end_id, job_progress[chat_id]['part'])
            
            for f in files + [zip_path]:
                try: os.remove(f)
                except: pass

        if is_finished: break

    stop_event.set()
    await delete_job(chat_id)
    shutil.rmtree(user_dir, ignore_errors=True)
    await main_app.send_message(chat_id, "✅ **Done.**")

# --- MAIN HANDLERS ---

@main_app.on_message(filters.command("start"))
async def start(c, m):
    print(f"✅ /start received from {m.chat.id}")
    if await get_job(m.chat.id):
        await m.reply_text("🔄 **Resuming...**")
        asyncio.create_task(manager_logic(m.chat.id))
    else:
        await m.reply_text(f"🤖 **Swarm Ready.**\nWorkers: {len(worker_apps)}\nSend **Last Link** to start.")

@main_app.on_message(filters.regex(r"(?:t\.me|telegram\.me|telegram\.dog)/") & filters.private)
async def step1(c, m):
    print(f"✅ Link received: {m.text}")
    if await get_job(m.chat.id): return await m.reply_text("⚠️ Busy.")
    
    link = m.text.strip()
    target_id, end_id = None, None
    if "t.me/c/" in link:
        match = re.search(r"t\.me/c/(\d+)/(\d+)", link)
        if match: target_id, end_id = int("-100" + match.group(1)), int(match.group(2))
    else:
        match = re.search(r"t\.me/([^/]+)/(\d+)", link)
        if match: target_id, end_id = match.group(1), int(match.group(2))

    if not target_id: return await m.reply_text("❌ Bad Link.")
    try: await c.get_chat(target_id)
    except: return await m.reply_text("❌ **Main Bot** cannot access channel.")

    setup_state[m.chat.id] = {"step": "name", "target": target_id, "end_id": end_id}
    await m.reply_text("✅ **Link OK.**\nEnter Naming (e.g. `Pack-{}`) or `default`.")

@main_app.on_message(filters.text & filters.private & ~filters.regex(r"^/") & ~filters.regex(r"t\.me"))
async def step2(c, m):
    print(f"✅ Text received: {m.text}")
    state = setup_state.get(m.chat.id)
    if not state: return

    if state["step"] == "name":
        name = m.text.strip()
        if "default" in name.lower(): name = "Batch_{}"
        if "{}" not in name: name += "_{}"
        state["naming"] = name
        state["step"] = "size"
        setup_state[m.chat.id] = state
        await m.reply_text("📦 **Max Zip Size (MB)?**")

    elif state["step"] == "size":
        try: sz = int(m.text.strip()) * 1024 * 1024
        except: return await m.reply_text("❌ Number only.")
        await save_job(m.chat.id, state["target"], 1, state["end_id"], sz, state["naming"])
        del setup_state[m.chat.id]
        await m.reply_text("🚀 **Unleashing the Swarm...**")
        asyncio.create_task(manager_logic(m.chat.id))

if __name__ == "__main__":
    if not os.path.exists(WORK_DIR): os.makedirs(WORK_DIR)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_db())
    print("🔥 Swarm Starting... (Logs will appear below)")
    compose(all_apps)
