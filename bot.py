import os
import asyncio
import logging
import zipfile
import shutil
import re
import aiosqlite
import time
import uvloop
from pyrogram import Client, filters, compose
from pyrogram.errors import FloodWait, PeerIdInvalid, ChannelInvalid
from dotenv import load_dotenv

# --- SPEED BOOST ---
uvloop.install()

# --- CONFIGURATION ---
load_dotenv()
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
MAIN_BOT_TOKEN = os.getenv("BOT_TOKEN", "")
# Parse Worker Tokens
WORKER_TOKENS_STR = os.getenv("WORKER_TOKENS", "")
WORKER_TOKENS = [t.strip() for t in WORKER_TOKENS_STR.split(",") if t.strip()]

WORK_DIR = os.getenv("WORK_DIR", "downloads")
DB_NAME = "zipper_state.db"
SESSION_DIR = "sessions"

# Concurrency per Bot (Keep safe to avoid bans)
# Total Concurrency = (1 Main + N Workers) * PER_BOT_LIMIT
PER_BOT_CONCURRENCY = 10 
QUEUE_MAX_SIZE = 1000

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure Session Dir Exists (Fixes Persistence)
if not os.path.exists(SESSION_DIR):
    os.makedirs(SESSION_DIR)

# --- CLIENT FACTORY ---
def create_client(name, token):
    return Client(
        name,
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=token,
        workdir=SESSION_DIR, # Persist session here
        ipv6=False,
        max_concurrent_transmissions=64,
        workers=8
    )

# Initialize Swarm
main_app = create_client("main_bot", MAIN_BOT_TOKEN)
worker_apps = [create_client(f"worker_{i+1}", t) for i, t in enumerate(WORKER_TOKENS)]
all_apps = [main_app] + worker_apps

logger.info(f"🔥 Initialized Swarm: 1 Main + {len(worker_apps)} Workers")

# Global State
setup_state = {}
job_progress = {} 
active_downloads = {} # Global live tracking

# --- DATABASE ---

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

# --- WORKER LOGIC ---

async def progress_callback(current, total, unique_id, chat_id):
    if chat_id in active_downloads:
        active_downloads[chat_id][unique_id] = current

async def fetch_worker(client, target_chat, start_id, end_id, queue, stop_event, progress_key):
    """The 'Brain' - Runs on Main Bot to find messages."""
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
                    # We pass the message ID. Workers must fetch/download it themselves 
                    # or allow Pyrogram to resolve it.
                    await queue.put(msg)
            
            current = batch_end
            
        except FloodWait as e:
            logger.warning(f"Fetch FloodWait: {e.value}s")
            await asyncio.sleep(e.value)
        except Exception as e:
            logger.error(f"Fetch Error: {e}")
            await asyncio.sleep(2)
            
    await queue.put(None) # End signal (One per worker needed? No, handled in consumer)

async def download_worker(client_inst, queue, chat_id, user_dir, semaphore, stop_event):
    """The 'Laborer' - Runs on ALL bots."""
    
    while not stop_event.is_set():
        try:
            # Non-blocking get with timeout to allow checking stop_event
            msg = await asyncio.wait_for(queue.get(), timeout=2.0)
        except asyncio.TimeoutError:
            continue
        
        if msg is None:
            # Put it back for other workers to see the end signal
            await queue.put(None)
            break

        unique_id = f"{client_inst.name}_{msg.id}"
        
        async with semaphore:
            try:
                active_downloads[chat_id][unique_id] = 0
                
                # Check if this worker actually has access
                # Pyrogram can sometimes download using the main bot's message reference
                # provided both bots are in the channel.
                f_path = await client_inst.download_media(
                    msg,
                    file_name=os.path.join(user_dir, ""),
                    progress=progress_callback,
                    progress_args=(unique_id, chat_id)
                )
                
                if f_path:
                    f_size = os.path.getsize(f_path)
                    
                    # Update Global Stats (Thread Safe in Asyncio)
                    if chat_id in job_progress:
                        job_progress[chat_id]['finished_bytes'] += f_size
                        job_progress[chat_id]['current_chunk_size'] += f_size
                        job_progress[chat_id]['files'].append(f_path)
                        
                        if msg.id > job_progress[chat_id]['highest_id']:
                            job_progress[chat_id]['highest_id'] = msg.id

                    # Clean live tracking
                    if unique_id in active_downloads[chat_id]:
                        del active_downloads[chat_id][unique_id]

            except (PeerIdInvalid, ChannelInvalid):
                logger.error(f"Worker {client_inst.name} cannot access the channel! ignoring file.")
                # We do not put it back, we skip to avoid infinite loops.
                # User is warned at start.
            except Exception as e:
                logger.error(f"{client_inst.name} DL Fail: {e}")
                if unique_id in active_downloads[chat_id]:
                    del active_downloads[chat_id][unique_id]
        
        queue.task_done()

async def status_monitor(client, chat_id, stop_event):
    msg = await client.send_message(chat_id, "⏳ **Warming up Worker Swarm...**")
    last_text = ""
    start_time = time.time()
    last_bytes = 0
    last_check = time.time()

    while not stop_event.is_set():
        try:
            data = job_progress.get(chat_id)
            if not data: break

            # Aggregate Live + Finished
            live_bytes = sum(active_downloads.get(chat_id, {}).values())
            total_now = data['finished_bytes'] + live_bytes
            
            # Speed Calc
            now = time.time()
            if now - last_check > 3:
                speed = ((total_now - last_bytes) / (now - last_check)) / (1024 * 1024)
                last_bytes = total_now
                last_check = now
            else:
                speed = getattr(status_monitor, "last_speed", 0.0)
            status_monitor.last_speed = speed

            active_workers = len(all_apps)
            
            text = (
                f"🤖 **Swarm Active: {active_workers} Bots**\n"
                f"⚡ Speed: `{speed:.2f} MB/s` ({(speed * 8):.0f} Mbps)\n"
                f"📥 Queue: `{data['queue_obj'].qsize()}`\n"
                f"💾 Chunk: `{(data['current_chunk_size'] + live_bytes)/1024/1024:.2f} MB`\n"
                f"🔎 Scan: `{data['scanned']}` / `{data['total']}`\n"
                f"📦 Part: `{data['part']}`"
            )

            if text != last_text:
                await msg.edit_text(text)
                last_text = text
            
            await asyncio.sleep(4)
        except Exception:
            pass
    await msg.delete()

async def manager_logic(chat_id):
    """Orchestrates the whole process."""
    job = await get_job(chat_id)
    if not job: return

    user_dir = os.path.join(WORK_DIR, str(chat_id))
    if os.path.exists(user_dir): shutil.rmtree(user_dir)
    os.makedirs(user_dir, exist_ok=True)
    
    queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    stop_event = asyncio.Event()
    
    # Global Shared State
    job_progress[chat_id] = {
        'scanned': job['last_id'], 'total': job['end_id'], 
        'current_chunk_size': 0, 'part': job['part'],
        'finished_bytes': 0, 'highest_id': job['last_id'],
        'files': [], 'queue_obj': queue
    }
    active_downloads[chat_id] = {}

    # 1. Start Fetcher (Producer) on Main Bot
    fetch_task = asyncio.create_task(
        fetch_worker(main_app, job['target'], job['last_id'], job['end_id'], queue, stop_event, chat_id)
    )

    # 2. Start Downloaders (Consumers) on ALL Bots
    worker_tasks = []
    semaphores = [asyncio.Semaphore(PER_BOT_CONCURRENCY) for _ in all_apps]
    
    for i, app_inst in enumerate(all_apps):
        t = asyncio.create_task(
            download_worker(app_inst, queue, chat_id, user_dir, semaphores[i], stop_event)
        )
        worker_tasks.append(t)

    # 3. Start UI
    ui_task = asyncio.create_task(status_monitor(main_app, chat_id, stop_event))

    # 4. Zip Monitor Loop
    # We check periodically if it's time to zip
    while not stop_event.is_set():
        await asyncio.sleep(2)
        
        # Check if fetcher is done and queue is empty
        fetcher_done = fetch_task.done()
        queue_empty = queue.empty()
        active_dls = len(active_downloads.get(chat_id, {}))
        
        # Trigger Zip?
        is_full = job_progress[chat_id]['current_chunk_size'] >= job['max_size']
        is_finished = fetcher_done and queue_empty and active_dls == 0
        
        if is_full or (is_finished and job_progress[chat_id]['files']):
            # PAUSE Consumers ideally, but here we just grab available files
            files_to_zip = list(job_progress[chat_id]['files'])
            
            # Reset Batch State
            job_progress[chat_id]['files'] = []
            job_progress[chat_id]['current_chunk_size'] = 0
            
            zip_name = job['naming'].format(job_progress[chat_id]['part'])
            if not zip_name.endswith(".zip"): zip_name += ".zip"
            zip_path = os.path.join(user_dir, zip_name)
            
            await main_app.send_message(chat_id, f"🤐 **Zipping {zip_name}...**")
            
            await asyncio.to_thread(zip_files, files_to_zip, zip_path)
            
            # Upload
            end_batch_id = job_progress[chat_id]['highest_id']
            await main_app.send_document(chat_id, zip_path, caption=f"🗂 **{zip_name}**\nUP TO ID: {end_batch_id}")
            
            # Commit & Cleanup
            job_progress[chat_id]['part'] += 1
            await update_checkpoint(chat_id, end_batch_id, job_progress[chat_id]['part'])
            
            for f in files_to_zip:
                try: os.remove(f)
                except: pass
            try: os.remove(zip_path)
            except: pass

        if is_finished:
            break

    stop_event.set()
    await delete_job(chat_id)
    shutil.rmtree(user_dir, ignore_errors=True)
    await main_app.send_message(chat_id, "✅ **Mission Complete.**")

def zip_files(file_list, output_path):
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_STORED) as zipf:
        for f in file_list:
            if os.path.exists(f):
                zipf.write(f, os.path.basename(f))

# --- HANDLERS (Main Bot Only) ---

@main_app.on_message(filters.command("start"))
async def start(c, m):
    # Verify Worker Access
    if await get_job(m.chat.id):
        await m.reply_text("🔄 **Resuming Swarm Job...**")
        asyncio.create_task(manager_logic(m.chat.id))
    else:
        await m.reply_text(
            f"🤖 **Swarm Ready.**\n"
            f"Workers: {len(worker_apps)}\n"
            f"Total Threads: {len(all_apps) * PER_BOT_CONCURRENCY}\n\n"
            "⚠️ **IMPORTANT:** Add ALL worker bots to the private channel before starting.\n\n"
            "Send the **Channel Link** to begin."
        )

@main_app.on_message(filters.regex(r"t\.me/") & filters.private)
async def step1(c, m):
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
    
    # Verify Main Bot Access
    try: await c.get_chat(target_id)
    except: return await m.reply_text("❌ **Main Bot** cannot access channel.")

    setup_state[m.chat.id] = {"step": "name", "target": target_id, "end_id": end_id}
    await m.reply_text("✅ **Link OK.**\nEnter Naming (e.g. `Pack-{}`) or `default`.")

@main_app.on_message(filters.text & filters.private & ~filters.regex(r"^/") & ~filters.regex(r"t\.me/"))
async def step2(c, m):
    state = setup_state.get(m.chat.id)
    if not state: return

    if state["step"] == "name":
        name = m.text.strip()
        if "default" in name.lower(): name = "Batch_{}"
        if "{}" not in name: name += "_{}"
        state["naming"] = name
        state["step"] = "size"
        setup_state[m.chat.id] = state
        await m.reply_text("📦 **Max Zip Size (MB)?** (e.g. `1900`)")

    elif state["step"] == "size":
        try:
            sz = int(m.text.strip()) * 1024 * 1024
        except: return await m.reply_text("❌ Number only.")

        await save_job(m.chat.id, state["target"], 1, state["end_id"], sz, state["naming"])
        del setup_state[m.chat.id]
        
        await m.reply_text("🚀 **Unleashing the Swarm...**")
        asyncio.create_task(manager_logic(m.chat.id))

if __name__ == "__main__":
    if not os.path.exists(WORK_DIR): os.makedirs(WORK_DIR)
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(init_db())
    
    # Run Swarm
    print("🔥 Swarm Starting...")
    compose(all_apps)
