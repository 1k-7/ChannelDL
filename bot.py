import os
import asyncio
import logging
import zipfile
import shutil
import re
import aiosqlite
import time
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import Message
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WORK_DIR = os.getenv("WORK_DIR", "downloads")
DB_NAME = "zipper_state.db"

# --- TUNING FOR SPEED ---
# Increased to 15 concurrent downloads
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(15) 
QUEUE_MAX_SIZE = 1000

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CLIENT OPTIMIZATION ---
# ipv6=False: Fixes peering slowness on many VPSs
# max_concurrent_transmissions: Allows more parallel data streams
app = Client(
    "zipper_bot", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    bot_token=BOT_TOKEN,
    ipv6=False, 
    max_concurrent_transmissions=20
)

setup_state = {}
job_progress = {} 

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

# --- PIPELINE WORKERS ---

async def fetch_worker(client, target_chat, start_id, end_id, queue, stop_event, progress_key):
    """PRODUCER: Scans high-speed."""
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

async def status_updater(client, chat_id, stop_event):
    msg = await client.send_message(chat_id, "⏳ **Initializing Pipeline...**")
    last_text = ""
    start_time = time.time()
    
    while not stop_event.is_set():
        try:
            data = job_progress.get(chat_id)
            if not data: break

            # Speed Calc
            elapsed = time.time() - start_time
            speed = (data['total_bytes_downloaded'] / (elapsed + 1)) / (1024 * 1024) # MB/s

            text = (
                f"🚀 **Speed Mode Active**\n"
                f"⚡ Speed: `{speed:.2f} MB/s`\n"
                f"📥 Queue: `{data['queue_size']}`\n"
                f"💾 Chunk: `{data['current_size']/1024/1024:.2f} MB`\n"
                f"🔎 Scan: `{data['scanned']}` / `{data['total']}`\n"
                f"📦 Part: `{data['part']}`"
            )
            
            if text != last_text:
                await msg.edit_text(text)
                last_text = text
            
            await asyncio.sleep(5)
        except Exception:
            pass
    
    await msg.delete()

async def download_manager(client, chat_id):
    job = await get_job(chat_id)
    if not job: return

    user_dir = os.path.join(WORK_DIR, str(chat_id))
    if os.path.exists(user_dir): shutil.rmtree(user_dir)
    os.makedirs(user_dir, exist_ok=True)
    
    queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    stop_event = asyncio.Event()
    
    job_progress[chat_id] = {
        'scanned': job['last_id'], 'total': job['end_id'], 
        'queue_size': 0, 'current_size': 0, 'part': job['part'],
        'total_bytes_downloaded': 0 # Global counter for speed calc
    }

    # Start Workers
    asyncio.create_task(
        fetch_worker(client, job['target'], job['last_id'], job['end_id'], queue, stop_event, chat_id)
    )
    asyncio.create_task(status_updater(client, chat_id, stop_event))

    current_chunk_size = 0
    batch_highest_id = job['last_id']
    files_in_batch = []
    
    while True:
        job_progress[chat_id]['queue_size'] = queue.qsize()
        msg = await queue.get()
        if msg is None: break 

        async with DOWNLOAD_SEMAPHORE:
            try:
                # Fast Stream Download
                f_path = await client.download_media(msg, file_name=os.path.join(user_dir, ""))
                
                if f_path:
                    f_size = os.path.getsize(f_path)
                    current_chunk_size += f_size
                    files_in_batch.append(f_path)
                    
                    if msg.id > batch_highest_id: batch_highest_id = msg.id
                    
                    # Update Stats
                    job_progress[chat_id]['current_size'] = current_chunk_size
                    job_progress[chat_id]['total_bytes_downloaded'] += f_size

                    # ZIP TRIGGER
                    if current_chunk_size >= job['max_size']:
                        zip_name = job['naming'].format(job['part'])
                        if not zip_name.endswith(".zip"): zip_name += ".zip"
                        zip_path = os.path.join(user_dir, zip_name)
                        
                        await client.send_message(chat_id, f"🤐 **Zipping {zip_name}...**")
                        
                        await asyncio.to_thread(zip_files, files_in_batch, zip_path)
                        await client.send_document(chat_id, zip_path, caption=f"🗂 **{zip_name}**\nID: {batch_highest_id}")
                        
                        # Cleanup
                        job['part'] += 1
                        job_progress[chat_id]['part'] = job['part']
                        await update_checkpoint(chat_id, batch_highest_id, job['part'])
                        
                        for f in files_in_batch:
                            try: os.remove(f)
                            except: pass
                        try: os.remove(zip_path)
                        except: pass
                        
                        files_in_batch = []
                        current_chunk_size = 0

            except Exception as e:
                logger.error(f"DL Error: {e}")
        
        queue.task_done()

    # Final Batch
    if files_in_batch:
        zip_name = job['naming'].format(job['part'])
        if not zip_name.endswith(".zip"): zip_name += ".zip"
        zip_path = os.path.join(user_dir, zip_name)
        await asyncio.to_thread(zip_files, files_in_batch, zip_path)
        await client.send_document(chat_id, zip_path, caption=f"🏁 **Final: {zip_name}**")

    stop_event.set()
    await delete_job(chat_id)
    if chat_id in job_progress: del job_progress[chat_id]
    shutil.rmtree(user_dir, ignore_errors=True)
    await client.send_message(chat_id, "✅ **Done.**")

def zip_files(file_list, output_path):
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_STORED) as zipf:
        for f in file_list:
            if os.path.exists(f):
                zipf.write(f, os.path.basename(f))

# --- HANDLERS ---

@app.on_message(filters.command("start"))
async def start(c, m):
    # Verify tgcrypto first
    try:
        import tgcrypto
    except ImportError:
        return await m.reply_text("❌ **CRITICAL:** `tgcrypto` is not installed. Speeds will be slow.")

    if await get_job(m.chat.id):
        await m.reply_text("🔄 **Resuming...**")
        asyncio.create_task(download_manager(c, m.chat.id))
    else:
        await m.reply_text("👋 **Send Channel Link.**")

@app.on_message(filters.regex(r"t\.me/") & filters.private)
async def step1_link(c, m):
    if await get_job(m.chat.id): return await m.reply_text("⚠️ Job Running.")
    
    link = m.text.strip()
    target_id, end_id = None, None

    if "t.me/c/" in link:
        match = re.search(r"t\.me/c/(\d+)/(\d+)", link)
        if match: target_id, end_id = int("-100" + match.group(1)), int(match.group(2))
    else:
        match = re.search(r"t\.me/([^/]+)/(\d+)", link)
        if match: target_id, end_id = match.group(1), int(match.group(2))

    if not target_id: return await m.reply_text("❌ Invalid Link.")
    
    try: await c.get_chat(target_id)
    except: return await m.reply_text("❌ **Access Denied.** Make me admin?")

    setup_state[m.chat.id] = {"step": "name", "target": target_id, "end_id": end_id}
    await m.reply_text("✅ **Link OK.**\n\nEnter Naming Format (e.g. `Pack-{}`).\nReply `default` for standard.")

@app.on_message(filters.text & filters.private & ~filters.regex(r"^/") & ~filters.regex(r"t\.me/"))
async def step_handler(c, m):
    state = setup_state.get(m.chat.id)
    if not state: return

    if state["step"] == "name":
        name = m.text.strip()
        if name.lower() == "default": name = "Batch_{}"
        if "{}" not in name: name += "_{}"
        state["naming"] = name
        state["step"] = "size"
        setup_state[m.chat.id] = state
        await m.reply_text("📦 **Enter Max Zip Size (MB):**\nExample: `1900`")

    elif state["step"] == "size":
        try:
            size_mb = int(m.text.strip())
            if not (10 <= size_mb <= 2000): raise ValueError
        except: return await m.reply_text("❌ Enter number 10-2000.")

        await save_job(m.chat.id, state["target"], 1, state["end_id"], size_mb * 1024 * 1024, state["naming"])
        del setup_state[m.chat.id]
        
        await m.reply_text(f"🚀 **Starting High-Speed Pipeline...**")
        asyncio.create_task(download_manager(c, m.chat.id))

if __name__ == "__main__":
    if not os.path.exists(WORK_DIR): os.makedirs(WORK_DIR)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_db())
    app.run()
