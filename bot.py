import os
import asyncio
import logging
import zipfile
import shutil
import re
import aiosqlite
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

# Performance Tuning
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(5)  # Concurrent downloads
QUEUE_MAX_SIZE = 1000  # Buffer size for the fetcher

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Client
app = Client("zipper_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# In-Memory Setup State
setup_state = {}

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
    """Updates the 'Safe' ID after a successful upload."""
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

async def fetch_worker(client, target_chat, start_id, end_id, queue, stop_event):
    """Producer: Fetches messages in batches and feeds the queue."""
    current = start_id
    batch_size = 200 # Pyrogram Limit
    
    while current <= end_id and not stop_event.is_set():
        batch_end = min(current + batch_size, end_id + 1)
        ids = list(range(current, batch_end))
        
        try:
            # Efficient fetching from Reference Repo
            messages = await client.get_messages(target_chat, ids)
            
            # Put VALID media messages into queue
            for msg in messages:
                if msg and (msg.document or msg.video or msg.audio or msg.photo):
                    await queue.put(msg)
            
            current = batch_end
            
        except FloodWait as e:
            logger.warning(f"FloodWait: Sleeping {e.value}s")
            await asyncio.sleep(e.value)
        except Exception as e:
            logger.error(f"Fetch Error at {current}: {e}")
            await asyncio.sleep(2) # Brief pause on error
            
    # Signal end of fetching
    await queue.put(None)

async def download_manager(client, chat_id):
    job = await get_job(chat_id)
    if not job: return

    # Setup
    user_dir = os.path.join(WORK_DIR, str(chat_id))
    if os.path.exists(user_dir): shutil.rmtree(user_dir) # Clean start for safety
    os.makedirs(user_dir, exist_ok=True)
    
    queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    stop_event = asyncio.Event()
    
    # Start Producer (Fetcher)
    fetch_task = asyncio.create_task(
        fetch_worker(client, job['target'], job['last_id'], job['end_id'], queue, stop_event)
    )

    # State Tracking
    current_size = 0
    batch_highest_id = job['last_id']
    files_in_batch = []
    
    status_msg = await client.send_message(
        chat_id, 
        f"🚀 **Starting Pipeline**\n"
        f"Fetching: `{job['last_id']}` -> `{job['end_id']}`\n"
        f"Concurrent Downloads: {DOWNLOAD_SEMAPHORE._value}"
    )

    while True:
        # Get message from queue
        msg = await queue.get()
        
        # None means Producer is done
        if msg is None:
            break

        # Download Task
        async with DOWNLOAD_SEMAPHORE:
            try:
                # Download
                f_path = await client.download_media(msg, file_name=os.path.join(user_dir, ""))
                
                if f_path:
                    f_size = os.path.getsize(f_path)
                    current_size += f_size
                    files_in_batch.append(f_path)
                    
                    # Track highest ID in this current batch of files
                    if msg.id > batch_highest_id:
                        batch_highest_id = msg.id

                    # --- ZIP TRIGGER ---
                    if current_size >= job['max_size']:
                        await status_msg.edit_text(f"📦 **Zipping Part {job['part']}...**\nSize: {current_size/1024/1024:.2f} MB")
                        
                        # Generate Name
                        zip_name_fmt = job['naming'].format(job['part'])
                        if not zip_name_fmt.endswith(".zip"): zip_name_fmt += ".zip"
                        zip_path = os.path.join(user_dir, zip_name_fmt)

                        # Zip
                        await asyncio.to_thread(zip_files, files_in_batch, zip_path)
                        
                        # Upload
                        await status_msg.edit_text(f"⬆️ **Uploading {zip_name_fmt}...**")
                        await client.send_document(chat_id, zip_path, caption=f"🗂 **{zip_name_fmt}**\nUp to ID: {batch_highest_id}")
                        
                        # Checkpoint & Cleanup
                        job['part'] += 1
                        await update_checkpoint(chat_id, batch_highest_id, job['part'])
                        
                        # Cleanup disk
                        for f in files_in_batch:
                            try: os.remove(f)
                            except: pass
                        os.remove(zip_path)
                        
                        # Reset Batch State
                        files_in_batch = []
                        current_size = 0
                        
                        await status_msg.edit_text(f"📥 **Resuming Downloads...**")

            except Exception as e:
                logger.error(f"Download fail: {e}")
        
        queue.task_done()

    # --- FINAL BATCH ---
    if files_in_batch:
        await status_msg.edit_text("📦 **Zipping Final Part...**")
        zip_name_fmt = job['naming'].format(job['part'])
        if not zip_name_fmt.endswith(".zip"): zip_name_fmt += ".zip"
        zip_path = os.path.join(user_dir, zip_name_fmt)

        await asyncio.to_thread(zip_files, files_in_batch, zip_path)
        await client.send_document(chat_id, zip_path, caption=f"🏁 **Final Part: {zip_name_fmt}**\nJob Complete.")
    
    # Finish
    stop_event.set()
    await delete_job(chat_id)
    shutil.rmtree(user_dir, ignore_errors=True)
    await status_msg.edit_text("✅ **All Done.**")

def zip_files(file_list, output_path):
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_STORED) as zipf:
        for f in file_list:
            if os.path.exists(f):
                zipf.write(f, os.path.basename(f))

# --- HANDLERS ---

@app.on_message(filters.command("start"))
async def start(c, m):
    # Check for resumption
    if await get_job(m.chat.id):
        await m.reply_text("🔄 **Resuming previous job...**")
        asyncio.create_task(download_manager(c, m.chat.id))
    else:
        await m.reply_text(
            "👋 **Advanced Channel Zipper**\n"
            "1. Send me the **Link** of the last message.\n"
            "2. I'll ask for a naming format (e.g., `Backup-Part {}`).\n"
            "3. I'll ask for the max zip size."
        )

@app.on_message(filters.regex(r"t\.me/") & filters.private)
async def step1_link(c, m):
    if await get_job(m.chat.id): return await m.reply_text("⚠️ Job already running.")

    # Extract ID
    link = m.text.strip()
    target_id = None
    end_id = None

    if "t.me/c/" in link: # Private
        match = re.search(r"t\.me/c/(\d+)/(\d+)", link)
        if match: target_id, end_id = int("-100" + match.group(1)), int(match.group(2))
    else: # Public
        match = re.search(r"t\.me/([^/]+)/(\d+)", link)
        if match: target_id, end_id = match.group(1), int(match.group(2))

    if not target_id: return await m.reply_text("❌ Invalid Link.")

    # Resolve Peer Check
    msg = await m.reply_text("🔎 **Verifying Channel Access...**")
    try:
        await c.get_chat(target_id)
    except Exception as e:
        return await msg.edit_text(f"❌ **Error:** I cannot access that channel.\nAre you sure I am a member/admin?\n`{e}`")

    setup_state[m.chat.id] = {"step": "name", "target": target_id, "end_id": end_id}
    await msg.edit_text(
        "✅ **Link Accepted.**\n\n"
        "🔡 **Enter Naming Format:**\n"
        "Use `{}` where the number should go.\n"
        "Example: `FansMTL-Part {}`\n"
        "Reply `default` for `Batch_{}`."
    )

@app.on_message(filters.text & filters.private & ~filters.regex(r"^/") & ~filters.regex(r"t\.me/"))
async def step_handler(c, m):
    state = setup_state.get(m.chat.id)
    if not state: return

    # Step 2: Naming Scheme
    if state["step"] == "name":
        name_input = m.text.strip()
        if name_input.lower() == "default": name_input = "Batch_{}"
        if "{}" not in name_input: name_input += "_{}" # Ensure valid format
        
        state["naming"] = name_input
        state["step"] = "size"
        setup_state[m.chat.id] = state
        
        await m.reply_text("📦 **Enter Max Zip Size (in MB):**\nExample: `1900`")

    # Step 3: Size & Launch
    elif state["step"] == "size":
        try:
            size_mb = int(m.text.strip())
            if not (10 <= size_mb <= 2000): raise ValueError
        except:
            return await m.reply_text("❌ Invalid size. Enter a number between 10 and 2000.")

        await m.reply_text(f"🚀 **Starting Job!**\nName: `{state['naming']}`\nSize: `{size_mb} MB`")
        
        await save_job(
            m.chat.id, state["target"], 1, state["end_id"], 
            size_mb * 1024 * 1024, state["naming"]
        )
        del setup_state[m.chat.id]
        asyncio.create_task(download_manager(c, m.chat.id))

if __name__ == "__main__":
    if not os.path.exists(WORK_DIR): os.makedirs(WORK_DIR)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_db())
    app.run()
