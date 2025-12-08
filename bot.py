import os
import asyncio
import logging
import zipfile
import shutil
import re
import aiosqlite
from pyrogram import Client, filters
from pyrogram.types import Message
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
WORK_DIR = os.getenv("WORK_DIR", "downloads")
DB_NAME = "zipper_state.db"

# Max concurrent downloads
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(4) 

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Client
app = Client("zipper_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- DATABASE HELPERS (Persistence) ---

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                chat_id INTEGER PRIMARY KEY,
                target_channel_id INTEGER,
                last_msg_id INTEGER,
                current_msg_id INTEGER,
                max_size_bytes INTEGER,
                status TEXT
            )
        """)
        await db.commit()

async def save_job(chat_id, target_channel_id, last_msg_id, max_size_bytes):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR REPLACE INTO jobs VALUES (?, ?, ?, ?, ?, ?)",
            (chat_id, target_channel_id, last_msg_id, 1, max_size_bytes, "RUNNING")
        )
        await db.commit()

async def update_progress(chat_id, current_msg_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE jobs SET current_msg_id = ? WHERE chat_id = ?", (current_msg_id, chat_id))
        await db.commit()

async def get_job(chat_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT * FROM jobs WHERE chat_id = ?", (chat_id,)) as cursor:
            return await cursor.fetchone()

async def delete_job(chat_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM jobs WHERE chat_id = ?", (chat_id,))
        await db.commit()

# --- LOGIC ---

async def extract_chat_info(link):
    """Adapted from your provided code to handle private/public links."""
    link = link.strip()
    if "t.me/c/" in link:
        match = re.search(r"t\.me/c/(\d+)/(\d+)", link)
        if match:
            return int("-100" + match.group(1)), int(match.group(2))
    else:
        match = re.search(r"t\.me/([^/]+)/(\d+)", link)
        if match:
            return match.group(1), int(match.group(2))
    return None, None

async def download_worker(client, message, folder):
    """Downloads a single file with semaphore protection."""
    async with DOWNLOAD_SEMAPHORE:
        try:
            # Check if message has media
            if not message.media: return None
            
            # Using pyrogram's download (streams to disk)
            file_path = await client.download_media(
                message, 
                file_name=os.path.join(folder, "")
            )
            return file_path
        except Exception as e:
            logger.error(f"Failed download {message.id}: {e}")
            return None

def zip_directory(folder_path, output_path):
    """Zips files in the folder."""
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_STORED) as zipf:
        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.endswith(".zip"): continue
                file_path = os.path.join(root, file)
                zipf.write(file_path, arcname=file)

async def process_job(client, chat_id):
    job = await get_job(chat_id)
    if not job: return

    target_channel, end_id, current_id, max_size, status = job[1], job[2], job[3], job[4], job[5]
    
    status_msg = await client.send_message(chat_id, f"🔄 **Resuming/Starting Job**\nScanning from ID {current_id} to {end_id}...")
    
    # Temp directory for this user
    user_dir = os.path.join(WORK_DIR, str(chat_id))
    os.makedirs(user_dir, exist_ok=True)

    # Batch iterator
    batch_size = 20 # Fetch 20 messages at a time
    
    while current_id <= end_id:
        batch_end = min(current_id + batch_size, end_id + 1)
        ids_to_fetch = list(range(current_id, batch_end))
        
        try:
            messages = await client.get_messages(target_channel, ids_to_fetch)
        except Exception as e:
            await status_msg.edit_text(f"❌ Error fetching messages: {e}")
            return

        # Create download tasks for this batch
        tasks = []
        for msg in messages:
            if msg.media or msg.document or msg.video or msg.audio or msg.photo:
                tasks.append(download_worker(client, msg, user_dir))

        # Run downloads concurrently
        if tasks:
            await asyncio.gather(*tasks)

        # Check directory size
        total_size = sum(os.path.getsize(os.path.join(user_dir, f)) for f in os.listdir(user_dir) if os.path.isfile(os.path.join(user_dir, f)))
        
        if total_size >= max_size or current_id + batch_size > end_id:
            # Trigger Zip & Upload
            await status_msg.edit_text(f"📦 **Zipping...**\nSize: {total_size/1024/1024:.2f} MB")
            
            zip_name = os.path.join(user_dir, f"Batch_{current_id}_{batch_end}.zip")
            await asyncio.to_thread(zip_directory, user_dir, zip_name)
            
            await status_msg.edit_text("⬆️ **Uploading...**")
            await client.send_document(chat_id, zip_name, caption=f"📁 **Batch:** IDs {current_id}-{batch_end}")
            
            # Cleanup
            shutil.rmtree(user_dir)
            os.makedirs(user_dir, exist_ok=True)

        # Update DB
        current_id = batch_end
        await update_progress(chat_id, current_id)
    
    await delete_job(chat_id)
    await status_msg.edit_text("✅ **All files processed and uploaded!**")
    shutil.rmtree(user_dir, ignore_errors=True)

# --- HANDLERS ---

@app.on_message(filters.command("start"))
async def start(c, m):
    await m.reply_text("Send me a link to the **last message** of a channel to backup.")
    # Check for interrupted jobs
    if await get_job(m.chat.id):
        asyncio.create_task(process_job(c, m.chat.id))

@app.on_message(filters.regex(r"t\.me/"))
async def new_job(c, m):
    # 1. Check existing
    if await get_job(m.chat.id):
        return await m.reply_text("⚠️ You already have a task running.")

    # 2. Parse Link
    target_id, end_msg_id = await extract_chat_info(m.text)
    if not target_id:
        return await m.reply_text("Invalid Link.")

    # 3. Ask for Size
    try:
        resp = await m.ask("📦 **Enter Max Zip Size (in MB):**\ne.g., `1900` for 1.9GB", timeout=60)
        max_size = int(resp.text) * 1024 * 1024
    except:
        return await m.reply_text("Invalid size or timeout.")

    # 4. Save & Start
    await m.reply_text("🚀 **Job Queued!**")
    await save_job(m.chat.id, target_id, end_msg_id, max_size)
    asyncio.create_task(process_job(c, m.chat.id))

if __name__ == "__main__":
    # Ensure Downloads Dir
    if not os.path.exists(WORK_DIR):
        os.makedirs(WORK_DIR)
    
    # Init DB
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_db())
    
    print("Bot Started...")
    app.run()
