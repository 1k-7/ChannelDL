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
API_ID = int(os.getenv("API_ID", 0))  # Replace with your ID if not in env
API_HASH = os.getenv("API_HASH", "")  # Replace with your Hash if not in env
BOT_TOKEN = os.getenv("BOT_TOKEN", "") # Replace with your Token if not in env
WORK_DIR = os.getenv("WORK_DIR", "downloads")
DB_NAME = "zipper_state.db"

# Max concurrent downloads
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(4) 

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Client
app = Client("zipper_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# In-Memory State for setup (chat_id -> dict)
# Used only when asking the user for the file size
setup_state = {}

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
    link = link.strip()
    # Handle private links: t.me/c/123456/100
    if "t.me/c/" in link:
        match = re.search(r"t\.me/c/(\d+)/(\d+)", link)
        if match:
            # -100 prefix is required for private channel IDs in API
            return int("-100" + match.group(1)), int(match.group(2))
    # Handle public links: t.me/username/100
    else:
        match = re.search(r"t\.me/([^/]+)/(\d+)", link)
        if match:
            return match.group(1), int(match.group(2))
    return None, None

async def download_worker(client, message, folder):
    """Downloads a single file with semaphore protection."""
    async with DOWNLOAD_SEMAPHORE:
        try:
            if not message.media: return None
            
            # Streams directly to disk
            file_path = await client.download_media(
                message, 
                file_name=os.path.join(folder, "")
            )
            return file_path
        except Exception as e:
            logger.error(f"Failed download {message.id}: {e}")
            return None

def zip_directory(folder_path, output_path):
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
    
    # Notify User
    status_msg = await client.send_message(chat_id, f"🔄 **Starting Job**\nScanning from ID {current_id} to {end_id}...")
    
    user_dir = os.path.join(WORK_DIR, str(chat_id))
    os.makedirs(user_dir, exist_ok=True)

    batch_size = 20 # Number of messages to check at once
    
    while current_id <= end_id:
        batch_end = min(current_id + batch_size, end_id + 1)
        ids_to_fetch = list(range(current_id, batch_end))
        
        try:
            messages = await client.get_messages(target_channel, ids_to_fetch)
        except Exception as e:
            logger.error(f"Fetch Error: {e}")
            await status_msg.edit_text(f"❌ Error fetching messages. Ensure I am a member/admin.\nReason: {e}")
            await delete_job(chat_id)
            return

        # Prepare tasks
        tasks = []
        for msg in messages:
            # Only download if it has a file
            if msg and (msg.document or msg.video or msg.audio or msg.photo):
                tasks.append(download_worker(client, msg, user_dir))

        # Download concurrently
        if tasks:
            await asyncio.gather(*tasks)

        # Check accumulated size
        try:
            total_size = sum(os.path.getsize(os.path.join(user_dir, f)) for f in os.listdir(user_dir) if os.path.isfile(os.path.join(user_dir, f)))
        except FileNotFoundError:
            total_size = 0
        
        if total_size >= max_size or (current_id + batch_size > end_id and total_size > 0):
            await status_msg.edit_text(f"📦 **Zipping Chunk...**\nSize: {total_size/1024/1024:.2f} MB")
            
            zip_name = os.path.join(user_dir, f"Files_{current_id}_{batch_end}.zip")
            await asyncio.to_thread(zip_directory, user_dir, zip_name)
            
            await status_msg.edit_text("⬆️ **Uploading Zip...**")
            try:
                await client.send_document(chat_id, zip_name, caption=f"📁 **Batch:** IDs {current_id}-{batch_end}")
            except Exception as e:
                 await client.send_message(chat_id, f"❌ Upload Failed: {e}")

            # Cleanup
            shutil.rmtree(user_dir)
            os.makedirs(user_dir, exist_ok=True)

        # Save Progress
        current_id = batch_end
        await update_progress(chat_id, current_id)
    
    # Final Cleanup
    await delete_job(chat_id)
    shutil.rmtree(user_dir, ignore_errors=True)
    await status_msg.edit_text("✅ **Job Complete!** All files processed.")

# --- HANDLERS ---

@app.on_message(filters.command("start"))
async def start(c, m):
    # Check if a job is already resuming
    if await get_job(m.chat.id):
        await m.reply_text("🔄 **Resuming interrupted job...**")
        asyncio.create_task(process_job(c, m.chat.id))
    else:
        await m.reply_text("👋 **Hi!**\nSend me the **Link** of the last message in the channel you want to back up.")

@app.on_message(filters.regex(r"t\.me/") & filters.private)
async def link_receiver(c, m):
    # 1. Check if job exists
    if await get_job(m.chat.id):
        return await m.reply_text("⚠️ You already have a task running! Wait for it to finish.")

    # 2. Parse Link
    target_id, end_msg_id = await extract_chat_info(m.text)
    if not target_id:
        return await m.reply_text("❌ **Invalid Link.**\nUse format: `https://t.me/channel/123`")

    # 3. Store in Memory & Ask for Size
    setup_state[m.chat.id] = {
        "target_id": target_id,
        "end_msg_id": end_msg_id
    }
    await m.reply_text(
        "✅ **Link Detected.**\n\n"
        "Now send me the **Max Zip Size (in MB)**.\n"
        "*(Example: Send `1900` for 1.9GB chunks)*"
    )

@app.on_message(filters.text & filters.private & ~filters.regex(r"^/") & ~filters.regex(r"t\.me/"))
async def size_receiver(c, m):
    # Check if we are waiting for a size from this user
    state = setup_state.get(m.chat.id)
    if not state:
        return # Ignore random text

    # Parse Size
    try:
        size_mb = int(m.text.strip())
        max_size_bytes = size_mb * 1024 * 1024
        
        # Validation
        if size_mb < 10 or size_mb > 2000:
             return await m.reply_text("❌ Please enter a size between **10 MB** and **2000 MB**.")

    except ValueError:
        return await m.reply_text("❌ **Invalid Number.**\nPlease send just the number (e.g., `1900`).")

    # Save to DB and Start
    await m.reply_text(f"🚀 **Starting Backup!**\nChunk Size: {size_mb} MB")
    
    await save_job(
        m.chat.id, 
        state["target_id"], 
        state["end_msg_id"], 
        max_size_bytes
    )
    
    # Clear memory state
    del setup_state[m.chat.id]
    
    # Run
    asyncio.create_task(process_job(c, m.chat.id))

if __name__ == "__main__":
    if not os.path.exists(WORK_DIR):
        os.makedirs(WORK_DIR)
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_db())
    
    print("Bot Started...")
    app.run()
