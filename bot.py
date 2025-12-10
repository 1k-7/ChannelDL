import os
import asyncio
import logging
import zipfile
import shutil
import re
import aiosqlite
import time
import glob
from pyrogram import Client, filters, compose, idle
from pyrogram.errors import (
    FloodWait, 
    PeerIdInvalid, 
    ChannelInvalid, 
    FileReferenceExpired, 
    AuthKeyUnregistered, 
    UserNotParticipant
)
from dotenv import load_dotenv

# --- SAFE IMPORTS ---
try:
    import uvloop
    uvloop.install()
    import tgcrypto
except ImportError:
    pass

# --- CONFIGURATION ---
load_dotenv()
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
MAIN_BOT_TOKEN = os.getenv("BOT_TOKEN", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")
BOT_USERNAME = os.getenv("BOT_USERNAME", "").replace("@", "")

WORKER_TOKENS_STR = os.getenv("WORKER_TOKENS", "")
WORKER_TOKENS = [t.strip() for t in WORKER_TOKENS_STR.split(",") if t.strip()]

if MAIN_BOT_TOKEN in WORKER_TOKENS:
    print("❌ FATAL: Main Bot Token is also in WORKER_TOKENS. Remove it.")
    exit(1)

WORK_DIR = os.getenv("WORK_DIR", "downloads")
DB_NAME = "zipper_state.db"
SESSION_DIR = "sessions"

# --- TUNING ---
BOT_MAX_LOAD = int(os.getenv("BOT_MAX_LOAD", 300))
QUEUE_MAX_SIZE = 15000 
TG_MAX_FILE_SIZE = 1900 * 1024 * 1024 

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SwarmBot")
logging.getLogger("pyrogram").setLevel(logging.ERROR)

if not os.path.exists(SESSION_DIR):
    os.makedirs(SESSION_DIR)

# --- LIGHTWEIGHT SEMAPHORE ---
class WeightedSemaphore:
    def __init__(self, value=1):
        self._value = value
        self._condition = asyncio.Condition()

    async def acquire(self, weight=1):
        async with self._condition:
            while self._value < weight:
                await self._condition.wait()
            self._value -= weight

    async def release(self, weight=1):
        async with self._condition:
            self._value += weight
            self._condition.notify_all()

# --- CLIENT FACTORY ---
def create_client(name, token=None, session=None, is_manager=False):
    concurrent = 64 if is_manager else 256
    return Client(
        name,
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=token,
        session_string=session,
        workdir=SESSION_DIR, 
        ipv6=False,
        max_concurrent_transmissions=concurrent, 
        workers=32
    )

print("🔥 Initializing Swarm...")
main_app = create_client("main_bot", token=MAIN_BOT_TOKEN, is_manager=True)

premium_app = None
if SESSION_STRING:
    try:
        premium_app = create_client("premium_uploader", session=SESSION_STRING, is_manager=True)
        print("💎 Premium Account Loaded!")
    except Exception as e:
        print(f"⚠️ Premium Error: {e}")

worker_apps = [create_client(f"worker_{i+1}", token=t) for i, t in enumerate(WORKER_TOKENS)]
all_apps = [main_app] + worker_apps
if premium_app: all_apps.append(premium_app)

print(f"✅ Swarm Loaded: {len(worker_apps)} Workers")

# Global State
setup_state = {}
job_progress = {} 
active_downloads = {} 
stop_events = {}
dedup_lists = {} # Stores {chat_id: set(filenames)}

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
                    "chat_id": row[0], "target": row[1], "last_processed_id": row[2],
                    "end_id": row[3], "max_size": row[4], "naming": row[5], "part": row[6]
                }
            return None

async def get_all_jobs():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT chat_id FROM jobs") as cursor:
            return await cursor.fetchall()

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

def parse_link(link):
    link = link.strip()
    target_id = None
    start_id = 1
    end_id = None
    if "t.me/c/" in link:
        match = re.search(r"t\.me/c/(\d+)/([\d\-]+)", link)
        if match:
            target_id = int("-100" + match.group(1))
            msg_part = match.group(2)
    elif "t.me/" in link:
        match = re.search(r"t\.me/([^/]+)/([\d\-]+)", link)
        if match:
            target_id = match.group(1) 
            msg_part = match.group(2)
    if target_id and msg_part:
        if "-" in msg_part:
            try:
                parts = msg_part.split("-")
                start_id = int(parts[0])
                end_id = int(parts[1])
            except ValueError: return None, None, None
        else:
            end_id = int(msg_part)
            start_id = 1
        return target_id, start_id, end_id
    return None, None, None

# --- WORKER LOGIC ---

async def swarm_warmup(chat_id, target_channel):
    logger.info("🔥 Warming up workers...")
    status_msg = await main_app.send_message(chat_id, "🛡 **Swarm Warmup...**")
    
    async def activate_bot(bot):
        try:
            await bot.get_chat(target_channel)
            return True
        except Exception: return False

    results = await asyncio.gather(*[activate_bot(bot) for bot in worker_apps])
    await status_msg.edit_text(f"🛡 **Swarm Warmup:** {sum(results)}/{len(worker_apps)} workers ready.")
    await asyncio.sleep(2)
    await status_msg.delete()

async def fetch_worker(client, target_chat, start_id, end_id, queue, stop_event, progress_key):
    current = start_id
    batch_size = 500 
    
    print(f"🚀 Fetcher Started: {start_id} -> {end_id}")

    while current <= end_id and not stop_event.is_set():
        batch_end = min(current + batch_size - 1, end_id)
        ids = list(range(current, batch_end + 1))
        
        try:
            if progress_key in job_progress:
                job_progress[progress_key]['scanned'] = batch_end

            messages = await client.get_messages(target_chat, ids)
            
            found = 0
            for msg in messages:
                if not msg or msg.empty: continue
                if msg.media:
                    if msg.document or msg.video or msg.audio or msg.photo:
                        await queue.put(msg.id)
                        found += 1
            
            print(f"🔎 Scanned {current}-{batch_end} | Found: {found}")
            current = batch_end + 1
            
        except FloodWait as e:
            print(f"⏳ Fetcher FloodWait: {e.value}s")
            await asyncio.sleep(e.value)
        except Exception as e:
            print(f"⚠️ Fetcher Error on {current}: {e}")
            await asyncio.sleep(1)
            current = batch_end + 1
            
    await queue.put(None)
    print("✅ Fetching Complete.")

async def perform_download(client_inst, msg_id, chat_id, target_chat_id, user_dir, semaphore):
    try:
        if chat_id in job_progress: job_progress[chat_id]['active_count'] += 1
        
        if not client_inst.is_connected:
             try: await client_inst.connect()
             except: pass

        f_path = None
        msg = None

        # Fetch Message Object
        try:
            msg = await client_inst.get_messages(target_chat_id, msg_id)
        except Exception: return

        if not msg or not msg.media: return

        # --- DEDUPLICATION CHECK ---
        # If we are in "Compare Mode", check filename against blocklist
        file_name = None
        if msg.document: file_name = msg.document.file_name
        elif msg.video: file_name = msg.video.file_name
        elif msg.audio: file_name = msg.audio.file_name
        
        # If filename exists in our dedupe list, SKIP DOWNLOAD
        if chat_id in dedup_lists and file_name and file_name in dedup_lists[chat_id]:
            # print(f"⏭️ Skipping duplicate: {file_name}")
            return # Exit successfully without downloading

        # Download
        for attempt in range(3):
            try:
                f_path = await client_inst.download_media(msg, file_name=os.path.join(user_dir, ""))
                if f_path: break
            except Exception:
                await asyncio.sleep(1)

        if f_path:
            f_size = os.path.getsize(f_path)
            if chat_id in job_progress:
                job_progress[chat_id]['finished_bytes'] += f_size
                job_progress[chat_id]['current_buffer_size'] += f_size
                job_progress[chat_id]['file_buffer'].append({
                    'path': f_path, 
                    'size': f_size, 
                    'id': msg_id
                })
                if msg_id > job_progress[chat_id]['highest_id']:
                    job_progress[chat_id]['highest_id'] = msg_id

    except Exception: pass
    finally:
        if chat_id in job_progress: job_progress[chat_id]['active_count'] -= 1
        await semaphore.release()

async def download_dispatcher(client_inst, queue, chat_id, target_chat_id, user_dir, semaphore, stop_event, pause_event):
    while not stop_event.is_set():
        await pause_event.wait()
        try:
            msg_id = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError: continue
        
        if msg_id is None:
            await queue.put(None)
            break

        await semaphore.acquire()
        asyncio.create_task(perform_download(
            client_inst, msg_id, chat_id, target_chat_id, user_dir, semaphore
        ))
        queue.task_done()

last_upload_edit = 0
async def upload_progress(current, total, message, zip_name):
    global last_upload_edit
    now = time.time()
    if now - last_upload_edit > 10: 
        percent = (current / total) * 100
        try:
            await message.edit_text(f"💎 **Premium Upload:** {zip_name}\n🚀 Progress: `{percent:.2f}%`")
            last_upload_edit = now
        except: pass

async def status_monitor(client, chat_id, stop_event):
    msg = await client.send_message(chat_id, "⏳ **Swarm Starting...**")
    last_text = ""
    last_bytes = 0
    last_time = time.time()

    while not stop_event.is_set():
        try:
            data = job_progress.get(chat_id)
            if not data: break
            
            now = time.time()
            total_now = data['finished_bytes']
            
            time_diff = now - last_time
            if time_diff >= 5.0:
                speed = ((total_now - last_bytes) / time_diff) / (1024 * 1024)
                last_bytes = total_now
                last_time = now
            else:
                speed = getattr(status_monitor, "last_speed", 0.0)
            status_monitor.last_speed = speed

            status_text = "🟢 **DOWNLOADING**" if data['pause_event'].is_set() else "🟠 **UPLOADING**"
            
            dedup_status = f"🚫 Ignored: {len(dedup_lists[chat_id])} files" if chat_id in dedup_lists else ""

            text = (
                f"🤖 **Swarm Status:** {status_text}\n"
                f"⚡ DL Speed: `{speed:.2f} MB/s`\n"
                f"🔥 Streams: `{data['active_count']}`\n"
                f"📥 Queue: `{data['queue_obj'].qsize()}`\n"
                f"💾 Buffer: `{(data['current_buffer_size'])/1024/1024:.2f} MB`\n"
                f"🔎 Scanned: `{data['scanned']}`\n"
                f"📦 Part: `{data['part']}`\n"
                f"{dedup_status}"
            )

            if text != last_text:
                await msg.edit_text(text)
                last_text = text
            await asyncio.sleep(20) 
        except Exception: pass
    await msg.delete()

async def zip_and_upload_logic(chat_id, user_dir, max_zip_size):
    buffer = job_progress[chat_id]['file_buffer']
    files_to_zip = []
    current_zip_size = 0
    
    buffer_copy = list(buffer)
    for file_obj in buffer_copy:
        if current_zip_size + file_obj['size'] > max_zip_size:
            break
        files_to_zip.append(file_obj)
        current_zip_size += file_obj['size']
    
    if not files_to_zip and buffer:
        files_to_zip.append(buffer[0])

    if not files_to_zip: return 

    for f in files_to_zip:
        if f in buffer: buffer.remove(f)
    
    removed_size = sum(f['size'] for f in files_to_zip)
    job_progress[chat_id]['current_buffer_size'] -= removed_size

    part_num = job_progress[chat_id]['part']
    zip_name = job_progress[chat_id]['naming'].format(part_num)
    if not zip_name.endswith(".zip"): zip_name += ".zip"
    zip_path = os.path.join(user_dir, zip_name)
    
    # ID Range Caption
    first_id = files_to_zip[0]['id']
    last_id = files_to_zip[-1]['id']
    
    status_msg = await main_app.send_message(chat_id, f"🤐 **Zipping {zip_name}...** ({len(files_to_zip)} files)")
    
    file_paths = [f['path'] for f in files_to_zip]
    highest_id_in_zip = max((f['id'] for f in files_to_zip), default=0)

    upload_success = False
    try:
        await asyncio.to_thread(zip_files, file_paths, zip_path)
        await status_msg.edit_text(f"⬆️ **Uploading {zip_name}...**")
        
        caption = f"🗂 **{zip_name}**\n🆔 End ID: `{highest_id_in_zip}`"

        if premium_app and BOT_USERNAME and premium_app.is_connected:
            try:
                async def upload_task():
                    return await premium_app.send_document(
                        BOT_USERNAME, zip_path, caption=caption,
                        progress=upload_progress, progress_args=(status_msg, zip_name)
                    )
                sent_msg = await asyncio.wait_for(upload_task(), timeout=7200)
                await main_app.send_document(chat_id, sent_msg.document.file_id, caption=caption)
                upload_success = True
            except asyncio.TimeoutError:
                await main_app.send_message(chat_id, "⚠️ **Premium Timeout!**")
                raise Exception("Timeout")
        else:
            raise Exception("No Premium")

    except Exception:
        try:
            await main_app.send_document(chat_id, zip_path, caption=caption)
            upload_success = True
        except Exception as e:
            await main_app.send_message(chat_id, f"❌ Upload Failed: {e}")

    await status_msg.delete()
    
    if upload_success:
        job_progress[chat_id]['part'] += 1
        if highest_id_in_zip > 0:
            await update_checkpoint(chat_id, highest_id_in_zip, job_progress[chat_id]['part'])
        for f in file_paths + [zip_path]:
            try: os.remove(f)
            except: pass
    else:
        try: os.remove(zip_path)
        except: pass
        await asyncio.sleep(5)

async def manager_logic(chat_id, start_override=None):
    job = await get_job(chat_id)
    if not job: return

    stop_event = asyncio.Event()
    stop_events[chat_id] = stop_event
    pause_event = asyncio.Event()
    pause_event.set() 

    target_chat_id = job['target'] 
    await swarm_warmup(chat_id, target_channel=target_chat_id)

    user_dir = os.path.join(WORK_DIR, str(chat_id))
    if not os.path.exists(user_dir): os.makedirs(user_dir, exist_ok=True)
    
    queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    
    # DETERMINE START
    if start_override:
        start_point = start_override
    else:
        start_point = max(job['last_processed_id'] or 1, 1)
        if job['last_processed_id'] and job['last_processed_id'] > 0:
            start_point = job['last_processed_id'] + 1

    job_progress[chat_id] = {
        'scanned': start_point, 'total': job['end_id'], 
        'current_buffer_size': 0, 'part': job['part'],
        'finished_bytes': 0, 'active_count': 0, 
        'highest_id': start_point, 'file_buffer': [], 
        'queue_obj': queue, 'pause_event': pause_event,
        'naming': job['naming']
    }

    fetch_task = asyncio.create_task(
        fetch_worker(main_app, job['target'], start_point, job['end_id'], queue, stop_event, chat_id)
    )

    semaphores = [WeightedSemaphore(BOT_MAX_LOAD) for _ in worker_apps]
    for i, app_inst in enumerate(worker_apps):
        asyncio.create_task(download_dispatcher(
            app_inst, queue, chat_id, target_chat_id, user_dir, semaphores[i], stop_event, pause_event
        ))

    asyncio.create_task(status_monitor(main_app, chat_id, stop_event))

    target_zip_size = job['max_size']

    while not stop_event.is_set():
        await asyncio.sleep(2)
        
        buffer_size = job_progress[chat_id]['current_buffer_size']
        fetch_done = fetch_task.done() and job_progress[chat_id]['active_count'] == 0
        has_files = len(job_progress[chat_id]['file_buffer']) > 0
        
        should_zip = (buffer_size >= target_zip_size) or (fetch_done and has_files)
        
        if should_zip:
            pause_event.clear()
            await zip_and_upload_logic(chat_id, user_dir, target_zip_size)
            pause_event.set()

        if fetch_done and not has_files: 
            break

    if chat_id in stop_events: del stop_events[chat_id]
    await delete_job(chat_id)
    shutil.rmtree(user_dir, ignore_errors=True)
    await main_app.send_message(chat_id, "✅ **Process Finished.**")

def zip_files(file_list, output_path):
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_STORED) as zipf:
        for f in file_list:
            if os.path.exists(f): zipf.write(f, os.path.basename(f))

# --- COMPARE HANDLERS ---
@main_app.on_message(filters.command("compare"))
async def compare_init(c, m):
    # Step 1: Ask for Zip
    setup_state[m.chat.id] = {"step": "compare_zip"}
    await m.reply_text("📂 **Send the Reference Zip (Part 8)**\nI will analyze it to skip duplicates.")

@main_app.on_message(filters.document & filters.private)
async def handle_zip_upload(c, m):
    state = setup_state.get(m.chat.id)
    if not state or state.get("step") != "compare_zip": return

    status = await m.reply_text("⏳ **Analyzing Zip...**")
    
    # Download zip to temp
    zip_path = await m.download()
    
    # Extract file list
    filenames = set()
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            filenames = set(z.namelist())
    except:
        await status.edit_text("❌ Invalid Zip.")
        return
    finally:
        try: os.remove(zip_path)
        except: pass

    # Store for session
    dedup_lists[m.chat.id] = filenames
    
    # Move to next step
    state["step"] = "compare_range"
    setup_state[m.chat.id] = state
    
    await status.edit_text(
        f"✅ **Indexed {len(filenames)} files.**\n"
        f"Now send the Scan Range:\n`StartID-EndID`\n"
        f"(e.g. `110000-126240`)"
    )

@main_app.on_message(filters.regex(r"^\d+-\d+$") & filters.private)
async def handle_range(c, m):
    state = setup_state.get(m.chat.id)
    if not state or state.get("step") != "compare_range": return

    try:
        start_id, end_id = map(int, m.text.split("-"))
    except: return

    # Ensure job exists or create dummy? 
    # User must have started a job before or we need to ask for link?
    # Assuming job exists or we can just update the existing one.
    job = await get_job(m.chat.id)
    if not job:
        await m.reply_text("❌ No active job configuration found. Send channel link first.")
        return

    # Update DB with new range
    # Note: We don't change 'last_processed_id' in DB yet, 
    # we just launch the manager with override.
    
    del setup_state[m.chat.id]
    await m.reply_text(f"🚀 **Resuming Scan: {start_id} -> {end_id}**\nSkipping {len(dedup_lists[m.chat.id])} known files.")
    
    # Launch Manager with Start Override
    asyncio.create_task(manager_logic(m.chat.id, start_override=start_id))

# --- STANDARD HANDLERS ---
@main_app.on_message(filters.command("stop"))
async def stop_command(c, m):
    if m.chat.id in stop_events:
        stop_events[m.chat.id].set()
        await m.reply_text("🛑 **Stopping...**")
    else: await m.reply_text("⚠️ No active process.")

@main_app.on_message(filters.command("start"))
async def start(c, m):
    if await get_job(m.chat.id):
        await m.reply_text("🔄 **Resuming...**")
        asyncio.create_task(manager_logic(m.chat.id))
    else: await m.reply_text(f"🤖 **Swarm Ready.**\nWorkers: {len(worker_apps)}\nSend **Link** to start.")

@main_app.on_message(filters.regex(r"(?:t\.me|telegram\.me|telegram\.dog)/") & filters.private)
async def step1(c, m):
    if await get_job(m.chat.id): return await m.reply_text("⚠️ Busy.")
    target_id, start_id, end_id = parse_link(m.text)
    if not target_id: return await m.reply_text("❌ Bad Link.")
    try: await c.get_chat(target_id)
    except: return await m.reply_text("❌ **Main Bot** cannot access channel.")
    setup_state[m.chat.id] = {"step": "name", "target": target_id, "last_id": start_id, "end_id": end_id}
    await m.reply_text(f"✅ **Link OK.**\nRange: `{start_id}` -> `{end_id}`\nEnter Naming (e.g. `Pack-{{}}`) or `default`.")

@main_app.on_message(filters.text & filters.private & ~filters.regex(r"^/") & ~filters.regex(r"t\.me"))
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
        await m.reply_text("📦 **Max Zip Size (MB)?**")
    elif state["step"] == "size":
        try: sz = int(m.text.strip()) * 1024 * 1024
        except: return await m.reply_text("❌ Number only.")
        await save_job(m.chat.id, state["target"], state["last_id"], state["end_id"], sz, state["naming"])
        del setup_state[m.chat.id]
        asyncio.create_task(manager_logic(m.chat.id))

if __name__ == "__main__":
    if not os.path.exists(WORK_DIR): os.makedirs(WORK_DIR)
    loop = asyncio.get_event_loop()
    async def runner():
        await init_db()
        print("🔥 Starting Swarm...")
        await main_app.start()
        if premium_app: await premium_app.start()
        for w in worker_apps: await w.start()
        print(f"✅ Bots Online.")
        await idle()
        await main_app.stop()
        if premium_app: await premium_app.stop()
        for w in worker_apps: await w.stop()
    loop.run_until_complete(runner())
