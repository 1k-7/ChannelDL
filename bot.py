import os
import asyncio
import logging
import zipfile
import shutil
import re
import aiosqlite
import time
import glob
import resource
from pyrogram import Client, filters, compose, idle
from pyrogram.errors import (
    FloodWait, 
    PeerIdInvalid, 
    ChannelInvalid, 
    FileReferenceExpired, 
    AuthKeyUnregistered, 
    UserNotParticipant,
    MessageNotModified
)
from dotenv import load_dotenv

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

# --- SYSTEM RESOURCE LIMITS ---
try:
    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    print(f"🖥️ System File Limit: {soft_limit}")
    # Reserve 200 handles for overhead, split rest among tasks
    SAFE_CONCURRENCY = max(10, soft_limit - 200)
except:
    SAFE_CONCURRENCY = 500

USER_SETTING = int(os.getenv("BOT_MAX_LOAD", 100)) 
GLOBAL_MAX_CONCURRENT = min(USER_SETTING, SAFE_CONCURRENCY)

print(f"🔒 Global Concurrency Cap: {GLOBAL_MAX_CONCURRENT} active downloads")

QUEUE_MAX_SIZE = 15000 
TG_MAX_FILE_SIZE = 3990 * 1024 * 1024 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SwarmBot")
logging.getLogger("pyrogram").setLevel(logging.ERROR)

if not os.path.exists(SESSION_DIR):
    os.makedirs(SESSION_DIR)

# --- CLIENT FACTORY ---
def create_client(name, token=None, session=None, is_manager=False):
    # Workers handle multiple concurrents now
    concurrent = 32
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
dedup_store = {} 
stop_events = {}

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
    logger.info("🔥 Warming up workers (Parallel)...")
    status_msg = await main_app.send_message(chat_id, "🛡 **Swarm Warmup...**")
    
    async def check_worker(bot):
        try:
            return await asyncio.wait_for(_safe_resolve(bot, target_channel), timeout=8)
        except: return False

    async def _safe_resolve(bot, target):
        try:
            await bot.resolve_peer(target)
            return True
        except: return False

    results = await asyncio.gather(*[check_worker(bot) for bot in worker_apps])
    success_count = sum(results)
    
    await status_msg.edit_text(f"🛡 **Swarm Warmup:** {success_count}/{len(worker_apps)} workers ready.\n🚀 Userbot Standby: {'Active' if premium_app else 'Inactive'}")
    await asyncio.sleep(2)
    await status_msg.delete()

async def fetch_worker(client, target_chat, start_id, end_id, queue, stop_event, chat_id):
    current = start_id
    print(f"🚀 Fetcher Started: {start_id} -> {end_id}")

    while current <= end_id and not stop_event.is_set():
        batch_end = min(current + 100 - 1, end_id)
        ids = list(range(current, batch_end + 1))
        
        for i in ids:
            await queue.put((i, 0)) 
        
        if chat_id in job_progress:
            job_progress[chat_id]['scanned'] = batch_end
            
        current = batch_end + 1
        # Throttle slightly to prevent queue explosion
        if queue.qsize() > 5000:
            await asyncio.sleep(1)
        else:
            await asyncio.sleep(0.05)
            
    await queue.put(None)
    print("✅ Fetcher: All IDs Queued.")

async def protected_download_task(client_inst, msg_id, chat_id, target_chat_id, user_dir, semaphore, queue, retry_count):
    # This wrapper ensures the semaphore is released no matter what
    try:
        await perform_download(client_inst, msg_id, chat_id, target_chat_id, user_dir, queue, retry_count)
    finally:
        semaphore.release()

async def perform_download(client_inst, msg_id, chat_id, target_chat_id, user_dir, queue, retry_count):
    try:
        if chat_id in job_progress: job_progress[chat_id]['active_count'] += 1
        
        msg = None
        current_downloader = client_inst 

        # 1. Try Worker
        try:
            if not client_inst.is_connected: await client_inst.connect()
            msg = await client_inst.get_messages(target_chat_id, msg_id)
        except Exception: 
            pass 

        # 2. Rescue: Userbot
        if (not msg or msg.empty) and premium_app and premium_app.is_connected:
            try:
                msg = await premium_app.get_messages(target_chat_id, msg_id)
                if msg and not msg.empty:
                    current_downloader = premium_app
            except Exception: pass

        # 3. Validate
        if not msg or msg.empty: return 
        if not msg.media: return 

        # Deduplication
        file_name = None
        if msg.document: file_name = msg.document.file_name
        elif msg.video: file_name = msg.video.file_name
        elif msg.audio: file_name = msg.audio.file_name
        
        if chat_id in dedup_store and file_name and file_name in dedup_store[chat_id]:
            return 

        # 4. Download (With Timeout)
        f_path = None
        try:
            # 20 MINUTE TIMEOUT to prevent Ghost Connections hanging the swarm
            async with asyncio.timeout(1200): 
                f_path = await current_downloader.download_media(msg, file_name=os.path.join(user_dir, ""))
        except asyncio.TimeoutError:
            print(f"⚠️ ID {msg_id} Timed Out. Retrying...")
            await queue.put((msg_id, retry_count + 1))
            return
        except Exception:
            pass # Handle in outer block

        # Integrity Check
        if f_path and os.path.exists(f_path) and os.path.getsize(f_path) > 0:
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
        else:
            if retry_count < 5:
                await queue.put((msg_id, retry_count + 1))
            else:
                print(f"❌ ID {msg_id} failed 5 times.")

    except Exception:
        if retry_count < 5: await queue.put((msg_id, retry_count + 1))
    finally:
        if chat_id in job_progress: job_progress[chat_id]['active_count'] -= 1

async def download_dispatcher(client_inst, queue, chat_id, target_chat_id, user_dir, semaphore, stop_event, pause_event):
    while not stop_event.is_set():
        await pause_event.wait()
        try:
            item = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError: continue
        
        if item is None:
            await queue.put(None)
            break
        
        msg_id, retry_count = item

        # Acquire Global Semaphore
        await semaphore.acquire()
        
        # Fire and Forget (Concurrency restored!)
        asyncio.create_task(protected_download_task(
            client_inst, msg_id, chat_id, target_chat_id, user_dir, semaphore, queue, retry_count
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
    last_time = time.time()

    while not stop_event.is_set():
        try:
            data = job_progress.get(chat_id)
            if not data: break
            
            now = time.time()
            total_now = data['finished_bytes']
            
            time_diff = now - last_time
            if time_diff >= 5.0:
                speed = ((total_now - getattr(status_monitor, "last_bytes", 0)) / time_diff) / (1024 * 1024)
                status_monitor.last_bytes = total_now
                last_time = now
            else:
                speed = getattr(status_monitor, "last_speed", 0.0)
            status_monitor.last_speed = speed

            status_text = "🟢 **DOWNLOADING**" if data['pause_event'].is_set() else "🟠 **UPLOADING**"
            dedup_info = f"\n🚫 Index: `{len(dedup_store.get(chat_id, []))}` files" if chat_id in dedup_store else ""

            text = (
                f"🤖 **Swarm Status:** {status_text}\n"
                f"⚡ DL Speed: `{speed:.2f} MB/s`\n"
                f"🔥 Streams: `{data['active_count']}`\n"
                f"📥 Queue: `{data['queue_obj'].qsize()}`\n"
                f"💾 Buffer: `{(data['current_buffer_size'])/1024/1024:.2f} MB`\n"
                f"🔎 Scanned: `{data['scanned']}`\n"
                f"📦 Part: `{data['part']}`"
                f"{dedup_info}"
            )

            if text != last_text:
                await msg.edit_text(text)
                last_text = text
            await asyncio.sleep(20) 
        except Exception: pass
    await msg.delete()

async def zip_and_upload_logic(chat_id, user_dir, max_zip_size):
    buffer = job_progress[chat_id]['file_buffer']
    buffer.sort(key=lambda x: x['id'])
    
    files_to_zip = []
    current_zip_size = 0
    
    buffer_copy = list(buffer)
    for file_obj in buffer_copy:
        if not os.path.exists(file_obj['path']) or os.path.getsize(file_obj['path']) == 0:
            buffer.remove(file_obj) 
            continue

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
    
    try:
        first_id = files_to_zip[0]['id']
        last_id = files_to_zip[-1]['id']
        caption = f"🗂 **{zip_name}**\n🆔 IDs: `{first_id}`-`{last_id}`"
    except: 
        caption = f"🗂 **{zip_name}**"

    status_msg = await main_app.send_message(chat_id, f"🤐 **Zipping {zip_name}...** ({len(files_to_zip)} files)")
    
    file_paths = [f['path'] for f in files_to_zip]
    highest_id_in_zip = last_id 

    upload_success = False
    sent_msg = None

    try:
        await asyncio.to_thread(zip_files, file_paths, zip_path)
        
        if not os.path.exists(zip_path) or os.path.getsize(zip_path) < 100:
            raise Exception("Zip Integrity Fail (Empty)")

        await status_msg.edit_text(f"⬆️ **Uploading {zip_name}...**")
        
        if premium_app and BOT_USERNAME and premium_app.is_connected:
            try:
                sent_msg = await premium_app.send_document(
                    BOT_USERNAME, zip_path, caption=caption,
                    progress=upload_progress, progress_args=(status_msg, zip_name)
                )
                upload_success = True
            except Exception as e:
                print(f"⚠️ Premium Upload Error: {e}")
                await asyncio.sleep(5)
                # Verification omitted to simplify flow
                if os.path.getsize(zip_path) <= 2000 * 1024 * 1024:
                     raise Exception("Fallback")
                else:
                     raise Exception(f"Upload Failed: {e}")
        else:
            raise Exception("No Premium")

    except Exception:
        try:
            if os.path.exists(zip_path) and os.path.getsize(zip_path) <= 2000 * 1024 * 1024:
                sent_msg = await main_app.send_document(chat_id, zip_path, caption=caption)
                upload_success = True
            else:
                await main_app.send_message(chat_id, "❌ Upload Failed (Too Large / Error)")
        except Exception as e:
            await main_app.send_message(chat_id, f"❌ Critical Failure: {e}")

    await status_msg.delete()
    
    if upload_success:
        if sent_msg and sent_msg.chat.username == BOT_USERNAME:
            try:
                await main_app.send_document(chat_id, sent_msg.document.file_id, caption=caption)
            except:
                try:
                    await main_app.copy_message(chat_id, sent_msg.chat.id, sent_msg.id, caption=caption)
                except:
                    link = f"https://t.me/{BOT_USERNAME}/{sent_msg.id}"
                    await main_app.send_message(chat_id, f"✅ **Done.**\n[Link]({link})", disable_web_page_preview=True)

        job_progress[chat_id]['part'] += 1
        await update_checkpoint(chat_id, highest_id_in_zip, job_progress[chat_id]['part'])
        
        for f in file_paths + [zip_path]:
            try: os.remove(f)
            except: pass
    else:
        try: os.remove(zip_path)
        except: pass
        for f in files_to_zip:
            job_progress[chat_id]['file_buffer'].append(f)
        job_progress[chat_id]['current_buffer_size'] += removed_size
        await asyncio.sleep(10)

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

    global_semaphore = asyncio.Semaphore(GLOBAL_MAX_CONCURRENT)

    for i, app_inst in enumerate(worker_apps):
        asyncio.create_task(download_dispatcher(
            app_inst, queue, chat_id, target_chat_id, user_dir, global_semaphore, stop_event, pause_event
        ))

    asyncio.create_task(status_monitor(main_app, chat_id, stop_event))

    target_zip_size = job['max_size']

    while not stop_event.is_set():
        await asyncio.sleep(2)
        
        buffer_size = job_progress[chat_id]['current_buffer_size']
        fetch_done = fetch_task.done() and job_progress[chat_id]['active_count'] == 0 and queue.empty()
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
            if os.path.exists(f) and os.path.getsize(f) > 0:
                zipf.write(f, os.path.basename(f))

# --- COMPARE HANDLERS ---
@main_app.on_message(filters.command("compare"))
async def compare_init(c, m):
    setup_state[m.chat.id] = {"step": "compare_zip"}
    dedup_store[m.chat.id] = set()
    await m.reply_text("📂 **Deduplication Mode**\nSend your existing ZIP files (one by one).\nWhen done, send `/done`.")

@main_app.on_message(filters.command("done"))
async def compare_done(c, m):
    state = setup_state.get(m.chat.id)
    if not state or state.get("step") != "compare_zip": return
    
    count = len(dedup_store.get(m.chat.id, []))
    del setup_state[m.chat.id]
    await m.reply_text(f"✅ **Index Ready.**\nIgnoring {count} known files.\nNow send `/start`.")

@main_app.on_message(filters.document & filters.private)
async def handle_zip_upload(c, m):
    state = setup_state.get(m.chat.id)
    if not state or state.get("step") != "compare_zip": return

    msg = await m.reply_text("⏳ Indexing...")
    try:
        path = await m.download()
        with zipfile.ZipFile(path, 'r') as z:
            current_set = dedup_store.get(m.chat.id, set())
            current_set.update(z.namelist())
            dedup_store[m.chat.id] = current_set
        os.remove(path)
        await msg.edit_text(f"✅ Indexed `{m.document.file_name}`.\nTotal ignored: {len(dedup_store[m.chat.id])}")
    except Exception as e:
        await msg.edit_text(f"❌ Error: {e}")

# --- STANDARD COMMANDS ---
@main_app.on_message(filters.command("stop"))
async def stop_command(c, m):
    if m.chat.id in stop_events:
        stop_events[m.chat.id].set()
        await m.reply_text("🛑 **Stopping...**")
    else: await m.reply_text("⚠️ No active process.")

@main_app.on_message(filters.command("start"))
async def start(c, m):
    job = await get_job(m.chat.id)
    if job:
        if m.chat.id in dedup_store:
             await update_checkpoint(m.chat.id, 1, job['part'])
             job['last_processed_id'] = 1
             await m.reply_text("🔄 **Starting Repair Scan (ID 1 -> End)...**")
             asyncio.create_task(manager_logic(m.chat.id, start_override=1))
        else:
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
        for w in worker_apps: 
            if not w.is_connected:
                await w.start()
        print(f"✅ Bots Online.")
        
        active_jobs = await get_all_jobs()
        if active_jobs:
            print(f"🔄 Resuming {len(active_jobs)} jobs.")
            for (chat_id,) in active_jobs:
                asyncio.create_task(manager_logic(chat_id))
        
        await idle()
        await main_app.stop()
        if premium_app: await premium_app.stop()
        for w in worker_apps: await w.stop()
    loop.run_until_complete(runner())
