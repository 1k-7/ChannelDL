import os
import asyncio
import shutil
import logging
import time
from zipfile import ZipFile
from pyrogram import Client, errors
from pyrogram.types import Message

# ================= CONFIGURATION =================
API_ID = 12345678              # Replace with your API ID
API_HASH = "YOUR_API_HASH"     # Replace with your API Hash
BOT_TOKEN = "YOUR_BOT_TOKEN"   # Replace with your Bot Token

SOURCE_CHANNEL = -100123456789 # The ID of the channel to scan
DUMP_CHANNEL = -100987654321   # Where to send the finished ZIPs (Optional)

START_ID = 1
END_ID = 126240
BATCH_SIZE = 500               # Messages per ZIP
MAX_CONCURRENT_BATCHES = 4     # Max active zipping tasks (prevents VPS crash)
RETRY_DELAY = 5                # Seconds to wait before retrying a failed download

ZIP_NAME_TEMPLATE = "FansMTL-part {}.zip"
LOG_FILE = "scraper.log"

# ================= LOGGING SETUP =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ================= APP INITIALIZATION =================
app = Client(
    "compressor_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# Semaphore to control how many batches are downloading simultaneously
semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)

async def download_file_safe(message: Message, path: str, retries=3):
    """
    Downloads a file with exponential backoff retries.
    Returns True if successful, False otherwise.
    """
    for attempt in range(1, retries + 1):
        try:
            # Check what kind of media it is and get file name
            if not (message.document or message.video or message.audio or message.photo):
                return False

            await app.download_media(message, file_name=path)
            return True
        except errors.FloodWait as e:
            logger.warning(f"FloodWait encountered: Sleeping {e.value}s")
            await asyncio.sleep(e.value)
        except Exception as e:
            logger.error(f"Download failed (Attempt {attempt}/{retries}) for Msg {message.id}: {e}")
            await asyncio.sleep(RETRY_DELAY * attempt)
    
    logger.critical(f"❌ PERMANENTLY FAILED TO DOWNLOAD MSG {message.id}")
    return False

async def process_batch(batch_num: int, messages: list):
    """
    Worker function: Downloads media -> Zips -> Uploads -> Cleans up.
    Protected by a semaphore to prevent overloading the VPS.
    """
    async with semaphore:
        start_time = time.time()
        temp_dir = f"temp_batch_{batch_num}"
        zip_filename = ZIP_NAME_TEMPLATE.format(batch_num)
        
        logger.info(f"⚙️ [Batch {batch_num}] Processing {len(messages)} messages...")
        
        # 1. Prepare Directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        os.makedirs(temp_dir, exist_ok=True)

        downloaded_count = 0
        
        # 2. Download Files
        # We create a sub-task for each file to download them somewhat in parallel 
        # (or sequentially depending on your preference. Sequential is safer for rate limits).
        for msg in messages:
            # Determine filename (fallback to msg_id if no name)
            fname = f"{msg.id}"
            if msg.document and msg.document.file_name:
                fname = f"{msg.id}_{msg.document.file_name}"
            
            save_path = os.path.join(temp_dir, fname)
            
            success = await download_file_safe(msg, save_path)
            if success:
                downloaded_count += 1

        # 3. Create Zip
        if downloaded_count > 0:
            logger.info(f"📦 [Batch {batch_num}] Zipping {downloaded_count} files...")
            try:
                with ZipFile(zip_filename, 'w') as zipf:
                    for root, _, files in os.walk(temp_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            zipf.write(file_path, arcname=file)
                
                logger.info(f"✅ [Batch {batch_num}] Zip created: {zip_filename}")
                
                # 4. Upload Zip (Optional - Uncomment if you want to upload back to Telegram)
                # logger.info(f"⬆️ [Batch {batch_num}] Uploading...")
                # await app.send_document(
                #     chat_id=DUMP_CHANNEL, 
                #     document=zip_filename, 
                #     caption=f"**Batch {batch_num}**\nRange: {messages[0].id} - {messages[-1].id}"
                # )
                
            except Exception as e:
                logger.error(f"❌ [Batch {batch_num}] Failed to zip/upload: {e}")
        else:
            logger.warning(f"⚠️ [Batch {batch_num}] No valid media found. Skipping zip.")

        # 5. Cleanup
        try:
            shutil.rmtree(temp_dir)
            # os.remove(zip_filename) # Uncomment if you uploaded it and want to save space
        except Exception as e:
            logger.error(f"Cleanup error batch {batch_num}: {e}")

        elapsed = time.time() - start_time
        logger.info(f"🏁 [Batch {batch_num}] Finished in {elapsed:.2f}s")

async def main():
    logger.info("🚀 Starting Scraper Bot...")
    async with app:
        
        tasks = []
        batch_counter = 1
        
        # Iterate through the range of IDs
        for i in range(START_ID, END_ID + 1, BATCH_SIZE):
            
            # Calculate the current chunk range
            current_range_end = min(i + BATCH_SIZE, END_ID + 1)
            ids_to_fetch = list(range(i, current_range_end))
            
            logger.info(f"🔍 Fetching Meta: IDs {i} to {current_range_end - 1}")

            batch_messages = []
            
            # Telegram 'get_messages' allows max 200 IDs per call.
            # We must split our 500 batch into sub-chunks of 200.
            API_CHUNK_LIMIT = 200
            
            for j in range(0, len(ids_to_fetch), API_CHUNK_LIMIT):
                chunk = ids_to_fetch[j : j + API_CHUNK_LIMIT]
                try:
                    msgs = await app.get_messages(SOURCE_CHANNEL, chunk)
                    
                    # Filter: Keep only messages that have media
                    for m in msgs:
                        if m and not m.empty and (m.document or m.video or m.audio or m.photo):
                            batch_messages.append(m)
                            
                except errors.FloodWait as e:
                    logger.warning(f"FloodWait during fetch: {e.value}s")
                    await asyncio.sleep(e.value)
                    # Retry the fetch logic for this chunk if needed (simplified here)
                except Exception as e:
                    logger.error(f"Error fetching chunk {chunk[0]}: {e}")

            # If we found media in this batch range, start a background processor
            if batch_messages:
                # asyncio.create_task fires the function in background
                # The 'semaphore' inside the function will handle waiting if too many are running.
                task = asyncio.create_task(
                    process_batch(batch_counter, batch_messages)
                )
                tasks.append(task)
                batch_counter += 1
            else:
                logger.info(f"ℹ️ Batch {batch_counter} (IDs {i}-{current_range_end}) was empty.")

            # CLEANUP TASKS: Check for finished tasks to free memory
            # We don't await them, just remove completed ones from the list
            tasks = [t for t in tasks if not t.done()]
            
            # Tiny sleep to ensure the loop yields control to the event loop
            await asyncio.sleep(0.1)

        logger.info("Waiting for remaining background tasks to finish...")
        await asyncio.gather(*tasks)
        logger.info("🎉 ALL OPERATIONS COMPLETE.")

if __name__ == "__main__":
    app.run(main())
