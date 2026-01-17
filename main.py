import os
import re
import asyncio
import nest_asyncio
import shutil
from pathlib import Path
from typing import Optional, Dict, Set
from urllib.parse import urlparse

# Telegram imports
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.constants import ChatAction

# Download utilities
import yt_dlp
import instaloader

# Enable nested event loops (CRITICAL for Kaggle/Jupyter)
nest_asyncio.apply()

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Bot configuration optimized for Kaggle"""
    
    # ⚠️ IMPORTANT: Add your bot token here
    BOT_TOKEN = '8073463553:AAGhNEdg39NrA4eYkSO2aaWmMrEYqN2pvTM' 
    
    # Kaggle-specific paths
    DOWNLOAD_DIR = '/kaggle/working/downloads'
    # REMOVED: Cookie paths are no longer needed
    
    # Telegram limits
    MAX_FILE_SIZE = 200 * 1024 * 1024  # 50MB
    
    # Performance settings
    MAX_WORKERS = 2  # Keep low on Kaggle (limited resources)
    
    # Supported platforms
    PLATFORMS = {
        'youtube': ['youtube.com', 'youtu.be', 'youtube.com/shorts'],
        'instagram': ['instagram.com', 'instagr.am'],
        'facebook': ['facebook.com', 'fb.watch', 'fb.com'],
        'terabox': [
            'terabox.com', '1024terabox.com', '1024tera.com', 
            'teraboxapp.com', 'nephobox.com', '4funbox.com', 'mirrobox.com'
        ]
    }

# ============================================================================
# KAGGLE-SPECIFIC FIXES
# ============================================================================

def cleanup_zombie_tasks():
    """CRITICAL: Kill pending asyncio tasks"""
    try:
        loop = asyncio.get_event_loop()
        pending = asyncio.all_tasks(loop)
        for task in pending:
            if not task.done():
                task.cancel()
        print(f"🧹 Cleaned up {len(pending)} zombie tasks")
    except Exception as e:
        print(f"⚠️ Cleanup warning: {e}")

def setup_kaggle_environment():
    """Setup Kaggle-specific environment (Download dir only)"""
    # Create download directory
    os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)
    # REMOVED: Cookie copying logic

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def detect_platform(url: str) -> Optional[str]:
    """Detect platform from URL"""
    try:
        domain = urlparse(url).netloc.lower()
        for platform, domains in Config.PLATFORMS.items():
            if any(d in domain for d in domains):
                return platform
    except:
        pass
    return None

def format_size(bytes: int) -> str:
    """Format file size"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes < 1024.0:
            return f"{bytes:.2f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.2f} TB"

# ============================================================================
# VIDEO DOWNLOADER (No Cookies)
# ============================================================================

class KaggleVideoDownloader:
    """Optimized video downloader for Kaggle environment (Public only)"""
    
    # REMOVED: init with cookies_file
        
    def get_yt_dlp_options(self, output_path: str, platform: str = 'generic') -> dict:
        """Get yt-dlp options optimized for public downloads"""
        options = {
            'outtmpl': output_path,
            'quiet': False,
            'no_warnings': False,
            'socket_timeout': 60,
            'retries': 5,
            'fragment_retries': 5,
            'nocheckcertificate': True,
            'prefer_insecure': False,
            'concurrent_fragment_downloads': 3,
            'no_check_certificate': True,
            'geo_bypass': True,
            'age_limit': None,
            # Use a generic user agent for most
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }
        
        if platform == 'youtube':
            options['format'] = 'best[ext=mp4][height<=720]/best[ext=mp4]/best'
            options['merge_output_format'] = 'mp4'
            
        elif platform == 'terabox':
            options['format'] = 'best'
            options['extractor_args'] = {'terabox': {'check_formats': None}}
            # Try strong headers for Terabox, though it likely won't work without cookies
            options['http_headers'] = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
                'Referer': 'https://www.terabox.com/',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
            }
        else:
            options['format'] = 'best[ext=mp4]/best'
        
        # REMOVED: Cookie file insertion logic
            
        return options
    
    async def download_youtube(self, url: str, output_dir: str) -> Optional[str]:
        """Download YouTube video"""
        output_path = os.path.join(output_dir, '%(title)s.%(ext)s')
        
        try:
            print(f"🔍 YouTube download started: {url}")
            ydl_opts = self.get_yt_dlp_options(output_path, 'youtube')
            loop = asyncio.get_event_loop()
            
            def download():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    print(f"📥 Extracting info...")
                    info = ydl.extract_info(url, download=False)
                    if info.get('is_live'): raise Exception("Live streams not supported")
                    print(f"⬇️ Downloading...")
                    info = ydl.extract_info(url, download=True)
                    return ydl.prepare_filename(info)
            
            filename = await loop.run_in_executor(None, download)
            
            if filename and os.path.exists(filename) and os.path.getsize(filename) > 0:
                return filename
            
        except Exception as e:
            print(f"❌ YouTube error: {str(e)}")
        return None
    
    async def download_instagram(self, url: str, output_dir: str) -> Optional[str]:
        """Download Instagram (Public only - highly unreliable without cookies)"""
        try:
            # Try yt-dlp first
            output_path = os.path.join(output_dir, '%(id)s.%(ext)s')
            ydl_opts = self.get_yt_dlp_options(output_path)
            loop = asyncio.get_event_loop()
            
            def download():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    return ydl.prepare_filename(info)
            
            try:
                filename = await loop.run_in_executor(None, download)
                if os.path.exists(filename) and os.path.getsize(filename) > 0: return filename
            except: pass
            
            # Fallback to instaloader (will likely fail without login)
            print("⚠️ Instaloader fallback (unreliable without login)")
            shortcode = re.search(r'/(?:p|reel)/([A-Za-z0-9_-]+)', url)
            if shortcode:
                L = instaloader.Instaloader(dirname_pattern=output_dir)
                post = instaloader.Post.from_shortcode(L.context, shortcode.group(1))
                L.download_post(post, target=output_dir)
                for file in os.listdir(output_dir):
                    if file.endswith('.mp4'): return os.path.join(output_dir, file)
                        
        except Exception as e:
            print(f"❌ Instagram error: {e} (Login likely required)")
        return None
    
    async def download_facebook(self, url: str, output_dir: str) -> Optional[str]:
        """Download Facebook video (Public only)"""
        output_path = os.path.join(output_dir, '%(id)s.%(ext)s')
        try:
            ydl_opts = self.get_yt_dlp_options(output_path)
            loop = asyncio.get_event_loop()
            def download():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    return ydl.prepare_filename(info)
            filename = await loop.run_in_executor(None, download)
            if os.path.exists(filename) and os.path.getsize(filename) > 0: return filename
        except Exception as e:
            print(f"❌ Facebook error: {e}")
        return None
    
    async def download_terabox(self, url: str, output_dir: str) -> Optional[str]:
        """Download Terabox video (Highly unlikely to work without cookies)"""
        output_path = os.path.join(output_dir, '%(title)s.%(ext)s')
        try:
            print(f"🔍 Terabox download started (No cookies Mode): {url}")
            
            # Update yt-dlp quietly
            loop = asyncio.get_event_loop()
            try:
                await asyncio.wait_for(loop.run_in_executor(None, lambda: __import__('subprocess').run(['pip', 'install', '-U', 'yt-dlp'], capture_output=True)), timeout=15)
            except: pass
            
            ydl_opts = self.get_yt_dlp_options(output_path, 'terabox')
            
            def download():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    print(f"⬇️ Attempting Terabox download...")
                    info = ydl.extract_info(url, download=True)
                    return ydl.prepare_filename(info)
            
            filename = await loop.run_in_executor(None, download)
            
            if filename and os.path.exists(filename) and os.path.getsize(filename) > 0:
                return filename
            else:
                 print(f"❌ File not found or empty. Cookies probably required.")
                
        except Exception as e:
            print(f"❌ Terabox error: {str(e)} (Cookies probably required)")
        return None

# ============================================================================
# BOT MANAGER
# ============================================================================

class BotManager:
    """Manages download queue and workers"""
    
    # REMOVED: cookies_file argument
    def __init__(self):
        self.download_queue = asyncio.Queue()
        self.active_users: Set[int] = set()
        # REMOVED: passing cookies to downloader
        self.downloader = KaggleVideoDownloader()
        
    async def add_to_queue(self, user_id: int, url: str, update: Update):
        """Add download to queue"""
        if user_id in self.active_users:
            await update.message.reply_text("⚠️ You already have an active download. Please wait.")
            return
        
        await self.download_queue.put((user_id, url, update))
        queue_size = self.download_queue.qsize()
        
        if queue_size > 1:
            await update.message.reply_text(f"⏳ Queued. Position: {queue_size}")
    
    async def worker(self, worker_id: int):
        """Download worker"""
        print(f"🤖 Worker {worker_id} started")
        while True:
            try:
                user_id, url, update = await self.download_queue.get()
                self.active_users.add(user_id)
                await self.process_download(user_id, url, update)
                self.active_users.discard(user_id)
                self.download_queue.task_done()
            except Exception as e:
                print(f"❌ Worker {worker_id} error: {e}")
                self.active_users.discard(user_id)
    
    async def process_download(self, user_id: int, url: str, update: Update):
        """Process download request"""
        platform = detect_platform(url)
        if not platform:
            await update.message.reply_text("❌ Unsupported platform or invalid URL.")
            return
        
        download_dir = os.path.join(Config.DOWNLOAD_DIR, f"user_{user_id}")
        os.makedirs(download_dir, exist_ok=True)
        
        try:
            status_msg = await update.message.reply_text(f"⬇️ Downloading from {platform.title()} (Public Mode)...")
            await update.message.chat.send_action(ChatAction.UPLOAD_VIDEO)
            
            filename = None
            if platform == 'youtube':
                filename = await self.downloader.download_youtube(url, download_dir)
            elif platform == 'instagram':
                filename = await self.downloader.download_instagram(url, download_dir)
            elif platform == 'facebook':
                filename = await self.downloader.download_facebook(url, download_dir)
            elif platform == 'terabox':
                filename = await self.downloader.download_terabox(url, download_dir)
            
            if not filename or not os.path.exists(filename):
                # Updated error message
                await status_msg.edit_text("❌ Download failed. The video is likely private or requires login (Cookies).")
                return
            
            file_size = os.path.getsize(filename)
            if file_size == 0:
                await status_msg.edit_text("❌ Downloaded file is empty.")
                os.remove(filename)
                return
            
            if file_size > Config.MAX_FILE_SIZE:
                await status_msg.edit_text(f"❌ File too large ({format_size(file_size)}). Max: 50MB")
                os.remove(filename)
                return
            
            await status_msg.edit_text(f"⬆️ Uploading... ({format_size(file_size)})")
            with open(filename, 'rb') as video_file:
                await update.message.reply_video(
                    video=video_file,
                    caption=f"✅ From {platform.title()}\n📦 {format_size(file_size)}",
                    supports_streaming=True
                )
            await status_msg.delete()
            os.remove(filename)
            print(f"✅ User {user_id} | {platform} | {format_size(file_size)}")
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")
            print(f"❌ User {user_id} | {platform} | Failed: {e}")
        finally:
            if os.path.exists(download_dir):
                shutil.rmtree(download_dir, ignore_errors=True)

# ============================================================================
# BOT HANDLERS
# ============================================================================

bot_manager = None

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start"""
    msg = """
🎥 ** Video Downloader **

Supported platforms (Public videos only):
🔴 YouTube
🟣 Instagram (Unreliable)
🔵 Facebook (Unreliable)
🟢 Terabox (Very Unreliable without login)

**Note:** Private or age-restricted videos will NOT download.
"""
    await update.message.reply_text(msg, parse_mode='Markdown')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help"""
    msg = """
📚 **MaHaKAAl**



**What will work:**
✅ Public YouTube videos
✅ Some very public Facebook/Instagram videos

**What will NOT work:**
❌ Private/Age-restricted videos
❌ Most Instagram Reels/Stories
❌ Most Terabox links (they require login)

Max size: 200MB
"""
    await update.message.reply_text(msg, parse_mode='Markdown')

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle URLs"""
    message_text = update.message.text.strip()
    user_id = update.effective_user.id
    url_pattern = r'https?://(?:www\.)?[\w\-\.]+\.\w+/[\w\-\._~:/?#[\]@!$&\'()*+,;=]+'
    urls = re.findall(url_pattern, message_text)
    if not urls:
        await update.message.reply_text("❌ No URL found.")
        return
    url = urls[0]
    await bot_manager.add_to_queue(user_id, url, update)

# ============================================================================
# MAIN FUNCTION
# ============================================================================

async def main():
    """Main bot function"""
    global bot_manager
    
    cleanup_zombie_tasks()
    
    # Setup environment (No cookies needed)
    setup_kaggle_environment()
    
    # Initialize bot manager (No cookies passed)
    bot_manager = BotManager()
    
    print("=" * 60)
    print("🚀 KAGGLE VIDEO DOWNLOADER BOT (NO COOKIES MODE)")
    print("=" * 60)
    print("⚠️ NOTE: Only public videos will work.")
    print("⚠️ Instagram/Facebook/Terabox will likely fail.")
    print("=" * 60)
    
    app = Application.builder().token(Config.BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    print(f"🤖 Starting {Config.MAX_WORKERS} workers...")
    for i in range(Config.MAX_WORKERS):
        asyncio.create_task(bot_manager.worker(i))
    
    print("✅ Bot running! Press ■ (Stop) to exit.\n")
    await app.run_polling(allowed_updates=Update.ALL_TYPES)

# ============================================================================
# RUN BOT
# ============================================================================

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⛔ Bot stopped")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
