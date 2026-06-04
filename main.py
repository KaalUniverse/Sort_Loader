import os
import re
import asyncio
import nest_asyncio
import shutil
import uuid
from pathlib import Path
from typing import Optional, Dict, Set, List
from urllib.parse import urlparse, quote
import urllib.request
import urllib.error
import ssl
from datetime import datetime
import json
from dotenv import load_dotenv
import urllib3

# Telegram imports
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from telegram.constants import ChatAction
from telegram.error import TelegramError

# Download utilities
import yt_dlp
from concurrent.futures import ThreadPoolExecutor

# Enable nested event loops
load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
nest_asyncio.apply()

# ============================================================================
# CONFIGURATION
# ============================================================================

def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default

class Config:
    BOT_TOKEN = os.getenv('BOT_TOKEN', '8499255651:AAG71ij2mYfnVIozzehMIq5ugiUG-m7kffQ')
    
    DOWNLOAD_DIR = '/kaggle/working/downloads'
    TEMP_DIR = '/kaggle/working/temp'
    HISTORY_FILE = '/kaggle/working/download_history.json'
    SESSION_FILE = '/kaggle/working/sessions.json'
    TERABOX_COOKIE_FILE = '/kaggle/working/terabox_cookies.json'
    
    MAX_FILE_SIZE_MB = env_int('MAX_DOWNLOAD_SIZE_MB', 4096)
    MAX_FILE_SIZE = MAX_FILE_SIZE_MB * 1024 * 1024
    TELEGRAM_LIMIT_MB = env_int('TELEGRAM_UPLOAD_LIMIT_MB', 48)
    TELEGRAM_LIMIT = TELEGRAM_LIMIT_MB * 1024 * 1024
    SPLIT_CHUNK_MB = max(8, min(TELEGRAM_LIMIT_MB - 3, 45))
    AUTO_COMPRESS_SINGLE = os.getenv('AUTO_COMPRESS_SINGLE', '1').lower() not in ('0', 'false', 'no')
    SINGLE_TARGET_MB = max(8, min(env_int('SINGLE_TARGET_MB', TELEGRAM_LIMIT_MB - 3), TELEGRAM_LIMIT_MB - 1))
    TERABOX_PUBLIC_RESOLVERS = os.getenv(
        'TERABOX_PUBLIC_RESOLVERS',
        'https://terabox.hnn.workers.dev/api?url={encoded_url};'
        'https://terabox.hnn.workers.dev/?url={encoded_url}'
    )
    TERABOX_TERADL_API = os.getenv('TERABOX_TERADL_API', 'https://teradl-api.dapuntaratya.com')
    TERABOX_ALLOW_INSECURE_SSL = os.getenv('TERABOX_ALLOW_INSECURE_SSL', '1').lower() not in ('0', 'false', 'no')
    # External public resolvers are currently unreliable/403/405, so keep them hard-off.
    TERABOX_ENABLE_EXTERNAL_RESOLVERS = False
    
    MAX_WORKERS = 4
    CONNECTION_TIMEOUT = 120
    RETRY_ATTEMPTS = 5
    DOWNLOAD_TIMEOUT_SECONDS = env_int('DOWNLOAD_TIMEOUT_SECONDS', 21600)
    
    QUALITY_PRESETS = {
        '360p':        'bv*[height<=360]+ba/b[height<=360]/best[height<=360]/worst',
        '480p':        'bv*[height<=480]+ba/b[height<=480]/best[height<=480]/best',
        '720p':        'bv*[height<=720]+ba/b[height<=720]/best[height<=720]/best',
        '1080p':       'bv*[height<=1080]+ba/b[height<=1080]/best[height<=1080]/best',
        '1440p':       'bv*[height<=1440]+ba/b[height<=1440]/best[height<=1440]/best',
        '4K':          'bv*[height<=2160]+ba/b[height<=2160]/best[height<=2160]/best',
        '8K':          'bv*[height<=4320]+ba/b[height<=4320]/best[height<=4320]/best',
        'BEST':        'bv*+ba/best',
        'AUDIO_ONLY':  'ba/best',
    }
    
    PLATFORMS = {
        'youtube':   ['youtube.com', 'youtu.be'],
        'instagram': ['instagram.com', 'instagr.am'],
        'facebook':  ['facebook.com', 'fb.watch', 'fb.com'],
        'terabox':   [
            'terabox.com', 'terabox.app', '1024terabox.com',
            '1024tera.com', 'teraboxlink.com', 'teraboxshare.com',
            'teraboxapp.com', 'freeterabox.com',
            'nephobox.com', '4funbox.com', 'mirrobox.com',
            'momerybox.com', 'tibibox.com', 'terasharelink.com',
            'terasharefile.com', 'terafileshare.com'
        ],
        'tiktok':    ['tiktok.com', 'vt.tiktok.com'],
        'twitter':   ['twitter.com', 'x.com'],
        'twitch':    ['twitch.tv', 'clips.twitch.tv'],
        'reddit':    ['reddit.com', 'redd.it'],
        'dailymotion': ['dailymotion.com', 'dai.ly'],
        'vimeo':     ['vimeo.com'],
    }

# ============================================================================
# SESSION MANAGER — stores URL temporarily using short keys
# Solves "Button_data_invalid" (Telegram 64-byte callback limit)
# ============================================================================

class SessionManager:
    """
    Stores URL → short UUID mapping so callback_data stays small.
    e.g.  callback_data = "dl_1080p_a3f9c2b1"  (well within 64 bytes)
    """
    def __init__(self):
        self._store: Dict[str, str] = {}   # token → url
    
    def save_url(self, url: str) -> str:
        """Save URL and return short token"""
        token = uuid.uuid4().hex[:12]
        self._store[token] = url
        return token
    
    def get_url(self, token: str) -> Optional[str]:
        """Get URL by token"""
        return self._store.get(token)
    
    def delete(self, token: str):
        self._store.pop(token, None)

# ============================================================================
# DOWNLOAD HISTORY
# ============================================================================

class DownloadHistory:
    def __init__(self, history_file: str):
        self.file = history_file
        self.data = self._load()
    
    def _load(self) -> Dict:
        if os.path.exists(self.file):
            try:
                with open(self.file, 'r') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}
    
    def save(self):
        os.makedirs(os.path.dirname(self.file), exist_ok=True)
        with open(self.file, 'w') as f:
            json.dump(self.data, f, indent=2)
    
    def add(self, user_id: int, url: str, platform: str, status: str, size: int = 0):
        key = str(user_id)
        if key not in self.data:
            self.data[key] = []
        self.data[key].append({
            'url': url[:80],
            'platform': platform,
            'status': status,
            'size': size,
            'timestamp': datetime.now().isoformat()
        })
        # Keep only last 50 per user
        self.data[key] = self.data[key][-50:]
        self.save()
    
    def get_user_history(self, user_id: int, limit: int = 10) -> List[Dict]:
        return self.data.get(str(user_id), [])[-limit:]
    
    def get_stats(self, user_id: int) -> Dict:
        records = self.data.get(str(user_id), [])
        total = len(records)
        success = sum(1 for r in records if r['status'] == 'SUCCESS')
        total_bytes = sum(r.get('size', 0) for r in records if r['status'] == 'SUCCESS')
        return {'total': total, 'success': success, 'total_bytes': total_bytes}

# ============================================================================
# UTILITIES
# ============================================================================

def setup_environment():
    for d in [Config.DOWNLOAD_DIR, Config.TEMP_DIR]:
        os.makedirs(d, exist_ok=True)

def detect_platform(url: str) -> Optional[str]:
    try:
        domain = urlparse(url).netloc.lower()
        for platform, domains in Config.PLATFORMS.items():
            if any(d in domain for d in domains):
                return platform
    except Exception:
        pass
    return 'generic'

def format_size(b: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if b < 1024.0:
            return f"{b:.1f} {unit}"
        b /= 1024.0
    return f"{b:.1f} TB"

def format_duration(seconds: int) -> str:
    if not seconds:
        return "Unknown"
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

def safe_filename(name: str, max_len: int = 60) -> str:
    name = re.sub(r'[\\/:*?"<>|]', '_', name)
    return name[:max_len]

# ============================================================================
# VIDEO DOWNLOADER
# ============================================================================

class VideoDownloader:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=Config.MAX_WORKERS)
        self.cancelled_users: Set[int] = set()
        self.terabox_cookies: Dict[str, str] = self._load_terabox_cookies()

    def _load_terabox_cookies(self) -> Dict[str, str]:
        if not os.path.exists(Config.TERABOX_COOKIE_FILE):
            return {}
        try:
            with open(Config.TERABOX_COOKIE_FILE, 'r') as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _save_terabox_cookies(self):
        os.makedirs(os.path.dirname(Config.TERABOX_COOKIE_FILE), exist_ok=True)
        with open(Config.TERABOX_COOKIE_FILE, 'w') as f:
            json.dump(self.terabox_cookies, f, indent=2)

    def set_terabox_cookie(self, user_id: int, cookie: str):
        cookie = cookie.strip()
        if 'ndus=' not in cookie:
            raise ValueError("Cookie must include ndus=...")
        if 'lang=' not in cookie:
            cookie = f"lang=en; {cookie}"
        self.terabox_cookies[str(user_id)] = cookie
        self._save_terabox_cookies()

    def clear_terabox_cookie(self, user_id: int):
        self.terabox_cookies.pop(str(user_id), None)
        self._save_terabox_cookies()

    def _get_terabox_cookie(self, user_id: int) -> str:
        return (
            self.terabox_cookies.get(str(user_id), '').strip()
            or os.getenv('TERABOX_COOKIE', '').strip()
        )
    
    def cancel(self, user_id: int):
        self.cancelled_users.add(user_id)
    
    def get_video_info(self, url: str) -> Optional[Dict]:
        if detect_platform(url) == 'terabox':
            return {
                'title': 'TeraBox file',
                'duration': 0,
                'uploader': 'TeraBox',
                'view_count': 0,
                'like_count': 0,
                'formats': 1,
                'thumbnail': '',
            }

        try:
            opts = {'quiet': True, 'no_warnings': True,
                    'socket_timeout': 30, 'skip_download': True,
                    'noplaylist': True}
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                return {
                    'title':    info.get('title', 'Unknown')[:80],
                    'duration': info.get('duration', 0),
                    'uploader': info.get('uploader', 'Unknown')[:40],
                    'view_count': info.get('view_count', 0),
                    'like_count': info.get('like_count', 0),
                    'formats': len(info.get('formats', [])),
                    'thumbnail': info.get('thumbnail', ''),
                }
        except Exception as e:
            print(f"⚠️ Info error: {e}")
            return None
    
    def _build_opts(self, output_path: str, quality: str, platform: str,
                    progress_hook=None) -> dict:
        fmt = Config.QUALITY_PRESETS.get(quality, Config.QUALITY_PRESETS['BEST'])
        
        # Audio-only → m4a
        if quality == 'AUDIO_ONLY':
            opts = {
                'format': fmt,
                'outtmpl': output_path.replace('.%(ext)s', '.%(ext)s'),
                'quiet': False,
                'noplaylist': True,
                'socket_timeout': Config.CONNECTION_TIMEOUT,
                'retries': Config.RETRY_ATTEMPTS,
                'fragment_retries': Config.RETRY_ATTEMPTS,
                'max_filesize': Config.MAX_FILE_SIZE,
                'windowsfilenames': True,
                'trim_file_name': 80,
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '320',
                }],
            }
            return opts
        
        opts = {
            'format': fmt,
            'outtmpl': output_path,
            'quiet': False,
            'no_warnings': False,
            'noplaylist': True,
            'socket_timeout': Config.CONNECTION_TIMEOUT,
            'retries': Config.RETRY_ATTEMPTS,
            'fragment_retries': Config.RETRY_ATTEMPTS,
            'max_filesize': Config.MAX_FILE_SIZE,
            'skip_unavailable_fragments': True,
            'concurrent_fragment_downloads': 4,
            'merge_output_format': 'mp4',
            'geo_bypass': True,
            'windowsfilenames': True,
            'trim_file_name': 80,
            'format_sort': ['res', 'ext:mp4:m4a'],
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        }
        
        if progress_hook:
            opts['progress_hooks'] = [progress_hook]
        
        # Platform tweaks
        if platform == 'tiktok':
            opts['http_headers'] = {
                'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X)'
            }
        elif platform == 'instagram':
            opts['socket_timeout'] = 120
        elif platform == 'twitch':
            opts['format'] = 'best'
        
        return opts

    def _fallback_format(self, quality: str) -> str:
        if quality == 'AUDIO_ONLY':
            return 'ba/best'
        height = {
            '360p': 360,
            '480p': 480,
            '720p': 720,
            '1080p': 1080,
            '1440p': 1440,
            '4K': 2160,
            '8K': 4320,
        }.get(quality)
        if height:
            return f'b[height<={height}]/best[height<={height}]/best'
        return 'best'

    def _instagram_shortcode(self, url: str) -> Optional[str]:
        try:
            parts = [p for p in urlparse(url).path.split('/') if p]
        except Exception:
            return None
        for key in ('p', 'reel', 'reels', 'tv'):
            if key in parts:
                idx = parts.index(key)
                if idx + 1 < len(parts):
                    return parts[idx + 1]
        return None

    def _download_remote_file(self, file_url: str, output_path: str) -> Optional[str]:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Referer': 'https://www.instagram.com/',
        }
        request = urllib.request.Request(file_url, headers=headers)
        with urllib.request.urlopen(request, timeout=Config.CONNECTION_TIMEOUT) as response:
            with open(output_path, 'wb') as f:
                shutil.copyfileobj(response, f)
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            return output_path
        return None

    def _stream_download(self, file_url: str, output_path: str,
                         progress_hook=None, headers: Optional[Dict[str, str]] = None,
                         reject_html: bool = False, min_bytes: int = 0) -> Optional[str]:
        request_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Accept': '*/*',
        }
        if headers:
            request_headers.update(headers)

        request = urllib.request.Request(file_url, headers=request_headers)
        with urllib.request.urlopen(request, timeout=Config.CONNECTION_TIMEOUT) as response:
            content_type = response.headers.get('Content-Type', '').lower()
            if reject_html and any(t in content_type for t in ('text/html', 'text/plain', 'application/json')):
                raise RuntimeError(f"Not a media file: {content_type or 'unknown content type'}")

            total = int(response.headers.get('Content-Length') or 0)
            downloaded = 0
            with open(output_path, 'wb') as f:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    if reject_html and downloaded == 0:
                        head = chunk[:512].lstrip().lower()
                        if head.startswith(b'<!doctype') or head.startswith(b'<html') or b'<html' in head:
                            raise RuntimeError("Resolver returned an HTML/error page, not media")
                    f.write(chunk)
                    downloaded += len(chunk)
                    if progress_hook and total:
                        pct = downloaded / total * 100
                        progress_hook({
                            'status': 'downloading',
                            '_percent_str': f'{pct:.1f}%',
                            '_speed_str': f'{format_size(downloaded)} downloaded',
                            '_eta_str': 'direct',
                        })

        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            final_size = os.path.getsize(output_path)
            if min_bytes and final_size < min_bytes:
                os.remove(output_path)
                raise RuntimeError(f"Downloaded file too small: {format_size(final_size)}")
            return output_path
        return None

    def _load_instagram_session(self, loader):
        username = os.getenv('INSTAGRAM_USERNAME')
        password = os.getenv('INSTAGRAM_PASSWORD')
        session_file = os.getenv('INSTAGRAM_SESSION_FILE')

        if username and session_file and os.path.exists(session_file):
            loader.load_session_from_file(username, session_file)
            return
        if username and password:
            loader.login(username, password)
            if session_file:
                loader.save_session_to_file(session_file)

    def _download_instagram_with_instaloader(self, url: str,
                                             output_dir: str) -> Optional[str]:
        shortcode = self._instagram_shortcode(url)
        if not shortcode:
            return None

        try:
            import instaloader

            loader = instaloader.Instaloader(
                download_videos=True,
                download_video_thumbnails=False,
                download_geotags=False,
                download_comments=False,
                save_metadata=False,
                compress_json=False,
                post_metadata_txt_pattern='',
            )
            self._load_instagram_session(loader)

            post = instaloader.Post.from_shortcode(loader.context, shortcode)
            video_url = post.video_url if post.is_video else None

            if not video_url and post.typename == 'GraphSidecar':
                for node in post.get_sidecar_nodes():
                    if node.is_video and node.video_url:
                        video_url = node.video_url
                        break

            if not video_url:
                return None

            output_path = os.path.join(output_dir, f"instagram_{shortcode}.mp4")
            return self._download_remote_file(video_url, output_path)
        except Exception as exc:
            print(f"⚠️ Instaloader fallback failed: {exc}")
            return None
    
    def _normalize_downloaded_file(self, filepath: str, output_dir: str,
                                   prefix: str, fallback_ext: str = '.mp4') -> Optional[str]:
        if not filepath or not os.path.exists(filepath):
            return None

        ext = os.path.splitext(filepath)[1] or fallback_ext
        safe_path = os.path.join(output_dir, f"{prefix}_{uuid.uuid4().hex[:8]}{ext}")
        if os.path.abspath(filepath) == os.path.abspath(safe_path):
            return filepath
        os.replace(filepath, safe_path)
        return safe_path

    def _terabox_candidate_urls(self, url: str) -> List[str]:
        urls = [url]
        parsed = urlparse(url)
        query = dict(part.split('=', 1) for part in parsed.query.split('&') if '=' in part)
        surl = query.get('surl')
        if surl:
            share_key = surl if surl.startswith('1') else f'1{surl}'
            urls.extend([
                f"https://www.1024tera.com/sharing/link?surl={surl}",
                f"https://www.terabox.com/sharing/link?surl={surl}",
                f"https://www.terabox.app/sharing/link?surl={surl}",
                f"https://1024terabox.com/s/{share_key}",
                f"https://www.1024terabox.com/s/{share_key}",
                f"https://terabox.com/s/{share_key}",
                f"https://www.terabox.com/s/{share_key}",
                f"https://terabox.app/s/{share_key}",
                f"https://www.terabox.app/s/{share_key}",
            ])
        return list(dict.fromkeys(urls))

    def _terabox_public_resolver_urls(self, url: str) -> List[str]:
        resolvers = []
        for candidate_url in self._terabox_candidate_urls(url):
            encoded = quote(candidate_url, safe='')
            for template in Config.TERABOX_PUBLIC_RESOLVERS.split(';'):
                template = template.strip()
                if not template:
                    continue
                resolver_url = template.replace('{encoded_url}', encoded).replace('{url}', candidate_url)
                if resolver_url not in resolvers:
                    resolvers.append(resolver_url)
        return resolvers

    def _extract_direct_links(self, value) -> List[str]:
        keys = {
            'download_link', 'direct_link', 'directLink', 'dlink',
            'downloadUrl', 'download_url', 'url', 'link'
        }
        found = []

        if isinstance(value, dict):
            for key, item in value.items():
                if key in keys and isinstance(item, str) and item.startswith('http'):
                    found.append(item)
                else:
                    found.extend(self._extract_direct_links(item))
        elif isinstance(value, list):
            for item in value:
                found.extend(self._extract_direct_links(item))
        elif isinstance(value, str):
            value = value.strip()
            if value.startswith('http') and '<html' not in value.lower():
                found.append(value)

        cleaned = []
        for link in found:
            if link not in cleaned:
                cleaned.append(link)
        return cleaned

    def _post_json(self, url: str, payload: Dict) -> Dict:
        data = json.dumps(payload).encode('utf-8')
        request = urllib.request.Request(
            url,
            data=data,
            headers={
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            },
            method='POST',
        )
        context = ssl._create_unverified_context() if Config.TERABOX_ALLOW_INSECURE_SSL else None
        try:
            with urllib.request.urlopen(
                request,
                timeout=Config.CONNECTION_TIMEOUT,
                context=context
            ) as response:
                raw = response.read().decode('utf-8', errors='ignore')
        except (ssl.SSLCertVerificationError, urllib.error.URLError) as exc:
            if not Config.TERABOX_ALLOW_INSECURE_SSL:
                raise
            print(f"TeraBox API urllib failed for {url}: {exc}. Retrying with requests verify=False")
            try:
                import requests

                response = requests.post(
                    url,
                    json=payload,
                    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'},
                    timeout=Config.CONNECTION_TIMEOUT,
                    verify=False,
                )
                response.raise_for_status()
                return response.json()
            except Exception as requests_exc:
                raise RuntimeError(f"TeraBox API request failed: {requests_exc}") from requests_exc
        return json.loads(raw)

    def _flatten_terabox_files(self, items: List[Dict]) -> List[Dict]:
        files = []
        for item in items or []:
            if item.get('is_dir'):
                files.extend(self._flatten_terabox_files(item.get('list', [])))
                continue
            files.append(item)
        return files

    def _choose_terabox_file(self, items: List[Dict]) -> Optional[Dict]:
        files = self._flatten_terabox_files(items)
        if not files:
            return None

        video_exts = ('.mp4', '.mkv', '.mov', '.m4v', '.avi', '.webm')
        videos = [
            f for f in files
            if f.get('type') == 'video' or str(f.get('name', '')).lower().endswith(video_exts)
        ]
        candidates = videos or files
        return max(candidates, key=lambda f: int(f.get('size') or 0))

    def _terabox_surl(self, url: str) -> Optional[str]:
        parsed = urlparse(url)
        query = dict(part.split('=', 1) for part in parsed.query.split('&') if '=' in part)
        if query.get('surl'):
            return query['surl'].lstrip('1')
        parts = [p for p in parsed.path.split('/') if p]
        if 's' in parts:
            idx = parts.index('s')
            if idx + 1 < len(parts):
                return parts[idx + 1].lstrip('1')
        return None

    def _terabox_requests_session(self):
        import requests

        session = requests.Session()
        session.verify = False
        session.cookies.set('lang', 'en')
        session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36'
            ),
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        return session

    def _terabox_share_list(self, session, host: str, surl: str,
                            js_token: str, referer: str,
                            remote_dir: str = '') -> Dict:
        params = {
            'app_id': '250528',
            'web': '1',
            'channel': 'dubox',
            'clienttype': '0',
            'shorturl': surl,
            'by': 'name',
            'order': 'asc',
            'num': '20000',
            'page': '1',
        }
        if js_token:
            params['jsToken'] = js_token
        if remote_dir:
            params['dir'] = remote_dir
        else:
            params['root'] = '1'

        response = session.get(
            f'{host}/share/list',
            params=params,
            headers={'Referer': referer},
            timeout=Config.CONNECTION_TIMEOUT,
        )
        if 'text/html' in response.headers.get('Content-Type', '').lower():
            raise RuntimeError(f"share/list returned HTML from {host}")
        return response.json()

    def _terabox_collect_files(self, session, host: str, surl: str,
                               js_token: str, referer: str,
                               remote_dir: str = '') -> List[Dict]:
        payload = self._terabox_share_list(session, host, surl, js_token, referer, remote_dir)
        if payload.get('errno') not in (0, '0', None):
            raise RuntimeError(f"share/list errno={payload.get('errno')} payload={payload}")

        files = []
        for item in payload.get('list', []):
            common = self._terabox_to_common_file(item)
            if common.get('is_dir'):
                folder_path = common.get('path') or item.get('path') or item.get('server_filename') or ''
                if folder_path:
                    files.extend(
                        self._terabox_collect_files(
                            session, host, surl, js_token, referer, folder_path
                        )
                    )
                continue
            files.append(common)
        return files

    def _terabox_js_token(self, session, host: str) -> str:
        try:
            response = session.get(
                f'{host}/main',
                timeout=Config.CONNECTION_TIMEOUT,
                allow_redirects=True,
            )
            text = response.text
            for pattern in (
                r'jsToken["\']?\s*[:=]\s*["\']([^"\']+)',
                r'%28%22([^"%]+)%22%29',
                r'fn%28%22([^"%]+)%22%29',
            ):
                match = re.search(pattern, text)
                if match:
                    return match.group(1)
        except Exception as exc:
            print(f"TeraBox jsToken fetch failed from {host}: {exc}")
        return ''

    def _terabox_to_common_file(self, item: Dict) -> Dict:
        return {
            'is_dir': int(item.get('isdir') or item.get('is_dir') or 0),
            'fs_id': item.get('fs_id'),
            'name': item.get('server_filename') or item.get('name') or 'terabox_file',
            'type': 'video' if int(item.get('category') or 0) == 1 else item.get('type', 'other'),
            'size': int(item.get('size') or 0),
            'path': item.get('path') or item.get('server_filename') or '',
            'dlink': item.get('dlink') or item.get('link') or item.get('download_url') or '',
            'list': [self._terabox_to_common_file(child) for child in item.get('list', [])],
        }

    def _download_terabox_official_public(self, url: str, output_dir: str,
                                          progress_hook=None) -> Optional[str]:
        surl = self._terabox_surl(url)
        if not surl:
            print("TeraBox official resolver: could not extract surl")
            return None

        session = self._terabox_requests_session()
        share_url = f'https://www.terabox.app/sharing/link?surl={surl}'
        hosts = [
            'https://www.terabox.app',
            'https://www.terabox.com',
            'https://www.1024tera.com',
            'https://www.1024terabox.com',
        ]

        for host in hosts:
            try:
                print(f"TeraBox official resolver trying: {host}")
                js_token = self._terabox_js_token(session, host)
                referer = f'{host}/sharing/link?surl={surl}'
                files = self._terabox_collect_files(session, host, surl, js_token, referer)
                selected = self._choose_terabox_file(files)
                if not selected:
                    print(f"TeraBox official resolver found no files from {host}")
                    continue

                if selected.get('size') and selected['size'] > Config.MAX_FILE_SIZE:
                    raise RuntimeError(
                        f"TeraBox file too large: {format_size(selected['size'])} "
                        f"(limit {format_size(Config.MAX_FILE_SIZE)})"
                    )

                links = []
                if selected.get('dlink'):
                    links.append(selected['dlink'])

                if not links and selected.get('path'):
                    meta = session.post(
                        f'{host}/api/filemetas',
                        params={
                            'app_id': '250528',
                            'web': '1',
                            'channel': 'dubox',
                            'clienttype': '0',
                        },
                        data={
                            'dlink': '1',
                            'origin': 'dlna',
                            'target': json.dumps([{
                                'path': selected['path'],
                                'fs_id': selected.get('fs_id'),
                            }]),
                        },
                        headers={
                            'Referer': referer,
                            'Content-Type': 'application/x-www-form-urlencoded',
                        },
                        timeout=Config.CONNECTION_TIMEOUT,
                    )
                    try:
                        meta_payload = meta.json()
                        links.extend(self._extract_direct_links(meta_payload))
                    except Exception:
                        pass

                if not links:
                    source_file = self._download_source_url_fallback(
                        selected, output_dir, progress_hook
                    )
                    if source_file:
                        return source_file

                    stream_file = self._download_terabox_stream(
                        session, host, selected, output_dir, progress_hook
                    )
                    if stream_file:
                        return stream_file
                    print(
                        f"TeraBox official resolver: no direct link/stream for "
                        f"{selected.get('name')} from {host}"
                    )
                    continue

                for direct_link in links:
                    try:
                        name = safe_filename(selected.get('name') or 'terabox', 40)
                        ext = os.path.splitext(urlparse(direct_link).path)[1]
                        if not ext:
                            ext = os.path.splitext(selected.get('name') or '')[1] or '.mp4'
                        output_path = os.path.join(output_dir, f"{name}_{uuid.uuid4().hex[:8]}{ext}")
                        cookie_header = '; '.join(
                            f'{k}={v}' for k, v in session.cookies.get_dict().items()
                        )
                        downloaded = self._stream_download(
                            direct_link,
                            output_path,
                            progress_hook,
                            headers={
                                'Referer': referer,
                                **({'Cookie': cookie_header} if cookie_header else {}),
                            },
                            reject_html=True,
                            min_bytes=512 * 1024,
                        )
                        if downloaded:
                            return downloaded
                    except Exception as exc:
                        print(f"TeraBox official direct link failed: {exc}")
            except Exception as exc:
                print(f"TeraBox official public resolver failed from {host}: {exc}")

        return None

    def _download_terabox_stream(self, session, host: str, selected: Dict,
                                 output_dir: str, progress_hook=None) -> Optional[str]:
        remote_path = selected.get('path')
        if not remote_path:
            return None

        stream_types = [
            'M3U8_AUTO_1080',
            'M3U8_AUTO_720',
            'M3U8_AUTO_480',
            'M3U8_AUTO_360',
            'M3U8_FLV_264_480',
        ]

        for stream_type in stream_types:
            try:
                response = session.post(
                    f'{host}/api/streaming',
                    data={
                        'path': remote_path,
                        'type': stream_type,
                        'vip': '2',
                    },
                    headers={
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'Referer': host,
                    },
                    timeout=Config.CONNECTION_TIMEOUT,
                )
                if 'text/html' in response.headers.get('Content-Type', '').lower():
                    continue

                try:
                    payload = response.json()
                except Exception:
                    payload = response.text

                links = [
                    link for link in self._extract_direct_links(payload)
                    if '.m3u8' in link or 'm3u8' in link.lower()
                ]
                if not links:
                    continue

                for stream_url in links:
                    downloaded = self._download_stream_url(
                        stream_url, selected, output_dir, progress_hook
                    )
                    if downloaded:
                        return downloaded
            except Exception as exc:
                print(f"TeraBox streaming failed ({stream_type}) from {host}: {exc}")

        return None

    def _source_url_from_filename(self, name: str) -> Optional[str]:
        if not name:
            return None

        cleaned = name.strip()
        if cleaned.startswith(('http://', 'https://')):
            return cleaned

        match = re.match(r'^(https?)_{2,3}(.+)$', cleaned, re.IGNORECASE)
        if not match:
            return None

        scheme = match.group(1).lower()
        rest = match.group(2)
        domain_match = re.match(r'^([A-Za-z0-9.-]+\.[A-Za-z]{2,})(?:_|/)(.+)$', rest)
        if not domain_match:
            return None

        domain = domain_match.group(1)
        path = domain_match.group(2).replace('_', '/')
        return f"{scheme}://{domain}/{path}"

    def _download_source_url_fallback(self, selected: Dict, output_dir: str,
                                      progress_hook=None) -> Optional[str]:
        source_url = self._source_url_from_filename(selected.get('name') or '')
        if not source_url:
            return None

        print(f"TeraBox filename source fallback trying: {source_url}")
        parsed = urlparse(source_url)
        ext = os.path.splitext(parsed.path)[1] or os.path.splitext(selected.get('name') or '')[1] or '.mp4'
        output_path = os.path.join(output_dir, f"source_{uuid.uuid4().hex[:8]}{ext}")

        try:
            return self._stream_download(
                source_url,
                output_path,
                progress_hook,
                headers={
                    'Referer': f'{parsed.scheme}://{parsed.netloc}/',
                    'Origin': f'{parsed.scheme}://{parsed.netloc}',
                },
                reject_html=True,
                min_bytes=512 * 1024,
            )
        except Exception as exc:
            print(f"TeraBox filename source fallback failed: {exc}")
            return None

    def _download_stream_url(self, stream_url: str, selected: Dict,
                             output_dir: str, progress_hook=None) -> Optional[str]:
        name = safe_filename(selected.get('name') or 'terabox_stream', 40)
        output_path = os.path.join(output_dir, f"{name}_{uuid.uuid4().hex[:8]}.%(ext)s")

        opts = {
            'format': 'best',
            'outtmpl': output_path,
            'quiet': False,
            'no_warnings': False,
            'merge_output_format': 'mp4',
            'retries': Config.RETRY_ATTEMPTS,
            'fragment_retries': Config.RETRY_ATTEMPTS,
            'concurrent_fragment_downloads': 4,
            'socket_timeout': Config.CONNECTION_TIMEOUT,
            'http_headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'Referer': 'https://www.terabox.com/',
            },
        }
        if progress_hook:
            opts['progress_hooks'] = [progress_hook]

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(stream_url, download=True)
            filename = ydl.prepare_filename(info) if info else None

        if filename:
            base = os.path.splitext(filename)[0]
            for ext in ['.mp4', '.mkv', '.webm', '.mov']:
                candidate = base + ext
                if os.path.exists(candidate) and os.path.getsize(candidate) > 0:
                    return candidate
            if os.path.exists(filename) and os.path.getsize(filename) > 0:
                return filename
        return None

    def _download_terabox_teradl_api(self, url: str, output_dir: str,
                                     progress_hook=None) -> Optional[str]:
        api_base = Config.TERABOX_TERADL_API.rstrip('/')
        if not api_base:
            return None

        try:
            file_payload = None
            for candidate_url in self._terabox_candidate_urls(url):
                candidate_payload = self._post_json(
                    f'{api_base}/generate_file',
                    {'url': candidate_url}
                )
                if candidate_payload.get('status') == 'success':
                    file_payload = candidate_payload
                    break
                print(f"TeraDL generate_file failed: {candidate_payload.get('message', candidate_payload)}")

            if not file_payload:
                return None

            selected = self._choose_terabox_file(file_payload.get('list', []))
            if not selected:
                print("TeraDL returned no downloadable file")
                return None

            size = int(selected.get('size') or 0)
            if size and size > Config.MAX_FILE_SIZE:
                raise RuntimeError(
                    f"TeraBox file too large: {format_size(size)} "
                    f"(limit {format_size(Config.MAX_FILE_SIZE)})"
                )

            link_payload = self._post_json(
                f'{api_base}/generate_link',
                {
                    'uk': file_payload.get('uk'),
                    'shareid': file_payload.get('shareid'),
                    'timestamp': file_payload.get('timestamp'),
                    'sign': file_payload.get('sign'),
                    'fs_id': selected.get('fs_id'),
                }
            )
            if link_payload.get('status') != 'success':
                print(f"TeraDL generate_link failed: {link_payload.get('message', link_payload)}")
                return None

            direct_links = self._extract_direct_links(link_payload.get('download_link', link_payload))
            for direct_link in direct_links:
                try:
                    name = safe_filename(selected.get('name') or 'terabox', 40)
                    ext = os.path.splitext(urlparse(direct_link).path)[1]
                    if not ext:
                        ext = os.path.splitext(selected.get('name') or '')[1] or '.mp4'
                    output_path = os.path.join(output_dir, f"{name}_{uuid.uuid4().hex[:8]}{ext}")
                    downloaded = self._stream_download(
                        direct_link,
                        output_path,
                        progress_hook,
                        headers={'Referer': api_base},
                        reject_html=True,
                        min_bytes=512 * 1024,
                    )
                    if downloaded:
                        return downloaded
                except Exception as exc:
                    print(f"TeraDL direct link failed: {exc}")
        except Exception as exc:
            print(f"TeraDL public API failed: {exc}")

        return None

    def _download_terabox_public(self, url: str, output_dir: str,
                                 progress_hook=None) -> Optional[str]:
        official_file = self._download_terabox_official_public(url, output_dir, progress_hook)
        if official_file:
            return official_file

        print("TeraBox official no-cookie resolver failed; external resolvers are hard-disabled")

        return None

    def _download_terabox(self, url: str, output_dir: str, user_id: int,
                          progress_hook=None) -> Optional[str]:
        public_file = self._download_terabox_public(url, output_dir, progress_hook)
        if public_file:
            return public_file

        cookie = self._get_terabox_cookie(user_id)
        if not cookie:
            raise RuntimeError(
                "No-cookie TeraBox failed. This TeraBox link did not expose a "
                "public direct link; use /setcookie with your own TeraBox cookie."
            )

        try:
            from TeraboxDL import TeraboxDL
        except Exception as exc:
            raise RuntimeError(
                "terabox-downloader package missing. Run: pip install -r requirements.txt"
            ) from exc

        terabox = TeraboxDL(cookie)

        try:
            file_info = terabox.get_file_info(url, direct_url=True)
        except TypeError:
            file_info = terabox.get_file_info(url)

        if isinstance(file_info, dict) and file_info.get('error'):
            raise RuntimeError(file_info['error'])

        size = 0
        if isinstance(file_info, dict):
            size = int(file_info.get('sizebytes') or file_info.get('size_bytes') or 0)
        if size and size > Config.MAX_FILE_SIZE:
            raise RuntimeError(
                f"TeraBox file too large: {format_size(size)} "
                f"(limit {format_size(Config.MAX_FILE_SIZE)})"
            )

        def callback(downloaded, total_size, percentage):
            if not progress_hook:
                return
            progress_hook({
                'status': 'downloading',
                '_percent_str': f'{percentage:.1f}%',
                '_speed_str': f'{format_size(downloaded)} downloaded',
                '_eta_str': 'TeraBox',
            })

        result = terabox.download(file_info, save_path=output_dir, callback=callback)
        if isinstance(result, dict):
            if result.get('error'):
                raise RuntimeError(result['error'])
            filepath = result.get('file_path') or result.get('path')
        else:
            filepath = result

        return self._normalize_downloaded_file(filepath, output_dir, 'terabox')

    async def download(self, url: str, output_dir: str, quality: str,
                       platform: str, user_id: int,
                       progress_hook=None) -> Optional[str]:
        """Core download method"""
        output_path = os.path.join(output_dir, '%(id)s.%(ext)s')
        opts = self._build_opts(output_path, quality, platform, progress_hook)
        
        loop = asyncio.get_running_loop()
        
        def _run_ydl(download_opts):
            if user_id in self.cancelled_users:
                self.cancelled_users.discard(user_id)
                return None
            with yt_dlp.YoutubeDL(download_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                if info:
                    return ydl.prepare_filename(info)
            return None

        def _do_download():
            if platform == 'terabox':
                if user_id in self.cancelled_users:
                    self.cancelled_users.discard(user_id)
                    return None
                return self._download_terabox(url, output_dir, user_id, progress_hook)

            last_error = None
            try:
                return _run_ydl(opts)
            except Exception as exc:
                last_error = exc
                message = str(exc)
                if 'Requested format is not available' not in message:
                    if platform == 'instagram':
                        print("yt-dlp failed for Instagram, trying instaloader fallback")
                        fallback_file = self._download_instagram_with_instaloader(url, output_dir)
                        if fallback_file:
                            return fallback_file
                    raise last_error
                fallback_opts = dict(opts)
                fallback_opts['format'] = self._fallback_format(quality)
                print(f"⚠️ Format unavailable, retrying with fallback: {fallback_opts['format']}")
                try:
                    return _run_ydl(fallback_opts)
                except Exception as fallback_exc:
                    last_error = fallback_exc
                    if platform == 'instagram':
                        print("yt-dlp failed for Instagram, trying instaloader fallback")
                        fallback_file = self._download_instagram_with_instaloader(url, output_dir)
                        if fallback_file:
                            return fallback_file
                    raise last_error
        
        try:
            filename = await asyncio.wait_for(
                loop.run_in_executor(self.executor, _do_download),
                timeout=Config.DOWNLOAD_TIMEOUT_SECONDS
            )
            # yt-dlp may change extension (e.g. .mp4)
            if filename:
                base = os.path.splitext(filename)[0]
                for ext in ['.mp4', '.mkv', '.webm', '.mp3', '.m4a']:
                    candidate = base + ext
                    if os.path.exists(candidate):
                        return candidate
                if os.path.exists(filename):
                    return filename
            return None
        except asyncio.TimeoutError:
            print("⏱️ Download timed out")
        except Exception as e:
            print(f"❌ Download error: {e}")
        return None
    
    async def split_video(self, filepath: str,
                          chunk_mb: int = None) -> List[str]:
        """Split large media into Telegram-safe chunks using ffprobe + ffmpeg."""
        import subprocess
        chunk_mb = chunk_mb or Config.SPLIT_CHUNK_MB
        
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        if size_mb <= chunk_mb:
            return [filepath]
        
        # Get duration via ffprobe
        try:
            result = subprocess.run(
                ['ffprobe', '-v', 'error', '-show_entries',
                 'format=duration', '-of', 'json', filepath],
                capture_output=True, text=True
            )
            duration = float(json.loads(result.stdout)['format']['duration'])
        except Exception:
            return [filepath]
        
        parts = int(size_mb / chunk_mb) + 1
        seg_dur = duration / parts
        base = os.path.splitext(filepath)[0]
        ext = os.path.splitext(filepath)[1] or '.mp4'
        chunks = []
        
        for i in range(parts):
            start = i * seg_dur
            out = f"{base}_part{i+1}{ext}"
            cmd = [
                'ffmpeg', '-y', '-ss', str(start), '-i', filepath,
                '-t', str(seg_dur),
                '-c', 'copy', out
            ]
            subprocess.run(cmd, capture_output=True)
            if os.path.exists(out) and os.path.getsize(out) > 0:
                chunks.append(out)
        
        return chunks if chunks else [filepath]

    async def compress_video_to_single(self, filepath: str,
                                       target_mb: int = None) -> Optional[str]:
        """Re-encode a video so Telegram can receive it as one MP4 file."""
        import subprocess

        target_mb = target_mb or Config.SINGLE_TARGET_MB
        target_bytes = target_mb * 1024 * 1024

        try:
            probe = subprocess.run(
                ['ffprobe', '-v', 'error', '-show_entries',
                 'format=duration', '-of', 'json', filepath],
                capture_output=True, text=True
            )
            duration = float(json.loads(probe.stdout)['format']['duration'])
        except Exception as exc:
            print(f"⚠️ Compress probe failed: {exc}")
            return None

        if duration <= 0:
            return None

        base = os.path.splitext(filepath)[0]
        audio_kbps = 96
        outputs = []

        for attempt, ratio in enumerate([0.92, 0.78, 0.64], 1):
            usable_bytes = int(target_bytes * ratio)
            total_kbps = int((usable_bytes * 8) / duration / 1000)
            video_kbps = max(180, total_kbps - audio_kbps)
            output = f"{base}_single_{attempt}.mp4"
            outputs.append(output)

            cmd = [
                'ffmpeg', '-y', '-i', filepath,
                '-map', '0:v:0', '-map', '0:a?',
                '-c:v', 'libx264', '-preset', 'veryfast',
                '-b:v', f'{video_kbps}k',
                '-maxrate', f'{video_kbps}k',
                '-bufsize', f'{video_kbps * 2}k',
                '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
                '-c:a', 'aac', '-b:a', f'{audio_kbps}k',
                '-movflags', '+faststart',
                output
            ]

            result = await asyncio.to_thread(
                subprocess.run, cmd, capture_output=True, text=True
            )
            if result.returncode != 0:
                print(f"⚠️ Compress attempt {attempt} failed: {result.stderr[-300:]}")
                continue
            if os.path.exists(output) and os.path.getsize(output) <= Config.TELEGRAM_LIMIT:
                for old in outputs:
                    if old != output and os.path.exists(old):
                        os.remove(old)
                return output

        for output in outputs:
            if os.path.exists(output):
                os.remove(output)
        return None

# ============================================================================
# BOT MANAGER
# ============================================================================

class BotManager:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.active_users: Set[int] = set()
        self.downloader = VideoDownloader()
        self.history = DownloadHistory(Config.HISTORY_FILE)
        self.sessions = SessionManager()
        self.default_quality: Dict[int, str] = {}
    
    # ------------------------------------------------------------------
    # FIX #1: add_to_queue now accepts a plain Message object
    # ------------------------------------------------------------------
    async def add_to_queue(self, user_id: int, url: str,
                           message,   # telegram.Message  (NOT Update)
                           quality: str = 'BEST'):
        if user_id in self.active_users:
            await message.reply_text("⚠️ You already have an active download. Please wait.")
            return
        
        self.default_quality[user_id] = quality
        await self.queue.put((user_id, url, message, quality))
        
        pos = self.queue.qsize()
        if pos > 1:
            await message.reply_text(f"⏳ Queued at position #{pos}. Please wait.")
    
    async def worker(self, wid: int):
        print(f"🤖 Worker-{wid} started")
        while True:
            try:
                user_id, url, message, quality = await self.queue.get()
                self.active_users.add(user_id)
                try:
                    await self._process(user_id, url, message, quality)
                finally:
                    self.active_users.discard(user_id)
                    self.queue.task_done()
            except Exception as e:
                print(f"❌ Worker-{wid} error: {e}")
    
    def _is_too_large_error(self, error: Exception) -> bool:
        return 'request entity too large' in str(error).lower()

    async def _send_media_file(self, message, filepath: str, caption: str,
                               is_audio: bool):
        with open(filepath, 'rb') as f:
            if is_audio:
                await message.reply_audio(audio=f, caption=caption)
                return
            try:
                await message.reply_video(
                    video=f, caption=caption, supports_streaming=True
                )
            except TelegramError as te:
                if self._is_too_large_error(te):
                    raise
                f.seek(0)
                await message.reply_document(document=f, caption=caption)

    async def _process(self, user_id: int, url: str, message, quality: str):
        platform = detect_platform(url)
        download_dir = os.path.join(Config.DOWNLOAD_DIR, f"u{user_id}_{uuid.uuid4().hex[:6]}")
        os.makedirs(download_dir, exist_ok=True)
        
        status_msg = None
        filename = None
        
        try:
            # ── Fetch info ──────────────────────────────────────────────
            info = await asyncio.to_thread(self.downloader.get_video_info, url)
            
            if info:
                txt = (
                    f"📺 *{info['title']}*\n"
                    f"⏱ Duration: {format_duration(info['duration'])}\n"
                    f"👤 {info['uploader']}\n"
                    f"👁 {info['view_count']:,} views\n"
                    f"🎯 Quality: `{quality}`\n\n"
                    f"⬇️ Downloading…"
                )
            else:
                txt = f"⬇️ Downloading from *{platform.title()}* (`{quality}`)…"
            
            status_msg = await message.reply_text(txt, parse_mode='Markdown')
            await message.chat.send_action(ChatAction.UPLOAD_VIDEO)
            
            # ── Progress hook ────────────────────────────────────────────
            last_update = [0.0]
            loop = asyncio.get_running_loop()
            
            def progress_hook(d):
                if d['status'] == 'downloading':
                    now = loop.time()
                    if now - last_update[0] < 4:   # throttle to every 4 s
                        return
                    last_update[0] = now
                    pct = d.get('_percent_str', '??%').strip()
                    speed = d.get('_speed_str', '??').strip()
                    eta = d.get('_eta_str', '??').strip()
                    try:
                        loop.call_soon_threadsafe(
                            asyncio.ensure_future,
                            status_msg.edit_text(
                                f"⬇️ Downloading… {pct}\n"
                                f"⚡ Speed: {speed}  |  ⏱ ETA: {eta}\n"
                                f"🎯 Quality: `{quality}`",
                                parse_mode='Markdown'
                            )
                        )
                    except Exception:
                        pass
            
            # ── Download ─────────────────────────────────────────────────
            filename = await self.downloader.download(
                url, download_dir, quality, platform, user_id, progress_hook
            )
            
            if not filename or not os.path.exists(filename):
                await status_msg.edit_text(
                    "❌ Download failed.\n"
                    "Possible reasons: private/geo-restricted video, unsupported format."
                )
                self.history.add(user_id, url, platform, 'FAILED')
                return
            
            file_size = os.path.getsize(filename)
            if file_size == 0:
                await status_msg.edit_text("❌ Downloaded file is empty.")
                self.history.add(user_id, url, platform, 'FAILED')
                return
            
            if file_size > Config.MAX_FILE_SIZE:
                await status_msg.edit_text(
                    f"❌ File too large: {format_size(file_size)} "
                    f"(limit {format_size(Config.MAX_FILE_SIZE)})."
                )
                self.history.add(user_id, url, platform, 'TOO_LARGE', file_size)
                return
            
            # ── Upload ───────────────────────────────────────────────────
            is_audio = quality == 'AUDIO_ONLY'
            
            chunks = [filename]

            if file_size > Config.TELEGRAM_LIMIT:
                if Config.AUTO_COMPRESS_SINGLE and not is_audio:
                    await status_msg.edit_text(
                        "🎞️ File large hai, single video banane ke liye compress kar raha hoon…"
                    )
                    compressed = await self.downloader.compress_video_to_single(
                        filename, Config.SINGLE_TARGET_MB
                    )
                    if compressed:
                        filename = compressed
                        file_size = os.path.getsize(filename)
                        await status_msg.edit_text(
                            f"⬆️ Uploading single video ({format_size(file_size)})…"
                        )
                        cap = (f"✅ Single video • {platform.title()} • {quality} • "
                               f"{format_size(file_size)}")
                        await self._send_media_file(message, filename, cap, is_audio)
                        await status_msg.delete()
                        self.history.add(user_id, url, platform, 'SUCCESS', file_size)
                        print(f"✅ uid={user_id} | {platform} | {quality} | {format_size(file_size)}")
                        return
                    await status_msg.edit_text(
                        "✂️ Single-file compression possible nahi hui, parts bhej raha hoon…"
                    )
                await status_msg.edit_text("✂️ File is large, splitting into parts…")
                chunks = await self.downloader.split_video(filename, Config.SPLIT_CHUNK_MB)
                
                for idx, chunk in enumerate(chunks, 1):
                    if not os.path.exists(chunk):
                        continue
                    chunk_size = os.path.getsize(chunk)
                    cap = (f"✅ Part {idx}/{len(chunks)} • "
                           f"{platform.title()} • {quality} • {format_size(chunk_size)}")
                    await status_msg.edit_text(
                        f"⬆️ Uploading part {idx}/{len(chunks)}…"
                    )
                    await self._send_media_file(message, chunk, cap, is_audio)
                    os.remove(chunk)
            else:
                await status_msg.edit_text(
                    f"⬆️ Uploading {format_size(file_size)}…"
                )
                with open(filename, 'rb') as f:
                    cap = (f"✅ {platform.title()} • {quality} • "
                           f"{format_size(file_size)}")
                    if is_audio:
                        await message.reply_audio(audio=f, caption=cap)
                    else:
                        await message.reply_video(
                            video=f, caption=cap, supports_streaming=True
                        )
            
            await status_msg.delete()
            self.history.add(user_id, url, platform, 'SUCCESS', file_size)
            print(f"✅ uid={user_id} | {platform} | {quality} | {format_size(file_size)}")
        
        except TelegramError as te:
            err = str(te)[:120]
            if status_msg:
                await status_msg.edit_text(f"❌ Telegram error: {err}")
            self.history.add(user_id, url, platform, 'TELEGRAM_ERROR')
            print(f"❌ TelegramError: {te}")
        
        except Exception as e:
            err = str(e)[:120]
            if status_msg:
                await status_msg.edit_text(f"❌ Error: {err}")
            self.history.add(user_id, url, platform, 'ERROR')
            print(f"❌ {e}")
        
        finally:
            shutil.rmtree(download_dir, ignore_errors=True)

# ============================================================================
# GLOBAL BOT MANAGER INSTANCE
# ============================================================================
bot_manager: Optional[BotManager] = None

# ============================================================================
# COMMAND HANDLERS
# ============================================================================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "🎥 *ULTIMATE VIDEO DOWNLOADER BOT v4.6*\n\n"
        "✨ *Supported Platforms:*\n"
        "• YouTube • Instagram • TikTok\n"
        "• Twitter/X • Facebook • Twitch\n"
        "• Reddit • Dailymotion • Vimeo • TeraBox\n\n"
        "🎯 *Quality Options:*\n"
        "360p • 480p • 720p • 1080p • 1440p\n"
        "4K • 8K • BEST • AUDIO ONLY 🎵\n\n"
        "📌 *Commands:*\n"
        "/help — Full guide\n"
        "/quality — Set default quality\n"
        "/history — Recent downloads\n"
        "/stats — Your download stats\n"
        "/cancel — Cancel active download\n\n"
        "👉 Just send a video URL to begin!"
    )
    await update.message.reply_text(msg, parse_mode='Markdown')


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "📚 *HELP GUIDE*\n\n"
        "*How to use:*\n"
        "1️⃣ Send a video URL\n"
        "2️⃣ Pick quality from buttons\n"
        "3️⃣ Bot downloads & sends file\n\n"
        f"*Large Videos (>{Config.TELEGRAM_LIMIT_MB} MB):*\n"
        "Auto-compress to one MP4 first; split only if needed\n\n"
        "*Tips:*\n"
        "💡 360p/480p → faster downloads\n"
        "💡 AUDIO ONLY → music only (MP3)\n"
        "💡 4K/8K → slower but best quality\n\n"
        "*Limits:*\n"
        f"📦 Max size: {format_size(Config.MAX_FILE_SIZE)}\n"
        f"⏱ Timeout: {format_duration(Config.DOWNLOAD_TIMEOUT_SECONDS)}\n"
        f"📤 Telegram limit: {Config.TELEGRAM_LIMIT_MB} MB/file\n\n"
        "*Errors:*\n"
        "❌ Private videos → can't download\n"
        "❌ Geo-restricted → may fail\n"
        "❌ DRM content → not supported"
    )
    await update.message.reply_text(msg, parse_mode='Markdown')


async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    records = bot_manager.history.get_user_history(uid, 10)
    
    if not records:
        await update.message.reply_text("📭 No download history yet.")
        return
    
    lines = ["📊 *Recent Downloads:*\n"]
    for i, r in enumerate(reversed(records), 1):
        emoji = '✅' if r['status'] == 'SUCCESS' else '❌'
        size_str = f" • {format_size(r['size'])}" if r.get('size') else ''
        ts = r['timestamp'][:10]
        lines.append(
            f"{emoji} {i}. *{r['platform'].title()}* — "
            f"`{r['status']}`{size_str} ({ts})"
        )
    
    await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    s = bot_manager.history.get_stats(uid)
    
    msg = (
        f"📈 *Your Download Stats*\n\n"
        f"📥 Total attempts: {s['total']}\n"
        f"✅ Successful: {s['success']}\n"
        f"❌ Failed: {s['total'] - s['success']}\n"
        f"📦 Total downloaded: {format_size(s['total_bytes'])}\n"
        f"🎯 Success rate: "
        f"{(s['success']/s['total']*100):.1f}%" if s['total'] else "No data yet."
    )
    await update.message.reply_text(msg, parse_mode='Markdown')


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid in bot_manager.active_users:
        bot_manager.downloader.cancel(uid)
        await update.message.reply_text("🛑 Cancel requested. Stopping after current segment…")
    else:
        await update.message.reply_text("ℹ️ No active download to cancel.")


async def setcookie_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    cookie = ' '.join(context.args).strip()

    if not cookie:
        await update.message.reply_text(
            "Send your TeraBox cookie like this:\n"
            "`/setcookie lang=en; ndus=YOUR_NDUS_VALUE;`\n\n"
            "Only your own logged-in cookie should be used.",
            parse_mode='Markdown'
        )
        return

    try:
        bot_manager.downloader.set_terabox_cookie(uid, cookie)
        try:
            await update.message.delete()
        except Exception:
            pass
        await update.effective_chat.send_message(
            "✅ TeraBox cookie saved for your account. Now resend the TeraBox link."
        )
    except Exception as exc:
        await update.message.reply_text(f"❌ Cookie not saved: {str(exc)[:120]}")


async def clearcookie_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    bot_manager.downloader.clear_terabox_cookie(uid)
    await update.message.reply_text("✅ Saved TeraBox cookie removed.")


async def quality_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    current = bot_manager.default_quality.get(uid, 'BEST')
    
    keyboard = [
        [InlineKeyboardButton("360p",  callback_data="setq_360p"),
         InlineKeyboardButton("480p",  callback_data="setq_480p"),
         InlineKeyboardButton("720p",  callback_data="setq_720p")],
        [InlineKeyboardButton("1080p", callback_data="setq_1080p"),
         InlineKeyboardButton("1440p", callback_data="setq_1440p"),
         InlineKeyboardButton("4K",    callback_data="setq_4K")],
        [InlineKeyboardButton("8K",    callback_data="setq_8K"),
         InlineKeyboardButton("⭐ BEST", callback_data="setq_BEST"),
         InlineKeyboardButton("🎵 Audio", callback_data="setq_AUDIO_ONLY")],
    ]
    await update.message.reply_text(
        f"🎯 Current default: *{current}*\nSelect new default quality:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

# ============================================================================
# MESSAGE HANDLER — FIX #2: URL stored in SessionManager, not in callback_data
# ============================================================================

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    uid  = update.effective_user.id
    
    url_pattern = r'https?://[^\s]+'
    urls = re.findall(url_pattern, text)
    
    if not urls:
        await update.message.reply_text(
            "❌ No valid URL found.\nPlease send a direct video link."
        )
        return
    
    url   = urls[0]
    token = bot_manager.sessions.save_url(url)   # store URL, get short token
    
    # callback_data format: "dl_{quality}_{token}"
    # max length: 3 + 10 + 1 + 12 = 26 chars → well within 64-byte Telegram limit
    keyboard = [
        [InlineKeyboardButton("360p",      callback_data=f"dl_360p_{token}"),
         InlineKeyboardButton("480p",      callback_data=f"dl_480p_{token}"),
         InlineKeyboardButton("720p",      callback_data=f"dl_720p_{token}")],
        [InlineKeyboardButton("1080p",     callback_data=f"dl_1080p_{token}"),
         InlineKeyboardButton("1440p",     callback_data=f"dl_1440p_{token}"),
         InlineKeyboardButton("4K",        callback_data=f"dl_4K_{token}")],
        [InlineKeyboardButton("8K",        callback_data=f"dl_8K_{token}"),
         InlineKeyboardButton("⭐ BEST",   callback_data=f"dl_BEST_{token}"),
         InlineKeyboardButton("🎵 Audio",  callback_data=f"dl_AUDIO_ONLY_{token}")],
    ]
    
    platform = detect_platform(url)
    await update.message.reply_text(
        f"🔗 *{platform.title()}* link detected.\n🎯 Select download quality:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

# ============================================================================
# CALLBACK HANDLERS
# ============================================================================

async def quality_set_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /quality button press — sets default quality"""
    query = update.callback_query
    await query.answer()
    uid     = query.from_user.id
    quality = query.data.replace("setq_", "")
    bot_manager.default_quality[uid] = quality
    await query.edit_message_text(f"✅ Default quality set to: *{quality}*",
                                  parse_mode='Markdown')


async def download_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle quality-selection button after URL is sent.
    callback_data = "dl_{quality}_{token}"
    FIX: split on '_' max 2 times so quality like 'AUDIO_ONLY' is preserved.
    """
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    
    # e.g. "dl_AUDIO_ONLY_a3f9c2b1ef12"
    # split("_", 2) → ['dl', 'AUDIO', 'ONLY_a3f9c2b1ef12']  ← wrong
    # Better: strip prefix, then rsplit on '_' once to get token from right
    data = query.data[3:]          # strip "dl_"  → "AUDIO_ONLY_a3f9c2b1ef12"
    token   = data.rsplit('_', 1)[1]    # last 12-char hex token
    quality = data.rsplit('_', 1)[0]    # everything before last '_'
    
    url = bot_manager.sessions.get_url(token)
    if not url:
        await query.edit_message_text("❌ Session expired. Please send the URL again.")
        return
    
    bot_manager.sessions.delete(token)   # clean up
    
    await query.delete_message()
    
    # FIX #1 applied here: pass query.message (a Message object), not an Update
    await bot_manager.add_to_queue(uid, url, query.message, quality)

# ============================================================================
# GLOBAL ERROR HANDLER  (new — so errors are logged properly)
# ============================================================================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    print(f"‼️ Unhandled exception: {context.error}")
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                f"⚠️ An unexpected error occurred:\n`{str(context.error)[:200]}`",
                parse_mode='Markdown'
            )
        except Exception:
            pass

# ============================================================================
# MAIN
# ============================================================================

async def main():
    global bot_manager
    
    setup_environment()
    bot_manager = BotManager()
    
    print("=" * 60)
    print("🚀  ULTIMATE VIDEO DOWNLOADER BOT  v4.6")
    print("=" * 60)
    
    app = Application.builder().token(Config.BOT_TOKEN).build()
    
    # Commands
    app.add_handler(CommandHandler("start",   start_command))
    app.add_handler(CommandHandler("help",    help_command))
    app.add_handler(CommandHandler("history", history_command))
    app.add_handler(CommandHandler("stats",   stats_command))
    app.add_handler(CommandHandler("cancel",  cancel_command))
    app.add_handler(CommandHandler("quality", quality_command))
    app.add_handler(CommandHandler("setcookie", setcookie_command))
    app.add_handler(CommandHandler("clearcookie", clearcookie_command))
    
    # Callbacks
    app.add_handler(CallbackQueryHandler(quality_set_callback, pattern=r"^setq_"))
    app.add_handler(CallbackQueryHandler(download_callback,    pattern=r"^dl_"))
    
    # Messages
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Global error handler
    app.add_error_handler(error_handler)
    
    # Start workers
    for i in range(Config.MAX_WORKERS):
        asyncio.create_task(bot_manager.worker(i))
    
    print(f"✅ {Config.MAX_WORKERS} workers started")
    print("🤖 Bot is running…\n")
    
    await app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⛔ Bot stopped.")
    except Exception as e:
        print(f"❌ Fatal: {e}")
