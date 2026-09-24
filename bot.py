import sys
import re
import time
import sqlite3
import json
import os
import urllib.parse
import requests
from datetime import datetime, timedelta
import threading
import traceback
from typing import Optional, List, Tuple
from http.server import HTTPServer, BaseHTTPRequestHandler

print("=" * 60)
print("🚀 БОТ ПОДПИСОК ЗАПУСКАЕТСЯ (Callback API + Subscriptions Check)...")
print("=" * 60)
sys.stdout.flush()

try:
    import vk_api
    from vk_api.exceptions import ApiError
    print("✅ Библиотека vk-api загружена")
    sys.stdout.flush()
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    sys.stdout.flush()
    raise

# ====================== НАСТРОЙКИ ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ======================
GROUP_TOKEN = os.getenv('GROUP_TOKEN', '')
USER_TOKEN = os.getenv('USER_TOKEN', '')
GROUP_ID = int(os.getenv('GROUP_ID', '241411539'))
CONFIRMATION_CODE = os.getenv('CONFIRMATION_CODE', 'b5c9cb4c')
PORT = int(os.getenv('PORT', '3000'))
ADMIN_IDS_STR = os.getenv('ADMIN_IDS', '447457340')
ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(',') if x.strip()]
DELETE_AFTER = 300  # 5 минут
# =============================================================================

MAX_QUEUE_SIZE = 10
VIP_DURATION_HOURS = 24
RATE_LIMIT_DELAY = 0.5  # Увеличено с 0.34 для избежания ошибок 29
DB_FILE = "subscriptions_bot.db"

queue = []                  # [{'group_id': int, 'link': str, 'user_id': int, 'timestamp': datetime}]
queue_lock = threading.Lock()

vip_groups = []             # [{'group_id': int, 'link': str, 'added_by': int, 'expires_at': datetime}]
vip_groups_lock = threading.Lock()

skip_subscriptions = {}     # {(user_id, group_id): added_at}
skip_subscriptions_lock = threading.Lock()

vk_group = None
vk_user = None

user_activity = {}
activity_lock = threading.Lock()

# Кэш имён пользователей
user_name_cache = {}  # {user_id: (name, timestamp)}
user_name_cache_lock = threading.Lock()
USER_NAME_CACHE_TTL = 3600  # 1 час

pending_deletions = []
deletions_lock = threading.Lock()

VK_API_VERSION = "5.131"


def make_clickable_link(group_link: str) -> str:
    """Превращает club123 в https://vk.com/club123"""
    if not group_link:
        return group_link
    if group_link.startswith('http'):
        return group_link
    return f"https://vk.com/{group_link}"


def is_owner(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def get_user_name(user_id: int) -> str:
    """Получает имя пользователя с кэшированием"""
    global vk_user
    if vk_user is None:
        return ""
    
    now = time.time()
    
    with user_name_cache_lock:
        if user_id in user_name_cache:
            name, ts = user_name_cache[user_id]
            if now - ts < USER_NAME_CACHE_TTL:
                return name
    
    try:
        rate_limit()
        user_info = vk_user.users.get(user_ids=[user_id])[0]
        name = f"{user_info['first_name']} {user_info['last_name']}"
        
        with user_name_cache_lock:
            user_name_cache[user_id] = (name, now)
        
        return name
    except Exception as e:
        print(f"⚠️ Не удалось получить имя пользователя {user_id}: {e}", flush=True)
        return ""


def get_mention(user_id: int) -> str:
    """Возвращает упоминание пользователя в формате ВК"""
    name = get_user_name(user_id)
    if name:
        return f"[id{user_id}|{name}]"
    return f"[id{user_id}|пользователь]"


def get_user_id_by_link(link: str) -> Optional[int]:
    """
    Получает ID пользователя по ссылке на его страницу ВКонтакте.
    """
    global vk_user
    if vk_user is None:
        return None
    
    match = re.search(r'(?:https?://)?(?:m\.)?vk\.(?:com|ru)/([a-zA-Z0-9_.]+)', link)
    if not match:
        return None
    
    username = match.group(1)
    
    if username.lower() in ('wall', 'photo', 'video', 'clip', 'audio', 'topic',
                            'market', 'album', 'poll', 'note', 'doc', 'feed',
                            'im', 'friends', 'groups', 'videos', 'audios', 'photos'):
        return None
    
    # Если это уже ID (id123)
    if username.startswith('id') and username[2:].isdigit():
        return int(username[2:])
    
    # Иначе резолвим через API
    try:
        rate_limit()
        response = vk_user.users.get(user_ids=[username])
        if isinstance(response, list) and len(response) > 0:
            return response[0].get('id')
    except Exception as e:
        print(f"⚠️ Ошибка получения ID: {e}", flush=True)
        return None
    
    return None


def add_skip_subscription(user_id: int, group_id: int):
    """Добавляет исключение: пользователь не проверяется на подписку к сообществу"""
    global skip_subscriptions
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO skip_subscriptions (user_id, group_id, added_at)
            VALUES (?, ?, ?)
        ''', (user_id, group_id, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        with skip_subscriptions_lock:
            skip_subscriptions[(user_id, group_id)] = datetime.now()
        print(f"✅ Исключение добавлено: user={user_id}, group={group_id}", flush=True)
    except Exception as e:
        print(f"⚠️ Ошибка добавления исключения: {e}", flush=True)


def remove_skip_subscription(user_id: int, group_id: int):
    """Удаляет исключение"""
    global skip_subscriptions
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM skip_subscriptions WHERE user_id = ? AND group_id = ?',
                      (user_id, group_id))
        conn.commit()
        conn.close()
        
        with skip_subscriptions_lock:
            skip_subscriptions.pop((user_id, group_id), None)
        print(f"🗑️ Исключение удалено: user={user_id}, group={group_id}", flush=True)
    except Exception as e:
        print(f"⚠️ Ошибка удаления исключения: {e}", flush=True)


def is_skip_subscription(user_id: int, group_id: int) -> bool:
    """Проверяет, есть ли исключение для пользователя и сообщества"""
    with skip_subscriptions_lock:
        return (user_id, group_id) in skip_subscriptions


def init_database():
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL,
                link TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                timestamp TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vip_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL UNIQUE,
                link TEXT NOT NULL,
                added_by INTEGER NOT NULL,
                expires_at TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_activity (
                user_id INTEGER PRIMARY KEY,
                last_post_time TEXT,
                post_count INTEGER DEFAULT 0
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                peer_id INTEGER NOT NULL,
                conv_message_id INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS skip_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                group_id INTEGER NOT NULL,
                added_at TEXT NOT NULL,
                UNIQUE(user_id, group_id)
            )
        ''')
        conn.commit()
        conn.close()
        print("✅ База данных инициализирована")
    except Exception as e:
        print(f"❌ Ошибка БД: {e}")
    sys.stdout.flush()


def load_data():
    global queue, vip_groups, user_activity, pending_deletions, skip_subscriptions
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT group_id, link, user_id, timestamp FROM queue ORDER BY id DESC LIMIT ?', (MAX_QUEUE_SIZE,))
        rows = cursor.fetchall()
        queue = []
        for row in reversed(rows):
            queue.append({
                'group_id': row[0],
                'link': row[1],
                'user_id': row[2],
                'timestamp': datetime.fromisoformat(row[3])
            })

        cursor.execute('SELECT group_id, link, added_by, expires_at FROM vip_groups')
        vip_rows = cursor.fetchall()
        vip_groups = []
        now = datetime.now()
        for row in vip_rows:
            expires_at = datetime.fromisoformat(row[3])
            if expires_at > now:
                vip_groups.append({
                    'group_id': row[0],
                    'link': row[1],
                    'added_by': row[2],
                    'expires_at': expires_at
                })

        cursor.execute('SELECT user_id, last_post_time, post_count FROM user_activity')
        for row in cursor.fetchall():
            user_activity[row[0]] = {
                'last_post_time': datetime.fromisoformat(row[1]) if row[1] else None,
                'post_count': row[2]
            }

        cursor.execute('SELECT peer_id, conv_message_id, created_at FROM bot_messages')
        for row in cursor.fetchall():
            pending_deletions.append({
                'peer_id': row[0],
                'conv_message_id': row[1],
                'created_at': datetime.fromisoformat(row[2])
            })

        cursor.execute('SELECT user_id, group_id, added_at FROM skip_subscriptions')
        skip_subscriptions = {}
        for row in cursor.fetchall():
            skip_subscriptions[(row[0], row[1])] = datetime.fromisoformat(row[2])

        conn.close()
        print(f"📂 Загружено: {len(queue)} сообществ, {len(vip_groups)} VIP, {len(pending_deletions)} на удаление, {len(skip_subscriptions)} исключений")
    except Exception as e:
        print(f"⚠️ Ошибка загрузки: {e}")
    sys.stdout.flush()


def save_bot_message(peer_id: int, conv_message_id: int):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('INSERT INTO bot_messages (peer_id, conv_message_id, created_at) VALUES (?, ?, ?)',
                      (peer_id, conv_message_id, datetime.now().isoformat()))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Ошибка сохранения: {e}")


def remove_bot_message(conv_message_id: int):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM bot_messages WHERE conv_message_id = ?', (conv_message_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Ошибка удаления из БД: {e}")


def save_user_activity(user_id: int):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO user_activity (user_id, last_post_time, post_count)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                last_post_time = excluded.last_post_time,
                post_count = excluded.post_count
        ''', (user_id, datetime.now().isoformat(), user_activity.get(user_id, {}).get('post_count', 0) + 1))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Ошибка сохранения активности: {e}")


def save_queue():
    global queue
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM queue')
        for item in queue:
            cursor.execute('INSERT INTO queue (group_id, link, user_id, timestamp) VALUES (?, ?, ?, ?)',
                          (item['group_id'], item['link'], item['user_id'], item['timestamp'].isoformat()))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Ошибка сохранения очереди: {e}")


def save_vip_groups():
    global vip_groups
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM vip_groups')
        for item in vip_groups:
            cursor.execute('INSERT INTO vip_groups (group_id, link, added_by, expires_at) VALUES (?, ?, ?, ?)',
                          (item['group_id'], item['link'], item['added_by'], item['expires_at'].isoformat()))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Ошибка сохранения VIP: {e}")


def reload_vip_groups():
    global vip_groups
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT group_id, link, added_by, expires_at FROM vip_groups')
        vip_rows = cursor.fetchall()
        vip_groups = []
        now = datetime.now()
        for row in vip_rows:
            expires_at = datetime.fromisoformat(row[3])
            if expires_at > now:
                vip_groups.append({
                    'group_id': row[0],
                    'link': row[1],
                    'added_by': row[2],
                    'expires_at': expires_at
                })
        conn.close()
        print(f"🔄 VIP-сообщества перезагружены: {len(vip_groups)}", flush=True)
    except Exception as e:
        print(f"⚠️ Ошибка перезагрузки VIP: {e}", flush=True)


def reload_queue():
    global queue
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT group_id, link, user_id, timestamp FROM queue ORDER BY id DESC LIMIT ?', (MAX_QUEUE_SIZE,))
        rows = cursor.fetchall()
        queue = []
        for row in reversed(rows):
            queue.append({
                'group_id': row[0],
                'link': row[1],
                'user_id': row[2],
                'timestamp': datetime.fromisoformat(row[3])
            })
        conn.close()
        print(f"🔄 Очередь перезагружена: {len(queue)}", flush=True)
    except Exception as e:
        print(f"⚠️ Ошибка перезагрузки очереди: {e}", flush=True)


def cleanup_expired_vip():
    global vip_groups
    with vip_groups_lock:
        now = datetime.now()
        vip_groups = [v for v in vip_groups if v['expires_at'] > now]
        save_vip_groups()


def cleanup_old_queue():
    global queue
    with queue_lock:
        if len(queue) > MAX_QUEUE_SIZE:
            queue = queue[-MAX_QUEUE_SIZE:]
            save_queue()


def rate_limit():
    time.sleep(RATE_LIMIT_DELAY)


def extract_group_short_name(text: str) -> Optional[str]:
    if not text:
        return None

    text = text.strip()

    forbidden_patterns = [
        r'wall-?\d+_\d+',
        r'photo-?\d+_\d+',
        r'video-?\d+_\d+',
        r'clip-?\d+_\d+',
        r'audio-?\d+_\d+',
        r'topic-?\d+_\d+',
        r'market-?\d+_\d+',
        r'album-?\d+_\d+',
        r'poll-?\d+_\d+',
        r'note-?\d+_\d+',
        r'doc-?\d+_\d+',
    ]
    for pattern in forbidden_patterns:
        if re.search(pattern, text):
            return None

    match = re.search(r'\b(club\d+|public\d+|event\d+)\b', text)
    if match:
        return match.group(1)

    url_match = re.search(r'(?:https?://)?(?:m\.)?vk\.(?:com|ru)/([a-zA-Z0-9_.]+)', text)
    if url_match:
        candidate = url_match.group(1)
        if candidate.lower() in ('wall', 'photo', 'video', 'clip', 'audio', 'topic',
                                 'market', 'album', 'poll', 'note', 'doc', 'feed',
                                 'im', 'friends', 'groups', 'videos', 'audios', 'photos'):
            return None
        return candidate

    return None


def resolve_group(short_name_or_id: str) -> Optional[dict]:
    global vk_group
    if vk_group is None:
        return None

    group_id_param = short_name_or_id

    try:
        rate_limit()
        response = vk_group.groups.getById(group_id=group_id_param)

        if isinstance(response, list) and len(response) > 0:
            group = response[0]
        elif isinstance(response, dict) and 'groups' in response:
            group = response['groups'][0] if response['groups'] else None
        elif isinstance(response, dict):
            group = response
        else:
            return None

        if not group:
            return None

        return {
            'id': group.get('id'),
            'screen_name': group.get('screen_name', ''),
            'name': group.get('name', ''),
            'is_closed': group.get('is_closed', 0)
        }
    except ApiError as e:
        print(f"   ⚠️ VK API ошибка resolve_group: {e}", flush=True)
        return None
    except Exception as e:
        print(f"   ⚠️ Ошибка resolve_group: {e}", flush=True)
        return None


def parse_is_member_response(item) -> Optional[bool]:
    """
    Разбирает ответ от groups.isMember.
    Возвращает:
      True  — точно подписан
      False — точно не подписан
      None  — не удалось проверить (скрытые подписчики, ошибка)
    """
    if item is None:
        return None
    
    # Если это ошибка
    if isinstance(item, dict) and 'error' in item:
        error_code = item['error'].get('error_code')
        print(f"   ⚠️ Ошибка в ответе: {error_code}", flush=True)
        return None
    
    # Если это список (старый формат)
    if isinstance(item, list):
        if len(item) == 0:
            return None
        item = item[0]
    
    # Если это словарь
    if isinstance(item, dict):
        # Если вообще нет ключа 'member' — значит, не смогли проверить
        # (например, у сообщества скрыты подписчики)
        if 'member' not in item:
            print(f"   ⚠️ Ключ 'member' отсутствует (скрытые подписчики?)", flush=True)
            return None
        
        member = item.get('member')
        if member == 1:
            return True
        elif member == 0:
            return False
        else:
            return None
    
    # Если это число
    if isinstance(item, int):
        if item == 1:
            return True
        elif item == 0:
            return False
        else:
            return None
    
    return None


def check_subscriptions_batch(user_id: int, group_ids: list) -> dict:
    """
    Проверяет подписки на несколько сообществ за один запрос через execute.
    Возвращает: {group_id: True/False/None}
      True  — точно подписан
      False — точно не подписан
      None  — не смогли проверить (скрытые подписчики, ошибка, rate limit)
    """
    global vk_group
    if vk_group is None or not group_ids:
        return {}
    
    results = {}
    batch_size = 25  # Максимум 25 запросов за раз
    
    for i in range(0, len(group_ids), batch_size):
        batch = group_ids[i:i + batch_size]
        
        # Формируем код для execute
        code_parts = []
        for gid in batch:
            code_parts.append(f'API.groups.isMember({{"group_id": {gid}, "user_id": {user_id}}})')
        
        code = f'return [{", ".join(code_parts)}];'
        
        try:
            rate_limit()
            result = vk_group.execute(code=code)
            
            if isinstance(result, list):
                for j, gid in enumerate(batch):
                    if j < len(result):
                        item = result[j]
                        parsed = parse_is_member_response(item)
                        results[gid] = parsed
                    else:
                        results[gid] = None
            else:
                for gid in batch:
                    results[gid] = None
                    
        except ApiError as e:
            print(f"   ⚠️ Ошибка execute (batch): {e}", flush=True)
            for gid in batch:
                results[gid] = None
        except Exception as e:
            print(f"   ⚠️ Ошибка сети: {e}", flush=True)
            for gid in batch:
                results[gid] = None
    
    return results


def check_user_subscription(user_id: int, group_id: int) -> Optional[bool]:
    """
    Проверяет подписку одного пользователя на одно сообщество.
    (Используется как fallback)
    """
    global vk_user
    if vk_user is None:
        return None

    if not group_id:
        return True

    try:
        rate_limit()
        response = vk_user.groups.isMember(
            group_id=group_id,
            user_id=user_id
        )

        parsed = parse_is_member_response(response)
        return parsed

    except ApiError as e:
        print(f"   ⚠️ VK API ошибка: {e}", flush=True)
        return None
    except Exception as e:
        print(f"   ⚠️ Ошибка сети: {e}", flush=True)
        return None


def can_user_post(user_id: int) -> bool:
    global queue
    with queue_lock:
        user_posts = [i for i, item in enumerate(queue) if item['user_id'] == user_id]
        if not user_posts:
            return True
        return len(queue) - user_posts[-1] - 1 >= 5


def get_posts_after_user(user_id: int) -> int:
    global queue
    with queue_lock:
        user_posts = [i for i, item in enumerate(queue) if item['user_id'] == user_id]
        if not user_posts:
            return 0
        return len(queue) - user_posts[-1] - 1


def vk_api_request(method: str, params: dict) -> dict:
    url = f"https://api.vk.com/method/{method}"

    params['v'] = VK_API_VERSION
    params['access_token'] = GROUP_TOKEN

    try:
        response = requests.post(url, data=params, timeout=10)
        result = response.json()

        if 'error' in result:
            code = result['error'].get('error_code')
            msg = result['error'].get('error_msg')
            print(f"⚠️ VK API error_code={code}: {msg}", flush=True)
            return {'error': result['error']}

        return result.get('response', {})
    except Exception as e:
        print(f"⚠️ Ошибка запроса к VK API: {e}", flush=True)
        return {'error': str(e)}


def send_message(peer_id: int, text: str) -> Optional[int]:
    global pending_deletions

    try:
        rate_limit()
        random_id = int(time.time() * 1000)

        params = {
            'peer_ids': peer_id,
            'message': text,
            'random_id': random_id,
            'group_id': GROUP_ID
        }

        result = vk_api_request('messages.send', params)

        print(f"✅ Отправлено: {text[:50]}...", flush=True)

        conv_msg_id = None

        if isinstance(result, list) and len(result) > 0:
            conv_msg_id = result[0].get('conversation_message_id')
        elif isinstance(result, dict):
            conv_msg_id = result.get('conversation_message_id')
        elif isinstance(result, int) and result != 0:
            conv_msg_id = result

        if conv_msg_id:
            print(f"📦 Получен conversation_message_id: {conv_msg_id}", flush=True)

            with deletions_lock:
                pending_deletions.append({
                    'peer_id': peer_id,
                    'conv_message_id': conv_msg_id,
                    'created_at': datetime.now()
                })
                save_bot_message(peer_id, conv_msg_id)
            print(f"✅ Сообщение будет удалено через {DELETE_AFTER} секунд", flush=True)
            return conv_msg_id
        else:
            print(f"⚠️ Не удалось получить conversation_message_id", flush=True)
            return None

    except Exception as e:
        print(f"❌ Ошибка отправки: {e}")
        return None


def delete_message_by_conv_id(peer_id: int, conv_message_id: int) -> bool:
    try:
        rate_limit()

        params = {
            'peer_id': peer_id,
            'cmids': conv_message_id,
            'delete_for_all': 1,
            'group_id': GROUP_ID
        }

        result = vk_api_request('messages.delete', params)

        if isinstance(result, dict) and 'error' in result:
            error_msg = str(result['error'])
            if 'message can not be found' in error_msg or 'message not found' in error_msg:
                print(f"⚠️ Сообщение {conv_message_id} уже не существует, удаляем запись", flush=True)
                remove_bot_message(conv_message_id)
                return True

        if isinstance(result, dict):
            key = f"{peer_id}_{conv_message_id}"
            if key in result and result[key] == 1:
                print(f"🗑️ Удалено сообщение {conv_message_id}", flush=True)
                return True
            for k, v in result.items():
                if str(conv_message_id) in k or str(peer_id) in k:
                    if v == 1:
                        print(f"🗑️ Удалено сообщение {conv_message_id}", flush=True)
                        return True
            if result.get('status') == 'ok' or result.get('deleted') == 1:
                print(f"🗑️ Удалено сообщение {conv_message_id}", flush=True)
                return True

        if isinstance(result, int):
            if result == 1:
                print(f"🗑️ Удалено сообщение {conv_message_id}", flush=True)
                return True

        print(f"⚠️ Не удалось удалить сообщение {conv_message_id}", flush=True)
        return False

    except Exception as e:
        print(f"⚠️ Ошибка удаления: {e}", flush=True)
        return False


def cleanup_worker():
    global pending_deletions
    print("🔄 Воркер удаления запущен", flush=True)

    while True:
        try:
            time.sleep(30)

            now = datetime.now()
            to_delete = []

            with deletions_lock:
                remaining = []
                for item in pending_deletions:
                    elapsed = (now - item['created_at']).total_seconds()
                    if elapsed >= DELETE_AFTER:
                        to_delete.append(item)
                    else:
                        remaining.append(item)
                pending_deletions = remaining

            for item in to_delete:
                print(f"🔍 Удаляю сообщение {item['conv_message_id']}...", flush=True)
                if delete_message_by_conv_id(item['peer_id'], item['conv_message_id']):
                    remove_bot_message(item['conv_message_id'])
        except Exception as e:
            print(f"❌ Ошибка воркера: {e}", flush=True)
            time.sleep(5)


def get_inactive_users(peer_id: int) -> str:
    global user_activity
    now = datetime.now()
    inactive = []

    with activity_lock:
        for user_id, data in user_activity.items():
            if data.get('last_post_time'):
                last_post = data['last_post_time']
                days_inactive = (now - last_post).days
                if days_inactive > 10:
                    inactive.append((user_id, days_inactive))
            else:
                inactive.append((user_id, 999))

    if not inactive:
        return "✅ Все участники активны!"

    text = "📋 Неактивные участники (более 10 дней без публикаций):\n\n"
    for user_id, days in inactive:
        try:
            rate_limit()
            user_info = vk_user.users.get(user_ids=[user_id])[0]
            name = f"{user_info['first_name']} {user_info['last_name']}"
            text += f"👤 {name} (ID: {user_id}) — {days} дней\n"
        except:
            text += f"👤 ID: {user_id} — {days} дней\n"

    return text


def handle_admin_commands(text: str, user_id: int, peer_id: int, message_id: int) -> bool:
    global vip_groups, queue

    text_lower = text.lower().strip()

    if not is_owner(user_id):
        if message_id:
            delete_message_by_conv_id(peer_id, message_id)
        send_message(peer_id, "❌ Только владелец чата может использовать команды!")
        return True

    if message_id:
        delete_message_by_conv_id(peer_id, message_id)

    cleanup_expired_vip()

    # ===== КОМАНДА !vip =====
    if text_lower.startswith('!vip '):
        try:
            raw_arg = text.split()[1]
        except IndexError:
            send_message(peer_id, "⚠️ Использование: !vip [ссылка на сообщество]")
            return True

        short_name = extract_group_short_name(raw_arg)
        if not short_name:
            send_message(peer_id, "⚠️ Это не похоже на ссылку на сообщество!")
            return True

        group_info = resolve_group(short_name)
        if not group_info:
            send_message(peer_id, "⚠️ Не удалось найти сообщество!")
            return True

        if group_info['is_closed'] != 0:
            send_message(peer_id, "⚠️ Только открытые сообщества!")
            return True

        gid = group_info['id']
        display_link = f"club{gid}"

        with vip_groups_lock:
            for vip in vip_groups:
                if vip['group_id'] == gid:
                    send_message(peer_id, f"⚠️ Сообщество уже в VIP!")
                    return True
            vip_groups.append({
                'group_id': gid,
                'link': display_link,
                'added_by': user_id,
                'expires_at': datetime.now() + timedelta(hours=VIP_DURATION_HOURS)
            })
            save_vip_groups()
            reload_vip_groups()
        send_message(peer_id, f"⭐ VIP-сообщество добавлено на 24 часа!\n🔗 {make_clickable_link(display_link)}\n📛 {group_info['name']}")
        return True

    # ===== КОМАНДА !delvip =====
    if text_lower.startswith('!delvip'):
        parts = text.split()
        if len(parts) >= 2:
            short_name = extract_group_short_name(parts[1])
            if not short_name:
                send_message(peer_id, "⚠️ Это не похоже на ссылку на сообщество!")
                return True

            group_info = resolve_group(short_name)
            if not group_info:
                send_message(peer_id, "⚠️ Не удалось найти сообщество!")
                return True

            gid = group_info['id']
            with vip_groups_lock:
                initial_count = len(vip_groups)
                vip_groups = [v for v in vip_groups if v['group_id'] != gid]
                removed = initial_count - len(vip_groups)
                save_vip_groups()
                reload_vip_groups()
            if removed > 0:
                send_message(peer_id, f"✅ VIP-сообщество удалено!")
            else:
                send_message(peer_id, f"⚠️ Сообщество не найдено в VIP!")
        else:
            send_message(peer_id, "⚠️ Использование: !delvip [ссылка]")
        return True

    # ===== КОМАНДА !vip_list =====
    if text_lower == '!vip_list':
        with vip_groups_lock:
            if not vip_groups:
                send_message(peer_id, "📭 VIP-сообществ нет")
                return True
            result = "⭐ VIP-сообщества:\n\n"
            now = datetime.now()
            for vip in vip_groups:
                remaining = vip['expires_at'] - now
                hours = int(remaining.total_seconds() // 3600)
                result += f"🔗 {make_clickable_link(vip['link'])}\n⏳ Осталось: {hours}ч\n\n"
            send_message(peer_id, result)
        return True

    # ===== КОМАНДА !inactive =====
    if text_lower == '!inactive':
        inactive_text = get_inactive_users(peer_id)
        send_message(peer_id, inactive_text)
        return True

    # ===== КОМАНДА !delqueue =====
    if text_lower.startswith('!delqueue'):
        parts = text.split()
        if len(parts) >= 2:
            short_name = extract_group_short_name(parts[1])
            if not short_name:
                send_message(peer_id, "⚠️ Это не похоже на ссылку на сообщество!")
                return True

            group_info = resolve_group(short_name)
            if not group_info:
                send_message(peer_id, "⚠️ Не удалось найти сообщество!")
                return True

            gid = group_info['id']
            with queue_lock:
                initial_count = len(queue)
                queue = [item for item in queue if item['group_id'] != gid]
                removed_count = initial_count - len(queue)
                save_queue()
                reload_queue()

            if removed_count > 0:
                send_message(peer_id, f"✅ Сообщество удалено из очереди ({removed_count} шт.)!")
            else:
                send_message(peer_id, f"⚠️ Сообщество не найдено в очереди!")
        else:
            send_message(peer_id, "⚠️ Использование: !delqueue [ссылка]")
        return True

    # ===== КОМАНДА !clearqueue =====
    if text_lower == '!clearqueue':
        with queue_lock:
            count = len(queue)
            queue = []
            save_queue()
            reload_queue()
        send_message(peer_id, f"✅ Очередь полностью очищена! (удалено {count})")
        return True

    # ===== КОМАНДА !queue_list =====
    if text_lower == '!queue_list':
        with queue_lock:
            if not queue:
                send_message(peer_id, "📭 Очередь пустая")
                return True
            result = "📋 Очередь сообществ:\n\n"
            for i, item in enumerate(queue, 1):
                result += f"{i}. 🔗 {make_clickable_link(item['link'])}\n"
            send_message(peer_id, result)
        return True

    # ===== КОМАНДА !skip =====
    if text_lower.startswith('!skip '):
        parts = text.split()
        if len(parts) < 3:
            send_message(peer_id, "⚠️ Использование: !skip [ссылка на участника] [ссылка на сообщество]\n\nПример: !skip vk.com/id123456789 vk.com/club241426297")
            return True

        user_link = parts[1]
        group_link = parts[2]

        target_user_id = get_user_id_by_link(user_link)
        if not target_user_id:
            send_message(peer_id, "❌ Не удалось найти пользователя по ссылке!")
            return True

        short_name = extract_group_short_name(group_link)
        if not short_name:
            send_message(peer_id, "❌ Не удалось распознать ссылку на сообщество!")
            return True

        group_info = resolve_group(short_name)
        if not group_info:
            send_message(peer_id, "❌ Не удалось найти сообщество!")
            return True

        gid = group_info['id']
        add_skip_subscription(target_user_id, gid)

        user_mention = get_mention(target_user_id)
        send_message(peer_id, f"✅ Исключение добавлено!\n\n👤 Участник: {user_mention}\n📛 Сообщество: {group_info['name']}\n\nТеперь этот участник может пропустить проверку подписки на это сообщество.")
        return True

    # ===== КОМАНДА !unskip =====
    if text_lower.startswith('!unskip '):
        parts = text.split()
        if len(parts) < 3:
            send_message(peer_id, "⚠️ Использование: !unskip [ссылка на участника] [ссылка на сообщество]")
            return True

        user_link = parts[1]
        group_link = parts[2]

        target_user_id = get_user_id_by_link(user_link)
        if not target_user_id:
            send_message(peer_id, "❌ Не удалось найти пользователя по ссылке!")
            return True

        short_name = extract_group_short_name(group_link)
        if not short_name:
            send_message(peer_id, "❌ Не удалось распознать ссылку на сообщество!")
            return True

        group_info = resolve_group(short_name)
        if not group_info:
            send_message(peer_id, "❌ Не удалось найти сообщество!")
            return True

        gid = group_info['id']
        remove_skip_subscription(target_user_id, gid)

        user_mention = get_mention(target_user_id)
        send_message(peer_id, f"✅ Исключение удалено!\n\n👤 Участник: {user_mention}\n📛 Сообщество: {group_info['name']}\n\nТеперь участник снова проверяется на подписку.")
        return True

    # ===== КОМАНДА !skip_list =====
    if text_lower == '!skip_list':
        with skip_subscriptions_lock:
            if not skip_subscriptions:
                send_message(peer_id, "📭 Исключений нет")
                return True
            result = "📋 Активные исключения:\n\n"
            for (uid, gid) in skip_subscriptions.keys():
                user_mention = get_mention(uid)
                result += f"👤 {user_mention} → 🔗 club{gid}\n"
            send_message(peer_id, result)
        return True

    return False


def process_message(peer_id: int, user_id: int, text: str, message_id: int, event_id: str = ""):
    global queue

    print(f"\n📩 {user_id}: {text[:80]}", flush=True)
    sys.stdout.flush()

    if user_id < 0:
        return

    text_lower = text.lower().strip()

    command_prefixes = ['!vip', '!delvip', '!inactive', '!delqueue', '!clearqueue', '!queue_list', '!skip', '!unskip', '!skip_list']
    is_command = any(text_lower.startswith(cmd) for cmd in command_prefixes)

    if is_command:
        handle_admin_commands(text, user_id, peer_id, message_id)
        return

    # Получаем упоминание пользователя
    mention = get_mention(user_id)

    # === ПУБЛИКАЦИЯ СООБЩЕСТВА АДМИНИСТРАТОРОМ ===
    if is_owner(user_id):
        short_name = extract_group_short_name(text)
        if not short_name:
            return

        group_info = resolve_group(short_name)
        if not group_info or group_info['is_closed'] != 0:
            send_message(peer_id, f"{mention}, ⚠️ публикуем только открытые сообщества!")
            return

        gid = group_info['id']
        display_link = f"club{gid}"

        with queue_lock:
            queue.append({
                'group_id': gid,
                'link': display_link,
                'user_id': user_id,
                'timestamp': datetime.now()
            })
            if len(queue) > MAX_QUEUE_SIZE:
                queue.pop(0)
            save_queue()
        send_message(peer_id, f"{mention}, ✅ сообщество опубликовано!\n🔗 {make_clickable_link(display_link)}\n📛 {group_info['name']}")
        return

    # === ДЛЯ ОБЫЧНЫХ ПОЛЬЗОВАТЕЛЕЙ ===

    stripped = text.strip()
    short_name = extract_group_short_name(stripped)

    if not short_name:
        if message_id:
            delete_message_by_conv_id(peer_id, message_id)
        send_message(peer_id, f"{mention}, 🔗 публикуем только ссылки на ОТКРЫТЫЕ сообщества!\n\nПример: vk.com/club123 или vk.com/public123\n\n💎 По вопросам и для покупки VIP — пишите: https://vk.com/id1121274330")
        return

    base_patterns = [
        short_name,
        f"https://vk.com/{short_name}",
        f"https://vk.ru/{short_name}",
        f"http://vk.com/{short_name}",
        f"http://vk.ru/{short_name}",
        f"vk.com/{short_name}",
        f"vk.ru/{short_name}",
        f"m.vk.com/{short_name}",
        f"m.vk.ru/{short_name}",
        f"https://m.vk.com/{short_name}",
        f"https://m.vk.ru/{short_name}",
    ]
    if stripped not in base_patterns:
        if message_id:
            delete_message_by_conv_id(peer_id, message_id)
        send_message(peer_id, f"{mention}, 🔗 сообщение должно содержать ТОЛЬКО ссылку на сообщество!\n\n💎 По вопросам и для покупки VIP — пишите: https://vk.com/id1121274330")
        return

    group_info = resolve_group(short_name)
    if not group_info:
        if message_id:
            delete_message_by_conv_id(peer_id, message_id)
        send_message(peer_id, f"{mention}, 🔗 публикуем только ссылки на ОТКРЫТЫЕ сообщества!\n\nЛичные страницы, посты, фото и видео — не принимаются.\n\n💎 По вопросам и для покупки VIP — пишите: https://vk.com/id1121274330")
        return

    if group_info['is_closed'] != 0:
        if message_id:
            delete_message_by_conv_id(peer_id, message_id)
        send_message(peer_id, f"{mention}, 🔗 публикуем только ОТКРЫТЫЕ сообщества!\n\nЗакрытые и частные сообщества не принимаются.\n\n💎 По вопросам и для покупки VIP — пишите: https://vk.com/id1121274330")
        return

    gid = group_info['id']
    display_link = f"club{gid}"

    if not can_user_post(user_id):
        need = max(0, 5 - get_posts_after_user(user_id))
        if message_id:
            delete_message_by_conv_id(peer_id, message_id)
        send_message(peer_id, f"{mention}, ⏳ ждем Вас через {need} сообществ!\n\n💎 По вопросам и для покупки VIP — пишите: https://vk.com/id1121274330")
        return

    # ===== ПРОВЕРКА VIP-СООБЩЕСТВ (пакетно через execute) =====
    cleanup_expired_vip()

    with vip_groups_lock:
        if vip_groups:
            # Собираем group_ids, исключая те, для которых есть skip
            vip_group_ids = []
            for vip in vip_groups:
                if is_skip_subscription(user_id, vip['group_id']):
                    print(f"   ⏭️ Пропуск проверки (исключение) для VIP club{vip['group_id']}", flush=True)
                    continue
                vip_group_ids.append(vip['group_id'])

            if vip_group_ids:
                # Пакетная проверка
                vip_subscriptions = check_subscriptions_batch(user_id, vip_group_ids)
                
                missing_vip = []
                for vip in vip_groups:
                    if is_skip_subscription(user_id, vip['group_id']):
                        continue
                    subscribed = vip_subscriptions.get(vip['group_id'])
                    if subscribed is False:
                        missing_vip.append(vip)
                    elif subscribed is None:
                        print(f"   ⏭️ Пропуск (не удалось проверить) VIP club{vip['group_id']}", flush=True)
                    # Если True — пропускаем

                if missing_vip:
                    if message_id:
                        delete_message_by_conv_id(peer_id, message_id)
                    text = f"{mention}, ⭐ обязательно подпишись на VIP-сообщества:\n\n"
                    for vip in missing_vip:
                        text += f"⭐ {make_clickable_link(vip['link'])}\n"
                    text += f"\n{'─' * 30}\n"
                    text += "⏳ На выполнение даётся 5 минут!\n"
                    text += "✅ После того, как подпишешься, отправь свою ссылку снова.\n\n"
                    text += "💎 По вопросам и для покупки VIP — пишите: https://vk.com/id1121274330"
                    send_message(peer_id, text)
                    return

    # ===== ПРОВЕРКА ОБЫЧНЫХ СООБЩЕСТВ (пакетно через execute) =====
    with queue_lock:
        regular_groups = [item for item in queue[-10:]]

    if regular_groups:
        # Собираем group_ids, исключая те, для которых есть skip
        regular_group_ids = []
        for item in regular_groups:
            if is_skip_subscription(user_id, item['group_id']):
                print(f"   ⏭️ Пропуск проверки (исключение) для club{item['group_id']}", flush=True)
                continue
            regular_group_ids.append(item['group_id'])

        if regular_group_ids:
            # Пакетная проверка
            regular_subscriptions = check_subscriptions_batch(user_id, regular_group_ids)
            
            missing_regular = []
            for item in regular_groups:
                if is_skip_subscription(user_id, item['group_id']):
                    continue
                subscribed = regular_subscriptions.get(item['group_id'])
                if subscribed is False:
                    missing_regular.append(item)
                elif subscribed is None:
                    print(f"   ⏭️ Пропуск (не удалось проверить) club{item['group_id']}", flush=True)
                # Если True — пропускаем

            if missing_regular:
                if message_id:
                    delete_message_by_conv_id(peer_id, message_id)
                text = f"{mention}, 📋 обязательно подпишись на предыдущие 10 сообществ:\n\n"
                for item in missing_regular:
                    text += f"▫️ {make_clickable_link(item['link'])}\n"
                text += f"\n{'─' * 30}\n"
                text += "⏳ На выполнение даётся 5 минут!\n"
                text += "✅ После того, как подпишешься, отправь свою ссылку снова.\n\n"
                text += "💎 По вопросам и для покупки VIP — пишите: https://vk.com/id1121274330"
                send_message(peer_id, text)
                return

    # ========== ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ - ПУБЛИКУЕМ ==========

    with queue_lock:
        queue.append({
            'group_id': gid,
            'link': display_link,
            'user_id': user_id,
            'timestamp': datetime.now()
        })
        if len(queue) > MAX_QUEUE_SIZE:
            queue.pop(0)
        save_queue()

    with activity_lock:
        user_activity[user_id] = {
            'last_post_time': datetime.now(),
            'post_count': user_activity.get(user_id, {}).get('post_count', 0) + 1
        }
    save_user_activity(user_id)

    text = f"{mention}, ✅ ваше сообщество опубликовано!\n🔗 {make_clickable_link(display_link)}\n📛 {group_info['name']}\n📊 В очереди: {len(queue)}\n\n"
    text += "⏳ Ждем Вас через 5 сообществ!\n\n"
    text += "💎 По вопросам и для покупки VIP — пишите: https://vk.com/id1121274330"
    send_message(peer_id, text)
    print(f"   ✅ Опубликовано!", flush=True)


class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = CONFIRMATION_CODE.encode() if self.path in ['/', '/callback'] else b'Bot is running'
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)

        try:
            data = json.loads(body)
            event_type = data.get('type', '')
            print(f"📥 Событие: {event_type}", flush=True)

            if event_type == 'confirmation':
                rb = CONFIRMATION_CODE.encode()
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain')
                self.send_header('Content-Length', str(len(rb)))
                self.end_headers()
                self.wfile.write(rb)

            elif event_type == 'message_new':
                msg = data.get('object', {}).get('message', {})

                action = msg.get('action', {})
                if action and action.get('type') in ['chat_invite_user', 'chat_invite_user_by_link']:
                    rb = b'ok'
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/plain')
                    self.send_header('Content-Length', str(len(rb)))
                    self.end_headers()
                    self.wfile.write(rb)
                    return

                event_id = data.get('event_id', '')
                thread = threading.Thread(target=process_message, args=(
                    msg.get('peer_id', 0),
                    msg.get('from_id', 0),
                    msg.get('text', ''),
                    msg.get('conversation_message_id', msg.get('id', 0)),
                    event_id
                ), daemon=True)
                thread.start()

                rb = b'ok'
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain')
                self.send_header('Content-Length', str(len(rb)))
                self.end_headers()
                self.wfile.write(rb)

            else:
                rb = b'ok'
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain')
                self.send_header('Content-Length', str(len(rb)))
                self.end_headers()
                self.wfile.write(rb)
        except Exception as e:
            print(f"❌ Ошибка: {e}", flush=True)
            rb = b'ok'
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.send_header('Content-Length', str(len(rb)))
            self.end_headers()
            self.wfile.write(rb)

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    init_database()
    load_data()
    cleanup_old_queue()

    vk_group_session = vk_api.VkApi(token=GROUP_TOKEN)
    vk_group = vk_group_session.get_api()
    print("✅ Групповой API подключен", flush=True)

    vk_user_session = vk_api.VkApi(token=USER_TOKEN)
    vk_user = vk_user_session.get_api()
    print("✅ Пользовательский API подключен", flush=True)

    cleanup_thread = threading.Thread(target=cleanup_worker)
    cleanup_thread.daemon = True
    cleanup_thread.start()
    print("✅ Воркер удаления запущен", flush=True)

    print(f"📡 Порт: {PORT}", flush=True)
    sys.stdout.flush()

    server = HTTPServer(('0.0.0.0', PORT), CallbackHandler)
    print(f"✅ Сервер запущен на 0.0.0.0:{PORT}", flush=True)
    sys.stdout.flush()
    server.serve_forever()
