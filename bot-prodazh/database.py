# database.py

import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# Путь к файлу данных
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
USERS_FILE = os.path.join(DATA_DIR, 'users.json')
ADS_FILE = os.path.join(DATA_DIR, 'ads.json')
FAVORITES_FILE = os.path.join(DATA_DIR, 'favorites.json')
SUBSCRIPTIONS_FILE = os.path.join(DATA_DIR, 'subscriptions.json')
PRICE_OFFERS_FILE = os.path.join(DATA_DIR, 'price_offers.json')
STATE_FILE = os.path.join(DATA_DIR, 'state.json')


class Database:
    """Lightweight in-memory storage used for stateless deployments."""

    def __init__(self) -> None:
        # Создаем директорию для данных, если её нет
        os.makedirs(DATA_DIR, exist_ok=True)
        
        self.users: Dict[int, Dict[str, Any]] = {}
        self.ads: Dict[int, Dict[str, Any]] = {}
        self.favorites: Dict[int, set[int]] = defaultdict(set)
        self.subscriptions: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        self.bot_open = True
        self._next_ad_id = 1
        self._next_subscription_id = 1
        self.price_offers: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        
        # Загружаем данные из файлов
        self._load_data()
        
        # Если данных нет, создаем тестовые объявления
        if not self.ads:
            self._seed_sample_ads()
            self._save_data()

    def _serialize_datetime(self, obj):
        """Преобразует datetime в строку для JSON."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    def _deserialize_datetime(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Преобразует строки ISO обратно в datetime."""
        result = {}
        for key, value in data.items():
            if isinstance(value, str) and key in ('created_at', 'last_active', 'added_date', 'timestamp'):
                try:
                    result[key] = datetime.fromisoformat(value)
                except (ValueError, AttributeError):
                    result[key] = value
            else:
                result[key] = value
        return result

    def _load_data(self):
        """Загружает данные из JSON файлов."""
        try:
            # Загружаем пользователей
            if os.path.exists(USERS_FILE):
                with open(USERS_FILE, 'r', encoding='utf-8') as f:
                    users_data = json.load(f)
                    for user_id_str, user_data in users_data.items():
                        self.users[int(user_id_str)] = self._deserialize_datetime(user_data)
                logger.info("Загружено %d пользователей из файла", len(self.users))
            
            # Загружаем объявления
            if os.path.exists(ADS_FILE):
                with open(ADS_FILE, 'r', encoding='utf-8') as f:
                    ads_data = json.load(f)
                    for ad_id_str, ad_data in ads_data.items():
                        ad_data = self._deserialize_datetime(ad_data)
                        self.ads[int(ad_id_str)] = ad_data
                    # Определяем следующий ID объявления
                    if self.ads:
                        self._next_ad_id = max(self.ads.keys()) + 1
                logger.info("Загружено %d объявлений из файла", len(self.ads))
            
            # Загружаем избранное
            if os.path.exists(FAVORITES_FILE):
                with open(FAVORITES_FILE, 'r', encoding='utf-8') as f:
                    favorites_data = json.load(f)
                    for user_id_str, ad_ids in favorites_data.items():
                        self.favorites[int(user_id_str)] = set(ad_ids)
                logger.info("Загружено избранное для %d пользователей", len(self.favorites))
            
            # Загружаем подписки
            if os.path.exists(SUBSCRIPTIONS_FILE):
                with open(SUBSCRIPTIONS_FILE, 'r', encoding='utf-8') as f:
                    subscriptions_data = json.load(f)
                    for user_id_str, subs_list in subscriptions_data.items():
                        self.subscriptions[int(user_id_str)] = subs_list
                    # Определяем следующий ID подписки
                    max_rowid = 0
                    for subs_list in self.subscriptions.values():
                        for sub in subs_list:
                            max_rowid = max(max_rowid, sub.get('rowid', 0))
                    self._next_subscription_id = max_rowid + 1
                logger.info("Загружено подписок для %d пользователей", len(self.subscriptions))
            
            # Загружаем предложения цен
            if os.path.exists(PRICE_OFFERS_FILE):
                with open(PRICE_OFFERS_FILE, 'r', encoding='utf-8') as f:
                    price_offers_data = json.load(f)
                    for ad_id_str, offers_list in price_offers_data.items():
                        self.price_offers[int(ad_id_str)] = [
                            self._deserialize_datetime(offer) for offer in offers_list
                        ]
                logger.info("Загружено предложений цен для %d объявлений", len(self.price_offers))
            
            # Загружаем состояние бота
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    state_data = json.load(f)
                    self.bot_open = state_data.get('bot_open', True)
                    self._next_ad_id = state_data.get('next_ad_id', self._next_ad_id)
                    self._next_subscription_id = state_data.get('next_subscription_id', self._next_subscription_id)
        except Exception as e:
            logger.error("Ошибка при загрузке данных: %s", e)

    def _save_data(self):
        """Сохраняет данные в JSON файлы."""
        try:
            # Сохраняем пользователей
            users_data = {}
            for user_id, user_data in self.users.items():
                users_data[str(user_id)] = {
                    k: self._serialize_datetime(v) if isinstance(v, datetime) else v
                    for k, v in user_data.items()
                }
            with open(USERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(users_data, f, ensure_ascii=False, indent=2)
            
            # Сохраняем объявления
            ads_data = {}
            for ad_id, ad_data in self.ads.items():
                ads_data[str(ad_id)] = {
                    k: self._serialize_datetime(v) if isinstance(v, datetime) else v
                    for k, v in ad_data.items()
                }
            with open(ADS_FILE, 'w', encoding='utf-8') as f:
                json.dump(ads_data, f, ensure_ascii=False, indent=2)
            
            # Сохраняем избранное
            favorites_data = {
                str(user_id): list(ad_ids) for user_id, ad_ids in self.favorites.items()
            }
            with open(FAVORITES_FILE, 'w', encoding='utf-8') as f:
                json.dump(favorites_data, f, ensure_ascii=False, indent=2)
            
            # Сохраняем подписки
            subscriptions_data = {
                str(user_id): subs_list for user_id, subs_list in self.subscriptions.items()
            }
            with open(SUBSCRIPTIONS_FILE, 'w', encoding='utf-8') as f:
                json.dump(subscriptions_data, f, ensure_ascii=False, indent=2)
            
            # Сохраняем предложения цен
            price_offers_data = {}
            for ad_id, offers_list in self.price_offers.items():
                price_offers_data[str(ad_id)] = [
                    {
                        k: self._serialize_datetime(v) if isinstance(v, datetime) else v
                        for k, v in offer.items()
                    }
                    for offer in offers_list
                ]
            with open(PRICE_OFFERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(price_offers_data, f, ensure_ascii=False, indent=2)
            
            # Сохраняем состояние бота
            state_data = {
                'bot_open': self.bot_open,
                'next_ad_id': self._next_ad_id,
                'next_subscription_id': self._next_subscription_id,
            }
            with open(STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error("Ошибка при сохранении данных: %s", e)

    async def connect(self):
        logger.info("Инициализировано хранилище с сохранением в JSON файлы.")

    async def close(self):
        logger.info("Завершение работы встроенного хранилища.")

    async def add_user(self, user_id, username=None, status='pending'):
        user = self.users.get(user_id)
        if not user:
            user = {
                'user_id': user_id,
                'username': username,
                'status': status,
                'name': None,
                'phone': None,
                'city': None,
                'cheque_file_id': None,
                'created_at': datetime.utcnow(),
                'last_active': datetime.utcnow(),
            }
        else:
            user.update({
                'username': username,
                'status': status,
                'created_at': user.get('created_at') or datetime.utcnow(),
                'last_active': user.get('last_active') or datetime.utcnow(),
            })
        self.users[user_id] = user
        self._save_data()
        logger.info("Пользователь %s добавлен/обновлен со статусом %s", user_id, status)

    async def get_user(self, user_id):
        user = self.users.get(user_id)
        return dict(user) if user else None

    async def update_user_contact(self, user_id, name, phone, city):
        user = self.users.setdefault(user_id, {
            'user_id': user_id,
            'username': None,
            'status': 'pending',
            'name': None,
            'phone': None,
            'city': None,
            'cheque_file_id': None,
            'created_at': datetime.utcnow(),
            'last_active': datetime.utcnow(),
        })
        user.update({'name': name, 'phone': phone, 'city': city})
        self._save_data()
        logger.info("Контактная информация пользователя %s обновлена", user_id)

    async def update_user_status(self, user_id, status):
        user = self.users.setdefault(user_id, {
            'user_id': user_id,
            'username': None,
            'status': status,
            'name': None,
            'phone': None,
            'city': None,
            'cheque_file_id': None,
            'created_at': datetime.utcnow(),
            'last_active': datetime.utcnow(),
        })
        user['status'] = status
        self._save_data()
        logger.info("Статус пользователя %s обновлен на %s", user_id, status)

    async def update_last_active(self, user_id):
        user = self.users.setdefault(user_id, {
            'user_id': user_id,
            'username': None,
            'status': 'pending',
            'name': None,
            'phone': None,
            'city': None,
            'cheque_file_id': None,
            'created_at': datetime.utcnow(),
            'last_active': datetime.utcnow(),
        })
        user['last_active'] = datetime.utcnow()

    async def update_username(self, user_id, username):
        if username is None:
            return
        user = self.users.setdefault(user_id, {
            'user_id': user_id,
            'username': username,
            'status': 'pending',
            'name': None,
            'phone': None,
            'city': None,
            'cheque_file_id': None,
            'created_at': datetime.utcnow(),
            'last_active': datetime.utcnow(),
        })
        if user.get('username') != username:
            user['username'] = username
            logger.info("Username пользователя %s обновлён: %s", user_id, username)

    async def is_bot_open(self):
        return self.bot_open

    async def set_bot_state(self, state: bool):
        self.bot_open = state
        self._save_data()
        logger.info("Состояние бота установлено на %s", 'открыт' if state else 'закрыт')

    async def add_ad(self, title, price, description, photos, inspection_photos, thickness_photos, model, year):
        ad_id = self._next_ad_id
        self._next_ad_id += 1
        ad = {
            'ad_id': ad_id,
            'title': title,
            'model': model,
            'year': year,
            'price': price,
            'description': description,
            'photos': list(photos or []),
            'inspection_photos': list(inspection_photos or []),
            'thickness_photos': list(thickness_photos or []),
            'added_date': datetime.utcnow(),
            'auction_mode': 'off',
        }
        self.ads[ad_id] = ad
        self._save_data()
        logger.info("Объявление '%s' добавлено с ID %s", title, ad_id)
        return ad_id

    def _seed_sample_ads(self) -> None:
        sample_ads = [
            {
                'title': 'Toyota Camry 2.5 AT',
                'model': 'Camry',
                'year': 2019,
                'price': 17500000,
                'description': (
                    'Официальный дилерский автомобиль. Один владелец, полный комплект ключей, '
                    'сервисная история. Комплектация Luxe: камера заднего вида, подогрев сидений, '
                    'бесключевой доступ.'
                ),
                'photos': [
                    'AQADnuQxG_9z0Ul-',
                    'AQADoOQxG_9z0Ul-',
                    'AQADOeQxG4co0Ul-',
                ],
                'inspection_photos': [],
                'thickness_photos': [
                    'AQADO-QxG4co0Ul9',
                    'AQADOuQxG4co0Ul9',
                ],
            },
            {
                'title': 'Hyundai Tucson 1.6 Turbo',
                'model': 'Tucson',
                'year': 2020,
                'price': 15800000,
                'description': (
                    'Полноприводный кроссовер. Турбированный двигатель, автоматическая коробка, '
                    'кожаный салон, панорамная крыша. Пройдена комплексная диагностика.'
                ),
                'photos': [],
                'inspection_photos': [],
                'thickness_photos': [],
            },
            {
                'title': 'Kia Rio X-Line',
                'model': 'Rio',
                'year': 2021,
                'price': 9200000,
                'description': (
                    'Хэтчбек в отличном состоянии. Комплектация Comfort: мультимедиа с CarPlay/Android Auto, '
                    'круиз-контроль, камера заднего вида. Проведена химчистка салона.'
                ),
                'photos': [],
                'inspection_photos': [],
                'thickness_photos': [],
            },
        ]

        for ad in sample_ads:
            ad_id = self._next_ad_id
            self._next_ad_id += 1
            self.ads[ad_id] = {
                'ad_id': ad_id,
                'title': ad['title'],
                'model': ad['model'],
                'year': ad['year'],
                'price': ad['price'],
                'description': ad['description'],
                'photos': ad['photos'],
                'inspection_photos': ad['inspection_photos'],
                'thickness_photos': ad['thickness_photos'],
                'added_date': datetime.utcnow(),
                'auction_mode': 'off',
            }

        if sample_ads:
            logger.info("Добавлено %s тестовых объявления(ий) для демонстрации", len(sample_ads))

    async def append_ad_media(self, ad_id: int, file_id: str, media_type: str):
        ad = self.ads.get(ad_id)
        if not ad:
            raise ValueError(f"Объявление с ID {ad_id} не найдено")

        key_map = {
            'photo': 'photos',
            'inspection': 'inspection_photos',
            'thickness': 'thickness_photos',
        }

        key = key_map.get(media_type)
        if not key:
            raise ValueError(f"Неизвестный тип медиа: {media_type}")

        media_list = ad.setdefault(key, [])
        media_list.append(file_id)
        self._save_data()
        logger.info("Добавлен файл %s в '%s' объявления %s", file_id, key, ad_id)

    async def update_ad(self, ad_id: int, **fields):
        ad = self.ads.get(ad_id)
        if not ad:
            raise ValueError(f"Объявление с ID {ad_id} не найдено")

        allowed_fields = {'title', 'model', 'year', 'price', 'description'}
        for key, value in fields.items():
            if key in allowed_fields:
                ad[key] = value
        self._save_data()
        logger.info("Обновлено объявление %s: %s", ad_id, fields)

    async def set_auction_mode(self, ad_id: int, mode: str):
        ad = self.ads.get(ad_id)
        if not ad:
            raise ValueError(f"Объявление с ID {ad_id} не найдено")
        if mode not in {'off', 'up', 'down'}:
            raise ValueError(f"Недопустимый режим аукциона: {mode}")
        ad['auction_mode'] = mode
        self._save_data()
        logger.info("Режим аукциона объявления %s установлен в '%s'", ad_id, mode)

    async def add_price_offer(self, ad_id: int, user_id: int, price: int, kind: str):
        self.price_offers[ad_id].append({
            'user_id': user_id,
            'price': price,
            'kind': kind,
            'created_at': datetime.utcnow(),
        })
        self._save_data()
        logger.info("Предложение цены добавлено: ad=%s user=%s price=%s kind=%s", ad_id, user_id, price, kind)

    async def consume_price_offer(self, ad_id: int, user_id: int, price: int, kind: str):
        offers = self.price_offers.get(ad_id, [])
        for idx, offer in enumerate(offers):
            if offer['user_id'] == user_id and offer['price'] == price and offer['kind'] == kind:
                offers.pop(idx)
                self._save_data()
                logger.info("Предложение цены подтверждено/удалено: ad=%s user=%s price=%s kind=%s", ad_id, user_id, price, kind)
                return offer
        logger.info("Предложение цены не найдено для ad=%s user=%s price=%s kind=%s", ad_id, user_id, price, kind)
        return None

    async def get_ads(self):
        return [dict(ad) for ad in sorted(self.ads.values(), key=lambda x: x['added_date'], reverse=True)]

    async def get_ad(self, ad_id):
        ad = self.ads.get(ad_id)
        return dict(ad) if ad else None

    async def delete_ad(self, ad_id):
        if ad_id in self.ads:
            self.ads.pop(ad_id, None)
            for favs in self.favorites.values():
                favs.discard(ad_id)
            self._save_data()
            logger.info("Объявление с ID %s удалено", ad_id)

    async def is_favorite(self, user_id, ad_id):
        return ad_id in self.favorites[user_id]

    async def add_to_favorites(self, user_id, ad_id):
        if ad_id in self.ads:
            self.favorites[user_id].add(ad_id)
            self._save_data()
            logger.info("Объявление %s добавлено в избранное пользователя %s", ad_id, user_id)

    async def remove_from_favorites(self, user_id, ad_id):
        if ad_id in self.favorites[user_id]:
            self.favorites[user_id].discard(ad_id)
            self._save_data()
            logger.info("Объявление %s удалено из избранного пользователя %s", ad_id, user_id)

    async def get_favorite_ads(self, user_id):
        ad_ids = self.favorites[user_id]
        return [dict(self.ads[ad_id]) for ad_id in ad_ids if ad_id in self.ads]

    async def add_subscription(self, user_id, model=None, price_min=None, price_max=None, year_min=None, year_max=None):
        rowid = self._next_subscription_id
        self._next_subscription_id += 1
        subscription = {
            'rowid': rowid,
            'user_id': user_id,
            'model': model,
            'price_min': price_min,
            'price_max': price_max,
            'year_min': year_min,
            'year_max': year_max,
        }
        self.subscriptions[user_id].append(subscription)
        self._save_data()
        logger.info("Пользователь %s добавил новую подписку (rowid=%s)", user_id, rowid)

    async def get_subscriptions(self, user_id):
        return [dict(sub) for sub in self.subscriptions.get(user_id, [])]

    async def delete_subscription(self, rowid):
        removed = False
        for user_id, subs in self.subscriptions.items():
            before = len(subs)
            subs[:] = [sub for sub in subs if sub['rowid'] != rowid]
            if len(subs) != before:
                removed = True
                self._save_data()
                logger.info("Подписка с rowid %s удалена (user_id=%s)", rowid, user_id)
                break
        if not removed:
            logger.debug("Подписка с rowid %s не найдена для удаления", rowid)

    async def get_user_contacts(self):
        contacts = []
        for user in self.users.values():
            if user.get('name') and user.get('city') and user.get('phone'):
                contacts.append((user['name'], user['city'], user['phone']))
        return contacts

    async def get_all_subscriptions(self):
        result: List[Dict[str, Any]] = []
        for subs in self.subscriptions.values():
            result.extend(dict(sub) for sub in subs)
        return result

    async def get_approved_users(self):
        return [user_id for user_id, user in self.users.items() if user.get('status') == 'approved']

    async def get_statistics(self):
        return len(self.users), len(self.ads)

    async def get_active_users_count(self):
        threshold = datetime.utcnow() - timedelta(days=7)
        return sum(1 for user in self.users.values() if user.get('last_active') and user['last_active'] >= threshold)

    async def get_inactive_users(self, cutoff_time):
        return [user_id for user_id, user in self.users.items() if user.get('last_active') and user['last_active'] <= cutoff_time]

    async def update_user_cheque(self, user_id, cheque_file_id):
        user = self.users.setdefault(user_id, {'user_id': user_id})
        user['cheque_file_id'] = cheque_file_id
        logger.info("Чек пользователя %s обновлен", user_id)

    async def get_user_contacts_for_export(self):
        return [
            {'name': user['name'], 'city': user['city'], 'phone': user['phone']}
            for user in self.users.values()
            if user.get('name') and user.get('city') and user.get('phone')
        ]

    async def get_new_ads_count(self, cutoff_time):
        return sum(1 for ad in self.ads.values() if ad['added_date'] >= cutoff_time)

    async def get_users_registered_before(self, cutoff_time):
        result = []
        for user_id, user in self.users.items():
            created_at = user.get('created_at')
            if created_at and created_at <= cutoff_time:
                result.append({'user_id': user_id, **user})
        return result
