# database.py

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class Database:
    """Lightweight in-memory storage used for stateless deployments."""

    def __init__(self) -> None:
        self.users: Dict[int, Dict[str, Any]] = {}
        self.ads: Dict[int, Dict[str, Any]] = {}
        self.favorites: Dict[int, set[int]] = defaultdict(set)
        self.subscriptions: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        self.bot_open = True
        self._next_ad_id = 1
        self._next_subscription_id = 1

    async def connect(self):
        logger.info("Инициализировано встроенное хранилище (без базы данных).")

    async def close(self):
        logger.info("Завершение работы встроенного хранилища.")

    async def add_user(self, user_id, username=None, status='pending'):
        user = self.users.get(user_id, {})
        user.update({
            'user_id': user_id,
            'username': username,
            'status': status,
            'name': user.get('name'),
            'phone': user.get('phone'),
            'city': user.get('city'),
            'cheque_file_id': user.get('cheque_file_id'),
            'last_active': user.get('last_active') or datetime.utcnow(),
        })
        self.users[user_id] = user
        logger.info("Пользователь %s добавлен/обновлен со статусом %s", user_id, status)

    async def get_user(self, user_id):
        user = self.users.get(user_id)
        return dict(user) if user else None

    async def update_user_contact(self, user_id, name, phone, city):
        user = self.users.setdefault(user_id, {'user_id': user_id})
        user.update({'name': name, 'phone': phone, 'city': city})
        logger.info("Контактная информация пользователя %s обновлена", user_id)

    async def update_user_status(self, user_id, status):
        user = self.users.setdefault(user_id, {'user_id': user_id})
        user['status'] = status
        logger.info("Статус пользователя %s обновлен на %s", user_id, status)

    async def update_last_active(self, user_id):
        user = self.users.setdefault(user_id, {'user_id': user_id})
        user['last_active'] = datetime.utcnow()

    async def is_bot_open(self):
        return self.bot_open

    async def set_bot_state(self, state: bool):
        self.bot_open = state
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
        }
        self.ads[ad_id] = ad
        logger.info("Объявление '%s' добавлено с ID %s", title, ad_id)
        return ad_id

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
            logger.info("Объявление с ID %s удалено", ad_id)

    async def is_favorite(self, user_id, ad_id):
        return ad_id in self.favorites[user_id]

    async def add_to_favorites(self, user_id, ad_id):
        if ad_id in self.ads:
            self.favorites[user_id].add(ad_id)
            logger.info("Объявление %s добавлено в избранное пользователя %s", ad_id, user_id)

    async def remove_from_favorites(self, user_id, ad_id):
        if ad_id in self.favorites[user_id]:
            self.favorites[user_id].discard(ad_id)
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