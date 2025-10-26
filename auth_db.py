import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorClient

_MONGO = None

def _db():
    global _MONGO
    if _MONGO is None:
        uri = os.environ.get('MONGO_URI')
        if not uri:
            raise RuntimeError('MONGO_URI not set')
        _MONGO = AsyncIOMotorClient(uri)
    return _MONGO.get_default_database()

async def set_authorised(telegram_id: int, gplinks_token: str, hours_valid: int = 12):
    db = _db()
    expires_at = datetime.now(timezone.utc) + timedelta(hours=hours_valid)
    await db.auth.update_one(
        {'telegram_id': telegram_id},
        {'$set': {'telegram_id': telegram_id, 'gplinks_token': gplinks_token, 'expires_at': expires_at}},
        upsert=True,
    )
    return expires_at

async def get_authorisation(telegram_id: int) -> Optional[dict]:
    return await _db().auth.find_one({'telegram_id': telegram_id})

async def is_authorised(telegram_id: int) -> bool:
    doc = await get_authorisation(telegram_id)
    if not doc:
        return False
    exp = doc.get('expires_at')
    if not exp:
        return False
    return datetime.now(timezone.utc) < exp


# --- Admin helpers ---
async def add_admin(telegram_id: int):
    await _db().admins.update_one({"telegram_id": telegram_id}, {"$set": {"telegram_id": telegram_id}}, upsert=True)

async def remove_admin(telegram_id: int):
    await _db().admins.delete_one({"telegram_id": telegram_id})

async def is_admin(telegram_id: int) -> bool:
    doc = await _db().admins.find_one({"telegram_id": telegram_id})
    return bool(doc)

async def list_admins():
    cur = _db().admins.find({})
    return [doc["telegram_id"] async for doc in cur]
