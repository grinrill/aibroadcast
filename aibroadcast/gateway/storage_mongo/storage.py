from ... import storage

from aiogram import types
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorCollection


class Storage(storage.Storage):
    db: AsyncIOMotorCollection

    def __init__(self, db: AsyncIOMotorCollection):
        self.db = db

    async def create(self, r: storage.BroadcastDB) -> storage.BroadcastDB:
        messages = []
        for m in r.messages:
            if isinstance(m, types.Message):
                messages.append(m.to_python())
            else:
                messages.append(m)

        next_id = await self.db.count_documents({})
        await self.db.insert_one(
            dict(
                _id=next_id,
                messages=messages,
                forward=r.forward,
                disable_web_page_preview=r.disable_web_page_preview,
                schedule=r.schedule,
                done=r.done,
                created_at=r.created_at,
                finished_at=r.finished_at,
            )
        )
        r.id = next_id
        return r

    async def update(self, id: int, r: storage.BroadcastDB):
        messages = []
        for m in r.messages:
            if isinstance(m, types.Message):
                messages.append(m.to_python())
            else:
                messages.append(m)

        await self.db.update_one(
            dict(_id=r.id),
            {
                "$set": dict(
                    messages=messages,
                    forward=r.forward,
                    disable_web_page_preview=r.disable_web_page_preview,
                    schedule=r.schedule,
                    done=r.done,
                    created_at=r.created_at,
                    finished_at=r.finished_at,
                ),
            },
        )

    async def get_first_running(self) -> storage.BroadcastDB:
        bc = await self.db.find_one(
            {"done": False, "schedule": {"$lt": datetime.now()}}
        )
        if not bc:
            return None

        r = storage.BroadcastDB(
            bc["messages"],
            bc["forward"],
            bc["disable_web_page_preview"],
            bc["schedule"],
        )
        r.id = bc["_id"]
        r.done = bc["done"]
        r.created_at = bc["created_at"]
        r.finished_at = bc["finished_at"]
        return r
