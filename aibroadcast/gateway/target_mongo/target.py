from ... import target

import typing
from aiogram import types
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorCollection


class Target(target.Target):
    db: AsyncIOMotorCollection
    get_users: typing.Callable

    async def __init__(self, db: AsyncIOMotorCollection, get_users: typing.Callable):
        self.db = db
        self.get_users = get_users

    async def init(self, broadcast: int, filter: target.TargetFilter = None) -> int:
        ids = await self.get_users(filter)
        recs = []
        for i in ids:
            recs.append(
                dict(
                    id=i,
                    broadcast=broadcast,
                    processed=False,
                    success=None,
                    message_id=None,
                    date=None,
                    error=None,
                )
            )

        await self.db.insert_many(recs)
        return len(recs)

    async def get(self, broadcast: int, lenght: int) -> list:
        recs = await self.db.find(
            dict(broadcast=broadcast, processed=False), limit=lenght
        )
        res = []
        async for r in recs:
            t = target.Item(r["id"])
            t.processed = r["processed"]
            t.success = r["success"]
            t.message_id = r["message_id"]
            t.date = r["date"]
            t.error = r["error"]
            res.append(t)
        return res

    async def update(self, broadcast: int, done: typing.List[target.Item]):
        for t in done:
            await self.db.update_one(
                dict(id=t.id, broadcast=broadcast),
                {
                    "$set": dict(
                        processed=True,
                        success=t.success,
                        message_id=t.message_id,
                        date=t.date,
                        error=t.error,
                    )
                },
            )

    async def info(self, broadcast: int) -> target.TargetInfo:
        info = target.TargetInfo()
        info.total = await self.db.count_documents(dict(broadcast=broadcast))
        info.processed = await self.db.count_documents(
            dict(broadcast=broadcast, processed=True)
        )
        info.success = await self.db.count_documents(
            dict(broadcast=broadcast, success=True)
        )
        info.error = await self.db.count_documents(
            dict(broadcast=broadcast, error={"$new": None})
        )
        return info
