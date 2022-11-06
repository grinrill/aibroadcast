from datetime import datetime
from dataclasses import dataclass
from aiogram import types


class BroadcastDB:
    messages: list
    forward: bool
    disable_web_page_preview: bool
    schedule: datetime

    id: int
    done: bool

    created_at: datetime
    finished_at: datetime = None

    def __init__(
        self,
        messages: list,
        forward: bool,
        disable_web_page_preview: bool,
        schedule: datetime,
    ):
        self.messages = []
        for m in messages:
            if isinstance(m, types.Message):
                self.messages.append(m.to_python())
            else:
                self.messages.append(m)

        self.forward = forward
        self.disable_web_page_preview = disable_web_page_preview
        self.schedule = schedule
        self.created_at = datetime.now()


class Storage:
    async def create(self, r: BroadcastDB) -> BroadcastDB:
        pass

    async def update(self, id: int, r: BroadcastDB):
        pass

    async def get_first_running(self) -> BroadcastDB:
        pass
