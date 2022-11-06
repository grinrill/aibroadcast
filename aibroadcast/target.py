from datetime import datetime


class TargetFilter:
    pass


class TargetItem:
    id: int
    processed: bool = False
    success: bool = None
    message_id: int = None
    date: datetime = None
    error: str = None

    def __init__(self, id):
        self.id = id


class TargetInfo:
    total: int
    processed: int
    success: int
    error: int


class Target:
    async def init(self, broadcast: int, filter: TargetFilter = None) -> int:
        pass

    async def get(self, broadcast: int, lenght: int) -> list:
        pass

    async def update(self, broadcast: int, done: list):
        pass

    async def info(self, broadcast: int) -> TargetInfo:
        pass
