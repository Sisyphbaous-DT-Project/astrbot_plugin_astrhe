"""storage 包入口：聚合各仓库。"""

from __future__ import annotations

from pathlib import Path

from .alarms import AlarmsRepo
from .db import Database
from .ledger import LedgerRepo
from .memory import MemoryRepo
from .persons import PersonsRepo
from .plans import PlansRepo
from .runs import RunsRepo
from .sent_receipts import SentReceiptsRepo


class Storage:
    """聚合入口：一个 Database + 各仓库。"""

    def __init__(self, data_dir: Path):
        self.db = Database(data_dir / "astrhe.db")
        self.ledger = LedgerRepo(self.db)
        self.runs = RunsRepo(self.db)
        self.plans = PlansRepo(self.db)
        self.alarms = AlarmsRepo(self.db)
        self.persons = PersonsRepo(self.db)
        self.memory = MemoryRepo(self.db)
        self.sent_receipts = SentReceiptsRepo(self.db)

    async def open(self) -> None:
        await self.db.open()

    async def close(self) -> None:
        await self.db.close()
