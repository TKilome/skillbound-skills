from __future__ import annotations

from sql_engine.config import EngineConfig
from sql_engine.profile import STARROCKS_PROFILE
from sql_engine.sqlalchemy_engine import SqlAlchemyEngine


class StarRocksEngine(SqlAlchemyEngine):
    def __init__(self, cfg: EngineConfig) -> None:
        super().__init__(cfg, STARROCKS_PROFILE)
