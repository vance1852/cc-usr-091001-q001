"""储能电站调度回放后端。

对外只暴露 :class:`replay.service.ReplayService` 与命令行入口 ``python -m replay``。
所有结论均来自事件发生时间（occurred_at），接收时间（received_at）仅用于
重建“当时能看到什么”，不参与调度判断本身。
"""

from .db import open_db
from .engine import Capability
from .service import ReplayService

__all__ = ["ReplayService", "Capability", "open_db"]
__version__ = "1.0.0"
