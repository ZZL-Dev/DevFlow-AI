# 周期仓库同步协程：每轮创建/释放自己的 Session，按配置决定全量或数量上限，再等待下一轮。
# 这是进程内后台任务，不是持久任务队列；多进程部署时需另外考虑重复调度。
import asyncio
import logging

from app.core.config import settings
from app.db.concurrency import run_blocking
from app.db.models import Repository
from app.db.session import SessionLocal
from app.schemas.repos import RepoSyncRequest
from app.services.repos_sync import sync_repository_by_id

logger = logging.getLogger(__name__)


# 定时枚举同样在工作线程完成，避免连接池等待影响 API 事件循环。
def _repository_ids():
    # 应用启动后先短暂等待，减少刚启动就与初始化争用资源；这是启动缓冲，不是正式同步周期。
    with SessionLocal() as db:
        return [row.id for row in db.query(Repository).filter(Repository.provider.in_(["github", "github_compatible"])).all()]


async def periodic_repo_sync_loop() -> None:
    await asyncio.sleep(5)
    while settings.auto_sync_enabled:
        repo_ids = await run_blocking(_repository_ids)

        for repo_id in repo_ids:
            try:
                await sync_repository_by_id(repo_id, RepoSyncRequest(limit=settings.auto_sync_limit))
            except Exception as exc:
                logger.warning("Periodic repo sync failed for %s: %s", repo_id, exc)

        await asyncio.sleep(max(settings.auto_sync_interval_seconds, 30))
