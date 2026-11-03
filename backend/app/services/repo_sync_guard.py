# 同一仓库的同步不能重叠；本进程快速拒绝重复请求，PostgreSQL 同时提供跨进程互斥。
import asyncio
import hashlib
import threading
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from app.db.concurrency import run_blocking


_ACTIVE: set[tuple[Engine, UUID]] = set()
_ACTIVE_GUARD = threading.Lock()


def _busy() -> HTTPException:
    return HTTPException(status_code=409, detail="该仓库已有同步任务正在运行，请等待完成后再刷新。")


def _advisory_key(repo_id: UUID) -> int:
    return int.from_bytes(hashlib.blake2b(b"devflow:repo-sync:" + repo_id.bytes, digest_size=8).digest(), "big", signed=True)


# 使用非阻塞、会话级 advisory lock；AUTOCOMMIT 避免等待网络时留下 idle in transaction。
def _acquire_database_lock(engine: Engine, repo_id: UUID) -> Connection:
    connection = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
    try:
        acquired = connection.scalar(text("SELECT pg_try_advisory_lock(:key)"), {"key": _advisory_key(repo_id)})
        if not acquired:
            raise _busy()
        return connection
    except BaseException:
        connection.close()
        raise


# 解锁失败必须使连接失效，不能把可能仍持锁的连接交回连接池。
def _release_database_lock(connection: Connection, repo_id: UUID) -> None:
    try:
        connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": _advisory_key(repo_id)})
    except BaseException:
        # Never return a possibly locked session to the connection pool.
        connection.invalidate()
        raise
    finally:
        connection.close()


@asynccontextmanager
# 互斥覆盖网络读取与数据库发布；异常、请求取消和工作线程退出均走同一清理路径。
async def repository_sync_guard(engine: Engine, repo_id: UUID):
    key = (engine, repo_id)
    with _ACTIVE_GUARD:
        if key in _ACTIVE:
            raise _busy()
        _ACTIVE.add(key)
    connection = None
    try:
        if engine.dialect.name == "postgresql":
            # Session-level advisory locks span network waits without an open
            # write transaction, and also exclude other backend processes.
            pending = asyncio.create_task(asyncio.to_thread(_acquire_database_lock, engine, repo_id))
            try:
                connection = await asyncio.shield(pending)
            except asyncio.CancelledError as cancelled:
                while not pending.done():
                    try:
                        await asyncio.shield(pending)
                    except asyncio.CancelledError:
                        continue
                connection = pending.result()
                raise cancelled
        yield
    finally:
        try:
            if connection is not None:
                await run_blocking(_release_database_lock, connection, repo_id)
        finally:
            with _ACTIVE_GUARD:
                _ACTIVE.discard(key)
