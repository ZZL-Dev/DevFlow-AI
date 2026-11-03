# 每条评测复制当前仓库输入到私有内存数据库，保持生产 ChatAgent 与工具链，隔离所有运行时写入。
import copy
from contextlib import contextmanager
from uuid import UUID

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from app.db.models import (
    Base,
    Document,
    Issue,
    KnowledgeBaseConfig,
    KnowledgeSourceDocument,
    PRFile,
    PRReviewComment,
    PullRequest,
    Repository,
    WorkflowRun,
)


@contextmanager
# 不复制其他仓库或正式聊天历史；保留文档/PR 明细/知识库配置与原有来源 UUID。
# JSON 深复制防止内存对象引用串用；结束时关闭 Session 并释放整个私有数据库。
def evaluation_session(source: Session, repo_id: UUID):
    """Copy project inputs, never production history or evaluation outputs."""
    engine = create_engine("sqlite+pysqlite:///:memory:")
    try:
        Base.metadata.create_all(engine)
        with Session(engine, autoflush=False) as sandbox:
            with source.get_bind().connect() as connection:
                if connection.dialect.name == "postgresql":
                    connection = connection.execution_options(isolation_level="REPEATABLE READ")
                with Session(bind=connection) as reader:
                    if connection.dialect.name == "postgresql":
                        reader.execute(text("SET TRANSACTION READ ONLY"))
                        reader.execute(text("SET LOCAL statement_timeout = '30s'"))
                    elif connection.dialect.name == "sqlite":
                        reader.execute(text("BEGIN"))
                    for model in (Repository, Issue, PullRequest, WorkflowRun, Document, KnowledgeBaseConfig, KnowledgeSourceDocument, PRFile, PRReviewComment):
                        table = model.__table__
                        if model is Repository:
                            predicate = table.c.id == repo_id
                        elif model in (PRFile, PRReviewComment):
                            predicate = table.c.pr_id.in_(select(PullRequest.id).where(PullRequest.repo_id == repo_id))
                        else:
                            predicate = table.c.repo_id == repo_id
                        rows = copy.deepcopy([dict(row) for row in reader.execute(select(table).where(predicate)).mappings()])
                        if rows:
                            sandbox.execute(table.insert(), rows)
            sandbox.commit()
            try:
                yield sandbox
            finally:
                sandbox.rollback()
    finally:
        engine.dispose()
