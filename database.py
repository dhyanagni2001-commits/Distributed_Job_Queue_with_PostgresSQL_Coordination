# database.py - Connection pool and low-level DB helpers

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import asyncpg

from config import config

logger = logging.getLogger(__name__)

_pool: Optional[asyncpg.Pool] = None


async def init_pool() -> asyncpg.Pool:
    global _pool
    _pool = await asyncpg.create_pool(
        dsn=config.db.dsn,
        min_size=config.db.min_connections,
        max_size=config.db.max_connections,
        command_timeout=30,
        # Custom codec so JSONB columns come back as dicts
        init=_init_connection,
    )
    logger.info("Database pool initialized (min=%d, max=%d)",
                config.db.min_connections, config.db.max_connections)
    return _pool


async def _init_connection(conn: asyncpg.Connection):
    """Register JSON codec for every new connection."""
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        await init_pool()
    return _pool


@asynccontextmanager
async def acquire():
    pool = await get_pool()
    async with pool.acquire() as conn:
        yield conn


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("Database pool closed")


async def apply_schema(schema_path: str = "schema.sql"):
    """Apply schema.sql to the database (idempotent)."""
    with open(schema_path, "r") as f:
        sql = f.read()
    async with acquire() as conn:
        await conn.execute(sql)
    logger.info("Schema applied from %s", schema_path)


# ---------------------------------------------------------------------------
# Raw helpers
# ---------------------------------------------------------------------------

async def fetch_one(query: str, *args) -> Optional[asyncpg.Record]:
    async with acquire() as conn:
        return await conn.fetchrow(query, *args)


async def fetch_all(query: str, *args) -> List[asyncpg.Record]:
    async with acquire() as conn:
        return await conn.fetch(query, *args)


async def execute(query: str, *args) -> str:
    async with acquire() as conn:
        return await conn.execute(query, *args)


async def execute_many(query: str, args_list: List[tuple]):
    async with acquire() as conn:
        await conn.executemany(query, args_list)