from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any

import sqlparse
from google.adk.agents.llm_agent import Agent
from sqlalchemy import text

from data_store import (
    get_doc_file,
    get_profiling_file,
    get_schema_file,
    load_credentials,
    read_json,
    slugify_database_name,
)
from db import build_connection_url, create_engine_from_url

MODEL_NAME = "gemini-2.5-flash"
MAX_SQL_ROW_LIMIT = 200
SQL_TIMEOUT_SECONDS = 10

MUTATING_OR_ADMIN_KEYWORDS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "CREATE",
    "REPLACE",
    "MERGE",
    "CALL",
    "EXEC",
    "GRANT",
    "REVOKE",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "SET",
    "USE",
    "ATTACH",
    "DETACH",
    "VACUUM",
    "ANALYZE",
    "REFRESH",
    "COPY",
    "LOAD",
    "UNLOAD",
    "INTO",
}

_ACTIVE_DATABASE: str | None = None
_ENV_FILES = (
    Path(__file__).resolve().parent / ".env",
    Path(__file__).resolve().parent.parent / ".env",
)


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue
        if (
            len(value) >= 2
            and ((value[0] == value[-1] == '"') or (value[0] == value[-1] == "'"))
        ):
            value = value[1:-1]
        os.environ[key] = value


def _ensure_agent_env() -> None:
    for env_file in _ENV_FILES:
        _load_env_file(env_file)

    # Keep API key aliases in sync for ADK/Gemini usage.
    if not os.getenv("GOOGLE_API_KEY") and os.getenv("GEMINI_API_KEY"):
        os.environ["GOOGLE_API_KEY"] = os.environ["GEMINI_API_KEY"]
    if not os.getenv("GEMINI_API_KEY") and os.getenv("GOOGLE_API_KEY"):
        os.environ["GEMINI_API_KEY"] = os.environ["GOOGLE_API_KEY"]


def _json_safe_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def _normalize_table_filter(table_name: str | None) -> str | None:
    if table_name is None:
        return None
    cleaned = str(table_name).strip()
    return cleaned or None


def _normalize_row_limit(row_limit: int) -> int:
    try:
        parsed = int(row_limit)
    except (TypeError, ValueError):
        return MAX_SQL_ROW_LIMIT
    return max(1, min(MAX_SQL_ROW_LIMIT, parsed))


def _active_db_or_error() -> tuple[str | None, dict[str, Any] | None, dict[str, Any] | None]:
    if not _ACTIVE_DATABASE:
        return (
            None,
            None,
            {
                "status": "error",
                "error_type": "missing_active_database",
                "message": "No active database is set. Select a database in the dashboard first.",
            },
        )

    try:
        credentials = load_credentials(_ACTIVE_DATABASE)
    except FileNotFoundError as exc:
        return (
            None,
            None,
            {
                "status": "error",
                "error_type": "credentials_not_found",
                "message": str(exc),
            },
        )
    except ValueError as exc:
        return (
            None,
            None,
            {
                "status": "error",
                "error_type": "invalid_credentials",
                "message": str(exc),
            },
        )
    except Exception as exc:
        return (
            None,
            None,
            {
                "status": "error",
                "error_type": "credentials_error",
                "message": f"Failed to load credentials: {exc}",
            },
        )

    return credentials["database"], credentials, None


def _build_connection_url(credentials: dict[str, Any]) -> str:
    return build_connection_url(
        credentials["db_type"],
        credentials["host"],
        credentials["port"],
        credentials["database"],
        credentials["username"],
        credentials["password"],
    )


def _extract_uppercase_keywords(statement) -> set[str]:
    keywords: set[str] = set()
    for token in statement.flatten():
        token_type = token.ttype
        if token_type is None:
            continue
        if token.is_whitespace:
            continue
        if token_type in sqlparse.tokens.Comment:
            continue
        if token_type not in sqlparse.tokens.Keyword:
            continue

        raw = token.value.strip().upper()
        if not raw:
            continue
        for piece in raw.replace(",", " ").replace("(", " ").replace(")", " ").split():
            if piece:
                keywords.add(piece)
    return keywords


def _validate_read_only_sql(query: str) -> dict[str, Any] | None:
    cleaned = query.strip()
    if not cleaned:
        return {
            "status": "error",
            "error_type": "invalid_sql",
            "message": "Query is empty. Provide a SELECT query.",
        }

    statements = [stmt for stmt in sqlparse.parse(cleaned) if str(stmt).strip()]
    if len(statements) != 1:
        return {
            "status": "error",
            "error_type": "unsafe_sql",
            "message": "Only a single read-only SQL statement is allowed.",
        }

    statement = statements[0]
    statement_type = statement.get_type().upper()
    if statement_type != "SELECT":
        return {
            "status": "error",
            "error_type": "unsafe_sql",
            "message": "Only read-only SELECT/CTE queries are allowed.",
        }

    found_keywords = _extract_uppercase_keywords(statement)
    blocked = sorted(found_keywords.intersection(MUTATING_OR_ADMIN_KEYWORDS))
    if blocked:
        return {
            "status": "error",
            "error_type": "unsafe_sql",
            "message": (
                "Query blocked because it contains forbidden keywords: "
                + ", ".join(blocked)
            ),
            "blocked_keywords": blocked,
        }

    return None


def _run_query(connection_url: str, query: str, row_limit: int) -> dict[str, Any]:
    engine = create_engine_from_url(connection_url)
    started = perf_counter()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            if not result.returns_rows:
                return {
                    "status": "error",
                    "error_type": "unsafe_sql",
                    "message": "Only queries that return rows are allowed.",
                }

            mapping_rows = result.mappings().fetchmany(row_limit + 1)
            truncated = len(mapping_rows) > row_limit
            if truncated:
                mapping_rows = mapping_rows[:row_limit]

            rows = [
                {str(key): _json_safe_value(value) for key, value in dict(row).items()}
                for row in mapping_rows
            ]
            columns = list(rows[0].keys()) if rows else list(result.keys())
    finally:
        engine.dispose()

    return {
        "status": "success",
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "truncated": truncated,
        "row_limit_applied": row_limit,
        "execution_ms": int((perf_counter() - started) * 1000),
    }


def _run_query_with_timeout(connection_url: str, query: str, row_limit: int) -> dict[str, Any]:
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(_run_query, connection_url, query, row_limit)

    try:
        return future.result(timeout=SQL_TIMEOUT_SECONDS)
    except FutureTimeoutError:
        future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        return {
            "status": "error",
            "error_type": "query_timeout",
            "message": (
                f"Query timed out after {SQL_TIMEOUT_SECONDS} seconds. "
                "Try adding tighter filters or aggregations."
            ),
        }
    except Exception as exc:
        executor.shutdown(wait=False, cancel_futures=True)
        return {
            "status": "error",
            "error_type": "query_execution_error",
            "message": f"Failed to execute query: {exc}",
        }
    finally:
        if not future.cancelled():
            executor.shutdown(wait=False, cancel_futures=True)


def set_active_database(database: str) -> dict[str, str]:
    """Set the active database for chat tools using data/<db-slug>/credentials.json."""
    global _ACTIVE_DATABASE

    database_name = str(database or "").strip()
    if not database_name:
        return {
            "status": "error",
            "message": "database is required.",
        }

    try:
        credentials = load_credentials(database_name)
        _ACTIVE_DATABASE = credentials["database"]
    except Exception as exc:
        return {
            "status": "error",
            "message": str(exc),
        }

    return {
        "status": "success",
        "database": _ACTIVE_DATABASE,
        "database_slug": slugify_database_name(_ACTIVE_DATABASE),
        "message": f"Active database set to `{_ACTIVE_DATABASE}`.",
    }


def read_schema_json(table_name: str | None = None) -> dict[str, Any]:
    """Read schema.json for the active database, optionally filtered by table name."""
    database, _, error = _active_db_or_error()
    if error:
        return error

    assert database is not None
    filter_value = _normalize_table_filter(table_name)

    try:
        payload = read_json(get_schema_file(database))
    except Exception as exc:
        return {
            "status": "error",
            "error_type": "schema_read_error",
            "message": f"Failed to read schema.json: {exc}",
        }

    rows = payload.get("schema", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        rows = []

    if filter_value:
        needle = filter_value.lower()
        rows = [
            row
            for row in rows
            if isinstance(row, dict)
            and str(row.get("table_name", "")).lower() == needle
        ]

    return {
        "status": "success",
        "database": database,
        "table_filter": filter_value,
        "count": len(rows),
        "schema": rows,
    }


def read_profiling_json(table_name: str | None = None) -> dict[str, Any]:
    """Read profiling.json for the active database, optionally filtered by table name."""
    database, _, error = _active_db_or_error()
    if error:
        return error

    assert database is not None
    filter_value = _normalize_table_filter(table_name)

    try:
        payload = read_json(get_profiling_file(database))
    except Exception as exc:
        return {
            "status": "error",
            "error_type": "profiling_read_error",
            "message": f"Failed to read profiling.json: {exc}",
        }

    rows = payload.get("profile", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        rows = []

    if filter_value:
        needle = filter_value.lower()
        rows = [
            row
            for row in rows
            if isinstance(row, dict)
            and str(row.get("table_name", "")).lower() == needle
        ]

    return {
        "status": "success",
        "database": database,
        "table_filter": filter_value,
        "count": len(rows),
        "profile": rows,
    }


def read_doc_json(table_name: str | None = None) -> dict[str, Any]:
    """Read doc.json for the active database, optionally filtered by table name."""
    database, _, error = _active_db_or_error()
    if error:
        return error

    assert database is not None
    filter_value = _normalize_table_filter(table_name)

    try:
        payload = read_json(get_doc_file(database))
    except Exception as exc:
        return {
            "status": "error",
            "error_type": "doc_read_error",
            "message": f"Failed to read doc.json: {exc}",
        }

    if not isinstance(payload, dict):
        payload = {}

    tables = payload.get("tables", [])
    if not isinstance(tables, list):
        tables = []

    if filter_value:
        needle = filter_value.lower()
        tables = [
            row
            for row in tables
            if isinstance(row, dict)
            and str(row.get("table_name", "")).lower() == needle
        ]

    return {
        "status": "success",
        "database": database,
        "table_filter": filter_value,
        "overview": payload.get("overview", {}),
        "count": len(tables),
        "tables": tables,
    }


def execute_read_only_sql(query: str, row_limit: int = MAX_SQL_ROW_LIMIT) -> dict[str, Any]:
    """Execute a single read-only SELECT/CTE query against the active database."""
    database, credentials, error = _active_db_or_error()
    if error:
        return error

    assert database is not None
    assert credentials is not None

    if not isinstance(query, str):
        return {
            "status": "error",
            "error_type": "invalid_sql",
            "message": "Query must be a string.",
        }

    safety_error = _validate_read_only_sql(query)
    if safety_error:
        return safety_error

    effective_limit = _normalize_row_limit(row_limit)
    connection_url = _build_connection_url(credentials)
    execution_result = _run_query_with_timeout(connection_url, query, effective_limit)
    execution_result.setdefault("database", database)
    execution_result.setdefault("query", query.strip())
    return execution_result


_ensure_agent_env()

root_agent = Agent(
    model=MODEL_NAME,
    name="root_agent",
    description=(
        "Database-only AI chat agent for schema/profiling/doc reasoning and "
        "strictly read-only SQL execution."
    ),
    instruction=(
        "You are DataLens DB Agent. Answer ONLY database-related questions for the active "
        "database. If the user asks anything not related to databases, schema, profiling, "
        "documentation, SQL, or data quality, politely refuse and state that you only answer "
        "database-related queries.\n"
        "Use tools to read schema.json, profiling.json, and doc.json whenever needed.\n"
        "You may execute SQL only through execute_read_only_sql and only for read-only analysis.\n"
        "Never run or suggest running SQL that modifies data or schema.\n"
        "If the user asks to suggest SQL/query/examples, provide SQL text only and do NOT "
        "execute it.\n"
        "If query execution is blocked by safety policy, explain why and provide a safe "
        "read-only alternative."
    ),
    tools=[
        read_schema_json,
        read_profiling_json,
        read_doc_json,
        execute_read_only_sql,
    ],
)
