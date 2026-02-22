import os
import shutil
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Literal

from fastapi.encoders import jsonable_encoder
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types as genai_types
from pydantic import BaseModel
from ai import generate_business_document
from chat_agent.agent import root_agent, set_active_database
from db import (
    build_connection_url,
    create_engine_from_url,
    test_connection,
    extract_schema,
    extract_data_profile,
)
from data_store import (
    DATA_DIR,
    get_database_dir,
    get_credentials_file,
    get_doc_file,
    get_profiling_file,
    get_schema_file,
    load_credentials,
    read_json,
    save_credentials,
    slugify_database_name,
    write_json,
)

app = FastAPI()
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# CORS: allow localhost by default; add production frontend via CORS_ORIGINS (comma-separated)
_default_origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "https://datalens-indol.vercel.app",  # production frontend on Vercel
]
_extra_origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]
allow_origins = _default_origins + _extra_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def ensure_data_dir():
    """Create data directory on startup so the app works on fresh deploys (e.g. Render)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)


@app.get("/")
@app.get("/health")
def health():
    """Health check for load balancers and deployment platforms."""
    return {
        "status": "ok",
        "service": "DataLens API",
        "docs": "/docs",
        "endpoints": ["/databases", "/health"],
    }


class CredentialsSaveRequest(BaseModel):
    db_type: Literal["mysql", "postgresql", "sqlserver"]
    host: str
    port: int
    database: str
    username: str
    password: str


class DatabaseTriggerRequest(BaseModel):
    database: str


class ChatMessageRequest(BaseModel):
    database: str
    message: str
    session_id: str | None = None
    user_id: str = "datalens-ui"


CHAT_APP_NAME = "datalens-db-chat"
CHAT_SESSION_SERVICE = InMemorySessionService()
CHAT_RUNNER = Runner(
    app_name=CHAT_APP_NAME,
    agent=root_agent,
    session_service=CHAT_SESSION_SERVICE,
)


def _decimal_encoder(value: Decimal):
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def _to_json_safe(payload: dict) -> dict:
    return jsonable_encoder(payload, custom_encoder={Decimal: _decimal_encoder})


def _relative_path(path: Path) -> str:
    return path.relative_to(PROJECT_ROOT).as_posix()


def _safe_read(path: Path):
    if not path.exists():
        return None
    try:
        return read_json(path)
    except Exception:
        return None


def _extract_event_text(event) -> str:
    content = getattr(event, "content", None)
    parts = getattr(content, "parts", None) if content else None
    if not parts:
        return ""

    chunks = []
    for part in parts:
        value = getattr(part, "text", None)
        if value:
            chunks.append(str(value))
    return "".join(chunks).strip()


def _fallback_database_reply(database: str, message: str) -> str:
    try:
        schema_payload = read_json(get_schema_file(database))
    except Exception:
        schema_payload = {}

    try:
        profiling_payload = read_json(get_profiling_file(database))
    except Exception:
        profiling_payload = {}

    try:
        doc_payload = read_json(get_doc_file(database))
    except Exception:
        doc_payload = {}

    schema_rows = schema_payload.get("schema", []) if isinstance(schema_payload, dict) else []
    profile_rows = (
        profiling_payload.get("profile", [])
        if isinstance(profiling_payload, dict)
        else []
    )
    doc_rows = doc_payload.get("tables", []) if isinstance(doc_payload, dict) else []

    query = message.strip().lower()

    table_names = [
        str(item.get("table_name"))
        for item in schema_rows
        if isinstance(item, dict) and item.get("table_name")
    ]

    if any(
        token in query
        for token in ("list tables", "table names", "what tables", "show tables")
    ):
        if not table_names:
            return (
                "Gemini quota is temporarily exhausted. I cannot call the LLM right now, "
                "and no schema table list is available in local files."
            )
        preview = ", ".join(table_names[:50])
        more = "" if len(table_names) <= 50 else f" ... (+{len(table_names) - 50} more)"
        return (
            "Gemini quota is temporarily exhausted, so this is a file-based answer.\n"
            f"Database `{database}` has {len(table_names)} table(s): {preview}{more}"
        )

    if "how many tables" in query or "table count" in query:
        return (
            "Gemini quota is temporarily exhausted, so this is a file-based answer.\n"
            f"Database `{database}` has {len(table_names)} table(s)."
        )

    if "how many columns" in query or "column count" in query:
        total_columns = sum(
            len(item.get("columns", []))
            for item in schema_rows
            if isinstance(item, dict) and isinstance(item.get("columns"), list)
        )
        return (
            "Gemini quota is temporarily exhausted, so this is a file-based answer.\n"
            f"Database `{database}` has {total_columns} column(s) across schema tables."
        )

    if "relation" in query or "foreign key" in query:
        relation_count = sum(
            len(item.get("foreign_keys", []))
            for item in schema_rows
            if isinstance(item, dict) and isinstance(item.get("foreign_keys"), list)
        )
        return (
            "Gemini quota is temporarily exhausted, so this is a file-based answer.\n"
            f"Database `{database}` has {relation_count} foreign-key relationship(s)."
        )

    if "recommendation" in query:
        recommendations = (
            doc_payload.get("overview", {}).get("global_recommendations", [])
            if isinstance(doc_payload, dict)
            else []
        )
        if isinstance(recommendations, list) and recommendations:
            lines = "\n".join(f"- {item}" for item in recommendations[:8])
            return (
                "Gemini quota is temporarily exhausted, so this is a file-based answer.\n"
                f"Global recommendations for `{database}`:\n{lines}"
            )

    if any(token in query for token in ("summary", "overview", "describe database")):
        summary = (
            doc_payload.get("overview", {}).get("summary")
            if isinstance(doc_payload, dict)
            else None
        )
        if summary:
            return (
                "Gemini quota is temporarily exhausted, so this is a file-based answer.\n"
                f"Overview for `{database}`:\n{summary}"
            )

    target_table = None
    for name in table_names:
        if name.lower() in query:
            target_table = name
            break

    if target_table:
        schema_entry = next(
            (
                row
                for row in schema_rows
                if isinstance(row, dict) and row.get("table_name") == target_table
            ),
            {},
        )
        profile_entry = next(
            (
                row
                for row in profile_rows
                if isinstance(row, dict) and row.get("table_name") == target_table
            ),
            {},
        )
        doc_entry = next(
            (
                row
                for row in doc_rows
                if isinstance(row, dict) and row.get("table_name") == target_table
            ),
            {},
        )

        column_count = (
            len(schema_entry.get("columns", []))
            if isinstance(schema_entry.get("columns"), list)
            else 0
        )
        pk_count = (
            len(schema_entry.get("primary_keys", []))
            if isinstance(schema_entry.get("primary_keys"), list)
            else 0
        )
        fk_count = (
            len(schema_entry.get("foreign_keys", []))
            if isinstance(schema_entry.get("foreign_keys"), list)
            else 0
        )
        completeness = (
            profile_entry.get("completeness", {}).get("table_completeness_pct")
            if isinstance(profile_entry, dict)
            else None
        )
        freshness = (
            profile_entry.get("freshness", {}).get("latest_timestamp")
            if isinstance(profile_entry, dict)
            else None
        )
        priority = doc_entry.get("priority") if isinstance(doc_entry, dict) else None
        business_summary = (
            doc_entry.get("business_summary") if isinstance(doc_entry, dict) else None
        )

        lines = [
            "Gemini quota is temporarily exhausted, so this is a file-based answer.",
            f"Table `{target_table}`:",
            f"- Columns: {column_count}",
            f"- Primary keys: {pk_count}",
            f"- Foreign keys: {fk_count}",
        ]
        if completeness is not None:
            lines.append(f"- Completeness: {completeness}%")
        if freshness:
            lines.append(f"- Latest timestamp: {freshness}")
        if priority:
            lines.append(f"- Priority: {priority}")
        if business_summary:
            lines.append(f"- Business summary: {business_summary}")
        return "\n".join(lines)

    return (
        "Gemini quota is temporarily exhausted, so I cannot use the AI model right now. "
        "I can still answer deterministic questions like: list tables, table counts, "
        "column counts, relations, overview summary, recommendations, or details for a specific table name."
    )


@app.get("/databases")
def list_saved_databases():
    try:
        if not DATA_DIR.exists():
            return {"status": "success", "count": 0, "databases": []}

        databases = []
        for entry in DATA_DIR.iterdir():
            if not entry.is_dir():
                continue

            credentials_file = entry / "credentials.json"
            if not credentials_file.exists():
                continue

            credentials = _safe_read(credentials_file) or {}
            schema_file = entry / "schema.json"
            profiling_file = entry / "profiling.json"
            doc_file = entry / "doc.json"

            schema_payload = _safe_read(schema_file)
            profiling_payload = _safe_read(profiling_file)
            doc_payload = _safe_read(doc_file)

            database_name = credentials.get("database") or entry.name
            schema_tables = schema_payload.get("schema", []) if schema_payload else []
            profiling_tables = (
                profiling_payload.get("profile", []) if profiling_payload else []
            )

            databases.append(
                {
                    "database": database_name,
                    "database_slug": entry.name,
                    "db_type": credentials.get("db_type"),
                    "host": credentials.get("host"),
                    "has_schema": schema_payload is not None,
                    "has_profiling": profiling_payload is not None,
                    "has_doc": doc_payload is not None,
                    "tables_found": len(schema_tables)
                    if isinstance(schema_tables, list)
                    else 0,
                    "tables_profiled": len(profiling_tables)
                    if isinstance(profiling_tables, list)
                    else 0,
                    "credentials_file": _relative_path(credentials_file),
                    "schema_file": _relative_path(schema_file)
                    if schema_file.exists()
                    else None,
                    "profiling_file": _relative_path(profiling_file)
                    if profiling_file.exists()
                    else None,
                    "doc_file": _relative_path(doc_file) if doc_file.exists() else None,
                }
            )

        databases.sort(key=lambda item: item["database"].lower())
        return {"status": "success", "count": len(databases), "databases": databases}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list databases: {e}")


@app.delete("/databases/{database}")
def delete_saved_database(database: str):
    try:
        database_slug = slugify_database_name(database)
        database_dir = get_database_dir(database, create=False)
        if not database_dir.exists() or not database_dir.is_dir():
            raise HTTPException(
                status_code=404,
                detail=f"Database folder not found for '{database}'.",
            )

        shutil.rmtree(database_dir)
        return {
            "status": "success",
            "database": database,
            "database_slug": database_slug,
            "deleted_dir": _relative_path(database_dir),
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete database: {e}")


@app.post("/databases/overview")
def get_saved_database_overview(request: DatabaseTriggerRequest):
    try:
        load_credentials(request.database)
        schema_payload = read_json(get_schema_file(request.database))
        profiling_payload = read_json(get_profiling_file(request.database))
        doc_payload = read_json(get_doc_file(request.database))
        database_slug = slugify_database_name(request.database)
        return {
            "status": "success",
            "database": request.database,
            "database_slug": database_slug,
            "schema": schema_payload.get("schema", []),
            "profile": profiling_payload.get("profile", []),
            "doc": doc_payload,
            "sources": {
                "schema_file": _relative_path(get_schema_file(request.database)),
                "profiling_file": _relative_path(get_profiling_file(request.database)),
                "doc_file": _relative_path(get_doc_file(request.database)),
            },
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to load database overview: {e}"
        )


@app.post("/databases/credentials")
def save_database_credentials(request: CredentialsSaveRequest):
    try:
        saved = save_credentials(request.model_dump())
        database_slug = slugify_database_name(saved["database"])
        credentials_file = get_credentials_file(saved["database"])
        return {
            "status": "success",
            "database": saved["database"],
            "database_slug": database_slug,
            "credentials_file": _relative_path(credentials_file),
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save credentials: {e}")


@app.post("/chat/message")
async def chat_with_database_agent(request: ChatMessageRequest):
    message = request.message.strip()
    if not message:
        raise HTTPException(status_code=400, detail="message is required.")

    if not request.database or not request.database.strip():
        raise HTTPException(status_code=400, detail="database is required.")

    set_db_result = set_active_database(request.database)
    if set_db_result.get("status") != "success":
        raise HTTPException(
            status_code=400,
            detail=set_db_result.get("message", "Failed to set active database."),
        )

    user_id = request.user_id.strip() or "datalens-ui"
    session_id = (request.session_id or "").strip() or str(uuid.uuid4())

    session = await CHAT_SESSION_SERVICE.get_session(
        app_name=CHAT_APP_NAME,
        user_id=user_id,
        session_id=session_id,
    )
    if session is None:
        session = await CHAT_SESSION_SERVICE.create_session(
            app_name=CHAT_APP_NAME,
            user_id=user_id,
            session_id=session_id,
        )

    prompt = (
        f"Active database is '{request.database}'. "
        "Use this database context unless user explicitly asks to switch.\n\n"
        f"User query:\n{message}"
    )
    new_message = genai_types.UserContent(parts=[genai_types.Part(text=prompt)])

    reply_chunks = []
    try:
        async for event in CHAT_RUNNER.run_async(
            user_id=user_id,
            session_id=session.id,
            new_message=new_message,
        ):
            text_chunk = _extract_event_text(event)
            if text_chunk:
                reply_chunks.append(text_chunk)
    except Exception as e:
        error_text = str(e)
        if "RESOURCE_EXHAUSTED" in error_text or "429" in error_text:
            fallback_reply = _fallback_database_reply(request.database, message)
            return {
                "status": "success",
                "database": request.database,
                "session_id": session.id,
                "user_id": user_id,
                "fallback_mode": "quota_file_based",
                "reply": fallback_reply,
            }
        if "UNAUTHENTICATED" in error_text or "API key" in error_text:
            raise HTTPException(
                status_code=401,
                detail=f"Agent authentication failed: {error_text}",
            )
        raise HTTPException(status_code=500, detail=f"Agent execution failed: {error_text}")

    reply = "\n".join(chunk for chunk in reply_chunks if chunk).strip()
    if not reply:
        reply = (
            "I could not generate a response from the agent. "
            "Please try rephrasing your database question."
        )

    return {
        "status": "success",
        "database": request.database,
        "session_id": session.id,
        "user_id": user_id,
        "reply": reply,
    }


@app.post("/databases/schema/extract")
def extract_db_schema(request: DatabaseTriggerRequest):
    try:
        credentials = load_credentials(request.database)
        connection_url = build_connection_url(
            credentials["db_type"],
            credentials["host"],
            credentials["port"],
            credentials["database"],
            credentials["username"],
            credentials["password"],
        )
        success, message = test_connection(connection_url)
        if not success:
            raise HTTPException(status_code=400, detail=message)

        engine = create_engine_from_url(connection_url)
        schema = extract_schema(engine)
        database_slug = slugify_database_name(request.database)
        schema_file = get_schema_file(request.database, create_dir=True)

        response = {
            "status": "success",
            "database": request.database,
            "database_slug": database_slug,
            "tables_found": len(schema),
            "schema": schema,
            "schema_file": _relative_path(schema_file),
        }
        safe_response = _to_json_safe(response)
        write_json(schema_file, safe_response)
        return safe_response

    except FileNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/databases/profiling/extract")
def profile_db_data(request: DatabaseTriggerRequest):
    try:
        credentials = load_credentials(request.database)
        connection_url = build_connection_url(
            credentials["db_type"],
            credentials["host"],
            credentials["port"],
            credentials["database"],
            credentials["username"],
            credentials["password"],
        )

        success, message = test_connection(connection_url)
        if not success:
            raise HTTPException(status_code=400, detail=message)

        engine = create_engine_from_url(connection_url)
        profile = extract_data_profile(engine)
        database_slug = slugify_database_name(request.database)
        profiling_file = get_profiling_file(request.database, create_dir=True)

        response = {
            "status": "success",
            "database": request.database,
            "database_slug": database_slug,
            "tables_profiled": len(profile),
            "profile": profile,
            "profiling_file": _relative_path(profiling_file),
        }
        safe_response = _to_json_safe(response)
        write_json(profiling_file, safe_response)
        return safe_response

    except FileNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/databases/doc/generate")
def generate_db_business_doc(request: DatabaseTriggerRequest):
    try:
        load_credentials(request.database)
        response = generate_business_document(request.database)
        response["doc_file"] = _relative_path(get_doc_file(request.database, create_dir=True))
        return response
    except FileNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to generate business document: {e}"
        )
