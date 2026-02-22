import shutil
from decimal import Decimal
from pathlib import Path
from typing import Literal

from fastapi.encoders import jsonable_encoder
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from ai import generate_business_document
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class CredentialsSaveRequest(BaseModel):
    db_type: Literal["mysql", "postgresql", "sqlserver"]
    host: str
    port: int
    database: str
    username: str
    password: str


class DatabaseTriggerRequest(BaseModel):
    database: str


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
