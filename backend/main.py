from decimal import Decimal
from pathlib import Path
from typing import Literal

from fastapi.encoders import jsonable_encoder
from fastapi import FastAPI, HTTPException
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
    get_credentials_file,
    get_doc_file,
    get_profiling_file,
    get_schema_file,
    load_credentials,
    save_credentials,
    slugify_database_name,
    write_json,
)

app = FastAPI()
PROJECT_ROOT = Path(__file__).resolve().parent.parent


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
