import json
from decimal import Decimal
from pathlib import Path

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

app = FastAPI()
DATA_DIR = Path(__file__).resolve().parent.parent / "data"


class DBConnectionRequest(BaseModel):
    db_type: str
    host: str
    port: int
    database: str
    username: str
    password: str


def _write_data_file(filename: str, payload: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DATA_DIR / filename
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _decimal_encoder(value: Decimal):
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def _to_json_safe(payload: dict) -> dict:
    return jsonable_encoder(payload, custom_encoder={Decimal: _decimal_encoder})


@app.post("/extract-schema")
def extract_db_schema(request: DBConnectionRequest):
    try:
        connection_url = build_connection_url(
            request.db_type,
            request.host,
            request.port,
            request.database,
            request.username,
            request.password,
        )

        success, message = test_connection(connection_url)

        if not success:
            raise HTTPException(status_code=400, detail=message)

        engine = create_engine_from_url(connection_url)

        schema = extract_schema(engine)

        response = {
            "status": "success",
            "tables_found": len(schema),
            "schema": schema
        }
        safe_response = _to_json_safe(response)
        _write_data_file("schema.json", safe_response)
        return safe_response

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/profile-data")
def profile_db_data(request: DBConnectionRequest):
    try:
        connection_url = build_connection_url(
            request.db_type,
            request.host,
            request.port,
            request.database,
            request.username,
            request.password,
        )

        success, message = test_connection(connection_url)
        if not success:
            raise HTTPException(status_code=400, detail=message)

        engine = create_engine_from_url(connection_url)
        profile = extract_data_profile(engine)

        response = {
            "status": "success",
            "tables_profiled": len(profile),
            "profile": profile,
        }
        safe_response = _to_json_safe(response)
        _write_data_file("profiling.json", safe_response)
        return safe_response

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/generate-business-doc")
def generate_db_business_doc():
    try:
        return generate_business_document()
    except FileNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to generate business document: {e}"
        )
