from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from db import (
    build_connection_url,
    create_engine_from_url,
    test_connection,
    extract_schema
)

app = FastAPI()


class DBConnectionRequest(BaseModel):
    db_type: str
    host: str
    port: int
    database: str
    username: str
    password: str


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

        return {
            "status": "success",
            "tables_found": len(schema),
            "schema": schema
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))