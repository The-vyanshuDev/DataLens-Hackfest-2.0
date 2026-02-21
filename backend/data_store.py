import json
import re
from pathlib import Path
from typing import Any

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

CREDENTIALS_FILENAME = "credentials.json"
SCHEMA_FILENAME = "schema.json"
PROFILING_FILENAME = "profiling.json"
DOC_FILENAME = "doc.json"

SUPPORTED_DB_TYPES = {"mysql", "postgresql", "sqlserver"}
REQUIRED_CREDENTIAL_FIELDS = (
    "db_type",
    "host",
    "port",
    "database",
    "username",
    "password",
)


def slugify_database_name(database: str) -> str:
    if not database or not database.strip():
        raise ValueError("database is required.")

    slug = re.sub(r"[^a-z0-9]+", "-", database.strip().lower())
    slug = re.sub(r"-{2,}", "-", slug).strip("-")

    if not slug:
        raise ValueError("database must contain at least one alphanumeric character.")

    return slug


def get_database_dir(database: str, create: bool = False) -> Path:
    slug = slugify_database_name(database)
    db_dir = DATA_DIR / slug
    if create:
        db_dir.mkdir(parents=True, exist_ok=True)
    return db_dir


def get_database_file_path(database: str, filename: str, create_dir: bool = False) -> Path:
    return get_database_dir(database, create=create_dir) / filename


def get_credentials_file(database: str, create_dir: bool = False) -> Path:
    return get_database_file_path(database, CREDENTIALS_FILENAME, create_dir=create_dir)


def get_schema_file(database: str, create_dir: bool = False) -> Path:
    return get_database_file_path(database, SCHEMA_FILENAME, create_dir=create_dir)


def get_profiling_file(database: str, create_dir: bool = False) -> Path:
    return get_database_file_path(database, PROFILING_FILENAME, create_dir=create_dir)


def get_doc_file(database: str, create_dir: bool = False) -> Path:
    return get_database_file_path(database, DOC_FILENAME, create_dir=create_dir)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")

    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        raise ValueError(f"Required file is empty: {path}")

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in file {path}: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"JSON root must be an object in {path}")

    return payload


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _normalize_credentials_payload(credentials: dict[str, Any]) -> dict[str, Any]:
    normalized = {key: credentials.get(key) for key in REQUIRED_CREDENTIAL_FIELDS}

    missing = [key for key, value in normalized.items() if value in (None, "")]
    if missing:
        raise ValueError(f"Missing required credential fields: {', '.join(missing)}")

    db_type = str(normalized["db_type"]).strip().lower()
    if db_type not in SUPPORTED_DB_TYPES:
        raise ValueError(
            f"Unsupported db_type '{normalized['db_type']}'. "
            "Supported values: mysql, postgresql, sqlserver."
        )

    try:
        normalized["port"] = int(normalized["port"])
    except (TypeError, ValueError) as exc:
        raise ValueError("port must be a valid integer.") from exc

    normalized["db_type"] = db_type
    normalized["host"] = str(normalized["host"]).strip()
    normalized["database"] = str(normalized["database"]).strip()
    normalized["username"] = str(normalized["username"]).strip()
    normalized["password"] = str(normalized["password"])

    return normalized


def save_credentials(credentials: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_credentials_payload(credentials)
    database = normalized["database"]
    path = get_credentials_file(database, create_dir=True)
    write_json(path, normalized)
    return normalized


def load_credentials(database: str) -> dict[str, Any]:
    path = get_credentials_file(database, create_dir=False)
    payload = read_json(path)
    return _normalize_credentials_payload(payload)
