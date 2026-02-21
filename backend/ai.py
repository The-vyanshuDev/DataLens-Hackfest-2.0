import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from google import genai
from google.genai import types

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
ENV_FILE = Path(__file__).resolve().parent / ".env"
SCHEMA_FILE = DATA_DIR / "schema.json"
PROFILING_FILE = DATA_DIR / "profiling.json"
DOC_FILE = DATA_DIR / "doc.json"
DEFAULT_MODEL = "gemini-flash-latest"


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")

    raw_text = path.read_text(encoding="utf-8").strip()
    if not raw_text:
        raise ValueError(f"Required file is empty: {path}")

    try:
        return json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in file {path}: {exc}") from exc


def _load_env_file(path: Path = ENV_FILE) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line[len("export "):].strip()

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


def _get_required_env_var(key: str) -> str:
    value = os.getenv(key)
    if value:
        return value

    _load_env_file()
    value = os.getenv(key)
    if value:
        return value

    raise ValueError(f"{key} environment variable is not set.")


def _safe_json_loads(raw_text: str) -> dict[str, Any]:
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        lines = [line for line in cleaned.splitlines() if not line.startswith("```")]
        cleaned = "\n".join(lines).strip()

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Gemini did not return valid JSON: {exc}") from exc

    if not isinstance(parsed, dict):
        raise ValueError("Gemini JSON response must be an object.")

    return parsed


def _build_genai_client(api_key: str) -> genai.Client:
    # Force Gemini API key mode to avoid accidental Vertex/OAuth routing from host env.
    return genai.Client(api_key=api_key, vertexai=False)


def _to_string_list(value: Any, fallback: list[str]) -> list[str]:
    if isinstance(value, list):
        cleaned = [str(item).strip() for item in value if str(item).strip()]
        if cleaned:
            return cleaned
    return fallback


def _normalize_priority(value: Any) -> str:
    priority = str(value or "").strip().lower()
    if priority in {"high", "medium", "low"}:
        return priority
    return "medium"


def _build_quality_observations(profile_entry: dict[str, Any]) -> list[str]:
    observations = []

    completeness = profile_entry.get("completeness", {})
    completeness_pct = completeness.get("table_completeness_pct")
    if completeness_pct is not None:
        observations.append(f"Table completeness is {completeness_pct}%.")

    freshness = profile_entry.get("freshness", {})
    latest_timestamp = freshness.get("latest_timestamp")
    staleness_days = freshness.get("staleness_days")
    if latest_timestamp:
        observations.append(
            f"Latest timestamp observed is {latest_timestamp} with staleness of {staleness_days} days."
        )
    else:
        observations.append("No temporal column was found for freshness analysis.")

    key_health = profile_entry.get("key_health", {})
    key_status = key_health.get("status")
    if key_status:
        observations.append(f"Key health status is {key_status}.")

    return observations


def _default_business_summary(table_name: str, table_schema: dict[str, Any]) -> str:
    column_count = len(table_schema.get("columns", []))
    return (
        f"{table_name} is a core business table with {column_count} columns. "
        "Use it as a governed source in analytics and reporting workflows."
    )


def _build_prompt(
    schema_payload: dict[str, Any], profiling_payload: dict[str, Any]
) -> str:
    return f"""
You are an expert analytics consultant.
Create a business-friendly documentation JSON from the provided schema and profiling data.

Return ONLY a valid JSON object with this exact top-level structure:
{{
  "overview_summary": "string",
  "global_recommendations": ["string", "string", "string"],
  "tables": [
    {{
      "table_name": "string",
      "business_summary": "string",
      "usage_recommendations": ["string", "string"],
      "data_quality_observations": ["string", "string"],
      "suggested_kpis": ["string", "string"],
      "priority": "high|medium|low"
    }}
  ]
}}

Rules:
1) Include every table from the schema exactly once.
2) Use the exact table_name values from schema input.
3) Keep writing practical, concise, and business-friendly.
4) No markdown, no extra keys, JSON only.

Schema JSON:
{json.dumps(schema_payload, ensure_ascii=True)}

Profiling JSON:
{json.dumps(profiling_payload, ensure_ascii=True)}
""".strip()


def _normalize_document(
    llm_payload: dict[str, Any],
    schema_payload: dict[str, Any],
    profiling_payload: dict[str, Any],
    model_name: str,
) -> dict[str, Any]:
    schema_tables = schema_payload.get("schema", [])
    profile_tables = profiling_payload.get("profile", [])
    profile_by_table = {
        entry.get("table_name"): entry for entry in profile_tables if isinstance(entry, dict)
    }

    llm_tables = llm_payload.get("tables", [])
    llm_by_table = {
        entry.get("table_name"): entry for entry in llm_tables if isinstance(entry, dict)
    }

    table_docs = []
    for table_entry in schema_tables:
        table_name = table_entry.get("table_name")
        if not table_name:
            continue

        llm_table = llm_by_table.get(table_name, {})
        profile_entry = profile_by_table.get(table_name, {})

        table_docs.append(
            {
                "table_name": table_name,
                "business_summary": str(
                    llm_table.get("business_summary")
                    or _default_business_summary(table_name, table_entry)
                ),
                "usage_recommendations": _to_string_list(
                    llm_table.get("usage_recommendations"),
                    ["Define clear ownership and dashboard use cases for this table."],
                ),
                "data_quality_observations": _to_string_list(
                    llm_table.get("data_quality_observations"),
                    _build_quality_observations(profile_entry),
                ),
                "suggested_kpis": _to_string_list(
                    llm_table.get("suggested_kpis"),
                    ["Define business KPIs based on this table and track trends weekly."],
                ),
                "priority": _normalize_priority(llm_table.get("priority")),
            }
        )

    overview_summary = str(
        llm_payload.get("overview_summary")
        or "Business documentation generated from schema and profiling outputs."
    )

    global_recommendations = _to_string_list(
        llm_payload.get("global_recommendations"),
        [
            "Adopt data ownership per table and define SLAs for quality metrics.",
            "Prioritize remediation on low-completeness and stale tables first.",
            "Use key-health checks as a release gate for downstream reporting.",
        ],
    )

    return {
        "status": "success",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": model_name,
        "sources": {
            "schema_file": "data/schema.json",
            "profiling_file": "data/profiling.json",
            "tables_in_schema": len(schema_tables),
            "tables_in_profile": len(profile_tables),
        },
        "overview": {
            "summary": overview_summary,
            "global_recommendations": global_recommendations,
        },
        "tables": table_docs,
    }


def generate_business_document() -> dict[str, Any]:
    api_key = _get_required_env_var("GEMINI_API_KEY")
    model_name = os.getenv("GEMINI_MODEL", DEFAULT_MODEL)
    schema_payload = _read_json_file(SCHEMA_FILE)
    profiling_payload = _read_json_file(PROFILING_FILE)

    prompt = _build_prompt(schema_payload, profiling_payload)
    client = _build_genai_client(api_key)

    try:
        response = client.models.generate_content(
            model=model_name,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0,
                response_mime_type="application/json",
            ),
        )
    except Exception as exc:
        error_text = str(exc)
        if "UNAUTHENTICATED" in error_text or "API keys are not supported" in error_text:
            raise ValueError(
                "Gemini authentication failed for API-key mode. "
                "Ensure GEMINI_API_KEY is a valid Gemini API key from Google AI Studio, "
                "and unset Vertex env flags such as GOOGLE_GENAI_USE_VERTEXAI."
            ) from exc
        raise

    raw_response = (response.text or "").strip()
    if not raw_response:
        raise ValueError("Gemini returned an empty response.")

    llm_payload = _safe_json_loads(raw_response)
    final_document = _normalize_document(
        llm_payload=llm_payload,
        schema_payload=schema_payload,
        profiling_payload=profiling_payload,
        model_name=model_name,
    )

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DOC_FILE.write_text(json.dumps(final_document, indent=2), encoding="utf-8")

    return final_document
