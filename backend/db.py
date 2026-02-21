from datetime import date, datetime, time, timezone

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus


def build_connection_url(db_type, host, port, database, username, password):
    username = quote_plus(username)
    password = quote_plus(password)

    if db_type == "postgresql":
        return f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}?sslmode=require"

    elif db_type == "mysql":
        return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"

    elif db_type == "sqlserver":
        return (
            f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
        )

    else:
        raise Exception("Unsupported database type")
    

def create_engine_from_url(connection_url):
    return create_engine(connection_url)


def test_connection(connection_url):
    try:
        engine = create_engine(connection_url)

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        return True, "Connection successful"

    except SQLAlchemyError as e:
        return False, str(e)
    
def extract_schema(engine):
    inspector = inspect(engine)

    schema_data = []

    tables = inspector.get_table_names()

    for table in tables:
        columns = []
        for col in inspector.get_columns(table):
            columns.append({
                "name": col["name"],
                "type": str(col["type"]),
                "nullable": col["nullable"],
                "default": str(col.get("default"))
            })

        pk = inspector.get_pk_constraint(table).get("constrained_columns", [])

        fks = []
        for fk in inspector.get_foreign_keys(table):
            fks.append({
                "column": fk.get("constrained_columns"),
                "referred_table": fk.get("referred_table"),
                "referred_columns": fk.get("referred_columns")
            })

        schema_data.append({
            "table_name": table,
            "columns": columns,
            "primary_keys": pk,
            "foreign_keys": fks
        })
        
    return schema_data


def _quote_identifier(engine, name):
    return engine.dialect.identifier_preparer.quote(name)


def _quoted_table(engine, table_name, schema_name=None):
    table = _quote_identifier(engine, table_name)
    if schema_name:
        schema = _quote_identifier(engine, schema_name)
        return f"{schema}.{table}"
    return table


def _as_utc_datetime(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, time.min)
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def _is_temporal_column(column_type):
    normalized = str(column_type).lower()
    return "date" in normalized or "time" in normalized


def _pct(part, total):
    if total == 0:
        return None
    return round((part / total) * 100, 2)


def extract_data_profile(engine):
    inspector = inspect(engine)
    profiles = []
    now_utc = datetime.now(timezone.utc)

    for table in inspector.get_table_names():
        quoted_table = _quoted_table(engine, table)
        columns = inspector.get_columns(table)
        column_names = [col["name"] for col in columns]

        with engine.connect() as conn:
            total_rows = conn.execute(
                text(f"SELECT COUNT(*) FROM {quoted_table}")
            ).scalar_one()

            column_stats = []
            non_null_cells = 0
            for column_name in column_names:
                quoted_column = _quote_identifier(engine, column_name)
                non_null_count = conn.execute(
                    text(
                        f"SELECT COUNT({quoted_column}) "
                        f"FROM {quoted_table}"
                    )
                ).scalar_one()

                null_count = total_rows - non_null_count
                non_null_cells += non_null_count

                column_stats.append(
                    {
                        "column": column_name,
                        "non_null_count": non_null_count,
                        "null_count": null_count,
                        "completeness_pct": _pct(non_null_count, total_rows),
                    }
                )

            total_cells = total_rows * len(column_names)
            completeness = {
                "row_count": total_rows,
                "column_count": len(column_names),
                "non_null_cells": non_null_cells,
                "null_cells": total_cells - non_null_cells,
                "table_completeness_pct": _pct(non_null_cells, total_cells),
                "columns": column_stats,
            }

            temporal_columns = [
                col["name"] for col in columns if _is_temporal_column(col["type"])
            ]
            latest_timestamp = None
            latest_column = None
            freshness_columns = []

            for column_name in temporal_columns:
                quoted_column = _quote_identifier(engine, column_name)
                max_value = conn.execute(
                    text(f"SELECT MAX({quoted_column}) FROM {quoted_table}")
                ).scalar_one()
                parsed_value = _as_utc_datetime(max_value)

                freshness_columns.append(
                    {
                        "column": column_name,
                        "latest_value": parsed_value.isoformat()
                        if parsed_value
                        else None,
                    }
                )

                if parsed_value and (
                    latest_timestamp is None or parsed_value > latest_timestamp
                ):
                    latest_timestamp = parsed_value
                    latest_column = column_name

            freshness = {
                "temporal_columns_checked": len(temporal_columns),
                "latest_column": latest_column,
                "latest_timestamp": latest_timestamp.isoformat()
                if latest_timestamp
                else None,
                "staleness_days": round(
                    (now_utc - latest_timestamp).total_seconds() / 86400, 2
                )
                if latest_timestamp
                else None,
                "columns": freshness_columns,
            }

            pk_columns = inspector.get_pk_constraint(table).get(
                "constrained_columns", []
            )
            if pk_columns:
                null_condition = " OR ".join(
                    f"{_quote_identifier(engine, col)} IS NULL" for col in pk_columns
                )
                duplicate_group_by = ", ".join(
                    _quote_identifier(engine, col) for col in pk_columns
                )

                pk_null_rows = conn.execute(
                    text(
                        f"SELECT COUNT(*) FROM {quoted_table} "
                        f"WHERE {null_condition}"
                    )
                ).scalar_one()

                pk_duplicate_groups = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM ("
                        f"SELECT {duplicate_group_by}, COUNT(*) AS dup_count "
                        f"FROM {quoted_table} "
                        f"GROUP BY {duplicate_group_by} "
                        "HAVING COUNT(*) > 1"
                        ") AS duplicate_groups"
                    )
                ).scalar_one()

                pk_duplicate_rows = conn.execute(
                    text(
                        "SELECT COALESCE(SUM(dup_count - 1), 0) FROM ("
                        f"SELECT COUNT(*) AS dup_count "
                        f"FROM {quoted_table} "
                        f"GROUP BY {duplicate_group_by} "
                        "HAVING COUNT(*) > 1"
                        ") AS duplicate_rows"
                    )
                ).scalar_one()
            else:
                pk_null_rows = None
                pk_duplicate_groups = None
                pk_duplicate_rows = None

            fk_details = []
            total_orphan_rows = 0
            foreign_keys = inspector.get_foreign_keys(table)
            for fk in foreign_keys:
                local_cols = fk.get("constrained_columns", [])
                referred_table = fk.get("referred_table")
                referred_cols = fk.get("referred_columns", [])
                referred_schema = fk.get("referred_schema")

                if (
                    not local_cols
                    or not referred_table
                    or not referred_cols
                    or len(local_cols) != len(referred_cols)
                ):
                    continue

                left_table = _quoted_table(engine, table)
                right_table = _quoted_table(engine, referred_table, referred_schema)

                join_conditions = " AND ".join(
                    f"l.{_quote_identifier(engine, local_col)} = "
                    f"p.{_quote_identifier(engine, referred_col)}"
                    for local_col, referred_col in zip(local_cols, referred_cols)
                )

                local_has_value = " OR ".join(
                    f"l.{_quote_identifier(engine, local_col)} IS NOT NULL"
                    for local_col in local_cols
                )

                parent_missing = " AND ".join(
                    f"p.{_quote_identifier(engine, referred_col)} IS NULL"
                    for referred_col in referred_cols
                )

                orphan_rows = conn.execute(
                    text(
                        "SELECT COUNT(*) "
                        f"FROM {left_table} AS l "
                        f"LEFT JOIN {right_table} AS p "
                        f"ON {join_conditions} "
                        f"WHERE ({local_has_value}) "
                        f"AND ({parent_missing})"
                    )
                ).scalar_one()

                total_orphan_rows += orphan_rows
                fk_details.append(
                    {
                        "local_columns": local_cols,
                        "referred_table": referred_table,
                        "referred_columns": referred_cols,
                        "orphan_rows": orphan_rows,
                    }
                )

            if pk_columns:
                key_status = (
                    "healthy"
                    if pk_null_rows == 0
                    and pk_duplicate_rows == 0
                    and total_orphan_rows == 0
                    else "issues_found"
                )
            else:
                key_status = "missing_primary_key"

            key_health = {
                "status": key_status,
                "primary_key": {
                    "columns": pk_columns,
                    "null_rows": pk_null_rows,
                    "duplicate_groups": pk_duplicate_groups,
                    "duplicate_rows": pk_duplicate_rows,
                },
                "foreign_keys": {
                    "relationships_checked": len(fk_details),
                    "orphan_rows": total_orphan_rows,
                    "details": fk_details,
                },
            }

        profiles.append(
            {
                "table_name": table,
                "completeness": completeness,
                "freshness": freshness,
                "key_health": key_health,
            }
        )

    return profiles
