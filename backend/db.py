from sqlalchemy import create_engine, text, inspect
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
