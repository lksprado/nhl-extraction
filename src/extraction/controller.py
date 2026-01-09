import pandas as pd
import sqlalchemy
from pathlib import Path
from typing import List, Optional, Callable
from psycopg2.extensions import connection as PGConn

def get_data_from_db(
    table: str,
    cols: List[str],
    schema: str = "staging",
    output_csv: Optional[Path] = None,
    return_as: str = "list",  # "list", "tuples", "df"
    bool_filter: Optional[tuple[str, bool]] = None,  # ("is_fully_synced", True)
    connection_provider: Optional[Callable[[], PGConn]] = None,
    engine: Optional[sqlalchemy.engine] = None,
):

    if not connection_provider and not engine:
        raise ValueError("Deve fornecer 'connection_provider' ou 'engine'")
    # -----------------------------
    # Monta query
    # -----------------------------
    columns_sql = ", ".join(cols)
    sql = f"SELECT {columns_sql} FROM {schema}.{table}"

    if bool_filter:
        col, value = bool_filter
        sql += f" WHERE {col} IS {'TRUE' if value else 'FALSE'}"

    # -----------------------------
    # Executa query
    # -----------------------------
    if connection_provider:
        # Usa connection_provider (psycopg2)
        conn = connection_provider()
        df = pd.read_sql(sql, con=conn)
        # Nota: não fechamos a conexão aqui, quem forneceu é responsável
    else:
        # Usa engine (SQLAlchemy)
        df = pd.read_sql(sql, con=engine)
    
    df = df.drop_duplicates(subset=cols)

    # -----------------------------
    # Salva CSV se solicitado
    # -----------------------------
    if output_csv:
        output_csv = Path(output_csv)
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_csv, index=False)

    # -----------------------------
    # Retornos
    # -----------------------------
    if return_as == "df":
        return df

    if return_as == "list":
        if len(cols) != 1:
            raise ValueError("list return requires exactly one column")
        return df[cols[0]].to_list()

    if return_as == "tuples":
        return list(df.itertuples(index=False, name=None))

    raise ValueError("return_as must be: 'list', 'tuples', or 'df'")
