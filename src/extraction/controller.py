import pandas as pd
import sqlalchemy
from pathlib import Path
from typing import List, Optional
import csv

def get_data_from_db(
    engine: sqlalchemy.Engine,
    table: str,
    cols: List[str],
    schema: str = "staging",
    output_csv: Optional[Path] = None,
    return_as: str = "list",  # "list", "tuples", "df"
):
    df = pd.read_sql_table(
        table_name=table,
        con=engine,
        schema=schema,
        columns=cols
    )

    df = df.drop_duplicates(subset=cols)

    # Salva CSV se solicitado
    if output_csv:
        output_csv = Path(output_csv)
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_csv, index=False)

    # Retorno controlado
    if return_as == "df":
        return df

    if return_as == "list":
        if len(cols) != 1:
            raise ValueError("list return requires exactly one column")
        return df[cols[0]].to_list()

    if return_as == "tuples":
        return list(df.itertuples(index=False, name=None))

    raise ValueError("return_as must be: 'list', 'tuples', or 'df'")


def open_csv(filepath: str | Path) -> list:
    filepath = Path(filepath)

    with filepath.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # pula header
        return [row[0] for row in reader]


# if __name__ == '__main__':
#   engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:pg12345@localhost:5435/postgres')
#   output = './data/new_for_game_details.csv'
#   pass
  