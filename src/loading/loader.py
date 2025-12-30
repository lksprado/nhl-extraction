import json
import io
import psycopg2
from pathlib import Path
from typing import Iterable
from endpoints import EndpointConfig
import logging

class Loader:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
    ):
        self._conn_params = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
        }
        logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        )
        self.logger = logging.getLogger()

    def _get_connection(self):
        return psycopg2.connect(**self._conn_params)

    # ------------------------
    # DDL
    # ------------------------
    def _ensure_table(self, config: EndpointConfig):
        ddl = f"""
        CREATE SCHEMA IF NOT EXISTS {config.schema};

        CREATE TABLE IF NOT EXISTS {config.schema}.{config.table_name} (
            payload JSONB NOT NULL,
            source_filename TEXT  NOT NULL
        );
        """
        with self._get_connection() as conn, conn.cursor() as cur:
            cur.execute(ddl)
            
        self.logger.info(f"Table created if needed: {config.schema}.{config.table_name}")

    def _prepare_table(self, config: EndpointConfig):
        with self._get_connection() as conn, conn.cursor() as cur:
            if config.load_mode == "overwrite":
                cur.execute(f"TRUNCATE TABLE {config.schema}.{config.table_name}")
                self.logger.info(f"Table truncated: {config.schema}.{config.table_name}")

    def _ensure_control_table(self):
        ddl = """
        CREATE TABLE IF NOT EXISTS raw.nhl_ingestion_control (
            table_schema TEXT NOT NULL,
            table_name   TEXT NOT NULL,
            filename     TEXT NOT NULL,
            ingested_at  TIMESTAMP NOT NULL DEFAULT now(),
            PRIMARY KEY (table_schema, table_name, filename)
        );
        """
        with self._get_connection() as conn, conn.cursor() as cur:
            cur.execute(ddl)
        
        self.logger.info(f"Ingestion control table created if needed: raw.nhl_ingestion_control")


    # ------------------------
    # JSON → NDJSON (streaming em memória)
    # ------------------------
    def _json_to_buffer(self, input_path: Path, array_key: str | None, source_filename: str) -> io.StringIO:
        buffer = io.StringIO()

        with input_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        # CASO 1 — array no root
        if isinstance(data, list):
            iterable = data

        # CASO 2 — array dentro de uma chave
        elif isinstance(data, dict) and array_key:
            if array_key not in data or not isinstance(data[array_key], list):
                raise ValueError(f"array_key '{array_key}' not found or it is not list of dicts")
            iterable = data[array_key]

        # CASO 3 — objeto único
        else:
            iterable = [data]

        for item in iterable:
            buffer.write(
                json.dumps(item, ensure_ascii=False)
                + "\t"
                + source_filename
                + "\n"
            )

        buffer.seek(0)
        return buffer


    # ------------------------
    # COPY
    # ------------------------
    def _copy_payload(self, buffer: io.StringIO, config):
        copy_sql = f"""
        COPY {config.schema}.{config.table_name} (payload, source_filename)
        FROM STDIN WITH (FORMAT text)
        """

        with self._get_connection() as conn, conn.cursor() as cur:
            cur.copy_expert(copy_sql, buffer)
            conn.commit()

    # ------------------------
    # IDEMPOTENCIA
    # ------------------------

    def _register_ingestion_conn(self, cur, config, filename: str):
        sql = """
            INSERT INTO raw.nhl_ingestion_control (
                table_schema, table_name, filename
            ) VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        cur.execute(
            sql,
            (config.schema, config.table_name, filename)
        )

    def _load_ingested_filenames(self, config) -> set[str]:
        sql = """
            SELECT filename
            FROM raw.nhl_ingestion_control
            WHERE table_schema = %s
            AND table_name   = %s
        """
        with self._get_connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (config.schema, config.table_name))
            return {row[0] for row in cur.fetchall()}


    # ------------------------
    # API pública
    # ------------------------
    def load(self, config:EndpointConfig):
        """ CARGA UNICA FULL LOAD"""
        self._ensure_table(config)
        self._prepare_table(config)
        buffer = self._json_to_buffer(config.build_filepath(), config.array_key, config.filename)

        self._copy_payload(buffer, config)
        self.logger.info(f"Loading complete! Data loaded on: {config.schema}.{config.table_name}")

    def load_files(self, config: EndpointConfig, filepaths: Iterable[Path]):
        """ CARGA DINAMICA INCREMENTAL """
        self._ensure_table(config)
        self._prepare_table(config)
        self._ensure_control_table()
        
        file_count = len(filepaths)
        
        self.logger.info(f"Loading {file_count} files from {config.output_dir}...")
        
        ingested = self._load_ingested_filenames(config)
        
        
        new_files = [
            p for p in filepaths
            if p.name not in ingested
        ]

        if not new_files:
            self.logger.warning(
                f"No new files to ingest for {config.schema}.{config.table_name}. "
                f"All {len(filepaths)} file(s) were already processed."
                f"Loading complete!"
            )
            return

        with self._get_connection() as conn, conn.cursor() as cur:
            for filepath in new_files:
                buffer = self._json_to_buffer(filepath, config.array_key, filepath.name)

                cur.copy_expert(
                    f"""
                    COPY {config.schema}.{config.table_name}
                    (payload, source_filename)
                    FROM STDIN WITH (FORMAT text)
                    """,
                    buffer
                )


                self._register_ingestion_conn(cur, config, filepath.name)

            conn.commit()
            self.logger.info("Loading complete!")

