import json
import io
import psycopg2
from psycopg2.extensions import connection as PGConn 
from pathlib import Path
from typing import Iterable, Callable, Optional
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
        connection_provider: Optional[Callable[[], PGConn]] = None,
    ):
        self._connection_provider = connection_provider
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
        self.logger = logging.getLogger(__name__)

    def _get_connection(self) -> PGConn:
        try:
            if self._connection_provider:
                return self._connection_provider()
            return psycopg2.connect(**self._conn_params)
        except:
            raise ConnectionError("Check credentials or if instance is running")
        
    # ------------------------
    # DDL
    # ------------------------
    def _ensure_table(self, config: EndpointConfig, cur):
        """Creates the table for the data to be loaded as JSONB using the provided cursor."""
        
        ddl = f"""
        CREATE SCHEMA IF NOT EXISTS {config.schema};

        CREATE TABLE IF NOT EXISTS {config.schema}.{config.table_name} (
            payload JSONB NOT NULL,
            source_filename TEXT  NOT NULL
        );
        """
        
        cur.execute(ddl)
        self.logger.info(f"Target table available: {config.schema}.{config.table_name}")

    def _prepare_table(self, config: EndpointConfig, cur):
        """If it is overwrite ingestion, truncate the target table using the provided cursor."""
        if config.is_overwrite:
            cur.execute(f"TRUNCATE TABLE {config.schema}.{config.table_name}")
            self.logger.info(f"Table truncated: {config.schema}.{config.table_name}")

    def _ensure_control_table(self, config: EndpointConfig, cur):
        """Create control table if needed and delete ingestion metadata records if is overwrite."""
        ddl = """
        CREATE TABLE IF NOT EXISTS raw.nhl_ingestion_control (
            table_schema TEXT NOT NULL,
            table_name   TEXT NOT NULL,
            filename     TEXT NOT NULL,
            ingested_at  TIMESTAMP NOT NULL DEFAULT now(),
            is_overwrite BOOLEAN NOT NULL DEFAULT FALSE,
            PRIMARY KEY (table_schema, table_name, filename)
        );
        """
        cur.execute(ddl)
        
        self.logger.info(f"Ingestion control table created if needed: raw.nhl_ingestion_control")

        if config.is_overwrite:
            ddl_delete = """
                DELETE FROM raw.nhl_ingestion_control
                WHERE table_schema=%s AND table_name=%s
            """
            cur.execute(
                ddl_delete,
                (config.schema, config.table_name)
            )
            self.logger.info(f"Preparing overwrite loading. Deleted ingestion records for {config.schema}.{config.table_name}")

    # ------------------------
    # JSON → NDJSON (streaming em memória)
    # ------------------------
    def _json_to_buffer(self, input_path: Path, array_key: str | None, source_filename: str) -> io.StringIO:
        """Serializes json file.

        Args:
            input_path (Path): JSON file to be serialized
            array_key (str | None): Parses through provided array key
            source_filename (str): Metadata

        Raises:
            ValueError: Invalid JSON in input_path
            ValueError: Invalid array key or not list of dicts

        Returns:
            io.StringIO: Buffer content
        """
        buffer = io.StringIO()

        with input_path.open("r", encoding="utf-8") as f:
            raw_text = f.read()

        try:
            data = json.loads(raw_text)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in file {input_path}: {e}")

        if isinstance(data, list):
            iterable = data
        elif isinstance(data, dict) and array_key:
            if array_key not in data or not isinstance(data[array_key], list):
                raise ValueError(f"array_key '{array_key}' not found or it is not list of dicts")
            iterable = data[array_key]
        else:
            iterable = [data]

        for idx, item in enumerate(iterable, start=1):
            try:
                # Gera JSON compactado
                json_text = json.dumps(item, ensure_ascii=False, separators=(',', ':'))
                json.loads(json_text)  # validação
            except Exception as e:
                raise ValueError(
                    f"Invalid JSON payload in file {input_path} "
                    f"(record {idx}): {e}"
                )

            # Escape para TEXT format do PostgreSQL
            escaped = (
                json_text
                .replace("\\", "\\\\")  # \ → \\
                .replace("\n", "\\n")   # newline → \n
                .replace("\r", "\\r")   # CR → \r
                .replace("\t", "\\t")   # tab → \t
            )
            
            buffer.write(f"{escaped}\t{source_filename}\n")

        buffer.seek(0)
        return buffer

    # ------------------------
    # COPY
    # ------------------------
    def _copy_payload(self, buffer: io.StringIO, config, cur):
        copy_sql = f"""
        COPY {config.schema}.{config.table_name} (payload, source_filename)
        FROM STDIN WITH (FORMAT text)
        """
        cur.copy_expert(copy_sql, buffer)

    # ------------------------
    # IDEMPOTENCIA
    # ------------------------

    def _register_ingestion_conn(self, cur, config, filename: str):
        """Register ingested files to raw.nhl_ingestion_control """
        sql = """
            INSERT INTO raw.nhl_ingestion_control (
                table_schema, table_name, filename, is_overwrite
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        cur.execute(
            sql,
            (config.schema, config.table_name, filename, config.is_overwrite)
        )

    def _load_ingested_filenames(self, config, cur) -> set[str]:
        sql = """
            SELECT filename
            FROM raw.nhl_ingestion_control
            WHERE table_schema = %s
            AND table_name   = %s
        """
        
        cur.execute(sql, (config.schema, config.table_name))
        return {row[0] for row in cur.fetchall()}


    # ------------------------
    # API pública
    # ------------------------
    def load(self, config:EndpointConfig):
        """Loads unique file full load"""
        with self._get_connection() as conn, conn.cursor() as cur:
            self._ensure_table(config, cur)
            self._prepare_table(config, cur)
            buffer = self._json_to_buffer(config.build_filepath(), config.array_key, config.filename)

            self._copy_payload(buffer, config, cur)
            conn.commit()
            self.logger.info(f"Loading complete! Data loaded on: {config.schema}.{config.table_name}")

    def load_files(self, config: EndpointConfig, filepaths: Iterable[Path]):
        """Loads multiple files with idempotency"""
        with self._get_connection() as conn, conn.cursor() as cur:
            self._ensure_table(config, cur)
            self._prepare_table(config, cur)
            self._ensure_control_table(config, cur)

            if config.is_overwrite:
                new_files = list(filepaths)
            else:
                ingested = self._load_ingested_filenames(config, cur)
                new_files = [
                    p for p in filepaths
                    if p.name not in ingested
                ]

            file_count = len(new_files)
            self.logger.info(f"Loading {file_count} files from {config.output_dir}...")
            
            if not new_files:
                self.logger.warning(
                    f"No new files to ingest for {config.schema}.{config.table_name}. "
                    f"All {len(filepaths)} file(s) were already processed."
                    f"Loading complete!"
                )
                return

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
