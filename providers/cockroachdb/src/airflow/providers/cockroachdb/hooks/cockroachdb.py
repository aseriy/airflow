from __future__ import annotations

import os
from contextlib import closing
from copy import deepcopy
from typing import TYPE_CHECKING, Any, TypeAlias, cast

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from psycopg2.extras import DictCursor, Json, NamedTupleCursor, RealDictCursor
from sqlalchemy.engine import URL

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.cockroachdb.dialects.cockroachdb import CockroachDBDialect

if TYPE_CHECKING:
    from psycopg2.extensions import connection

    from airflow.providers.common.sql.dialects.dialect import Dialect
    from airflow.providers.openlineage.sqlparser import DatabaseInfo

    try:
        from airflow.sdk import Connection
    except ImportError:
        from airflow.models.connection import Connection  # type: ignore[assignment]

CursorType: TypeAlias = DictCursor | RealDictCursor | NamedTupleCursor


class CockroachDBHook(DbApiHook):
    """
    Interact with CockroachDB using psycopg2.

    You can specify ssl parameters in the extra field of your connection
    as ``{"sslmode": "require", "sslcert": "/path/to/cert.pem", etc}``.
    Also you can choose cursor as ``{"cursor": "dictcursor"}``. Refer to the
    psycopg2.extras for more details.

    :param cockroachdb_conn_id: The Airflow connection ID to use.
    :param options: Optional command-line options to send to the server at connection start.
    :param enable_log_db_messages: If True, logs DB messages sent to the client during the session.
    """

    conn_name_attr = "cockroachdb_conn_id"
    default_conn_name = "cockroachdb_default"
    conn_type = "cockroachdb"
    hook_name = "CockroachDB"
    supports_autocommit = True
    supports_executemany = True
    ignored_extra_options = {
        "cursor",
        "sqlalchemy_scheme",
        "sqlalchemy_query",
    }

    def __init__(
        self, *args, options: str | None = None, enable_log_db_messages: bool = False, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.conn: connection = None
        self.database: str | None = kwargs.pop("database", None)
        self.options = options
        self.enable_log_db_messages = enable_log_db_messages

    @property
    def sqlalchemy_url(self) -> URL:
        conn = self.connection
        query = conn.extra_dejson.get("sqlalchemy_query", {})
        if not isinstance(query, dict):
            raise AirflowException("The parameter 'sqlalchemy_query' must be of type dict!")
        return URL.create(
            drivername="postgresql",
            username=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port,
            database=self.database or conn.schema,
            query=query,
        )

    @property
    def dialect_name(self) -> str:
        return "cockroachdb"

    @property
    def dialect(self) -> Dialect:
        return CockroachDBDialect(self)

    def _get_cursor(self, raw_cursor: str) -> CursorType:
        _cursor = raw_cursor.lower()
        cursor_types = {
            "dictcursor": psycopg2.extras.DictCursor,
            "realdictcursor": psycopg2.extras.RealDictCursor,
            "namedtuplecursor": psycopg2.extras.NamedTupleCursor,
        }
        if _cursor in cursor_types:
            return cursor_types[_cursor]
        valid_cursors = ", ".join(cursor_types.keys())
        raise ValueError(f"Invalid cursor passed {_cursor}. Valid options are: {valid_cursors}")

    def get_conn(self) -> connection:
        """Establish a connection to CockroachDB."""
        conn = deepcopy(self.connection)

        conn_args = {
            "host": conn.host,
            "user": conn.login,
            "password": conn.password,
            "dbname": self.database or conn.schema,
            "port": conn.port,
        }
        raw_cursor = conn.extra_dejson.get("cursor", False)
        if raw_cursor:
            conn_args["cursor_factory"] = self._get_cursor(raw_cursor)

        if self.options:
            conn_args["options"] = self.options

        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name not in self.ignored_extra_options:
                conn_args[arg_name] = arg_val

        self.conn = psycopg2.connect(**conn_args)
        return self.conn

    def get_uri(self) -> str:
        """
        Extract the URI from the connection.

        :return: the extracted URI in Sqlalchemy URI format.
        """
        return self.sqlalchemy_url.render_as_string(hide_password=False)

    def bulk_load(self, table: str, tmp_file: str) -> None:
        """Load a tab-delimited file into a database table."""
        self.copy_expert(f"COPY {table} FROM STDIN", tmp_file)

    def bulk_dump(self, table: str, tmp_file: str) -> None:
        """Dump a database table into a tab-delimited file."""
        self.copy_expert(f"COPY {table} TO STDOUT", tmp_file)

    def copy_expert(self, sql: str, filename: str) -> None:
        self.log.info("Running copy expert: %s, filename: %s", sql, filename)
        if not os.path.isfile(filename):
            with open(filename, "w"):
                pass

        with open(filename, "r+") as file, closing(self.get_conn()) as conn, closing(conn.cursor()) as cur:
            cur.copy_expert(sql, file)
            file.truncate(file.tell())
            conn.commit()

    @staticmethod
    def _serialize_cell(cell: object, conn: connection | None = None) -> Any:
        if isinstance(cell, (dict, list)):
            cell = Json(cell)
        return cell

    def get_table_primary_key(self, table: str, schema: str | None = "public") -> list[str] | None:
        return self.dialect.get_primary_keys(table=table, schema=schema)

    def get_openlineage_database_info(self, connection) -> DatabaseInfo:
        from airflow.providers.openlineage.sqlparser import DatabaseInfo

        authority = DbApiHook.get_openlineage_authority_part(connection, default_port=5432)

        return DatabaseInfo(
            scheme="postgres",
            authority=authority,
            database=self.database or connection.schema,
        )

    def get_openlineage_database_dialect(self, connection) -> str:
        return "cockroachdb"

    def get_openlineage_default_schema(self) -> str | None:
        return self.get_first("SELECT current_schema();")[0]

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": [],
            "relabeling": {
                "schema": "Database",
            },
        }

    def get_db_log_messages(self, conn) -> None:
        if self.enable_log_db_messages:
            for output in conn.notices:
                self.log.info(output)
