# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import json
import logging
import os
from unittest import mock

import psycopg2.extras
import pytest
import sqlalchemy
from psycopg2.extras import Json

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.cockroachdb.dialects.cockroachdb import CockroachDBDialect
from airflow.providers.cockroachdb.hooks.cockroachdb import CockroachDBHook
from airflow.utils.types import NOTSET

INSERT_SQL_STATEMENT = "INSERT INTO connection (id, conn_id, conn_type, description, host, {}, login, password, port, is_encrypted, is_extra_encrypted, extra) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"


class TestCockroachDBHookConn:
    def setup_method(self):
        self.connection = Connection(login="login", password="password", host="host", schema="database")

        class UnitTestCockroachDBHook(CockroachDBHook):
            conn_name_attr = "test_conn_id"

        self.db_hook = UnitTestCockroachDBHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @mock.patch("airflow.providers.cockroachdb.hooks.cockroachdb.psycopg2.connect")
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            user="login", password="password", host="host", dbname="database", port=None
        )

    def test_get_uri(self):
        self.connection.conn_type = "cockroachdb"
        self.connection.port = 26257
        hook = CockroachDBHook(connection=self.connection)
        assert hook.get_uri() == "postgresql://login:password@host:26257/database"

    def test_sqlalchemy_url(self):
        conn = Connection(login="login-conn", password="password-conn", host="host", schema="database")
        hook = CockroachDBHook(connection=conn)
        assert str(hook.sqlalchemy_url) == "postgresql://login-conn:password-conn@host/database"

    def test_sqlalchemy_url_with_sqlalchemy_query(self):
        conn = Connection(
            login="login-conn",
            password="password-conn",
            host="host",
            schema="database",
            extra=json.dumps({"sqlalchemy_query": {"sslmode": "require"}}),
        )
        hook = CockroachDBHook(connection=conn)
        assert str(hook.sqlalchemy_url) == "postgresql://login-conn:password-conn@host/database?sslmode=require"

    def test_sqlalchemy_url_with_wrong_sqlalchemy_query_value(self):
        conn = Connection(
            login="login-conn",
            password="password-conn",
            host="host",
            schema="database",
            extra=json.dumps({"sqlalchemy_query": "notadict"}),
        )
        hook = CockroachDBHook(connection=conn)
        with pytest.raises(AirflowException):
            _ = hook.sqlalchemy_url

    def test_dialect_name(self):
        hook = CockroachDBHook()
        assert hook.dialect_name == "postgresql"

    def test_dialect(self):
        hook = CockroachDBHook()
        assert isinstance(hook.dialect, CockroachDBDialect)

    def test_reserved_words(self):
        hook = CockroachDBHook()
        assert hook.reserved_words == sqlalchemy.dialects.postgresql.base.RESERVED_WORDS

    def test_generate_insert_sql_without_already_escaped_column_name(self):
        values = [
            "1", "crdb_conn", "cockroachdb", "CRDB connection", "localhost", "airflow",
            "admin", "admin", 26257, False, False, {}
        ]
        target_fields = [
            "id", "conn_id", "conn_type", "description", "host", "schema",
            "login", "password", "port", "is_encrypted", "is_extra_encrypted", "extra"
        ]
        hook = CockroachDBHook()
        assert hook._generate_insert_sql(
            table="connection", values=values, target_fields=target_fields
        ) == INSERT_SQL_STATEMENT.format("schema")

    def test_generate_insert_sql_with_already_escaped_column_name(self):
        values = [
            "1", "crdb_conn", "cockroachdb", "CRDB connection", "localhost", "airflow",
            "admin", "admin", 26257, False, False, {}
        ]
        target_fields = [
            "id", "conn_id", "conn_type", "description", "host", '"schema"',
            "login", "password", "port", "is_encrypted", "is_extra_encrypted", "extra"
        ]
        hook = CockroachDBHook()
        assert hook._generate_insert_sql(
            table="connection", values=values, target_fields=target_fields
        ) == INSERT_SQL_STATEMENT.format('"schema"')
