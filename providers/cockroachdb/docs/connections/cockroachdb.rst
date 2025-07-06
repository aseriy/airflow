.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

SQL Dialects
=============

The :class:`~airflow.providers.common.sql.dialects.dialect.Dialect` offers an abstraction layer between the
:class:`~airflow.providers.common.sql.hooks.sql.DbApiHook` implementation and the database.  For some databases,
multiple connection types are available, like native, ODBC, or JDBC. As the :class:`~airflow.providers.odbc.hooks.odbc.OdbcHook`
and the :class:`~airflow.providers.jdbc.hooks.jdbc.JdbcHook` are generic hooks allowing interaction with any
database that has a driver, a dialect abstraction layer was introduced to allow for specialized query execution
based on the database type.

The default :class:`~airflow.providers.common.sql.dialects.dialect.Dialect` class provides the following operations
(using SQLAlchemy under the hood), but they can be overridden with specialized implementations per database:

- ``placeholder``: placeholder format used in prepared statements (default: ``%s``)
- ``inspector``: returns a SQLAlchemy inspector to retrieve database metadata
- ``extract_schema_from_table``: extracts schema name from a table identifier
- ``get_column_names``: gets column names for a table/schema using the inspector
- ``get_primary_keys``: retrieves primary keys using the inspector
- ``get_target_fields``: returns column names excluding identity/autoincrement fields for use in `insert_rows` when
  ``core.dbapihook_resolve_target_fields`` is set to True
- ``reserved_words``: retrieves reserved SQL words for the database
- ``generate_insert_sql``: constructs insert statement for the target database
- ``generate_replace_sql``: constructs upsert statement for the target database

Currently supported dialects include:

- ``default`` :class:`~airflow.providers.common.sql.dialects.dialect.Dialect`: generic fallback
- ``mssql`` :class:`~airflow.providers.microsoft.mssql.dialects.mssql.MsSqlDialect`: for Microsoft SQL Server
- ``cockroachdb`` :class:`~airflow.providers.cockroachdb.dialects.cockroachdb.CockroachDBDialect`: for CockroachDB

The dialect is automatically selected based on the connection string. If this fails, you can manually specify it
via the connection's extra field:

.. code-block::

  dialect_name: 'cockroachdb'

If a specific dialect implementation doesn't exist or is misconfigured, the default dialect will be used.
