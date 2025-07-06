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

def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-cockroachdb",
        "name": "CockroachDB",
        "description": "`CockroachDB <https://www.cockroachlabs.com/>`__\n",
        "integrations": [
            {
                "integration-name": "CockroachDB",
                "external-doc-url": "https://www.cockroachlabs.com/",
                "how-to-guide": [
                    "/docs/apache-airflow-providers-cockroachdb/operators.rst"
                ],
                "logo": "/docs/integration-logos/CockroachDB.png",
                "tags": ["software"],
            }
        ],
        "dialects": [
            {
                "dialect-type": "cockroachdb",
                "dialect-class-name": "airflow.providers.cockroachdb.dialects.cockroachdb.CockroachDBDialect",
            }
        ],
        "hooks": [
            {
                "integration-name": "CockroachDB",
                "python-modules": ["airflow.providers.cockroachdb.hooks.cockroachdb"],
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.cockroachdb.hooks.cockroachdb.CockroachDBHook",
                "connection-type": "cockroachdb",
            }
        ],
        "asset-uris": [
            {
                "schemes": ["cockroachdb"],
                "handler": "airflow.providers.cockroachdb.assets.cockroachdb.sanitize_uri",
            }
        ],
        "dataset-uris": [
            {
                "schemes": ["cockroachdb"],
                "handler": "airflow.providers.cockroachdb.assets.cockroachdb.sanitize_uri",
            }
        ],
    }
    