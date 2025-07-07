import re
from sqlalchemy.dialects.postgresql.base import PGDialect

_original_get_server_version_info = PGDialect._get_server_version_info

def patched_get_server_version_info(self, connection):
    version = connection.scalar("select version()")
    if "CockroachDB" in version:
        match = re.search(r'v(\d+)\.(\d+)\.(\d+)', version)
        if match:
            return tuple(int(x) for x in match.groups())
        else:
            raise AssertionError(f"Could not parse CockroachDB version string: {version}")
    return _original_get_server_version_info(self, connection)

PGDialect._get_server_version_info = patched_get_server_version_info
