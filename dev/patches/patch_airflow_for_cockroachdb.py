from contextlib import contextmanager
from airflow.utils.db import DBLocks

def patch():
    import airflow.utils.db as db
    from airflow.utils.db import get_sqlalchemy_engine

    @contextmanager
    def noop_lock(session, lock):
        engine = get_sqlalchemy_engine()
        if "cockroachdb" in str(engine.url):
            yield
        else:
            yield from db._create_global_lock_real(session, lock)

    db.create_global_lock = noop_lock

patch()
