from src.config.database import Base, engine, Session
from src import models
JOB_COLUMNS = [
    ('lease_token', "VARCHAR(64) NOT NULL DEFAULT ''"),
    ('worker_id', "VARCHAR(120) NOT NULL DEFAULT ''"),
    ('lease_expires_at', 'DOUBLE'),
    ('claimed_at', 'DOUBLE'),
    ('attempts', 'INT NOT NULL DEFAULT 0'),
    ('takeover_reason', "VARCHAR(300) NOT NULL DEFAULT ''"),
]
def _existing_columns(conn, table):
    if engine.dialect.name == 'mysql':
        return {row[0] for row in conn.exec_driver_sql(f'SHOW COLUMNS FROM {table}')}
    return {row[1] for row in conn.exec_driver_sql(f'PRAGMA table_info({table})')}
def migrate():
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        existing = _existing_columns(conn, 'jobs')
        for name, ddl in JOB_COLUMNS:
            if name not in existing:
                conn.exec_driver_sql(f'ALTER TABLE jobs ADD COLUMN {name} {ddl}')
    from src.services.scheduler import ensure_state
    with Session() as db:
        ensure_state(db)
        db.commit()
if __name__=='__main__':migrate()
