from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from .settings import DATABASE_URL

class Base(DeclarativeBase):
    pass

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=1800)

if engine.dialect.name == 'sqlite':
    # pysqlite 默认模式下 SELECT 各自自动提交、首条 DML 才 BEGIN，
    # 领取/回写序列的多次读之间快照不一致。统一 BEGIN IMMEDIATE，
    # 使并发领取在本地库上同样串行化（生产 MySQL 由 InnoDB 行锁保证）。
    @event.listens_for(engine, 'connect')
    def _sqlite_driver_autocommit(dbapi_connection, connection_record):
        dbapi_connection.isolation_level = None

    @event.listens_for(engine, 'begin')
    def _sqlite_begin_immediate(conn):
        conn.exec_driver_sql('BEGIN IMMEDIATE')

Session = sessionmaker(engine, expire_on_commit=False)

def get_db():
    with Session() as db:
        yield db
