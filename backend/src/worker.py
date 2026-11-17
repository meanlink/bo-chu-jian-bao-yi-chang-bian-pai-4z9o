import json
import logging
import os
import socket
import threading
import time
import uuid
from pathlib import Path
from sqlalchemy import select, update
from src.config.database import Session
from src.models import Job, Dataset, Report
from src.services.business import analyze
from src.services.reports import deliver
from src.services.audit import record
from src.services import scheduler

logging.basicConfig(level=logging.INFO)
WORKER_ID = f'{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}'


def heartbeat_loop(job_id, token, stop):
    while not stop.wait(scheduler.HEARTBEAT_SECONDS):
        try:
            scheduler.heartbeat(job_id, token)
        except Exception:
            logging.exception('Heartbeat failed')


def execute(claimed, worker_id):
    job_id, token = claimed['id'], claimed['lease_token']
    with Session() as db:
        j = db.get(Job, job_id)
        if not j or j.status != 'running' or j.lease_token != token:
            return
        d = db.get(Dataset, j.dataset_id)
        records = d.records if d else None
        params = dict(j.params)
    stop = threading.Event()
    thread = threading.Thread(target=heartbeat_loop, args=(job_id, token, stop), daemon=True)
    thread.start()
    try:
        if records is None:
            raise ValueError('数据批次不存在')
        result = analyze(records, params)
        json.dumps(result, allow_nan=False)
        outcome, payload = 'completed', {'result': result, 'error': ''}
    except Exception as e:
        logging.exception('Analysis failed')
        outcome, payload = 'failed', {'error': str(e)[:990]}
    finally:
        stop.set()
    with Session() as db:
        scheduler.finish(db, job_id, token, worker_id, outcome, payload)


def work_once(worker_id):
    """领取并执行一个任务；返回是否有任务被领取。"""
    with Session() as db:
        claimed = scheduler.claim(db, worker_id)
    if claimed:
        execute(claimed, worker_id)
        return True
    return False


def deliver_due_reports():
    """两个处理进程共用投递：先原子推进 next_run 占位，未抢到则跳过。"""
    now = time.time()
    with Session() as db:
        ids = list(db.scalars(select(Report.id).where(Report.enabled == True, Report.next_run <= now)))
    for rid in ids:
        with Session() as db:
            r = db.get(Report, rid)
            if not r or not r.enabled or r.next_run > time.time():
                continue
            res = db.execute(
                update(Report).where(Report.id == rid, Report.next_run == r.next_run)
                .values(next_run=time.time() + r.interval_seconds)
                .execution_options(synchronize_session=False)
            )
            if res.rowcount != 1:
                db.rollback()
                continue
            d = deliver(db, r)
            if d:
                record(db, 'worker', 'report.delivered', {'id': r.id, 'delivery': d.id})
            db.commit()


def main():
    logging.info('Worker %s starting', WORKER_ID)
    # 中断恢复：候算任务保持原顺序等待，占位任务租约到期后由任一进程接管；
    # 不再把 running 重置为 queued（多进程下会重复执行且打乱排队顺序）。
    with Session() as db:
        try:
            scheduler.ensure_state(db)
            db.commit()
        except Exception:
            db.rollback()
    while True:
        Path('/tmp/heartbeat').touch()
        try:
            if not work_once(WORKER_ID):
                time.sleep(1)
            deliver_due_reports()
        except Exception:
            logging.exception('Worker retry')
            time.sleep(2)


if __name__ == '__main__':
    main()
