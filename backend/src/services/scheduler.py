"""全台执行名额与按负责人轮转的候算队列。

任务生命周期：候算(queued) → 占位执行(running，持有租约) → 释放名额(completed/failed)。
所有领取动作经 scheduler_state 单行锁与条件更新串行化：两个处理进程并发领取
不超额、不重领。进程失联后租约到期，原任务可被接管且不新增占位；接管与结果
回写都以 lease_token 作为 fencing token，旧执行迟到回写拒收；结果入库与释放
名额在同一个事务提交生效。
"""
import time
import uuid
from sqlalchemy import select, func, update, or_
from src.config.database import Session
from src.models import Job, SchedulerState, User
from src.services.audit import record

DEFAULT_CAPACITY = 2
MIN_CAPACITY = 1
MAX_CAPACITY = 16
LEASE_SECONDS = 90
HEARTBEAT_SECONDS = 30


def ensure_state(db):
    state = db.get(SchedulerState, 1)
    if state is None:
        state = SchedulerState(id=1, capacity=DEFAULT_CAPACITY, last_owner=0, updated_by='system', updated_at=time.time())
        db.add(state)
        db.flush()
    return state


def rotation_pick(owners, last_owner):
    """按负责人轮转：名额交给指针之后的第一位候算负责人，到队尾回绕。"""
    for owner in owners:
        if owner > last_owner:
            return owner
    return owners[0]


def projected_order(queued, last_owner):
    """把候算任务按轮转规则展开成预计派发顺序，供页面查队列顺序。"""
    remaining = sorted(queued, key=lambda j: j.id)
    order, pointer = [], last_owner
    while remaining:
        owners = sorted({j.created_by for j in remaining})
        owner = rotation_pick(owners, pointer)
        nxt = min((j for j in remaining if j.created_by == owner), key=lambda j: j.id)
        order.append(nxt)
        remaining.remove(nxt)
        pointer = owner
    return order


def claim(db, worker_id, now=None):
    """领取一个任务并占位。返回 {'id','lease_token','takeover'} 或 None。

    优先接管失联进程的任务（租约到期，沿用原占位，不新增名额占用）；
    有空位时按负责人轮转领取候算任务。已有运行不抢停：占位达到名额即不再放行。
    """
    now = time.time() if now is None else now
    state = db.scalar(select(SchedulerState).where(SchedulerState.id == 1).with_for_update())
    if state is None:
        ensure_state(db)
        db.commit()
        return None
    stale = db.scalars(
        select(Job)
        .where(Job.status == 'running', or_(Job.lease_expires_at.is_(None), Job.lease_expires_at < now))
        .order_by(Job.claimed_at)
        .limit(1)
    ).first()
    if stale is not None:
        token = uuid.uuid4().hex
        reason = f'原执行进程 {stale.worker_id or "未知"} 失联，租约到期由 {worker_id} 接管'
        res = db.execute(
            update(Job)
            .where(
                Job.id == stale.id,
                Job.status == 'running',
                Job.lease_token == stale.lease_token,
                or_(Job.lease_expires_at.is_(None), Job.lease_expires_at < now),
            )
            .values(lease_token=token, worker_id=worker_id, lease_expires_at=now + LEASE_SECONDS,
                    attempts=stale.attempts + 1, takeover_reason=reason)
            .execution_options(synchronize_session=False)
        )
        if res.rowcount == 1:
            record(db, worker_id, 'job.takeover', {'id': stale.id, 'from': stale.worker_id, 'attempts': stale.attempts + 1})
            db.commit()
            return {'id': stale.id, 'lease_token': token, 'takeover': True}
        db.rollback()
        return None
    running = db.scalar(select(func.count()).select_from(Job).where(Job.status == 'running'))
    if running >= state.capacity:
        db.rollback()
        return None
    owners = sorted(db.scalars(select(Job.created_by).where(Job.status == 'queued').distinct()).all())
    if not owners:
        db.rollback()
        return None
    owner = rotation_pick(owners, state.last_owner or 0)
    job = db.scalars(select(Job).where(Job.status == 'queued', Job.created_by == owner).order_by(Job.id).limit(1)).first()
    token = uuid.uuid4().hex
    res = db.execute(
        update(Job)
        .where(Job.id == job.id, Job.status == 'queued')
        .values(status='running', lease_token=token, worker_id=worker_id,
                lease_expires_at=now + LEASE_SECONDS, claimed_at=now, attempts=job.attempts + 1)
        .execution_options(synchronize_session=False)
    )
    if res.rowcount == 1:
        state.last_owner = owner
        db.commit()
        return {'id': job.id, 'lease_token': token, 'takeover': False}
    db.rollback()
    return None


def heartbeat(job_id, token):
    """续租；租约已易主时静默失效，该执行的最终回写会被拒收。"""
    with Session() as db:
        db.execute(
            update(Job)
            .where(Job.id == job_id, Job.lease_token == token, Job.status == 'running')
            .values(lease_expires_at=time.time() + LEASE_SECONDS)
            .execution_options(synchronize_session=False)
        )
        db.commit()


def finish(db, job_id, token, worker_id, outcome, payload):
    """结果入库与释放名额在同一事务生效；旧执行迟到回写拒收。"""
    values = {'status': outcome, 'finished_at': time.time(), 'lease_expires_at': None, **payload}
    res = db.execute(
        update(Job)
        .where(Job.id == job_id, Job.lease_token == token, Job.status == 'running')
        .values(**values)
        .execution_options(synchronize_session=False)
    )
    if res.rowcount == 1:
        record(db, worker_id, f'job.{outcome}', {'id': job_id})
        db.commit()
        return True
    record(db, worker_id, 'job.writeback_rejected', {'id': job_id, 'reason': '租约已易主，迟到回写拒收'})
    db.commit()
    return False


def set_capacity(db, capacity, actor):
    """负责人调整全台执行名额；已有运行不抢停，缩减后待空位再放行。"""
    state = ensure_state(db)
    state.capacity = capacity
    state.updated_by = actor
    state.updated_at = time.time()
    db.flush()
    return state


def snapshot(db):
    """页面数据：名额、占位（含接管原因）、按负责人轮转的预计候算顺位。"""
    state = db.get(SchedulerState, 1)
    capacity = state.capacity if state else DEFAULT_CAPACITY
    last_owner = (state.last_owner if state else 0) or 0
    now = time.time()
    running = db.scalars(select(Job).where(Job.status == 'running').order_by(Job.claimed_at)).all()
    queued = db.scalars(select(Job).where(Job.status == 'queued').order_by(Job.id)).all()
    names = {u.id: u.name for u in db.scalars(select(User))}
    return {
        'capacity': capacity,
        'running': len(running),
        'queued': len(queued),
        'overcommitted': len(running) > capacity,
        'updated_by': state.updated_by if state else '',
        'updated_at': state.updated_at if state else 0,
        'lease_seconds': LEASE_SECONDS,
        'slots': [
            {'job_id': j.id, 'name': j.name, 'owner': names.get(j.created_by, str(j.created_by)),
             'worker_id': j.worker_id, 'attempts': j.attempts, 'claimed_at': j.claimed_at,
             'lease_remaining': max(0, round((j.lease_expires_at or now) - now)),
             'takeover_reason': j.takeover_reason}
            for j in running
        ],
        'queue': [
            {'position': i + 1, 'job_id': j.id, 'name': j.name, 'owner': names.get(j.created_by, str(j.created_by)),
             'created_at': j.created_at, 'attempts': j.attempts}
            for i, j in enumerate(projected_order(queued, last_owner))
        ],
    }
