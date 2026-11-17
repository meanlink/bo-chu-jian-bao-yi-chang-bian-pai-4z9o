from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from src.config.database import get_db
from src.middlewares.auth import current_user, admin_only
from src.services import scheduler
from src.services.audit import record

router = APIRouter(prefix='/scheduler')


@router.get('')
def state(db=Depends(get_db), u=Depends(current_user)):
    return scheduler.snapshot(db)


class CapacityInput(BaseModel):
    capacity: int = Field(ge=scheduler.MIN_CAPACITY, le=scheduler.MAX_CAPACITY)


@router.put('/capacity')
def set_capacity(data: CapacityInput, db=Depends(get_db), u=Depends(admin_only)):
    scheduler.set_capacity(db, data.capacity, u.email)
    record(db, u.email, 'scheduler.capacity', {'capacity': data.capacity})
    db.commit()
    return scheduler.snapshot(db)
