from sqlalchemy import String, Double, Integer
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base
class SchedulerState(Base):
    __tablename__ = 'scheduler_state'
    id: Mapped[int] = mapped_column(primary_key=True)
    capacity: Mapped[int] = mapped_column(Integer, default=2)
    last_owner: Mapped[int] = mapped_column(Integer, default=0)
    updated_by: Mapped[str] = mapped_column(String(150), default='')
    updated_at: Mapped[float] = mapped_column(Double, default=0.0)
