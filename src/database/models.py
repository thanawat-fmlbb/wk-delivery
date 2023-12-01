from typing import Optional
from sqlmodel import SQLModel, Field, Session, select
from enum import Enum

from src.database.engine import get_engine

class StatusEnum(str, Enum):
    STAND_BY = "stand_by"
    PICK_UP = "pick_up"
    IN_PROGRESS = "in_progress"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    FAILED = "failed"

class DeliveryInfo(SQLModel, table=True):
    # delivery_id
    id: Optional[int] = Field(default=None, primary_key=True)
    main_id: int
    buyer_id: int # user id does not exist in this service
    buyer_address: str
    status: StatusEnum = Field(default=StatusEnum.STAND_BY)




def create_delivery(main_id: int, buyer_id: int, buyer_address: str):
    engine = get_engine()
    with Session(engine) as session:
        delivery = DeliveryInfo(main_id=main_id, buyer_id=buyer_id, buyer_address=buyer_address)
        session.add(delivery)
        session.commit()
        session.refresh(delivery)
        return delivery

# will become a separate task
def set_delivery_status(main_id: int, status: StatusEnum):
    engine = get_engine()
    with Session(engine) as session:
        statement = select(DeliveryInfo).where(DeliveryInfo.main_id == main_id)
        delivery = session.exec(statement).one()
        delivery.status = status
        session.add(delivery)
        session.commit()
        session.refresh(delivery)
        return delivery
