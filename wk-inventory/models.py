import os
from dotenv import load_dotenv
from typing import Optional
from sqlmodel import SQLModel, Field, Session, create_engine, select
from enum import Enum

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

load_dotenv()
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'password')
DB_HOSTNAME = os.environ.get('DB_HOSTNAME', 'localhost')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ.get('DB_NAME', 'postgres')

db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOSTNAME}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)
SQLModel.metadata.create_all(engine)


def create_delivery(main_id: int, buyer_id: int, buyer_address: str):
    with Session(engine) as session:
        delivery = DeliveryInfo(main_id=main_id, buyer_id=buyer_id, buyer_address=buyer_address)
        session.add(delivery)
        session.commit()
        session.refresh(delivery)
        return delivery

# will become a separate task
def set_delivery_status(delivery_id: int, status: StatusEnum):
    with Session(engine) as session:
        statement = select(DeliveryInfo).where(DeliveryInfo.id == delivery_id)
        delivery = session.exec(statement).one()
        if status == StatusEnum.FAILED:
            print("should roll back")
        delivery.status = status
        session.add(delivery)
        session.commit()
        session.refresh(delivery)
        return delivery
