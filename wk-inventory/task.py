import os

from celery import Celery
from dotenv import load_dotenv
from .models import create_delivery, create_item_check, create_items


def create_celery_app():
    load_dotenv()
    internal_app = Celery("inventory",
                          broker=f"redis://"
                                 f"{os.environ.get('REDIS_HOSTNAME', 'localhost')}"
                                 f":{os.environ.get('REDIS_PORT', '6381')}"
                                 f"/", # TODO: change channel
                          backend=f"redis://"
                                  f"{os.environ.get('REDIS_HOSTNAME', 'localhost')}"
                                  f":{os.environ.get('REDIS_PORT', '6381')}"
                                  f"/", # TODO: change channel
                          broker_connection_retry_on_startup=True)
    return internal_app

app = create_celery_app()

@app.task
def make_delivery(main_id: int, buyer_obj):
    try:
        create_delivery(main_id=main_id, buyer_id=buyer_obj.id, buyer_address=buyer_obj.address)
    except Exception as e:
        print(e)
        return False

@app.task
def rollback(main_id: int):
    try:
        print("WIP")
    except Exception as e:
        print(e)
        return False

