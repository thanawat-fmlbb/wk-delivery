import os

from celery import Celery
from dotenv import load_dotenv
from .models import create_delivery, create_db_and_tables


load_dotenv()
REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME', 'localhost')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')

def get_celery_app(channel_number: int):
    redis_url = f"redis://{REDIS_HOSTNAME}:{REDIS_PORT}/{channel_number}"
    return Celery(  "delivery",
                    broker=redis_url,
                    backend=redis_url,
                    broker_connection_retry_on_startup=True)

app = get_celery_app(2)
create_db_and_tables()

RESULT_TASK_NAME = "wk-irs.tasks.send_result"
result_collector = get_celery_app(4)

@app.task
def make_delivery(**kwargs):
    main_id = kwargs.get('main_id')
    user_id = kwargs.get('user_id')
    user_address = kwargs.get('user_address')
    deliver_info_created = False
    try:
        create_delivery(main_id=main_id, buyer_id=user_id, buyer_address=user_address)
        deliver_info_created = True
    except Exception as e:
        print(e)
        deliver_info_created = False
    
    result_object = {
        "main_id": main_id,
        "success": deliver_info_created,
        "service_name": "delivery",
        "payload": kwargs,
    }
    result_collector.send_task(
        RESULT_TASK_NAME,
        kwargs=result_object,
        task_id=main_id
    )

@app.task
def rollback(main_id: int):
    try:
        print("WIP")
    except Exception as e:
        print(e)
        return False

