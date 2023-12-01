from celery.exceptions import SoftTimeLimitExceeded
from .database.models import create_delivery, set_delivery_status, StatusEnum
from src import app, result_collector

RESULT_TASK_NAME = "wk-irs.tasks.send_result"

@app.task(
    soft_time_limit=30, 
    time_limit=60,
    name='wk-delivery.tasks.make_delivery'
)
def make_delivery(**kwargs):
    main_id = kwargs.get('main_id')
    user_id = kwargs.get('user_id')
    user_address = kwargs.get('user_address')
    success = True
    try:
        create_delivery(main_id=main_id, buyer_id=user_id, buyer_address=user_address)
    except SoftTimeLimitExceeded:
        print(e)
        success = False
        kwargs["error"] = "timeout"
    except Exception as e:
        print(e)
        success = False
        kwargs["error"] = str(e)
    
    result_object = {
        "main_id": main_id,
        "success": success,
        "service_name": "delivery",
        "payload": kwargs,
    }
    result_collector.send_task(
        RESULT_TASK_NAME,
        kwargs=result_object,
        task_id=str(main_id)
    )

@app.task(name='wk-delivery.tasks.rollback')
def rollback(**kwargs):
    main_id = kwargs.get('main_id')
    try:
        set_delivery_status(main_id, StatusEnum.FAILED)
    except Exception as e:
        print(e)
        kwargs["error"] = str(e)

    result_object = {
        "main_id": main_id,
        "success": False,
        "service_name": "delivery",
        "payload": kwargs,
    }
    result_collector.send_task(
        RESULT_TASK_NAME,
        kwargs=result_object,
        task_id=str(main_id)
    )
    return False
