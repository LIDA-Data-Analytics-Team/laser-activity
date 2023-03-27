import datetime
import logging

import azure.functions as func
from .LASER_budgets import updateBudgets
from ..SQL_stuff import server, database

def main(mytimer: func.TimerRequest) -> None:
    updateBudgets(server, database)

'''
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
'''