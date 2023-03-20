import datetime
import logging

import azure.functions as func
from .LASER_resources import updateResourceGroups
import pandas as pd

def main(mytimer: func.TimerRequest) -> None:
    server = 'lida-dat-cms-test.database.windows.net'
    database = 'lida_dat_cms_test'
    updateResourceGroups(server, database)

'''
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
'''
