import datetime
import logging

import azure.functions as func
from .Prism_run_proc import runSQL_sp_updateProjectsToDiscontinued
from ..SQL_stuff import server, database

def main(mytimer: func.TimerRequest) -> None:
    runSQL_sp_updateProjectsToDiscontinued(server, database)

'''
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
'''
