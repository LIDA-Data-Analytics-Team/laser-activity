#import datetime
#import logging

import azure.functions as func
from laser_activity.azure_activity import writeToExcel

'''
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(
        tzinfo=timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
'''

def main(write: func.TimerRequest) -> None:
    writeToExcel()
