#import datetime
#import logging

import azure.functions as func
from datetime import datetime
from .LASER_costs import get35daysOfCosts

def main(mytimer: func.TimerRequest) -> None:
    today = datetime.now().strftime('%Y-%m-%d')
    server = 'lida-dat-cms-test.database.windows.net'
    database = 'lida_dat_cms_test'
    get35daysOfCosts(today=today, server=server, database=database)


'''
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(
        tzinfo=timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
'''
