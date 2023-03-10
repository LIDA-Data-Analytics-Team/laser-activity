#import datetime
#import logging

import azure.functions as func
from laser_activity.Scratch import writeToSql_test


'''
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(
        tzinfo=timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
'''

def main(write: func.TimerRequest) -> None:
    server = 'lida-dat-cms-test.database.windows.net'
    database = 'lida_dat_cms_test'
    writeToSql_test(server=server, database=database)
