#import datetime
#import logging

import azure.functions as func
from datetime import datetime, timedelta
from .LASER_costs import writeToSql_Costs_SingleDay

def main(write: func.TimerRequest) -> None:
    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    server = 'lida-dat-cms-test.database.windows.net'
    database = 'lida_dat_cms_test'
    writeToSql_Costs_SingleDay(single_day=yesterday, server=server, database=database)


'''
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(
        tzinfo=timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
'''