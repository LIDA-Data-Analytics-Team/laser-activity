# import datetime
# import logging

import azure.functions as func
from datetime import datetime, timedelta
from ..LASER_costs import getCosts
from ..SQL_stuff import server, database

def main(mytimer: func.TimerRequest) -> None:
    # get costs from 35 days ago to 17 days ago
    start_date = (datetime.now() - timedelta(35)).strftime('%Y-%m-%d')
    end_date = (datetime.now() - timedelta(18)).strftime('%Y-%m-%d')
    getCosts(start_date, end_date, server, database)


'''
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(
        tzinfo=timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
'''
