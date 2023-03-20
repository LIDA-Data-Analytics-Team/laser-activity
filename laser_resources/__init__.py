import datetime
import logging

import azure.functions as func
from .LASER_resources import resourceGroups, resources
import pandas as pd

def main(mytimer: func.TimerRequest) -> None:
    df_rg = resourceGroups()
    print(f"Resource Group count: {df_rg.shape[0]}")
    df_r = resources(df_rg['ResourceGroup'].to_list())
    print(f"Resource count: {df_r.shape[0]}")

'''
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
'''