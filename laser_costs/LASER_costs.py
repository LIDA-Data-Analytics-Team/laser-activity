from azure.identity import AzureCliCredential, ManagedIdentityCredential, DefaultAzureCredential, ChainedTokenCredential
from azure.mgmt.costmanagement import CostManagementClient
from azure.core.exceptions import HttpResponseError
import pandas as pd
from datetime import timedelta
from ..SQL_stuff import getSqlConnection
from time import sleep

credential = ChainedTokenCredential(AzureCliCredential(), DefaultAzureCredential(), ManagedIdentityCredential())

#LASER
subscription_id = "7bf8fea8-fa06-4796-a265-a90a3de4dc10"

def costs(fromdate, todate):
    fromdate = pd.to_datetime(fromdate)
    todate = pd.to_datetime(todate)
    costmanagement_client = CostManagementClient(credential)
    c_df = pd.DataFrame({})
    # Potential for infinite loop mitigated by 10 minute max timeout of Consumption Function App
    while True:
        try:
            resource_cost = costmanagement_client.query.usage(
                # uri parameter (https://learn.microsoft.com/en-us/rest/api/cost-management/query/usage?tabs=HTTP#uri-parameters)
                scope = f"subscriptions/{subscription_id}", #/resourceGroups/{resource_group}",
                # request body (https://learn.microsoft.com/en-us/rest/api/cost-management/query/usage?tabs=HTTP#request-body)
                parameters={
                    "dataset": {
                        "aggregation": {"totalCost": {"function": "Sum", "name": "PreTaxCost"}},
                        "granularity": "Daily",
                        "grouping": [{"name": "ResourceGroupName", "type": "Dimension"}
                                    , {"name": "ResourceId", "type": "Dimension"}
                                    , {"name": "ResourceType", "type": "Dimension"}
                                    , {"name": "ServiceName", "type": "Dimension"}
                                    , {"name": "ServiceTier", "type": "Dimension"}
                                    , {"name": "Meter", "type": "Dimension"}
                                    , {"name": "MeterSubCategory", "type": "Dimension"}
                                    , {"name": "MeterCategory", "type": "Dimension"}
                                    , {"name": "Budget Code", "type": "TagKey"}],
                    },
                    "TimePeriod": {"from": fromdate, "to": todate},
                    "timeframe": "Custom",
                    "type": "ActualCost",
                },
            )
        # HttpResponseError Code: 429, Message: Too many requests. Please retry. 
        # If received then wait 15 seconds and try again (within the While loop)
        except HttpResponseError as e:
            if e.status_code == 429:
                sleep(15)
                continue
        # Break out of the While loop
        break
    c_df = pd.concat([c_df, pd.DataFrame(resource_cost.rows)], ignore_index=True)
    if not c_df.empty:
        c_df.columns = ['PreTaxCost', 'UsageDate','ResourceGroup', 'ResourceId', 'ResourceType'
                        , 'ServiceName', 'ServiceTier', 'Meter', 'MeterSubCategory', 'MeterCategory'
                        , 'TagKey', 'TagValue', 'Currency']
    return c_df

def querySql_Costs_SingleDay(single_day, server, database):
    single_day = pd.to_datetime(single_day).strftime("%Y%m%d")
    conn = getSqlConnection(server, database)
    query = f"select * from dbo.tblUsageCosts where UsageDate = {single_day}"
    df = pd.read_sql(query, conn)
    return df

def insertSql_Costs_DataFrame(data_frame, server, database):
    df = data_frame.fillna('Python NaN').replace(['Python NaN'], [None])
    # Insert new costs from dataframe into table dbo.tblUsageCosts
    conn = getSqlConnection(server, database)
    with conn.cursor() as cursor:
        for row in df.itertuples():
            cursor.execute(
            "insert into [dbo].[tblUsageCosts] ([PreTaxCost],[UsageDate],[ResourceGroup],[ResourceId]"
                + ",[ResourceType],[ServiceName],[ServiceTier],[Meter],[MeterSubCategory],[MeterCategory]"
                + ",[TagKey],[TagValue],[Currency]) "
                + "values (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                row.PreTaxCost_x
                ,int(row.UsageDate)
                ,row.ResourceGroup
                ,row.ResourceId
                ,row.ResourceType_x
                ,row.ServiceName_x
                ,row.ServiceTier_x
                ,row.Meter
                ,row.MeterSubCategory
                ,row.MeterCategory
                ,row.TagKey
                ,row.TagValue
                ,row.Currency_x
                )
        conn.commit()

def updateSql_Costs_DataFrame(data_frame, server, database):
    df = data_frame.fillna('Python NaN').replace(['Python NaN'], [None])
    conn = getSqlConnection(server, database)
    with conn.cursor() as cursor:
        # Clear out the staging table
        cursor.execute("truncate table [stg].[tblUsageCostsUpdate]")
        # Insert changing records into staging table
        for row in df.itertuples():
            cursor.execute(
                "insert into [stg].[tblUsageCostsUpdate] ([UsageCostsId], [PreTaxCost]) values (?,?)"
                    , int(row.UsageCostsId)
                    , row.PreTaxCost_x)
        # Update costs table
        cursor.execute("update [dbo].[tblUsageCosts] set [PreTaxCost] = t.[PreTaxCost] from [stg].[tblUsageCostsUpdate] t where [dbo].[tblUsageCosts].[UsageCostsId] = t.[UsageCostsId]")
        conn.commit()

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def get35daysOfCosts(today, server, database):
    # start getting costs for 35 days ago, as they change during billing period and for 72 hours after
    start_date = pd.to_datetime(today) - timedelta(35)
    end_date = pd.to_datetime(today)
    
    for single_date in daterange(start_date, end_date):
        # Fetch existing records for day in question from SQL Database
        df_e = querySql_Costs_SingleDay(single_date, server, database)
        df_e['TagValue'].fillna("No TagValue", inplace=True)
        df_e = df_e.convert_dtypes()
        # Fetch records from Cost Management API for day in question
        df_n = costs(single_date, single_date)
        df_n['TagValue'].fillna("No TagValue", inplace=True)
        df_n = df_n.convert_dtypes()
        
        # Fields used to identify unique records and join DataFrames
        merge_list = ['UsageDate','ResourceGroup','ResourceId', 'Meter', 'MeterSubCategory', 'MeterCategory', 'TagKey', 'TagValue']

        # Suffix '_x' is left, '_y' is right

        # Determine records fetched from API that are not already present in database
        df_insert = df_n.merge(df_e, how='left', on=merge_list, indicator=True)
        df_insert = df_insert.loc[df_insert['_merge'] == 'left_only']
        # If more than none insert them to database
        if df_insert.shape[0] > 0:
            df_insert = df_insert[['PreTaxCost_x', 'UsageDate', 'ResourceGroup', 'ResourceId' 
                , 'ResourceType_x', 'ServiceName_x', 'ServiceTier_x', 'Meter', 'MeterSubCategory', 'MeterCategory' 
                , 'TagKey', 'TagValue', 'Currency_x']]
            insertSql_Costs_DataFrame(df_insert, server, database)
        
        # Determine records fetched from API that are already present in database
        df_update = df_n.merge(df_e, how='inner', on=merge_list)
        df_update = df_update.loc[df_update['PreTaxCost_y'] != df_update['PreTaxCost_x']]
        # If more than none update PreTaxCost of each record
        if df_update.shape[0] > 0:
            updateSql_Costs_DataFrame(df_update[['UsageCostsId', 'PreTaxCost_x']], server, database)
