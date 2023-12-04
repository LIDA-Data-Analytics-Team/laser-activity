from azure.identity import AzureCliCredential, ManagedIdentityCredential, DefaultAzureCredential, ChainedTokenCredential
from azure.mgmt.costmanagement import CostManagementClient
from azure.core.exceptions import HttpResponseError
import pandas as pd
from datetime import timedelta
from .SQL_stuff import getSqlConnection
from time import sleep
import logging
from requests import post

credential = ChainedTokenCredential(AzureCliCredential(), DefaultAzureCredential(), ManagedIdentityCredential())

#LASER
subscription_id = "7bf8fea8-fa06-4796-a265-a90a3de4dc10"

def costs(fromdate, todate):
    costmanagement_client = CostManagementClient(credential)
    scope = f"subscriptions/{subscription_id}"
    next_link = f"https://management.azure.com/{scope}/providers/Microsoft.CostManagement/query?api-version=2019-11-01"
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
                                    , {"name": "Budget Code", "type": "TagKey"}
                                    , {"name": "budgetcode", "type": "TagKey"}],
                    },
                    "TimePeriod": {"from": fromdate, "to": todate},
                    "timeframe": "Custom",
                    "type": "ActualCost",
                    "nextLink": "nextLink",
                }
    df_all_costs = pd.DataFrame({})
    c_df = pd.DataFrame({})
    # example nextLink: 'https://management.azure.com/subscriptions/7bf8fea8-fa06-4796-a265-a90a3de4dc10/providers/Microsoft.CostManagement/query?api-version=2019-11-01&$skiptoken=AQAAAA%3D%3D'
    while next_link is not None and next_link.strip():
        resource_cost = post(
            url= next_link,
            headers= {
                "Authorization": f"Bearer {costmanagement_client._config.credential.get_token('https://management.azure.com/.default').token}",
                "Content-Type": "application/json"
                },
            json= parameters,
            )
        if resource_cost.status_code == 429:
            if "x-ms-ratelimit-microsoft.costmanagement-entity-retry-after" in resource_cost.headers:
                sleep_for = resource_cost.headers['x-ms-ratelimit-microsoft.costmanagement-entity-retry-after']
            elif "x-ms-ratelimit-microsoft.costmanagement-tenant-retry-after" in resource_cost.headers:
                sleep_for = resource_cost.headers['x-ms-ratelimit-microsoft.costmanagement-tenant-retry-after']
            elif "x-ms-ratelimit-microsoft.costmanagement-clienttype-retry-after" in resource_cost.headers:
                sleep_for = resource_cost.headers['x-ms-ratelimit-microsoft.costmanagement-clienttype-retry-after']
            logging.info(f"Sleeping for {sleep_for} seconds")
            sleep(int(sleep_for))
            continue
        resource_cost_data = resource_cost.json()
        data = pd.DataFrame(resource_cost_data["properties"]["rows"])
        df_all_costs = pd.concat([df_all_costs, data])
        next_link = resource_cost_data["properties"]["nextLink"]
    logging.info("All pages collected")
    #
    # IT Services are planning changes to how resources are tagged in Azure, to standardise across the estate.
    # All tags will be lowercase with no spaces. 
    # There will be a transition period so the following code block is to handle cases where one tag is present but not the other.
    # Above API call requests values for both tags, which returns one record for each (whether present or not).
    # This duplication needs consolidating. 
    #
    # Give columns names
    if not df_all_costs.empty:
        df_all_costs.columns = ['PreTaxCost', 'UsageDate','ResourceGroup', 'ResourceId', 'ResourceType', 'ServiceName'
                             , 'ServiceTier', 'Meter', 'MeterSubCategory', 'MeterCategory', 'TagKey', 'TagValue', 'Currency']
    # Standardise 'TagKey' values 
    df_all_costs['TagKey'] = 'budget code'
    # Isolate records with 'TagKey' that are missing 'TagValue' from those that aren't
    df_all_costs_noTagValue = df_all_costs.loc[df_all_costs['TagValue'].isnull()].drop_duplicates()
    df_all_costs_TagValue = df_all_costs.loc[df_all_costs['TagValue'].isnull() == False]
    # Merge these two DataFrames on everything but TagValue to bring two rows to single row with two columns for 'TagValue'
    merge_on = ['PreTaxCost', 'UsageDate','ResourceGroup', 'ResourceId', 'ResourceType'
                , 'ServiceName', 'ServiceTier', 'Meter', 'MeterSubCategory', 'MeterCategory'
                , 'TagKey', 'Currency']
    df_merge = df_all_costs_TagValue.merge(df_all_costs_noTagValue, on=merge_on, how='outer')
    # Create new column with value from one 'TagValue' if missing from first
    df_merge['TagValue'] = df_merge['TagValue_x'].fillna(df_merge['TagValue_y'])
    # Drop original _x & _y 'TagValue' columns
    df_merge = df_merge.drop(columns=['TagValue_x', 'TagValue_y'])
    
    c_df = pd.concat([c_df, df_merge], ignore_index=True)
    
    return c_df

def costs_SDK(fromdate, todate):
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
                                    , {"name": "Budget Code", "type": "TagKey"}
                                    , {"name": "budgetcode", "type": "TagKey"}],
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
    
    #
    # IT Services are planning changes to how resources are tagged in Azure, to standardise across the estate.
    # All tags will be lowercase with no spaces. 
    # There will be a transition period so the following code block is to handle cases where one tag is present but not the other.
    # Above API call requests values for both tags, which returns one record for each (whether present or not).
    # This duplication needs consolidating. 
    #
    df_single = pd.DataFrame(resource_cost.rows)
    # Give columns names
    if not df_single.empty:
        df_single.columns = ['PreTaxCost', 'UsageDate','ResourceGroup', 'ResourceId', 'ResourceType', 'ServiceName'
                             , 'ServiceTier', 'Meter', 'MeterSubCategory', 'MeterCategory', 'TagKey', 'TagValue', 'Currency']
    # Standardise 'TagKey' values 
    df_single['TagKey'] = 'budget code'
    # Isolate records with 'TagKey' that are missing 'TagValue' from those that aren't
    df_single_noTagValue = df_single.loc[df_single['TagValue'].isnull()].drop_duplicates()
    df_single_TagValue = df_single.loc[df_single['TagValue'].isnull() == False]
    # Merge these two DataFrames on everything but TagValue to bring two rows to single row with two columns for 'TagValue'
    merge_on = ['PreTaxCost', 'UsageDate','ResourceGroup', 'ResourceId', 'ResourceType'
                , 'ServiceName', 'ServiceTier', 'Meter', 'MeterSubCategory', 'MeterCategory'
                , 'TagKey', 'Currency']
    df_merge = df_single_TagValue.merge(df_single_noTagValue, on=merge_on, how='outer')
    # Create new column with value from one 'TagValue' if missing from first
    df_merge['TagValue'] = df_merge['TagValue_x'].fillna(df_merge['TagValue_y'])
    # Drop original _x & _y 'TagValue' columns
    df_merge = df_merge.drop(columns=['TagValue_x', 'TagValue_y'])
    
    c_df = pd.concat([c_df, df_merge], ignore_index=True)
    
    return c_df

def querySql_Costs_DateRange(fromdate, todate, server, database):
    fromdate = pd.to_datetime(fromdate).strftime("%Y%m%d")
    todate = pd.to_datetime(todate).strftime("%Y%m%d")
    conn = getSqlConnection(server, database)
    query = f"select * from dbo.tblLaserUsageCosts where UsageDate between {fromdate} and {todate}"
    df = pd.read_sql(query, conn)
    return df

def querySql_Costs_SingleDay(single_day, server, database):
    single_day = pd.to_datetime(single_day).strftime("%Y%m%d")
    conn = getSqlConnection(server, database)
    query = f"select * from dbo.tblLaserUsageCosts where UsageDate = {single_day}"
    df = pd.read_sql(query, conn)
    return df

def insertSql_Costs_DataFrame(data_frame, server, database):
    df = data_frame.fillna('Python NaN').replace(['Python NaN'], [None])
    # Insert new costs from dataframe into table dbo.tblLaserUsageCosts
    conn = getSqlConnection(server, database)
    with conn.cursor() as cursor:
        for row in df.itertuples():
            cursor.execute(
            "insert into [dbo].[tblLaserUsageCosts] ([PreTaxCost],[UsageDate],[ResourceGroup],[ResourceId]"
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
        cursor.execute("truncate table [stg].[tblLaserUsageCostsUpdate]")
        # Insert changing records into staging table
        for row in df.itertuples():
            cursor.execute(
                "insert into [stg].[tblLaserUsageCostsUpdate] ([UsageCostsId], [PreTaxCost]) values (?,?)"
                    , int(row.UsageCostsId)
                    , row.PreTaxCost_x)
        # Update costs table
        cursor.execute("update [dbo].[tblLaserUsageCosts] set [PreTaxCost] = t.[PreTaxCost] from [stg].[tblLaserUsageCostsUpdate] t where [dbo].[tblLaserUsageCosts].[UsageCostsId] = t.[UsageCostsId]")
        conn.commit()

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def getCosts(start_date, end_date, server, database):
    logging.info(f"Retrieving costs for {start_date} to {end_date}")
    # Fetch existing records for day in question from SQL Database
    df_e = querySql_Costs_DateRange(start_date, end_date, server, database)
    df_e['TagValue'].fillna("No TagValue", inplace=True)
    df_e = df_e.convert_dtypes()
    # Fetch records from Cost Management API for day in question
    df_n = costs(start_date, end_date)
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

def getDateRangeOfCosts(start_date, end_date, server, database):
    # start getting costs for 35 days ago, as they change during billing period and for 72 hours after
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    for single_date in daterange(start_date, end_date):
        logging.info(f"Retrieving costs for {single_date}")
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
