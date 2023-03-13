from azure.identity import DefaultAzureCredential
from azure.mgmt.costmanagement import CostManagementClient
import pandas as pd
from datetime import timedelta
from .SQL_stuff import getSqlConnection

credential = DefaultAzureCredential()

#LASER
subscription_id = "7bf8fea8-fa06-4796-a265-a90a3de4dc10"

def costs(fromdate, todate):
    costmanagement_client = CostManagementClient(credential)
    c_df = pd.DataFrame({})
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
    c_df = pd.concat([c_df, pd.DataFrame(resource_cost.rows)], ignore_index=True)
    if not c_df.empty:
        c_df.columns = ['PreTaxCost', 'UsageDate','ResourceGroup', 'ResourceId', 'ResourceType'
                        , 'ServiceName', 'ServiceTier', 'Meter', 'MeterSubCategory', 'MeterCategory'
                        , 'TagKey', 'TagValue', 'Currency']
    return c_df

def writeToSql_Costs_SingleDay(single_day, server, database):
    single_day = pd.to_datetime(single_day)
    # Get the days costs from Azure API and put into DataFrame
    df = costs(single_day, single_day)
    df = df.fillna('Python NaN').replace(['Python NaN'], [None])
    # Insert dataframe into table
    conn = getSqlConnection(server, database)
    cursor = conn.cursor()
    for row in df.itertuples():
        cursor.execute(
            "insert into dbo.tblUsageCosts ([PreTaxCost],[UsageDate],[ResourceGroup],[ResourceId]"
            + ",[ResourceType],[ServiceName],[ServiceTier],[Meter],[MeterSubCategory],[MeterCategory]"
            + ",[TagKey],[TagValue],[Currency]) "
            + "values (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            row.PreTaxCost
            ,row.UsageDate
            ,row.ResourceGroup
            ,row.ResourceId
            ,row.ResourceType
            ,row.ServiceName
            ,row.ServiceTier
            ,row.Meter
            ,row.MeterSubCategory
            ,row.MeterCategory
            ,row.TagKey
            ,row.TagValue
            ,row.Currency
            )
    conn.commit()
    cursor.close()
