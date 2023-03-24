from azure.identity import AzureCliCredential, ManagedIdentityCredential, DefaultAzureCredential, ChainedTokenCredential
from azure.mgmt.consumption import ConsumptionManagementClient
import pandas as pd
from ..SQL_stuff import getSqlConnection, updateSQL_ValidTo

credential = ChainedTokenCredential(AzureCliCredential(), DefaultAzureCredential(), ManagedIdentityCredential())

#LASER
subscription_id = "7bf8fea8-fa06-4796-a265-a90a3de4dc10"

def budgets():
    consumption_client = ConsumptionManagementClient(credential, subscription_id)

    # https://learn.microsoft.com/en-us/rest/api/consumption/budgets/get?tabs=HTTP

    budgets = consumption_client.budgets.list(scope=f"/subscriptions/{subscription_id}/", )

    df_b = pd.DataFrame({})
    for budget in budgets:
        df = pd.DataFrame({
            'budget_id': [budget.id]
            , 'budget_name': [budget.name]
            , 'time_grain': [budget.time_grain]
            , 'start_date': [budget.time_period.start_date]
            , 'end_date': [budget.time_period.end_date]
            , 'amount': [budget.amount]
        })
        df_b = pd.concat([df_b, df], ignore_index=True)
    return df_b

def querySQL_budgets(server, database):
    conn = getSqlConnection(server, database)
    query = "select * from dbo.tblLaserBudgets where ValidTo is null"
    df = pd.read_sql(query, conn)
    df = df.fillna('')
    return df

def insertSql_Budgets(data_frame, server, database):
    df = data_frame.fillna('Python NaN').replace(['Python NaN'], [None])
    # Insert new Budgets from dataframe into table dbo.tblLaserBudgets
    conn = getSqlConnection(server, database)
    with conn.cursor() as cursor:
        for row in df.itertuples():
            cursor.execute("insert into dbo.tblLaserBudgets ([BudgetID],[BudgetName],[TimeGrain],[StartDate],[EndDate],[Amount]) "
                           + "values (?, ?, ?, ?, ?, ?)"
                           , row.budget_id
                           , row.budget_name
                           , row.time_grain
                           , row.start_date
                           , row.end_date
                           , row.amount)
        conn.commit()

####################################################################
####################################################################

def updateBudgets(server, database):
    # get existing records from sql database to dataframe
    df_e = querySQL_budgets(server, database)
    # get azure VM Sizes to dataframe
    df_n = budgets()
    
    # outer join dataframes, left_on = sql right_on = azure
    df_all = df_e.merge(df_n, how='outer', left_on='BudgetID', right_on='budget_id', indicator=True)
    
    # left_only = present in sql not in azure 
    df_delete = df_all.loc[df_all['_merge'] == 'left_only']
    # logically delete in sql
    if df_delete.shape[0] > 0:
        updateSQL_ValidTo(server=server, database=database, table='dbo.tblLaserBudgets'
                          , pk='bid', id_list=df_delete['bid'].to_list())
    print(f"{df_delete.shape[0]} Budgets date deleted")
    
    # both = present in sql and in azure  
        # no difference = no action 
        # difference = logically delete in sql and insert new record 
    df_update = df_all.loc[df_all['_merge'] == 'both']
    if df_update.shape[0] > 0:
        df_update = df_update.loc[ (df_update['budget_name'] != df_update['BudgetName'])
                                | (df_update['time_grain'] != df_update['TimeGrain'])
                                | (df_update['start_date'].dt.date != df_update['StartDate'].dt.date)   # dates are fuckey and Azure API returns a "+00:00" timestamp modifier 
                                | (df_update['end_date'].dt.date != df_update['EndDate'].dt.date)       # need to add the .dt.date to compare just the date component
                                | (df_update['amount'] != df_update['Amount'])
                                ]
        if df_update.shape[0] > 0:
            df_update = df_update[['bid', 'budget_id', 'budget_name', 'time_grain', 'start_date', 'end_date', 'amount']]
            updateSQL_ValidTo(server=server, database=database, table='dbo.tblLaserBudgets'
                              , pk='bid', id_list=df_update['bid'].to_list())
            insertSql_Budgets(df_update, server, database)
    print(f"{df_update.shape[0]} Budgets date deleted and updated record inserted")
    
    # right_only = present in azure not in sql = insert new record    
    df_insert = df_all.loc[df_all['_merge'] == 'right_only']
    if df_insert.shape[0] > 0:
        df_insert = df_insert[['budget_id', 'budget_name', 'time_grain', 'start_date', 'end_date', 'amount']]
        insertSql_Budgets(df_insert, server, database)
    print(f"{df_insert.shape[0]} new Budgets created")
