from azure.identity import AzureCliCredential, ManagedIdentityCredential, DefaultAzureCredential, ChainedTokenCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.core.exceptions import HttpResponseError
from ..SQL_stuff import getSqlConnection
import pandas as pd
from time import sleep

credential = ChainedTokenCredential(AzureCliCredential(), DefaultAzureCredential(), ManagedIdentityCredential())

#LASER
subscription_id = "7bf8fea8-fa06-4796-a265-a90a3de4dc10"

server = 'lida-dat-cms-test.database.windows.net'
database = 'lida_dat_cms_test'

resource_client = ResourceManagementClient(credential, subscription_id)

def resourceGroups():
    # Retrieve the list of resource groups
    # Potential for infinite loop mitigated by 10 minute max timeout of Consumption Function App
    while True:
        try:
            group_list = resource_client.resource_groups.list()
            df_rg = pd.DataFrame({})
            for group in list(group_list):
                if group.tags is not None:
                    budget_code = group.tags.get('Budget Code', 'No budget Code')
                    project_id = group.tags.get('Project ID', 'No Project ID')
                    project_name = group.tags.get('Project Name', 'No Project Name')
                    project_vre = group.tags.get('Project VRE', 'No Project VRE')
                else:
                    budget_code = 'No tags in Azure'
                    project_id = 'No tags in Azure'
                    project_name = 'No tags in Azure'
                    project_vre = 'No tags in Azure'
                df = pd.DataFrame({'ResourceGroup': [group.name], 'Budget Code': [budget_code], 'Project ID': [project_id]
                                , 'Project Name': [project_name], 'Project VRE': [project_vre]})
                df_rg = pd.concat([df_rg, df], ignore_index=True)
        # HttpResponseError Code: 429, Message: Too many requests. Please retry. 
        # If received then wait 15 seconds and try again (within the While loop)
        except HttpResponseError as e:
            if e.status_code == 429:
                sleep(15)
                continue
        # Break out of the While loop
        break
    return df_rg

def resources(rg_list):
    df_r = pd.DataFrame({})
    for resource_group in rg_list:
        # Retrieve the list of resources in group
        # The expand argument includes additional properties in the output.
        # Potential for infinite loop mitigated by 10 minute max timeout of Consumption Function App
        while True:
            try:
                #https://learn.microsoft.com/en-us/rest/api/resources/resources/list-by-resource-group
                resource_list = resource_client.resources.list_by_resource_group(
                    resource_group, expand = "createdTime,changedTime")
                for resource in list(resource_list):
                    if resource.tags is not None:
                        budget_code = resource.tags.get('Budget Code', 'No Budget Code')
                        project_id = resource.tags.get('Project ID', 'No Project ID')
                        project_name = resource.tags.get('Project Name', 'No Project Name')
                        project_vre = resource.tags.get('Project VRE', 'No Project VRE')
                    else:
                        budget_code = 'No tags in Azure'
                        project_id = 'No tags in Azure'
                        project_name = 'No tags in Azure'
                        project_vre = 'No tags in Azure'
                    df = pd.DataFrame({'ResourceGroup': [resource_group], 'Resource': [resource.name]
                                       , 'ResourceId': [resource.id], 'ResourceKind': [resource.kind]
                                       , 'ResourceType': [resource.type]
                                       , 'BudgetCode': [budget_code], 'Project ID': [project_id]
                                       , 'Project Name': [project_name], 'Project VRE': [project_vre]
                                       , 'CreatedDate': [resource.created_time]})
                    df['CreatedDate'] = df['CreatedDate'].apply(lambda a: pd.to_datetime(a).date()) 
                    df_r = pd.concat([df_r, df], ignore_index=True)
            # HttpResponseError Code: 429, Message: Too many requests. Please retry. 
            # If received then wait 15 seconds and try again (within the While loop)
            except HttpResponseError as e:
                if e.status_code == 429:
                    sleep(15)
                    continue
            # Break out of the While loop
            break
    return df_r

def querySQL_ResourceGroups():
    conn = getSqlConnection(server, database)
    query = "select * from dbo.tblLaserResourceGroups where ValidTo is null"
    df = pd.read_sql(query, conn)
    return df

def querySQL_Resources():
    print('Run SQL query to fetch current records')

def insertSQL_ResourceGroups(data_frame):
    df = data_frame.fillna('Python NaN').replace(['Python NaN'], [None])
    # Insert new costs from dataframe into table dbo.tblUsageCosts
    conn = getSqlConnection(server, database)
    with conn.cursor() as cursor:
        for row in df.itertuples():
            cursor.execute("insert into dbo.tblLaserResourceGroups (ResourceGroup, BudgetCode "
                           + ", ProjectID, ProjectName, ProjectVRE) "
                           + "values (?, ?, ?, ?, ?)"
                           , row.ResourceGroup
                           , row.BudgetCode
                           , row.ProjectID
                           , row.ProjectName
                           , row.ProjectVRE)
        conn.commit()


def insertSQL_Resources():
    print('run query to insert new records')

def updateSQL_ValidTo(table, id_list):
    conn = getSqlConnection(server, database)
    with conn.cursor() as cursor:
        for id in id_list:
            cursor.execute(f"update {table} set ValidTo = getdate() where rgid = ?"
            , id
            )
        conn.commit()

####################################################################
####################################################################

def updateResourceGroups():
    # get existing records from sql database to dataframe
    df_e = querySQL_ResourceGroups()
    # azure resourceGroups to dataframe
    df_n = resourceGroups()
    
    # outer join dataframes, left_on = sql right_on = azure
    df_all = df_e.merge(df_n, how='outer', on='ResourceGroup', indicator=True)
    
    # left_only = present in sql not in azure 
    df_delete = df_all.loc[df_all['_merge'] == 'left_only']
    # logically delete in sql
    if df_delete.shape[0] > 0:
        updateSQL_ValidTo(table='dbo.tblLaserResourceGroups', id_list=df_delete['rgid'].to_list())
    print(f"{df_delete.shape[0]} resource groups date deleted")
    
    # both = present in sql and in azure  
        # no difference = no action 
        # difference = logically delete in sql and insert new record 
    df_update = df_all.loc[df_all['_merge'] == 'both']
    if df_update.shape[0] > 0:
        df_update = df_update.loc[(df_update['BudgetCode'] != df_update['Budget Code']) 
                                | (df_update['ProjectID'] != df_update['Project ID']) 
                                | (df_update['ProjectName'] != df_update['Project Name']) 
                                | (df_update['ProjectVRE'] != df_update['Project VRE'])]
        if df_update.shape[0] > 0:
            df_update = df_update[['rgid','ResourceGroup', 'Budget Code', 'Project ID', 'Project Name', 'Project VRE' ]]
            df_update = df_update.rename({'Budget Code': 'BudgetCode'
                                        , 'Project ID': 'ProjectID'
                                        , 'Project Name': 'ProjectName'
                                        , 'Project VRE': 'ProjectVRE'
                                        }, axis='columns')
            updateSQL_ValidTo(table='dbo.tblLaserResourceGroups', id_list=df_update['rgid'].to_list())
            insertSQL_ResourceGroups(df_update)
    print(f"{df_update.shape[0]} resource groups date deleted and updated record inserted")
    
    # right_only = present in azure not in sql = insert new record    
    df_insert = df_all.loc[df_all['_merge'] == 'right_only']
    if df_insert.shape[0] > 0:
        df_insert = df_insert[['rgid','ResourceGroup', 'Budget Code', 'Project ID', 'Project Name', 'Project VRE']]
        df_insert = df_insert.rename({'Budget Code': 'BudgetCode'
                                    , 'Project ID': 'ProjectID'
                                    , 'Project Name': 'ProjectName'
                                    , 'Project VRE': 'ProjectVRE'
                                    }, axis='columns')
        insertSQL_ResourceGroups(df_insert)
    print(f"{df_insert.shape[0]} new records created")

def updateResources():
    print('method to compare new & old records, update and insert as appropriate')

####################################################################
####################################################################
