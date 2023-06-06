from azure.identity import AzureCliCredential, ManagedIdentityCredential, DefaultAzureCredential, ChainedTokenCredential
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.compute import ComputeManagementClient
from ..SQL_stuff import getSqlConnection, updateSQL_ValidTo
import pandas as pd
from datetime import timedelta
import logging

credential = ChainedTokenCredential(AzureCliCredential(), DefaultAzureCredential(), ManagedIdentityCredential())

#LASER
subscription_id = "7bf8fea8-fa06-4796-a265-a90a3de4dc10"

def vmSizes():
    # https://learn.microsoft.com/en-us/rest/api/compute/virtual-machines/list-all?tabs=HTTP
    comp_client = ComputeManagementClient(credential, subscription_id)

    df_vm = pd.DataFrame({})
    vm_list = comp_client.virtual_machines.list_all()
    for vm in vm_list: 
        df = pd.DataFrame({'ResourceId': [vm.id]
                        , 'vm_name': [vm.name]
                        , 'vm_size': [vm.hardware_profile.vm_size]})
        df_vm = pd.concat([df_vm, df], ignore_index=True)
    df_vm.fillna('')
    return df_vm

def vmActivity(start_date, end_date, df_vm):
    # https://learn.microsoft.com/en-us/rest/api/monitor/activity-logs/list?tabs=HTTP
    monitor_client = MonitorManagementClient(credential, subscription_id)

    df_vm_activity = pd.DataFrame({})
    for resource_id in df_vm['ResourceId']:
        filter = f"eventTimestamp ge '{start_date}' and eventTimestamp le '{end_date}' and resourceUri eq '{resource_id}'"
        activity_log = monitor_client.activity_logs.list(filter=filter)

        for log in activity_log:
            if log.http_request is not None:
                client_request_id = log.http_request.__getattribute__("client_request_id") 
            else:
                client_request_id = None
            if log.operation_name is not None:
                operation_name = log.operation_name.__getattribute__("localized_value") 
            else:
                operation_name = None
            df = pd.DataFrame({'resource_group_name': [log.resource_group_name]
                                , 'resource_id': [log.resource_id]
                                , 'correlation_id': [log.correlation_id]
                                , 'event_data_id': [log.event_data_id]
                                , 'event_timestamp': [log.event_timestamp]
                                , 'category': [log.category.__getattribute__("localized_value")]
                                , 'operation_name': [operation_name]
                                , 'status': [log.status.__getattribute__("value")]
                                , 'client_request_id': [client_request_id]
                                , 'caller': log.caller})
            df = df.loc[df['category'] == 'Administrative']
            df_vm_activity = pd.concat([df_vm_activity, df], ignore_index=True)
    df_vm_activity.fillna('')
    return df_vm_activity

def querySQL_VmSizes(server, database):
    conn = getSqlConnection(server, database)
    query = "select * from dbo.tblLaserVmSizes where ValidTo is null"
    df = pd.read_sql(query, conn)
    df = df.fillna('')
    return df

def querySQL_VmActivity(event_date, server, database):
    event_date = pd.to_datetime(event_date).strftime('%Y-%m-%d')
    conn = getSqlConnection(server, database)
    query = f"select * from dbo.tblLaserVmActivity where cast([EventTimestamp] as date) = '{event_date}'"
    df = pd.read_sql(query, conn)
    df = df.fillna('')
    return df

def insertSql_VmSizes_DataFrame(data_frame, server, database):
    # df = data_frame.fillna('Python NaN').replace(['Python NaN'], [None])
    df = data_frame
    # Insert new VmSizes from dataframe into table dbo.tblLaserVmSizes
    conn = getSqlConnection(server, database)
    with conn.cursor() as cursor:
        for row in df.itertuples():
            cursor.execute("insert into dbo.tblLaserVmSizes ([ResourceId], [VmName], [VmSize]) "
                           + "values (?, ?, ?)"
                           , row.ResourceId
                           , row.VmName
                           , row.VmSize)
        conn.commit()

def insertSql_VmActivity_DataFrame(data_frame, server, database):
    # df = data_frame.fillna('Python NaN').replace(['Python NaN'], [None])
    df = data_frame
    # Insert new VmSizes from dataframe into table dbo.tblLaserVmSizes
    conn = getSqlConnection(server, database)
    with conn.cursor() as cursor:
        for row in df.itertuples():
            cursor.execute("insert into dbo.tblLaserVmActivity ([ResourceGroup],[ResourceId] "
                           + ",[CorrelationId],[EventDataId],[EventTimestamp],[Category],[OperationName] "
                           + ",[Status],[ClientRequestId], [Caller]) "
                           + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                           , row.resource_group_name
                           , row.resource_id
                           , row.correlation_id
                           , row.event_data_id
                           , row.event_timestamp
                           , row.category
                           , row.operation_name
                           , row.status
                           , row.client_request_id
                           , row.caller)
        conn.commit()

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

####################################################################
####################################################################

def updateVmSizes(server, database):
    # get existing records from sql database to dataframe
    df_e = querySQL_VmSizes(server, database)
    # get azure VM Sizes to dataframe
    df_n = vmSizes()
    
    # outer join dataframes, left_on = sql right_on = azure
    df_all = df_e.merge(df_n, how='outer', on='ResourceId', indicator=True)
    
    # left_only = present in sql not in azure 
    df_delete = df_all.loc[df_all['_merge'] == 'left_only']
    # logically delete in sql
    if df_delete.shape[0] > 0:
        updateSQL_ValidTo(server=server, database=database, table='dbo.tblLaserVmSizes'
                          , pk='vmid', id_list=df_delete['vmid'].to_list())
    logging.info(f"{df_delete.shape[0]} VM Sizes date deleted")
    
    # both = present in sql and in azure  
        # no difference = no action 
        # difference = logically delete in sql and insert new record 
    df_update = df_all.loc[df_all['_merge'] == 'both']
    if df_update.shape[0] > 0:
        df_update = df_update.loc[ (df_update['vm_name'] != df_update['VmName'])
                                | (df_update['vm_size'] != df_update['VmSize'])]
        if df_update.shape[0] > 0:
            df_update = df_update[['vmid','ResourceId', 'vm_name', 'vm_size']]
            df_update = df_update.rename({'vm_name': 'VmName'
                                        , 'vm_size': 'VmSize'
                                        }, axis='columns')
            updateSQL_ValidTo(server=server, database=database, table='dbo.tblLaserVmSizes'
                              , pk='vmid', id_list=df_update['vmid'].to_list())
            insertSql_VmSizes_DataFrame(df_update, server, database)
    logging.info(f"{df_update.shape[0]} VM Sizes date deleted and updated record inserted")
    
    # right_only = present in azure not in sql = insert new record    
    df_insert = df_all.loc[df_all['_merge'] == 'right_only']
    if df_insert.shape[0] > 0:
        df_insert = df_insert[['vmid','ResourceId', 'vm_name', 'vm_size']]
        df_insert = df_insert.rename({'vm_name': 'VmName'
                                        , 'vm_size': 'VmSize'
                                    }, axis='columns')
        insertSql_VmSizes_DataFrame(df_insert, server, database)
    logging.info(f"{df_insert.shape[0]} new VM Sizes created")

def getYesterdaysVmActivity(today, server, database):
    start_date = pd.to_datetime(today) - timedelta(1)
    end_date = pd.to_datetime(today)
    
    # get azure VM Sizes to dataframe
    df_vm = vmSizes()
    # feed it to vmActivity
    df_vm_activity = vmActivity(start_date, end_date, df_vm)
    
    # Check if [EventDataId] already exists in database
    df_e = querySQL_VmActivity(start_date, server, database)
    if df_e.shape[0] > 0 or df_vm_activity.shape[0] > 0:
        df_insert = df_vm_activity.merge(df_e, how='left', left_on='event_data_id', right_on='EventDataId', indicator=True)
        df_insert = df_insert.loc[df_insert['_merge'] == 'left_only']

        # write to SQL database
        insertSql_VmActivity_DataFrame(df_insert, server, database)
        logging.info(f"{df_insert.shape[0]} VM Activity records created")
    else:
        logging.info(f"0 VM Activity records created")
