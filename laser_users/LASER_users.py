from azure.identity import AzureCliCredential, ManagedIdentityCredential, DefaultAzureCredential, ChainedTokenCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from msgraph.core import GraphClient
import pandas as pd 
from ..SQL_stuff import getSqlConnection, updateSQL_ValidTo
import logging

# https://learn.microsoft.com/en-us/graph/sdks/sdks-overview
# https://learn.microsoft.com/en-us/azure/key-vault/secrets/quick-create-python?tabs=azure-cli

# Set variables for App Registration ("LASER Dashboard Prod") and Key Vault where the App Registration Secret is stored
tenantID = 'bdeaeda8-c81d-45ce-863e-5232a535b7cb'
clientID = '6b8bf186-0012-4d02-9e18-b462bf34a154'
keyVaultName = "UoL-uks-LRDP-dashboard" 
secretName = "LASERDashboardProd"

# System Managed Identity used to access Key Vault to retrieve Secret
keyVault_credential = ChainedTokenCredential(AzureCliCredential(), DefaultAzureCredential(), ManagedIdentityCredential())
KVUri = f"https://{keyVaultName}.vault.azure.net"
keyVault_client = SecretClient(vault_url=KVUri, credential=keyVault_credential)
retrieved_secret = keyVault_client.get_secret(secretName)

# Client Secret retrieved from Key Vault used to authenticate with MS Graph
clientSecret = retrieved_secret.value
graph_credential = ClientSecretCredential(tenant_id=tenantID,client_secret=clientSecret,client_id=clientID)
graph_client = GraphClient(credential=graph_credential)

def Groups():
    # https://learn.microsoft.com/en-us/graph/api/group-list?view=graph-rest-1.0&tabs=http

    # filter for AAD Group names that follow LASER VRE naming conventions (and a couple that don't)
    groups_filter = ("(startswith(displayName,'VRE-p') " 
                + "or startswith(displayName,'VRE-s') " 
                + "or startswith(displayName,'VRE-u') " 
                + "or startswith(displayName,'VRE-t') " 
                + "or startswith(displayName,'PICANet-') "
                + "or startswith(displayName,'PICANetv2-') "
                + "or displayName eq 'LRDP-All-Citrix-Users' "
                + "or displayName eq 'LRDP-All-Citrix-SafeRoom-Users') "
                )

    # DataFrame to contain all LASER AAD groups
    df_groups = pd.DataFrame({})

    # first run before any nextLink pagination 
    groups = graph_client.get('/groups',
        params={
            '$select': 'id,' 'displayName'
            ,'$filter': groups_filter
        })
    groups = groups.json()
    # Get the nextLink property if present
    try:
        nextlink = groups['@odata.nextLink']
    except:
        nextlink = None
    groups_nested_list = pd.json_normalize(groups, record_path =['value'])
    # Add result to waiting DataFrame that will contain all LASER AAD groups
    df_groups = pd.concat([df_groups, groups_nested_list], ignore_index=True)

    # subsequent loop of runs while nextLink pagination is active
    # @odata.nextLink contains full URL including select & filter parameters defined in first loop 
    while nextlink is not None:
        groups = graph_client.get(nextlink)
        groups = groups.json()
        # Get the nextLink property if present
        try:
            nextlink = groups['@odata.nextLink']
        except:
            nextlink = None
        groups_nested_list = pd.json_normalize(groups, record_path =['value'])
        # Add result to waiting DataFrame that will contain all LASER AAD groups
        df_groups = pd.concat([df_groups, groups_nested_list], ignore_index=True)
    
    return df_groups

def GroupMembers(df_groups):
    # https://learn.microsoft.com/en-us/graph/api/group-list-members?view=graph-rest-1.0&tabs=http

    # DataFrame to contain all members of LASER AAD groups
    df_members = pd.DataFrame({})
    for id in df_groups['id']:
        group = df_groups['displayName'].loc[df_groups['id'] == id]
        
        # First run before any nextLink pagination
        # '/microsoft.graph.user' limits the response to users, nested groups are ignored
        members = graph_client.get(f"/groups/{id}/members/microsoft.graph.user"
                            , params={
                                '$select': 'id, displayName, givenName, surname, mail, userPrincipalName'
                            })
        members = members.json()
        # Get the nextLink property if present
        try:
            nextlink = members['@odata.nextLink']
        except:
            nextlink = None
        members_nested_list = pd.json_normalize(members, record_path =['value'])
        if members_nested_list.shape[0] > 0:
            for row in members_nested_list.itertuples():
                df = pd.DataFrame({
                    'group_id': id
                    , 'group_displayName': group
                    , 'user_id': row.id
                    , 'user_displayName': row.displayName
                    , 'givenName': row.givenName
                    , 'surname': row.surname
                    , 'mail': row.mail
                    , 'userPrincipalName': row.userPrincipalName
                })
                df_members = pd.concat([df_members, df], ignore_index=True)

        # subsequent loop of runs while nextLink pagination is active
        # @odata.nextLink contains full URL including select & filter parameters defined in first loop
        while nextlink is not None:
            members = graph_client.get(nextlink)
            members = members.json()
            # Get the nextLink property if present
            try:
                nextlink = members['@odata.nextLink']
            except:
                nextlink = None
            members_nested_list = pd.json_normalize(members, record_path =['value'])
            if members_nested_list.shape[0] > 0:
                for row in members_nested_list.itertuples():
                    df = pd.DataFrame({
                        'group_id': id
                        , 'group_displayName': group
                        , 'user_id': row.id
                        , 'user_displayName': row.displayName
                        , 'givenName': row.givenName
                        , 'surname': row.surname
                        , 'mail': row.mail
                        , 'userPrincipalName': row.userPrincipalName
                    })
                    df_members = pd.concat([df_members, df], ignore_index=True)

    return df_members

def querySQL_Groups(server, database):
    conn = getSqlConnection(server, database)
    query = "select * from dbo.tblLaserAADGroups where ValidTo is null"
    df = pd.read_sql(query, conn)
    df = df.fillna('')
    return df

def querySQL_GroupMembers(server, database):
    conn = getSqlConnection(server, database)
    query = "select * from dbo.tblLaserAADGroupMembers where ValidTo is null"
    df = pd.read_sql(query, conn)
    df = df.fillna('')
    return df

def insertSql_Groups(data_frame, server, database):
    df = data_frame.fillna('Python NaN').replace(['Python NaN'], [None])
    # Insert new VmSizes from dataframe into table dbo.tblLaserVmSizes
    conn = getSqlConnection(server, database)
    with conn.cursor() as cursor:
        for row in df.itertuples():
            cursor.execute("insert into dbo.tblLaserAADGroups ([GroupID], [GroupDisplayName]) "
                           + "values (?, ?)"
                           , row.id
                           , row.displayName)
        conn.commit()

def insertSql_GroupMembers(data_frame, server, database):
    df = data_frame.fillna('Python NaN').replace(['Python NaN'], [None])
    # Insert new VmSizes from dataframe into table dbo.tblLaserVmSizes
    conn = getSqlConnection(server, database)
    with conn.cursor() as cursor:
        for row in df.itertuples():
            cursor.execute("insert into dbo.tblLaserAADGroupMembers ([GroupID],[GroupDisplayName] "
                           + ",[UserID],[UserDisplayName],[GivenName],[Surname],[Mail],[UserPrincipalName]) "
                           + "values (?, ?, ?, ?, ?, ?, ?, ?)"
                           , row.group_id
                           , row.group_displayName
                           , row.user_id
                           , row.user_displayName
                           , row.givenName
                           , row.surname
                           , row.mail
                           , row.userPrincipalName)
        conn.commit()

def updateGroups(server, database):
    # get existing records from sql database to dataframe
    df_e = querySQL_Groups(server, database)
    # get azure AAD Groups to dataframe
    df_n = Groups()
    
    # outer join dataframes, left_on = sql right_on = azure
    df_all = df_e.merge(df_n, how='outer', left_on='GroupID', right_on='id', indicator=True)
    
    # left_only = present in sql not in azure 
    df_delete = df_all.loc[df_all['_merge'] == 'left_only']
    # logically delete in sql
    if df_delete.shape[0] > 0:
        updateSQL_ValidTo(server=server, database=database, table='dbo.tblLaserAADGroups'
                          , pk='gid', id_list=df_delete['gid'].to_list())
    logging.info(f"{df_delete.shape[0]} AAD Groups date deleted")
    
    # both = present in sql and in azure  
        # no difference = no action 
        # difference = logically delete in sql and insert new record 
    df_update = df_all.loc[df_all['_merge'] == 'both']
    if df_update.shape[0] > 0:
        df_update = df_update.loc[ (df_update['displayName'] != df_update['GroupDisplayName']) ]
        if df_update.shape[0] > 0:
            df_update = df_update[['id','displayName']]
            updateSQL_ValidTo(server=server, database=database, table='dbo.tblLaserAADGroups'
                              , pk='gid', id_list=df_update['gid'].to_list())
            insertSql_Groups(df_update, server, database)
    logging.info(f"{df_update.shape[0]} AAD Groups date deleted and updated record inserted")
    
    # right_only = present in azure not in sql = insert new record    
    df_insert = df_all.loc[df_all['_merge'] == 'right_only']
    if df_insert.shape[0] > 0:
        df_insert = df_insert[['id','displayName']]
        insertSql_Groups(df_insert, server, database)
    logging.info(f"{df_insert.shape[0]} new AAD Groups created")

def updateGroupMembers(server, database):
    # get existing records from sql database to dataframe
    df_e = querySQL_GroupMembers(server, database)
    # get AAD Groups to DataFrame
    df_groups = Groups()
    # feed it to GroupMembers to get azure Group Members to dataframe
    df_n = GroupMembers(df_groups)
    
    # outer join dataframes, left_on = sql right_on = azure
    df_all = df_e.merge(df_n, how='outer', left_on=['GroupID', 'UserID'], right_on=['group_id', 'user_id'], indicator=True)
    
    # left_only = present in sql not in azure 
    df_delete = df_all.loc[df_all['_merge'] == 'left_only']
    # logically delete in sql
    if df_delete.shape[0] > 0:
        updateSQL_ValidTo(server=server, database=database, table='dbo.tblLaserAADGroupMembers'
                          , pk='gmid', id_list=df_delete['gmid'].to_list())
    logging.info(f"{df_delete.shape[0]} AAD Group Members date deleted")
    
    # both = present in sql and in azure  
        # no difference = no action 
        # difference = logically delete in sql and insert new record 
    df_update = df_all.loc[df_all['_merge'] == 'both']
    if df_update.shape[0] > 0:
        df_update = df_update.loc[ (df_update['group_displayName'] != df_update['GroupDisplayName'])
                                  | (df_update['user_displayName'] != df_update['UserDisplayName'])
                                  | (df_update['givenName'] != df_update['GivenName'])
                                  | (df_update['surname'] != df_update['Surname'])
                                  | (df_update['mail'].fillna('') != df_update['Mail'].fillna(''))
                                  | (df_update['userPrincipalName'] != df_update['UserPrincipalName'])
                                  ]
        if df_update.shape[0] > 0:
            df_update = df_update[['gmid', 'group_id','group_displayName', 'user_id', 'user_displayName'
                                   , 'givenName', 'surname', 'mail', 'userPrincipalName']]
            updateSQL_ValidTo(server=server, database=database, table='dbo.tblLaserAADGroupMembers'
                             , pk='gmid', id_list=df_update['gmid'].to_list())
            insertSql_GroupMembers(df_update, server, database)
    logging.info(f"{df_update.shape[0]} AAD Group Members date deleted and updated record inserted")
    
    # right_only = present in azure not in sql = insert new record    
    df_insert = df_all.loc[df_all['_merge'] == 'right_only']
    if df_insert.shape[0] > 0:
        df_insert = df_insert[['group_id','group_displayName', 'user_id', 'user_displayName'
                                , 'givenName', 'surname', 'mail', 'userPrincipalName']]
        insertSql_GroupMembers(df_insert, server, database)
    logging.info(f"{df_insert.shape[0]} new AAD Group Members created")
