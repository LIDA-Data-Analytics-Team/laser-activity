"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Authenticating using AAD MFA from MacOS / Linux
https://stackoverflow.com/questions/58440480/connect-to-azure-sql-in-python-with-mfa-active-directory-interactive-authenticat

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
from azure.identity import AzureCliCredential, ManagedIdentityCredential, DefaultAzureCredential, ChainedTokenCredential
import struct
import pyodbc 

server = 'lida-dat-cms.database.windows.net'
database = 'lida_dat_cms'

def getSqlConnection(server, database):
    # Use the cli credential to get a token after the user has signed in via the Azure CLI 'az login' command.
    credential = ChainedTokenCredential(AzureCliCredential(), DefaultAzureCredential(), ManagedIdentityCredential())
    databaseToken = credential.get_token('https://database.windows.net/.default')

    # get bytes from token obtained
    tokenb = bytes(databaseToken[0], "UTF-8")
    exptoken = b'';
    for i in tokenb:
        exptoken += bytes({i});
        exptoken += bytes(1);
    tokenstruct = struct.pack("=i", len(exptoken)) + exptoken;

    # build connection string using acquired token
    connString = "Driver={ODBC Driver 17 for SQL Server};SERVER="+server+";DATABASE="+database+""
    SQL_COPT_SS_ACCESS_TOKEN = 1256 
    conn = pyodbc.connect(connString, attrs_before = {SQL_COPT_SS_ACCESS_TOKEN:tokenstruct});

    return conn

def updateSQL_ValidTo(server, database, table, pk, id_list):
    conn = getSqlConnection(server, database)
    with conn.cursor() as cursor:
        for id in id_list:
            cursor.execute(f"update {table} set ValidTo = getdate() where {pk} = ?"
            , id
            )
        conn.commit()