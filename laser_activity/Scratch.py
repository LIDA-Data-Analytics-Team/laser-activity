from .SQL_stuff import getSqlConnection

def writeToSql_test(server, database):
    conn = getSqlConnection(server, database)
    cursor = conn.cursor()
    cursor.execute(
        "insert into dbo.testTable (runTime) values (getdate())"
        )
    conn.commit()
    cursor.close()