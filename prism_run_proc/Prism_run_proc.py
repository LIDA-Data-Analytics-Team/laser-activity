from ..SQL_stuff import getSqlConnection
import logging

def runSQL_sp_updateProjectsToDiscontinued(server, database):
    try:
        conn = getSqlConnection(server, database)
        query = "{CALL dbo.sp_updateProjectsToDiscontinued}"

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()

        logging.info("dbo.sp_updateProjectsToDiscontinued successfully executed")

    except:
        logging.exception("An error occurred:")
        conn.rollback()
        raise

    finally:
        if 'conn' in locals():
            conn.close()
