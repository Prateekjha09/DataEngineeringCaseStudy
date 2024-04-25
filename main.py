import utils.allLogsCaptured as allLogsCaptured
import utils.connection as connection
import utils.rawDataIngestion as rawDataIngestion
import utils.ProcessedDataIngestion as ProcessedDataIngestion
import sys

if __name__ == "__main__":

    try:
        # Calling a function in allLogsCaptured to return logger object along with configuration object
        log_obj, config_obj = allLogsCaptured.log_object_creation()

    except Exception as e:
        log_obj.error("main.py| Exiting the system...")
        sys.exit(0)

    try:
        # Establishing connection to Snowflake database with actively used schema containing required tables
        conn_params = config_obj['CONNECTION_NAME']
        mainConnection = connection.getConnection(conn_params['WAREHOUSE'],
                                                  log_obj, conn_params['DATABASE'], log_obj,
                                                  conn_params['SCHEMA'])
        
        results = rawDataIngestion.rawDataIngestion(mainConnection, log_obj)


        if results == 'SUCCESS':
            # Proceed to data MERGE from raw data to the cleanest version in a different set of tables
            dataFinalStatus = ProcessedDataIngestion.ProcessedDataIngestion(mainConnection,log_obj)

            if dataFinalStatus == "SUCCESS":
                log_obj.info("All data import complete, exiting the system")

                connection.closeConnection(mainConnection,log_obj)
                sys.exit(0)

    except Exception as e:
        log_obj.error("main.py| Encountered error: " +str(e))
        connection.closeConnection(mainConnection,log_obj)
        sys.exit()

        # Exiting the execution if there is any issue encountered 