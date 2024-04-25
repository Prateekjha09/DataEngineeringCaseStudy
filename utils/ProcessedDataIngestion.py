

def ProcessedDataIngestion(mainConnection,log_obj):
    base_sql_query = """EXECUTE TASK DATA_MOVT_FROM_STG_TO_STG;"""

    query_cursor = mainConnection.cursor()
    query_cursor.execute(base_sql_query)
    query_id = query_cursor.sfqid
    query_status = mainConnection.get_query_status(query_id)

    if str(query_status).split(".")[-1] == "SUCCESS":
        mainConnection.commit()
        log_obj.info("Task related to data migration is now complete.")

        status = "SUCCESS"

    else:
        log_obj.info("Task did not complete, please check the reason for the same.")
        status = None

    return status