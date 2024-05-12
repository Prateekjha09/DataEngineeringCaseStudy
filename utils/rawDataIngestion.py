from kafka import KafkaConsumer
from confluent_kafka.avro import AvroConsumer
import json
import csv
import pandas as pd
import sys
import jsonschema

from avro.schema import Parse
from avro.io import DatumReader, BinaryDecoder

# Functions to validate data format coming in from different sources
def validate_ad_impressions(data,log_obj):
    """This function is for validating raw data coming in for AD Impressions data"""

    # Variable to store schema information from Kafka source providing ad_impressions data
    ad_impressions_schema = {
    "type": "object",
    "properties": {
        "ad_creative_id": {"type": "integer"},
        "user_id": {"type": "integer"},
        "timestamp": {"type": "string", "format": "date-time"},
        "website": {"type": "string"}
    },
    "required": ["ad_creative_id", "user_id", "timestamp", "website"]
    }

    try:
        log_obj.info("Ad Impressions data validation has started")
        jsonschema.validate(instance=data, schema=ad_impressions_schema)
        return True

        log_obj.info("Ad Impressions data validation is now completed.")
    except Exception as e:
        log_obj.info("Ad Impressions data validation failed due to data type issue from source side.")
        return False

def click_conversions_data(file_path,log_obj):
    """Function to validate click conversions data supplied via a csv file."""
    try:
        with open(file_path, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                # Check if all required columns are present
                required_columns = ['event_timestamp', 'user_id', 'ad_campaign_id', 'conversion_type']
                for column in required_columns:
                    if column not in row:
                        log_obj.error("Missing required column: {column} ".format(column))

        
        log_obj.info("Click conversions data schema is valid.")
        return True
    
    except Exception as e:
        log_obj.error("Please check out this error while validating csv data: "+str(e))

def validate_bid_requests_data(df,rules,log_obj):
    try:
        is_valid = True
        for column, rule in rules.items():
            if column not in df.columns:
                log_obj.error(f"Validation error: Column '{column}' not found.")
                is_valid = False
                continue
            try:
                if not df[column].apply(rule).all():
                    log_obj.error(f"Validation error: Column '{column}' contains invalid values.")
                    is_valid = False
            except Exception as e:
                log_obj.error(f"Validation error: {e}")
                is_valid = False

        return is_valid
    
    except Exception as e:
        log_obj.error("please check out this issue: "+str(e))
        return False

def rawDataIngestion(mainConnection, log_obj):
    log_obj.info("Processing raw data load from original sources to snowflake's staging tables.")
    try:
        # Raw data ingestion for ad impressions from Kafka Producer assuming it is producing one sample row at a given time

        """Sample data produced for ad impressions is:
        {
        'impression_id': 1,
        'ad_creative_id': 1001,
        'user_id': 12345,
        'timestamp': '2024-04-25T08:30:00',
        'website': 'example.com'}
        """

        ad_impressions_consumer_raw_status = ''
        ad_impressions_consumer_raw = KafkaConsumer('ad_impressions_topic',  # Kafka Consumer consuming data from a bootstrap server
                                                bootstrap_servers=['localhost:9092'],
                                                value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        
        # Creation of a dataframe and insertion into a staging table
        if type(ad_impressions_consumer_raw) == dict():

            # Added a data validation function here
            if validate_ad_impressions(ad_impressions_consumer_raw) == True:
                new_data = pd.DataFrame.from_dict(ad_impressions_consumer_raw)

                write_pandas(mainConnection,new_data,"ad_impressions_stg",auto_create_table = True)
                ad_impressions_consumer_raw_status = 'SUCCESS'

                log_obj.info("Ad Impressions data has been loading to staging table.")
            elif validate_ad_impressions(ad_impressions_consumer_raw) == False:
                log_obj.info("There was an issue with format of data supplied for ad impressions, please rectify it")

        else:
            log_obj.info("There was an issue with format of data supplied for ad impressions, please rectify it")
            log_obj.info("Exiting the system..")
            sys.exit(0)
            

        # Raw data ingestion for ad impressions from CSV file generated
        path = 'clicks_conversions.csv'
        clicks_conversions_raw_status = ''
        clicks_conversions_raw = csv.DictReader(open(path, 'r'))

        # Validating data from csv for click conversions
        if click_conversions_data(path) == True:
            # Converting dict data to a python dataframe
            new_data = pd.DataFrame.from_dict(clicks_conversions_raw)

            # Writing data back to snowflake table
            write_pandas(mainConnection,new_data,"clicks_conversions_stg",auto_create_table = True)
            # Setting raw data fetch and load as success
            clicks_conversions_raw_status = 'SUCCESS'

            log_obj.info("Click conversions data has been loaded into staging environment.")

        elif click_conversions_data(path) == False:
            log_obj.error("Data not loaded for click conversions into staging environment.")

        # Raw data ingestion for bid requests from AVRO Producer
        bid_requests_consumer_raw_status = ''
        bid_requests_consumer_raw = AvroConsumer({'bootstrap.servers': 'localhost:9092',
                                          'group.id': 'bid_requests_group',
                                          'schema.registry.url': 'http://localhost:8081',
                                          'auto.offset.reset': 'earliest'})
        # Subscribe to the Kafka topic
        bid_requests_consumer_raw.subscribe(['your_topic_name'])

        # List to store deserialized data
        data_list = []
        
        while True:
            msg = bid_requests_consumer_raw.poll(1.0)  # Adjust the poll timeout as needed
            if msg is None:
                continue
            if msg.error():
                log_obj.info("Consumer error: {}".format(msg.error()))
                continue

            # Decode Avro message
            decoded_msg = msg.value()
            # Append decoded message to data list
            data_list.append(decoded_msg)

            # Commit the message offsets

            bid_requests_consumer_raw.commit(msg)
            # Break the loop if needed

            if len(data_list) >= 100:  # Exit loop after consuming 100 messages
                break

        # Convert data list to Pandas DataFrame
        df_raw = pd.DataFrame(data_list) # type is pandas dataframe

        # Close the AvroConsumer
        bid_requests_consumer_raw.close()

        # Validating the dataframe storing the Consumer data
        validation_rules = {
            'user_id': lambda x: x.is_integer(),
            'auction_details': lambda x: isinstance(x, str),
            'ad_targeting_criteria': lambda x: isinstance(x, str)}
        
        log_obj.info("BId requests data validation has started")
        if validate_bid_requests_data(df_raw,validation_rules) == True:
            # Writing the raw dataframe to snowflake database
            write_pandas(mainConnection,df_raw,"bid_requests_stg",auto_create_table = True)

            # Status updation
            bid_requests_consumer_raw_status = 'SUCCESS'
            log_obj.info("Bid requests data has been loaded into staging environment.")

        else:
            bid_requests_consumer_raw_status = 'ERROR'
            log_obj.info("Bid requests data cannot be loaded into staging environment, please check the logs for the reasons.")
        
        log_obj.info("Bid requests data validation is now complete.")

        if bid_requests_consumer_raw_status == clicks_conversions_raw_status and \
        clicks_conversions_raw_status == ad_impressions_consumer_raw_status and \
        bid_requests_consumer_raw_status == ad_impressions_consumer_raw_status:
            log_obj.info("The status is SUCCESS since every data for staging env is complete.")
            return 'SUCCESS' # finally the all raw data dump from source to staging complete
        
        else:
            return 'ERROR'

    except Exception as e:
        log_obj.error("Please check this error while writing data to staging tables: "
                      +str(e))
        log_obj.error("Exiting the system")
        sys.exit(1)


