from kafka import KafkaConsumer
from confluent_kafka.avro import AvroConsumer
import json
import csv
import pandas as pd
import sys


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
            new_data = pd.DataFrame.from_dict(ad_impressions_consumer_raw)

            write_pandas(mainConnection,new_data,"ad_impressions_stg",auto_create_table = True)
            ad_impressions_consumer_raw_status = 'SUCCESS'

            log_obj.info("Ad Imprssions data has been loading to staging table.")

        else:
            log_obj.info("There was an issue with format of data supplied for ad impressions, please recitfy it")
            log_obj.info("Exiting the system..")
            sys.exit(0)
            

        # Raw data ingestion for ad impressions from CSV file generated
        clicks_conversions_raw_status = ''
        clicks_conversions_raw = csv.DictReader(open('clicks_conversions.csv', 'r'))

        # Converting dict data to a python dataframe
        new_data = pd.DataFrame.from_dict(clicks_conversions_raw)

        # Writing data back to snowflake table
        write_pandas(mainConnection,new_data,"clicks_conversions_stg",auto_create_table = True)
        # Setting raw data fetch and load as success
        clicks_conversions_raw_status = 'SUCCESS'

        log_obj.info("Click conversions data has been loaded into staging environment.")

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

        # Writing the raw dataframe to snowflake database
        write_pandas(mainConnection,df_raw,"bid_requests_stg",auto_create_table = True)

        # Status updation
        bid_requests_consumer_raw_status = 'SUCCESS'
        log_obj.info("Bid requests data has been loaded into staging environment.")
    
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


