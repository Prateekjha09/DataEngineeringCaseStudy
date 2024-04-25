create or replace PROCEDURE STG_TO_PROD_DATA_MOVT()
returns string not null
LANGUAGE PYTHON
RUNTIME_VERSION = '2.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
execute as caller

AS
$$
from snowflake.snowpark import session as snowpark_session
from datetime import datetime

-- Function to merge ad_impressions data
def move_ad_impressions_data(snowpark_session):
    snowpark_session.sql(f"MERGE INTO ad_impressions a using ad_impressions_stg b on \
    a.impression_id = b.impression_id when matched and ifnull(a.ad_creative_id,0) <> ifnull(b.ad_creative_id,0) or \
    ifnull(a.user_id,0) <> ifnull(b.user_id,0) or a.timestamp <> b.timestamp or ifnull(a.website,'') <> ifnull(b.website,'') \
    then update set a.ad_creative_id = b.ad_creative_id, a.user_id=b.user_id. a.timestamp=b.timestamp, \
    a.website = b.website when not matched then insert (impression_id, ad_creative_id, user_id, timestamp, website) \
    values (b.impression_id,b.ad_creative_id, b.user_id, b.timestamp, b.website);").collect()

    return 1

def move_clicks_conversions(snowpark_session):
    snowpark_session.sql(f"MERGE INTO clicks_conversions a using clicks_conversions_stg b on \
    a.click_conversion_id = b.click_conversion_id when matched and a.event_timestamp <> b.event_timestamp \
    ifnull(a.user_id,0) <> ifnull(b.user_id,0) or ifnull(a.ad_campaign_id,0) <> ifnull(b.ad_campaign_id,0) \
    or ifnull(a.conversion_type,'') <> ifnull(b.conversion_type,'') \
    then update set a.event_timestamp = b.event_timestamp, a.user_id = b.user_id, a.ad_campaign_id = b.ad_campaign_id \
    a.conversion_type = b.conversion_type when not matched then insert \
    (click_conversion_id,event_timestamp, user_id, ad_campaign_id, conversion_type) values \
    (b.click_conversion_id,b.event_timestamp, b.user_id, b.ad_campaign_id, b.conversion_type)").collect()

    return 1

def move_bid_requests(snowpark_session):
    snowpark_session.sql(f"MERGE INTO bid_requests a using bid_requests_stg b on \
    a.bid_request_id = b.bid_request_id when matched ifnull(a.user_id,0) <> ifnull(b.user_id,0) or \
    ifnull(a.auction_details,'') <> ifnull(b.auction_details,'') or \
    ifnull(a.ad_targeting_criteria,'') <> ifnull(b.ad_targeting_criteria,'') \
    when not matched then insert \
    (bid_request_id,user_id,auction_details,ad_targeting_criteria) \
    values (b.bid_request_id, b.user_id, b.auction_details, b.ad_targeting_criteria, b.ad_targeting_criteria);")

    return 1

    def run(snowpark_session: snowpark_session):
        a = move_ad_impressions_data(snowpark_session)

        b = move_clicks_conversions(snowpark_session)

        c = move_bid_requests(snowpark_session)

        if a ==b and b == c and a == c:
            return 'SUCCESS'
        else:
            return 'FAILURE'
$$


-- TO BE RUN ONLY ONCE
CREATE OR REPLACE TASK DATA_MOVT_FROM_STG_TO_STG
WAREHOUSE = WAREHOUSE -- WAREHOUSE DETAIL
AS
CALL STG_TO_PROD_DATA_MOVT();

SHOW TASKS;

-- for executing the task 
EXECUTE TASK DATA_MOVT_FROM_STG_TO_STG;

