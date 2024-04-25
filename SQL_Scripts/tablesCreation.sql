USE SNOWFLAKE_DB.SCHEMA;

-- Ad Impressions Table Creation Script
CREATE TABLE ad_impressions_stg (
    impression_id INT PRIMARY KEY,
    ad_creative_id INT,
    user_id INT,
    timestamp TIMESTAMP,
    website VARCHAR(255)
);

CREATE TABLE ad_impressions (
    impression_id INT PRIMARY KEY,
    ad_creative_id INT,
    user_id INT,
    timestamp TIMESTAMP,
    website VARCHAR(255)
);

-- Clicks and Conversions Table Creation Script
CREATE TABLE clicks_conversions_stg (
    click_conversion_id INT PRIMARY KEY,
    event_timestamp TIMESTAMP,
    user_id INT,
    ad_campaign_id INT,
    conversion_type VARCHAR(50)
);

CREATE TABLE clicks_conversions (
    click_conversion_id INT PRIMARY KEY,
    event_timestamp TIMESTAMP,
    user_id INT,
    ad_campaign_id INT,
    conversion_type VARCHAR(50)
);

-- Bid Requests Table Creation Script
CREATE TABLE bid_requests_stg (
    bid_request_id INT PRIMARY KEY,
    user_id INT,
    auction_details VARCHAR(255),
    ad_targeting_criteria VARCHAR(255)
);

CREATE TABLE bid_requests (
    bid_request_id INT PRIMARY KEY,
    user_id INT,
    auction_details VARCHAR(255),
    ad_targeting_criteria VARCHAR(255)
);