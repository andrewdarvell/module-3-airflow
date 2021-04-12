use adyakov;

-- STG

CREATE EXTERNAL TABLE stg_billing(user_id INT, billing_period STRING, service STRING, tariff STRING, sum STRING, created_at STRING)
STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-adyakov/data_lake/stg/billing';


CREATE EXTERNAL TABLE stg_issue(user_id string, start_time STRING, end_time STRING, title STRING, description STRING, service STRING)
    STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-adyakov/data_lake/stg/issue';


CREATE EXTERNAL TABLE stg_traffic(user_id INT, `timestamp` STRING, device_id STRING, device_ip_addr STRING, bytes_sent BIGINT, bytes_received BIGINT)
    STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-adyakov/data_lake/stg/traffic';

CREATE EXTERNAL TABLE stg_payment(user_id INT, pay_doc_type STRING, pay_doc_num BIGINT, account STRING, phone STRING, billing_period STRING, pay_date STRING, sum STRING)
    STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-adyakov/data_lake/stg/payment';

-- ODS

CREATE EXTERNAL TABLE ods_billing(user_id INT, billing_period STRING, service STRING, tariff STRING, sum DECIMAL(10,0), created_at TIMESTAMP)
PARTITIONED BY (year STRING)
    STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-adyakov/data_lake/ods/billing';


CREATE EXTERNAL TABLE ods_issue(user_id INT, start_time TIMESTAMP, end_time TIMESTAMP, title STRING, description STRING, service STRING)
PARTITIONED BY (year STRING) STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-adyakov/data_lake/ods/issue';


CREATE EXTERNAL TABLE ods_traffic(user_id INT, `timestamp` TIMESTAMP, device_id STRING, device_ip_addr STRING, bytes_sent BIGINT, bytes_received BIGINT) PARTITIONED BY (year STRING)
    STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-adyakov/data_lake/ods/traffic';


CREATE EXTERNAL TABLE ods_payment(user_id INT, pay_doc_type STRING, pay_doc_num BIGINT, account STRING, phone STRING, billing_period STRING, pay_date DATE, sum DECIMAL(10,2)) PARTITIONED BY (year STRING)
    STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-adyakov/data_lake/ods/payment';


-- DM

CREATE EXTERNAL TABLE dm_traffic(user_id INT, max_bytes_received BIGINT, min_bytes_received BIGINT, avg_bytes_received BIGINT)
PARTITIONED BY (year STRING) STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-adyakov/data_lake/dm/traffic';
