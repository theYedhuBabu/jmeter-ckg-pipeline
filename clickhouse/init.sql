CREATE DATABASE IF NOT EXISTS metrics;

USE metrics;


CREATE TABLE jmeter_metrics
(
    timestamp DateTime,
    test_name String,
    thread_name String,
    label String,
    agent_id String,

    elapsed UInt32,
    response_code UInt16,
    response_message String,

    latency UInt32,
    success UInt8,
    bytes UInt32,
    threads UInt32,

    data_type String,
    failure_message String,

    sent_bytes UInt32,
    group_threads UInt32,
    all_threads UInt32,
    responseBody String,

    url String,

    idle_time UInt32,
    connect UInt32
)
ENGINE = MergeTree
ORDER BY (timestamp, label);



-- Kafka stream table

CREATE TABLE kafka_jmeter_metrics
(
    timestamp UInt64,
    test_name String,
    thread_name String,
    label String,
    agent_id String,

    elapsed UInt32,
    response_code UInt16,
    response_message String,

    latency UInt32,
    success UInt8,
    bytes UInt32,
    threads UInt32,

    data_type String,
    failure_message String,

    sent_bytes UInt32,
    group_threads UInt32,
    all_threads UInt32,
    responseBody String,


    url String,

    idle_time UInt32,
    connect UInt32
)
ENGINE = Kafka
SETTINGS
kafka_broker_list = 'kafka:9092',
kafka_topic_list = 'jmeter_metrics',
kafka_group_name = 'clickhouse-consumer',
kafka_format = 'JSONEachRow',
kafka_num_consumers = 4,
kafka_max_block_size = 10000;



-- Materialized view (replaces consumer.py)

CREATE MATERIALIZED VIEW jmeter_metrics_mv
TO jmeter_metrics
AS
SELECT
    toDateTime(timestamp / 1000) AS timestamp,
    test_name,
    thread_name,
    label,
    agent_id,

    elapsed,
    response_code,
    response_message,

    latency,
    success,
    bytes,
    threads,

    data_type,
    failure_message,

    sent_bytes,
    group_threads,
    all_threads,
    responseBody,

    url,

    idle_time,
    connect

FROM kafka_jmeter_metrics;