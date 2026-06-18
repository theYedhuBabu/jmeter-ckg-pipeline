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
kafka_flush_interval_ms = 2000,
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



CREATE TABLE metrics.telegraf_kafka_queue
(
    name String,
    timestamp UInt64,
    fields Map(String, Float64),
    tags Map(String, String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'lg-metrics',
    kafka_group_name = 'ch_telegraf_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 2000,
    kafka_max_block_size = 10000;


CREATE TABLE metrics.jmeter_infra_metrics (
    metric_name LowCardinality(String),
    timestamp DateTime64(0, 'UTC'),
    
    -- Hardware Metrics
    cpu_usage_user Float64 DEFAULT 0,
    mem_used_percent Float64 DEFAULT 0,
    
    -- JVM Metrics from Jolokia
    jvm_heap_used Float64 DEFAULT 0,
    jvm_gc_time_ms Float64 DEFAULT 0,
    
    -- Metadata Context Tags
    host LowCardinality(String),
    agent_name LowCardinality(String),
    test_id String
) ENGINE = MergeTree()
ORDER BY (test_id, metric_name, timestamp);


CREATE MATERIALIZED VIEW metrics.jmeter_infra_metrics_mv TO default.jmeter_infra_metrics AS
SELECT
    name AS metric_name,
    toDateTime64(timestamp, 0) AS timestamp,
    
    -- Safely extract numeric fields based on metric stream type
    if(name = 'cpu', fields['usage_user'], 0) AS cpu_usage_user,
    if(name = 'mem', fields['used_percent'], 0) AS mem_used_percent,
    if(name = 'jmeter_runtime', fields['TestId'], 0) AS jvm_heap_used, -- Maps your Groovy object payload
    if(name = 'jvm_gc', fields['CollectionTime'], 0) AS jvm_gc_time_ms,
    
    -- Extract strings from the tags object map
    tags['host'] AS host,
    tags['agent_name'] AS agent_name,
    
    -- Pull the runtime string test_id cleanly from the fields object mapping
    if(name = 'jmeter_runtime', toString(fields['TestId']), '') AS test_id
FROM metrics.telegraf_kafka_queue;
