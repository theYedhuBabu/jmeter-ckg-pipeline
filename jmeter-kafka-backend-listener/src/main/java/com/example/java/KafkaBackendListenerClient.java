package com.example.java;

import org.apache.jmeter.visualizers.backend.AbstractBackendListenerClient;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.apache.jmeter.samplers.SampleResult;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.*;


public class KafkaBackendListenerClient extends AbstractBackendListenerClient{

    private Producer<String, String> producer;
    private String topic;
    private String testName;
    private String agentId;
    private ObjectMapper mapper = new ObjectMapper();


    @Override
    public void setupTest(BackendListenerContext context) {

        String brokers = context.getParameter("kafka.bootstrap.servers");
        topic = context.getParameter("kafka.topic");

        this.testName = context.getParameter("test.name", "unknown_test");
        this.agentId = context.getParameter("agent.id", "unknown_agent");



        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864");
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        producer = new KafkaProducer<>(props);
    }

    @Override
    public void handleSampleResults(List<SampleResult> results, BackendListenerContext context){


        for (SampleResult result : results) {
            try{
                Map<String, Object> metric = new HashMap<>();

                metric.put("timestamp", result.getTimeStamp());
                metric.put("label", result.getSampleLabel());
                metric.put("thread_name", result.getThreadName());
                metric.put("elapsed", result.getTime());
                metric.put("latency", result.getLatency());
                metric.put("success", result.isSuccessful());
                metric.put("bytes", result.getBytesAsLong());
                metric.put("threads", result.getAllThreads());
                metric.put("response_code", result.getResponseCode());
                metric.put("response_message", result.getResponseMessage());
                metric.put("data_type", result.getDataType());
                metric.put("failure_message", result.getFirstAssertionFailureMessage());
                metric.putIfAbsent("failure_message", ""); //null handling
                metric.put("sent_bytes", result.getSentBytes());
                metric.put("group_threads", result.getGroupThreads());
                metric.put("all_threads", result.getAllThreads());

                if (!result.isSuccessful()){
                    String body = result.getResponseDataAsString();

                    if (body.length() > 5000) {
                        body = body.substring(0, 5000);
                    }

                    metric.put("responseBody", body);
                }else{
                    metric.put("responseBody", "");
                }

                String urlAsString = result.getURL() != null ? result.getURL().toString() : "";
                metric.put("url", urlAsString);

                metric.put("idle_time", result.getIdleTime());
                metric.put("connect", result.getConnectTime());
                metric.put("test_name", this.testName);
                metric.put("agent_id", this.agentId);


                // 1. Convert the Map to a JSON String
                String jsonPayload = mapper.writeValueAsString(metric);

                // 2. Create a ProducerRecord and send it to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, jsonPayload);
                producer.send(record);

            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    @Override
    public void teardownTest(BackendListenerContext context) {

        if (producer != null) {
            producer.flush();
            producer.close(Duration.ofSeconds(10));
        }
    }

}
