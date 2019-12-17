package com.wbbigdata.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.UUID;

/**
 * 模拟kafka生产者，向kafka集群发送消息
 */
public class MyKafkaProducer {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("metadata.broker.list", "wbbigdata00:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer<String, String> producer = new Producer<String, String>(producerConfig);

        String topic = "test_offset_topic";

        for (int i = 0; i < 100; i++) {
            producer.send(new KeyedMessage<String, String>(topic, i + "", UUID.randomUUID() + ""));
        }
        System.out.println("kafka生产者发送数据完毕~~~");
    }
}
