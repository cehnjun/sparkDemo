package com.chenjun.spark.KafkaStreaming;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * consumer demo
 *
 */
public class consumer {
    public static void main( String[] args ) {
        String topicName = "result";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //订阅topic主题消息
        consumer.subscribe(Arrays.asList(topicName));
        //打印主题名
        System.out.println("Subscribed to topic " + topicName);
        while (true) {
            //一次拉取多个消息
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                //打印消费者消息
                System.out.printf("%s\n", record.value());
            }
        }
    }
}
