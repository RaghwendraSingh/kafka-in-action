package org.raghwendra.streaming;

import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SampleProducer {
    public static void main(final String[] args) throws InterruptedException {
        if (args.length != 2) {
            System.out.println("please provide the broker details & topic -> host:port topic-name");
            System.exit(1);
        }
        String broker = args[0];
        String topic = args[1];
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", broker);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("acks", "all");
        producerProps.put("retries", 1);
        producerProps.put("batch.size", 20000);
        producerProps.put("linger.ms", 1);
        producerProps.put("buffer.memory", 24568545);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);
        for (int j = 0; j < 100; j++) {
            for (int i = 0; i < 200000; i++) {
                ProducerRecord data = new ProducerRecord<String, String>(topic, "user" + i + ",path" + i);
                Future<RecordMetadata> recordMetadata = producer.send(data);
            }
            Thread.sleep(500);
        }
        producer.close();
    }
}
