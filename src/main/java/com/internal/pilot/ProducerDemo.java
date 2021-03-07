package com.internal.pilot;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        logger.info("Worked");

        String  TOPIC = "KafkaSecond";
        //create Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create producer
        KafkaProducer <String,String> producer = new KafkaProducer<>(properties);

        ProducerRecord producerRecord = new ProducerRecord(TOPIC,"Message");
        producer.send(producerRecord);

        producer.flush();
        producer.close();
    }
}
