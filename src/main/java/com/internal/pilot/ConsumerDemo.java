package com.internal.pilot;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class ConsumerDemo {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String groupID ="Spring-Application";
        String TOPIC = "KafkaSecond";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //Create Consumer - For single topic.Can be done with multiple topic using array
        consumer.subscribe(Collections.singleton(TOPIC));

        //Poll data
        while(true)
        {
            ConsumerRecords<String,String> consumerRecords =
                    consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> csr : consumerRecords)
            {
                logger.info("Key ::"+csr.key()+ " Value: "+csr.value());
                logger.info("Partition ::"+csr.partition()+ " Offset: "+csr.offset());
            }
        }

    }
}
