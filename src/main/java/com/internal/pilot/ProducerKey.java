package com.internal.pilot;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;


/*
* Following program pushes Messages in Key ordered manner to 3 partitions that are available for the
* Topic.This is since we included key
* */
public class ProducerKey {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerKey.class);

        logger.info("Worked");

        String  TOPIC = "KafkaSecond";
        //create Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //For Safe Producer
        //Set Producer as Idempotence to ensure kafka does not duplicate messages.
        // This is taken care with producer id which is assigned for every message.
        //enable.idempotence = true appears in logs
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"True");
        //Applicable for kafka >2 else set as 1
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));


        KafkaProducer <String,String> producer = new KafkaProducer<>(properties);
        for (int i=0;i<10;i++)
        {
            //Create producer
            String key = "ID_"+Integer.toString(i);
//          If we try with same key the program will push the message to same partitionin this case it
//          pushes to partition 2
//          String key = "ID_"+Integer.toString(5);
            String value = "Message With Key ::"+Integer.toString(i);
            ProducerRecord producerRecord = new ProducerRecord(TOPIC,key,value);
            final Future error_while_sending = producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("RecordMetadata Topic:: " + recordMetadata.topic() + " Offset ::" +
                                recordMetadata.offset() +
                                "Key :: " + key + " Partition:: " +

                                recordMetadata.partition() + " TS:: " +
                                recordMetadata.timestamp());
//                                logger.info("RecordMetadata Offset:: " + recordMetadata.offset() + "");
//                                logger.info("RecordMetadata Partition:: " + recordMetadata.partition()+ "");
//                                 logger.info("RecordMetadata TS:: " + recordMetadata.timestamp() + "");
                    } else {
                        logger.error("Error While Sending");
                    }
                }
            });
            producer.flush();
            //producer.close();
        }



    }
}
