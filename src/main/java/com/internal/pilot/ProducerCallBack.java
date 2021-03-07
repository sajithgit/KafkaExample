package com.internal.pilot;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/*
* Following program pushes Messages in RRB manner to 3 partitions that are available for the
* Topic.If key was included Message would have gone only to specific partition
* */
public class ProducerCallBack {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerCallBack.class);

        logger.info("Worked");

        String  TOPIC = "KafkaSecond";
        //create Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        KafkaProducer <String,String> producer = new KafkaProducer<>(properties);
        for (int i=0;i<10;i++)
        {
            //Create producer

            ProducerRecord producerRecord = new ProducerRecord(TOPIC,"Message ::"+i);
            producer.send(producerRecord, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e == null) {
                                logger.info("RecordMetadata Topic:: " + recordMetadata.topic() + " Offset ::"+
                                        recordMetadata.offset() + " Partition:: " +
                                        recordMetadata.partition()+ " TS:: " +
                                        recordMetadata.timestamp());
//                                logger.info("RecordMetadata Offset:: " + recordMetadata.offset() + "");
//                                logger.info("RecordMetadata Partition:: " + recordMetadata.partition()+ "");
//                                 logger.info("RecordMetadata TS:: " + recordMetadata.timestamp() + "");
                            }
                            else
                            {
                                logger.error("Error While Sending");
                            }
                        }
                    });
            producer.flush();
            //producer.close();
        }



    }
}
