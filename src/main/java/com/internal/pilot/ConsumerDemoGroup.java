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


/*
* Consumer Group re-balancing . consumers rebalance as and when new consumers join Group
*
* Previous allocation was 2 partition , once one of the consumer was taken off line, rebaalnce happened and new partition
* was assigned to the consumer.
Revoke previously assigned partitions KafkaSecond-0
[main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=consumer-Spring-Application-New-1, groupId=Spring-Application-New] (Re-)joining group
[main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=consumer-Spring-Application-New-1, groupId=Spring-Application-New] Successfully joined group with generation Generation{generationId=4, memberId='consumer-Spring-Application-New-1-2ad23308-0951-4a52-ba01-be9280b4e3a8', protocol='range'}
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-Spring-Application-New-1, groupId=Spring-Application-New] Finished assignment for group at generation 4: {consumer-Spring-Application-New-1-2ad23308-0951-4a52-ba01-be9280b4e3a8=Assignment(partitions=[KafkaSecond-0, KafkaSecond-1]), consumer-Spring-Application-New-1-bf2ee14e-e53e-4d6e-860f-d7bf0ac92c5b=Assignment(partitions=[KafkaSecond-2])}
[main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=consumer-Spring-Application-New-1, groupId=Spring-Application-New] Successfully synced group in generation Generation{generationId=4, memberId='consumer-Spring-Application-New-1-2ad23308-0951-4a52-ba01-be9280b4e3a8', protocol='range'}
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-Spring-Application-New-1, groupId=Spring-Application-New] Notifying assignor about the new Assignment(partitions=[KafkaSecond-0, KafkaSecond-1])
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-Spring-Application-New-1, groupId=Spring-Application-New] Adding newly assigned partitions: KafkaSecond-1, KafkaSecond-0
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-Spring-Application-New-1, groupId=Spring-Application-New] Setting offset for partition KafkaSecond-1 to the committed offset FetchPosition{offset=67, offsetEpoch=Optional[0], currentLeader=LeaderAndEpoch{leader=Optional[host.docker.internal:9092 (id: 0 rack: null)], epoch=0}}
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-Spring-Application-New-1, groupId=Spring-Application-New] Setting offset for partition KafkaSecond-0 to the committed offset FetchPosition{offset=51, offsetEpoch=Optional[0], currentLeader=LeaderAndEpoch{leader=Optional[host.docker.internal:9092 (id: 0 rack: null)], epoch=0}}
* */
public class ConsumerDemoGroup {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroup.class.getName());

        String groupID ="Spring-Application-New";
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
