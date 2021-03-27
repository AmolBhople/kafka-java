package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo2 {
    public static void main(String[] args) {
        System.out.println("Hello");
        final Logger logger = LoggerFactory.getLogger(ProducerDemo2.class);

        //producer properties

        Properties properties=new Properties();
//        https://kafka.apache.org/documentation/#producerconfigs
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        OR
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //SAFE PRODUCER
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");




        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        int i =0;
        do {
            //create producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello_world_"+i);
            //send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Execute every time a record is sent or exception
                    if (e == null) {
                        logger.info("\n----------------------------\n" +
                                "RECEIVERD METADATA \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp() +
                                "\n----------------------------\n");

                    } else {
                        logger.error("ERROR while producing... " + e.getMessage());
                    }
                }
            });
            i++;
        }while (i!=10);
        producer.flush();
        producer.close();
    }
}
