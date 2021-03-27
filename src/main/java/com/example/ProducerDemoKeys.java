package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("Hello");
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

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
        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        int i =0;
        do {
            String topic = "first_topic";
            String value = "HelloWorld_"+i;
            String key = "id_"+Integer.toString(i);

            //create producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            logger.info("Key: "+key);  // Check all keys in second run goes to same partition as first run.
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
            }).get(); //block the send to make it sync   //not for prod
            i++;
        }while (i!=30);
        producer.flush();
        producer.close();
    }
}
