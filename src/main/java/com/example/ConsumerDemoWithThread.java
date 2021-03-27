package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {

        new ConsumerDemoWithThread().run();

    }
    private   ConsumerDemoWithThread (){
    }
    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerThread = new ConsumerThread(latch);
        Thread thread = new Thread(myConsumerThread);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("caught shutdownhook");
            ((ConsumerThread)myConsumerThread).shutdown();
        }));
        try {
            latch.wait();
        } catch (InterruptedException e) {
            logger.info("InterruptedException down",e);
        }finally {
            logger.info("app Shutting down");
        }
    }
    public class ConsumerThread implements Runnable{

        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
        private CountDownLatch latch;
        KafkaConsumer<String, String> consumer;

        public ConsumerThread(CountDownLatch latch){
            this.latch =latch;
            Properties properties=new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-java-application");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  //"latest", "none"


            consumer = new KafkaConsumer<String, String>(properties);
            //Subscribe consumer to topic
            consumer.subscribe(Arrays.asList("first_topic"));

        }

        @Override
        public void run() {
            //poll new data
            try{
            while (true){
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for(ConsumerRecord consumerRecord : consumerRecords){
                    logger.info("key: "+consumerRecord.key()+", Values: "+consumerRecord.value());
                    logger.info("Partition: " + consumerRecord.partition() +", Offset: " + consumerRecord.offset());
                }
            }}catch (WakeupException e){
                logger.error("Shutdown");
            }finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown(){
            consumer.wakeup();//to interupt consumer.poll. throws wakeupexception
        }
    }
}
