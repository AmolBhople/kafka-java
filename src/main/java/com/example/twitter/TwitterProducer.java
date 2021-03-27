package com.example.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    List<String> terms = Lists.newArrayList("kafka" );

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){
        logger.info("runnning..............");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        Client client= createTwitterClient(msgQueue);

        client.connect();


        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application");
            client.stop();
            kafkaProducer.flush();
            kafkaProducer.close();

        }));

        while (!client.isDone()){
            String msg =null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error("error");
                client.stop();
            }
            if (msg!=null){
                logger.info(msg);

                ProducerRecord<String, String> record = new ProducerRecord<String, String>("twitter_tweets", null, msg);
                //send data
                kafkaProducer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // Execute every time a record is sent or exception
                        if (e != null) {
                            logger.error("ERROR while producing... " + e.getMessage());
                        }
                    }
                });
            }
            logger.info("end");
        }

    }
    public Client createTwitterClient(BlockingQueue<String> msgQueue){
        logger.info("Twitter CLient........");



        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
//        List<Long> followings = Lists.newArrayList(1234L, 566788L);

//        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1("F4puyh9fO9PMiwy3UlGcgCXTv",
                "kv3cdo3b4ZSyUcxJtRGkmiy7GHh3EyhlKwj02pBqALnJjPRlL2",
                    "2516976474-CGriGe23ZjuXfy9Ze8I0XPjbDksalahhNV6QfgP", "8CDb2KWGxpGBwZJ1x21ZnaBmLoMX2keV6AGcSyNiGSe9x");


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));                         // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
// Attempts to establish a connection.
//        hosebirdClient.connect();
    }

    public KafkaProducer<String, String> createKafkaProducer(){
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

        return producer;

    }
}
