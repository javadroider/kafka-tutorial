package com.javadroider.kafka.two;

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

    public static void main(String[] args) {
        new TwitterProducer().run();

    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(TwitterProducer.class);



        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        //create twitter client
        Client client = createTwitterClient(msgQueue);
        logger.info("Client created");

        //create kafka producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();
        logger.info("Kafka producer created");
        String topic = "twitter_tweets";

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Stopping application");
            client.stop();
            kafkaProducer.close();
        }));
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Error while polling message", e);
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                kafkaProducer.send(new ProducerRecord<>(topic, null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Error while sending message to kafka", e);
                        }
                    }
                });
            } else {
                logger.info("Nothing found");
            }
        }


        //loop to send tweets to kafka
    }

    private static final String consumerKey = "GeTMvAoBzCN8D1euXnJsrMKSy";
    private static final String consumerSecret = "FOGX3P6kwgojjkDSLwZ0XiMrW2S2EWmj7uMEGCr0T5urobrwBP";
    private static final String token = "3989364013-uG11msacu6QDwfcxCkamswjrJQcQxdAF3sBHAAf";
    private static final String secret = "jpzufbnWw9n31m6HjX4wFdvFrcxr8BbBBSTm8NgV2II4J";

    private KafkaProducer<String, String> createKafkaProducer() {
        String bootStrapServers = "127.0.0.1:9092";

        //create java properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        return new KafkaProducer<>(properties);

    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("sarkar", "india", "bjp", "kafka");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
