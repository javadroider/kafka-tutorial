package com.javadroider.kafka.three;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ElasticSearchConsumer {
    private static Logger log = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    private void sample() throws IOException {
        ElasticSearchConsumer elasticSearchConsumer = new ElasticSearchConsumer();
        RestHighLevelClient client = elasticSearchConsumer.createClient();
        String jsonString = "{\"foo\":\"bar\"}";
        IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        log.info(id);

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ElasticSearchConsumer elasticSearchConsumer = new ElasticSearchConsumer();
        RestHighLevelClient client = elasticSearchConsumer.createClient();

        KafkaConsumer<String, String> kafkaConsumer = elasticSearchConsumer.createKafkaConsumer();

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            log.info("Received {} records", records.count());
            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {
                //where we insert data into ElasticSearch
                IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(record.value(), XContentType.JSON);
                bulkRequest.add(indexRequest);
            }
            if (records.count() > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                log.info("Committing offset");
                kafkaConsumer.commitSync();
                log.info("Offsets have been committed");
            }
        }

    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        String bootStrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";
        String topic = "twitter_tweets";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");//disable auto commit of pffsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        return kafkaConsumer;
    }

    private RestHighLevelClient createClient() {

        String host = "kafka-testing-3672147233.ap-southeast-2.bonsaisearch.net";
        String username = "cheufushz9";
        String password = "fczfd68els";


        CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(host, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder
                                .setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        return new RestHighLevelClient(builder);
    }
}
