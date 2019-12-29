package one.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        String topic = "first_topic";
        String groupId = "my-fourth-application";
        String bootStrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
       // properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        TopicPartition topicPartition = new TopicPartition(topic, 0);
        kafkaConsumer.assign(Collections.singletonList(topicPartition));
        kafkaConsumer.seek(topicPartition, 10);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        while (keepOnReading) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar++;
                logger.info("Key: " + record.key() + " Value: " + record.value());
                logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
                if(numberOfMessagesReadSoFar>numberOfMessagesToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }

    }
}
