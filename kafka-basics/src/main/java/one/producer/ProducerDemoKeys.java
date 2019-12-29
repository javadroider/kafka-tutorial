package one.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {


    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootStrapServers = "127.0.0.1:9092";

        //create java properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //send data
        for (int i = 1; i < 11; i++) {
            String topic = "first_topic";
            String value = "Hello World" + i;
            String key = "id_" + i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info("Receeived metadata : \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while sending message", e);
                    }
                }
            });
        }
        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
