package one.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        System.out.println("Hello");

        String bootStrapServers = "127.0.0.1:9092";

        //create java properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        //send data
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic", "Hello World");
        kafkaProducer.send(producerRecord);
        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
