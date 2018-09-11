package kafka.example.stream.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaHelper {
    private static Producer<String, String> producer = null;

    private static Producer<String, String> getProducer() {
        if (null == producer) {
            producer = new KafkaProducer<>(makeProps());
        }
        return producer;
    }

    public static void pushMessageToTopic(String topic, String message) {
        getProducer().send(new ProducerRecord<>(topic, message));
    }

    public static void destroyConnection() {
        if (null != producer) {
            producer.close();
        }
    }

    /**
     * @return
     */
    private static Properties makeProps() {
        Properties props = new Properties();

        props.put("bootstrap.servers", Configurations.getInstance().brokerList);

        props.put("acks", "0");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 1024 * 1024);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }
}
