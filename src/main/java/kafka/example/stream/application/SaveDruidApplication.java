package kafka.example.stream.application;

import kafka.example.stream.druid.DruidSaver;
import kafka.example.stream.model.Data;
import kafka.example.stream.util.Configurations;
import kafka.example.stream.util.GlobalConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class SaveDruidApplication {

    public static void main(String... a) {
        buildApplication();
    }

    public static void buildApplication() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, GlobalConstants.APPLICATION_SAVE_DRUID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Configurations.getInstance().brokerList);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream(Configurations.getInstance().dataTopic);
        textLines
                .mapValues(Data::fromStr)
                .filter((key, value) -> value.isValid())
                .process(new DruidSaver());

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
