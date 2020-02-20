package com.github.dabiggm0e.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavoriteColor {
    static  String applicationId = "word-count";
    static String bootstrapServers = "localhost:9092";
    static String inputTopic = "favcolor-input";
    static String intermediaryTopic = "favcolor-input-compacted";
    static String outputTopic = "favcolor-output";


    static Properties getStreamsConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return config;
    }

    static void createFavoriteColorStream(final StreamsBuilder builder) {
        // stream from kakfa
        List<String> allowedColors = Arrays.asList("red", "green", "blue");


        KStream<String, String> textLines = builder.stream(inputTopic);

        KStream<String, String> usersAndColors = textLines
                .filter((key, value) -> value.contains(","))
                .selectKey((key, value)-> value.split(",")[0].toLowerCase())
                .mapValues(value -> value.split(",")[1].toLowerCase())
                .filter((user, color) -> allowedColors.contains(color));

        usersAndColors.to(intermediaryTopic);

        KTable<String, String> usersAndColorsTable = builder.table(intermediaryTopic);
        KTable<String, Long> favColorOutput = usersAndColorsTable
                .groupBy((user, color)-> new KeyValue<>(color, color))
                .count(Named.as("CountsByColor"));


        favColorOutput.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        // write back to kafka
    }
    public static void main(String[] args) {

        Properties config = getStreamsConfig();

        StreamsBuilder builder = new StreamsBuilder();
        createFavoriteColorStream(builder);


        final KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.start();
        System.out.println(builder.build().describe());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
