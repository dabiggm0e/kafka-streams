package com.github.dabiggm0e.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavoriteColor {
    static  String applicationId = "word-count";
    static String bootstrapServers = "localhost:9092";
    static String inputTopic = "streams-favcolor-input";
    static String intermediaryTopic = "streams-favcolor-input-compacted";
    static String outputTopic = "streams-favcolor-output";


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

        KStream<String, String> favColorInput = builder.stream(inputTopic);
        KStream<String, String> allowedFavColorInput = favColorInput
                .filter((key, value) -> allowedColors.contains(value.toLowerCase()));

        KTable<String, String> reducedFavColorTable = allowedFavColorInput
                .groupByKey()
                .reduce((oldValue, newValue) -> newValue);

        KStream<String, String>  favColorFilteredStream = reducedFavColorTable.toStream();

        KTable<String, Long> favColorOutput = favColorFilteredStream
                .selectKey((key, value)->value.toLowerCase())
                .groupByKey()
                .count();

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
