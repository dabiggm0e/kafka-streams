package com.github.dabiggm0e.kafka.streams;

import com.fasterxml.jackson.databind.annotation.JsonAppend;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCount {
    static  String applicationId = "word-count";
    static String bootstrapServers = "localhost:9092";
    static String inputTopic = "streams-wordcount-input";
    static String outputTopic = "streams-wordcount-output";

    static Properties getStreamsConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return config;
    }

    static void createWordCountStream(final StreamsBuilder builder) {
        // stream from kakfa
        KStream<String, String> wordCountInput = builder.stream(inputTopic);

        KTable<String, Long> wordCounts = wordCountInput
                // map values to lowercase
                .mapValues(value -> value.toLowerCase())

                // flatmap values split by space
                .flatMapValues(lowercaseValues -> Arrays.asList( lowercaseValues.split(" ")))

                // select key
                .selectKey((ignoredKey, word) -> word)

                // group by key
                .groupByKey()
                // count
                .count();

        wordCounts.toStream().to( outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        // write back to kafka
    }
    public static void main(String[] args) {

        Properties config = getStreamsConfig();

        StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder);


       final KafkaStreams streams = new KafkaStreams(builder.build(), config);

       streams.start();
       System.out.println(streams.toString());

       Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
