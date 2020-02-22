package com.github.dabiggm0e.kafka.streams;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class BankBalanceApp {

    static  String applicationId = "bank-balance";
    static String bootstrapServers = "localhost:9092";
    static String inputTopic = "bank-transactions";
    static String outputTopic = "bank-accounts";

    static JsonParser jsonParser = new JsonParser();

    static Properties getStreamsConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");

        //  disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        return config;
    }

    public static String parseCustomerName(String jsonRequest) {
        Logger logger = Logger.getLogger(BankBalanceApp.class.getName());
        String name;

        try {
             name = jsonParser.parse(jsonRequest)
                    .getAsJsonObject()
                    .get("name")
                    .getAsString();

        } catch (NullPointerException e) {
            logger.info("Bad data: " + jsonRequest);
            return "";
        }

        return name;
    }

    public static Double parseTransactionAmount(String jsonRequest) {
        Logger logger = Logger.getLogger(BankBalanceApp.class.getName());
        Double amount;
        DecimalFormat df = new DecimalFormat("#.##");

        try {
            amount = jsonParser.parse(jsonRequest)
                    .getAsJsonObject()
                    .get("amount")
                    .getAsDouble();

        } catch (NullPointerException e) {
            logger.info("Bad data: " + jsonRequest);
            return 0.0;
        }

        return  Double.valueOf(df.format(amount));
    }

    public static String parseTransactionTime(String jsonRequest) {
        Logger logger = Logger.getLogger(BankBalanceApp.class.getName());
        String time;

        try {
            time = jsonParser.parse(jsonRequest)
                    .getAsJsonObject()
                    .get("time")
                    .getAsString();

        } catch (NullPointerException e) {
            logger.info("Bad data: " + jsonRequest);
            return "";
        }

        return time;
    }

    public static JsonObject getTransactionJson(String name, Double amount, String time) {
        JsonObject transaction = new JsonObject();

        DecimalFormat df = new DecimalFormat("#.##");


        transaction.addProperty("name", name);
        transaction.addProperty("amount", Double.valueOf(df.format(amount)));
        transaction.addProperty("time", time);

        return transaction;
    }

    static void createBankBalanceStream(final StreamsBuilder builder) {
        // stream from kakfa


        KStream<String, String> transactionsStream = builder.stream(inputTopic);
        KStream<String, String> filteredTransactionsStream = transactionsStream
                .filterNot((key, value) -> parseCustomerName(value).equals("") );// filter out transactions without names

        KTable<String, String> balancesTable = filteredTransactionsStream
                .selectKey((key, value) -> parseCustomerName(value)) // select customer name as key in case key wasn't populated by the producer
                .groupByKey()
                .reduce(
                        (oldValue, newValue) -> getTransactionJson(
                                parseCustomerName(newValue),
                                parseTransactionAmount(oldValue) + parseTransactionAmount(newValue),
                                parseTransactionTime(newValue)
                        ).toString()
                );

        balancesTable.toStream().to(
                outputTopic,
                Produced.with(Serdes.String(), Serdes.String()));
        // write back to kafka */

    }


    public static void main(String[] args) {

        Properties config = getStreamsConfig();

        StreamsBuilder builder = new StreamsBuilder();
        createBankBalanceStream(builder);


        final KafkaStreams streams = new KafkaStreams(builder.build(), config);

        // only do this in dev - not in prod
        streams.cleanUp();

        streams.start();

        // print the topology
        System.out.println(builder.build().describe());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
