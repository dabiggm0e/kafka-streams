package com.github.dabiggm0e.kafka.streams;


import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.text.DecimalFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import static org.apache.kafka.streams.kstream.Materialized.as;

public class BankBalanceApp {

    static  String applicationId = "bank-balance";
    static String bootstrapServers = "localhost:9092";
    static String inputTopic = "bank-transactions";
    static String outputTopic = "bank-accounts2";

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

    static String newBalance(String transactionString, String balanceString) {
        JsonParser jsonParser = new JsonParser();

        JsonObject transaction = jsonParser.parse(transactionString).getAsJsonObject();
        JsonObject balance = jsonParser.parse(balanceString).getAsJsonObject();

        JsonObject newBalance = new JsonObject();
        newBalance.addProperty("count", balance.get("count").getAsInt()+1);
        newBalance.addProperty("balance",
                transaction.get("amount").getAsDouble() + balance.get("balance").getAsDouble());

        Long balanceEpoch = Instant.parse(balance.get("time").getAsString()).toEpochMilli();
        Long transactionEpoch = null;
        Instant newBalanceInstant = Instant.now();
        try {
            transactionEpoch = Instant.parse(transaction.get("time").getAsString()).toEpochMilli();
            newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));

        } catch (Exception e) {
            System.out.println("Transaction time: " + transaction.get("time").getAsString() );
            System.out.println("Balance time: " + balance.get("time").getAsString() );
            e.printStackTrace();
        }
        newBalance.addProperty("time", newBalanceInstant.toString());
        return newBalance.toString();

    }
    static void createBankBalanceStream(final StreamsBuilder builder) {
        // stream from kakfa

        // json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        JsonObject initialBalance = new JsonObject();
        initialBalance.addProperty("balance", 0d);
        initialBalance.addProperty("count", 0);
        initialBalance.addProperty("time", Instant.ofEpochMilli(0L).toString());
        String initialBalanceString = initialBalance.toString();


        KStream<String, String> transactionsStream = builder.stream(
                inputTopic/*,
                Consumed.with(Serdes.String(),jsonSerde)*/);


        KStream<String, String> filteredTransactionsStream = transactionsStream
                .filterNot((key, value) -> parseCustomerName(value).equals("") );// filter out transactions without names

        KTable<String, String> balancesTable = filteredTransactionsStream
                .selectKey((key, value) -> parseCustomerName(value.toString())) // select customer name as key in case key wasn't populated by the producer
                .groupByKey(
                     /*   Grouped.with(Serdes.String(), jsonSerde)*/)
                .aggregate(new Initializer<String>() {
                    @Override
                    public String apply() {
                        return initialBalanceString.toString();
                    }
                }, new Aggregator<String, String, String>() {
                    @Override
                    public String apply(final String key, final String transaction,final String balance) {
                        return newBalance(transaction, balance).toString();
                    }
                },Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(outputTopic));
             //   );
                        //Materialized.<String, JsonObject, KeyValueStore<Bytes, byte[]>>as("counts-store"));



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
