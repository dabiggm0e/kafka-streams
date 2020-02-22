package com.github.dabiggm0e.kafka.streams;

import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.time.Instant;
import java.util.*;

public class BankTransactionsProducer {
    static Logger logger = LoggerFactory.getLogger(BankTransactionsProducer.class);
    static final Double maxAmount = 10.0;
    static final String bootstrap_servers = "localhost:9092";
    static final String topic = "bank-transactions";

    public static  ProducerRecord<String, String> createTransactionRecord(String name) {
        Random ran = new Random();
        DecimalFormat df = new DecimalFormat("#.##");

        Double randomAmount = (ran.nextDouble() * maxAmount);
        randomAmount = Double.valueOf(df.format(randomAmount));

        JsonObject transactionJson = new JsonObject();

        DecimalFormat dcf = new DecimalFormat("#.##");

        Instant now =  Instant.now();

        transactionJson.addProperty("name", name);
        transactionJson.addProperty("amount", randomAmount);
        transactionJson.addProperty("time", now.toString() );

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, name, transactionJson.toString());

        return record;
    }


    public static void main(String[] args) throws InterruptedException {


        String[] customers =  new String[]{"John", "Smith", "Mike", "David", "Brown", "White"};

        // create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");


        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        Integer index = 0;

        while(true) {
            ++index;

            Random ran = new Random();
            Integer randomCustomerIndex = ran.nextInt(customers.length);

            String customer = customers[randomCustomerIndex];

            // create producer record
            ProducerRecord<String, String> record = createTransactionRecord(customer);
            producer.send(record);

            logger.info(record.value().toString());
            if(index==100) {
                index = 0;
                Thread.sleep(1000);
            }

        }

    }
}
