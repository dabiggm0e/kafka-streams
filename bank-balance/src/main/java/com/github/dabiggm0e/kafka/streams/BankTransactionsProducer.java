package com.github.dabiggm0e.kafka.streams;

import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class BankTransactionsProducer {
    static Logger logger = LoggerFactory.getLogger(BankTransactionsProducer.class);

    public static JsonObject createTransaction(String name, Double amount) {
        JsonObject transactionJson = new JsonObject();

        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);
        String nowAsISO = df.format(new Date());

        DecimalFormat dcf = new DecimalFormat("#.##");

        transactionJson.addProperty("name", name);
        transactionJson.addProperty("amount", Double.valueOf(dcf.format(amount)));
        transactionJson.addProperty("time", nowAsISO );

        return transactionJson;
    }


    public static void main(String[] args) throws InterruptedException {
        String bootstrap_servers = "localhost:9092";
        String topic = "bank-transactions";

        String[] customers =  new String[]{"John", "Smith", "Mike", "David", "Brown", "White"};
        Double maxAmount = 10.0;

        // create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        Random ran = new Random();
        DecimalFormat df = new DecimalFormat("#.##");

        Integer index = 0;

        while(true) {
            ++index;

            Integer randomCustomerIndex = ran.nextInt(customers.length);
            Double randomAmount = (ran.nextDouble() * maxAmount);
            randomAmount = Double.valueOf(df.format(randomAmount));
            String customer = customers[randomCustomerIndex];

            JsonObject transaction = createTransaction(customer, randomAmount);

            // create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, customer, transaction.toString());
            producer.send(record);

            logger.info(transaction.toString());
            if(index==100) {
                index = 0;
                Thread.sleep(1000);
            }

        }

    }
}
