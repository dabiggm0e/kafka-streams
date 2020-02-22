package com.github.dabiggm0e.kafka.streams;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BankTransactionsProducerTests {

    @Test
    public void newRandomTransactionTest() {
        String name = "moe";

        ProducerRecord<String, String> record = BankTransactionsProducer.createTransactionRecord(name);
        System.out.println(record.toString());


        String key = record.key();
        String value = record.value();

        JsonParser jsonParser = new JsonParser();

        assertEquals(key, name);

        assertTrue("Amount should be less than " + BankTransactionsProducer.maxAmount,
                jsonParser.parse(value)
                .getAsJsonObject()
                .get("amount")
                .getAsDouble() < BankTransactionsProducer.maxAmount);


    }

}
