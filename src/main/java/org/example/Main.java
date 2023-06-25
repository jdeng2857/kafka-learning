package org.example;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        producer();
    }

    public static void producer(){
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);

        ProducerRecord<String, String> record =
                new ProducerRecord<>("CustomerCountry", "Precision Products", "France");
        try {
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Synchronous message send (not recommended for production)
        ProducerRecord<String, String> record2 =
                new ProducerRecord<>("CustomerCountry", "Precision Products", "Canada");
        try {
            System.out.println(record2);
            producer.send(record2).get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Async callback
        ProducerRecord<String, String> record3 =
                new ProducerRecord<>("CustomerCountry", "Biomedical Materials","USA");
        producer.send(record3, new DemoProducerCallback());
    }

    static class DemoProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e){
            if (e != null) {
                e.printStackTrace();
            }
        }
    }
}

