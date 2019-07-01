package com.github.sunilcode369.kafka.tutorial;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create a Producer Class
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // Create a Producer record /* Without keys it wont be round robin */
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("new_topic2","KARNATAKA");
        ProducerRecord<String,String> record1 = new ProducerRecord<String, String>("new_topic2","TAMILNADU");
        ProducerRecord<String,String> record2 = new ProducerRecord<String, String>("new_topic2","KERALA");
        ProducerRecord<String,String> record3 = new ProducerRecord<String, String>("new_topic2","ANDHRA");
        ProducerRecord<String,String> record4 = new ProducerRecord<String, String>("new_topic2","TELANGANA");


        ProducerRecord<String,String> record5 = new ProducerRecord<String, String>("new_topic2","1","1");
        ProducerRecord<String,String> record6 = new ProducerRecord<String, String>("new_topic2","2","2");
        ProducerRecord<String,String> record7 = new ProducerRecord<String, String>("new_topic2","3","3");
        ProducerRecord<String,String> record8 = new ProducerRecord<String, String>("new_topic2","4","4");
        ProducerRecord<String,String> record9 = new ProducerRecord<String, String>("new_topic2","5","5");

        // Send data
        producer.send(record,new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                 System.out.println(recordMetadata.topic());
            }
        });
        producer.send(record1,new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println(recordMetadata.topic());
            }
        });
        producer.send(record2,new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println(recordMetadata.topic());
            }
        });
        producer.send(record3,new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println(recordMetadata.topic());
            }
        });
        producer.send(record4,new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println(recordMetadata.topic());
            }
        });
        producer.send(record5,new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println(recordMetadata.topic());
            }
        });
        producer.send(record6,new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println(recordMetadata.topic());
            }
        });
        producer.send(record7,new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println(recordMetadata.topic());
            }
        });
        producer.send(record8,new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println(recordMetadata.topic());
            }
        });
        producer.send(record9,new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println(recordMetadata.topic());
            }
        });

        // Flush and close
        producer.flush();
        producer.close();
    }
}
