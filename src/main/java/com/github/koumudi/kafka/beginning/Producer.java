package com.github.koumudi.kafka.beginning;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.util.Properties;

public class Producer {
    public static void main(String args[]){
        //logger
        Logger logger = LoggerFactory.getLogger(Producer.class);

       String bootstrapServers = "127.0.0.1:9092";

       //create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //whatever the values we give to kafka gets converted to 0s and 1s. Here, we are giving string values
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        Thread.currentThread().setContextClassLoader(null);

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World, I'm Koumudi");

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e==null){
                    logger.info("Record details \n"+ "Topic " + recordMetadata.topic() + "\n" + "partition " + recordMetadata.partition() + "\n" + "timestamp " + recordMetadata.timestamp());
                }
                else{
                    e.printStackTrace();
                }
            }
        });
        //async operations
        producer.flush();
        producer.close();
    }
}
