package com.github.koumudi.kafka.beginning;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        Logger logger = LoggerFactory.getLogger(Consumer.class);
        //set properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my_fourth_application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //replay action -- assign and seek
//        TopicPartition partitionToReadFrom = new TopicPartition("first_topic", 0);
//        long offsetRead = 15L;
//        int totalPartitionsToRead = 5;
//        int partitionsRead = 0;
//        boolean keepOnReadings = true;
//        //assign
//        consumer.assign(Arrays.asList(partitionToReadFrom));
//        //seek
//        consumer.seek(partitionToReadFrom, offsetRead);

        //subscribe consumer
       //consumer.subscribe(Collections.singleton("first_topic"));
        consumer.subscribe(Arrays.asList("first_topic"));

        //poll for the data
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record: records){
//                partitionsRead +=1;
                logger.info("Consumer Record" + "\n" + "Key " + record.key() + "Value " + record.value() + "\nPartition " + record.partition() + "\nTimestamp "+ record.timestamp());
//                if(partitionsRead >= totalPartitionsToRead){
//                    keepOnReadings = false;
//                    break;
//                }
            }
        }
    }
}
