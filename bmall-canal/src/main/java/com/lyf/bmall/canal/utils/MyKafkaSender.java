package com.lyf.bmall.canal.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author shkstart
 * @date 19:00
 */
public class MyKafkaSender {
    public static KafkaProducer<String,String> kafkaProducer = null;

    public static KafkaProducer<String,String> createKafkaProducer(){

        Properties properties = new Properties();

        properties.put("bootstrap.servers","bw77:9092,bw78:9092,bw79:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }

    public static void send(String topic,String msg){
        if(kafkaProducer==null){
            kafkaProducer=createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String, String>(topic,msg));
    }

}
