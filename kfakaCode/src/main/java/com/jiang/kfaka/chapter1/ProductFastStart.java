package com.jiang.kfaka.chapter1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProductFastStart {

    private static final String brokerList = "182.92.227.85:9092";

    // 创建的主题
    private static final String topic = "jiang-hhh";

    public static void main(String[] args) {
        Properties properties = new Properties();

        // 设置 key 序列化器
        // properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 设置重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        // 设置值序列化器
//        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置集群地址
        properties.put("bootstrap.servers", brokerList);

        // 自定义分区的使用
//        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, )
        // 自定义拦截器的使用
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName());

        //
        properties.put(ProducerConfig.ACKS_CONFIG, "0");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "kafka-demo", "hello,world to jiang");

        try {
            // 进行发送消息
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

}
