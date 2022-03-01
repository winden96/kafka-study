package com.jiang.kfaka.chapter1;

import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.transaction.annotation.Transactional;

import java.util.Properties;

/**
 * 配置事务
 */
public class ProducerTransactionSend {

    public static final String topic = "heima";
    public static final String brokerList = "182.92.227.85:9092";
    public static final String transactionId = "transactionId";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 初始化事务
        producer.initTransactions();
        // 开启事务
        producer.beginTransaction();
        try {
            ProducerRecord<String, String> record1 = new ProducerRecord<>(topic, "message-1");
            producer.send(record1);
            ProducerRecord<String, String> record2 = new ProducerRecord<>(topic, "message-2");
            producer.send(record2);
            ProducerRecord<String, String> record3 = new ProducerRecord<>(topic, "message-3");
            producer.send(record3);
        } catch (Exception e) {
            producer.abortTransaction();
        }

        // 结束事务
        producer.close();
    }

}
