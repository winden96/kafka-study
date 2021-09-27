package com.example.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @descriptions: 同步发送消息
 * @author: zhangfaquan
 * @date: 2021/9/23 16:07
 * @version: 1.0
 */
public class ProductDemo {

    private static final Logger logger = LoggerFactory.getLogger(ProductDemo.class);

    // 生产100个消息，消息内容为1-100（不含100）
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        KafkaProducer<String, String> kafkaProducer = null;
        try {
            // 1、创建用于连接kafka的配置。
            Properties props = new Properties();
            // 指定kafka集群地址
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.136.151:9092");
            // 指定acks模式，acks可取值1（默认值），0（不考虑消息是否丢失），all（保证消息不丢失，其实就是当消息同步到主题分区的副本中之后再返回值给生产者。）
            props.put("acks", "all");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            // 2、创建生产者
            kafkaProducer = new KafkaProducer<String, String>(props);

            // 3、发送消息
            for (int i = 0; i < 100; i++) {
                // 构建消息，注意消息的泛型要和生产者定义的泛型一致。
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first", null, String.valueOf(i));
                Future<RecordMetadata> res = kafkaProducer.send(producerRecord);

                // 等待返回消息
                res.get();
                logger.info("{} 写入成功", i);
            }
        } finally {
            // 4、关闭生产者
            if (kafkaProducer != null)
                kafkaProducer.close();
        }
    }
}
