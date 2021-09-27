package com.example.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @descriptions: 异步发送消息
 * @author: zhangfaquan
 * @date: 2021/9/23 16:06
 * @version: 1.0
 */
public class ProductDemo2 {

    private static final Logger logger = LoggerFactory.getLogger(ProductDemo2.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        KafkaProducer<String, String> kafkaProducer = null;
        try {
            // 1、创建用于连接kafka的配置。
            Properties props = new Properties();
            // 指定kafka集群地址
            props.put("bootstrap.servers", "192.168.136.151:9092");
            // 指定acks模式，acks可取值1（默认值），0（不考虑消息是否丢失），all（保证消息不丢失，其实就是当消息同步到主题分区的副本中之后再返回值给生产者。）
            props.put("acks", "all");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            // 2、创建生产者
            kafkaProducer = new KafkaProducer<String, String>(props);

            // 3、发送消息
            for (int i = 0; i < 100; i++) {
                // 构建消息，注意消息的泛型要和生产者定义的泛型一致。
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("test", null, String.valueOf(i));
                kafkaProducer.send(producerRecord, (RecordMetadata metadata, Exception e)->{
                    // 回调函数，该方法会在 Producer 收到 ack 时调用，为异步调用
                    // 写入kafka成功返回的是RecordMetadata，写入kafka失败返回的是Exception
                    // 如果异常为null，则代表发送成功。
                    // 注意：消息发送失败会自动重试，不需要我们在回调函数中手动重试。
                    if (e == null) {
                        String topic = metadata.topic();
                        long offset = metadata.offset();
                        int partition = metadata.partition();
                        logger.info("topic: {}, offset: {}, partition: {}", topic, offset, partition);
                        return;
                    }
                    logger.error("生产消息失败，出现异常");
                });
            }
        } finally {
            // 4、关闭生产者
            if (kafkaProducer != null)
                kafkaProducer.close();
        }
    }
}
