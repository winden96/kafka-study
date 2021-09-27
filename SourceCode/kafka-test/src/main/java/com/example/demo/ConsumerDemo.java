package com.example.demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * @descriptions: 提交offset
 * @author: zhangfaquan
 * @date: 2021/9/23 16:07
 * @version: 1.0
 */
public class ConsumerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    // 自动提交 offset
    @Test
    public void testAutoCommit() {
        //  1、创建用于连接kafka的配置。
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.136.151:9092");
        // 配置消费者组，将若干个消费者组织到一起，，共同消费kafka中的topic数据。
        props.put("group.id", "test");
        // 开启自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 配置自动提交的时间间隔
        props.put("auto.commit.interval.ms", "1000");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 2、创建Kafka消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);

        // 3、订阅要消费的主题
        kafkaConsumer.subscribe(Arrays.asList("first"));

        // 从半小时前开始消费
//        Map<TopicPartition, Long> map = new HashMap<>();
//        List<PartitionInfo> topicPartitions = kafkaConsumer.partitionsFor("first");
//        long time = new Date().getTime() - 1000 * 60 * 30;
//        topicPartitions.forEach(partitionInfo -> {
//            map.put(new TopicPartition("first", partitionInfo.partition()), time);
//        });
//        Map<TopicPartition, OffsetAndTimestamp> parMap = kafkaConsumer.offsetsForTimes(map);
//        parMap.forEach((k, v)->{
//            long offset = v.offset();
//            System.out.println("partition: "+ k.partition() + ", offset: "+ offset);
//            if (v != null) {
//                //没有这行代码会导致下面的报错信息
//                kafkaConsumer.assign(Arrays.asList(k));
//                kafkaConsumer.seek(k, offset);
//            }
//        });

        // 4、拉取数据
        while(true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));

            records.forEach(record->{
                // 获取主题
                String topic = record.topic();

                // 获取offset, 这条消息处于kafka分区中的哪个位置
                long offset = record.offset();

                // 获取key 和 value
                String key = record.key();
                String value = record.value();

                logger.info("topic: {}, offset: {}, key: {}, value: {}", topic, offset, key, value);
            });
        }
    }

    // 手动提交 offset - 同步方式
    @Test
    public void testCommitSync() {
        //  1、创建用于连接kafka的配置。
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.136.151:9092");
        // 配置消费者组，将若干个消费者组织到一起，，共同消费kafka中的topic数据。
        props.put("group.id", "test");
        // 关闭自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 2、创建Kafka消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        // 3、订阅要消费的主题
        kafkaConsumer.subscribe(Arrays.asList("first"));

        // 4、拉取数据
        while(true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));

            records.forEach(record->{
                // 获取主题
                String topic = record.topic();

                // 获取offset, 这条消息处于kafka分区中的哪个位置
                long offset = record.offset();

                // 获取key 和 value
                String key = record.key();
                String value = record.value();

                logger.info("topic: {}, offset: {}, key: {}, value: {}", topic, offset, key, value);
            });

            // 同步提交，当前线程会被阻塞。
            kafkaConsumer.commitSync();
        }
    }

    // 手动提交 offset - 异步方式
    @Test
    public void testCommitAsync() {
        //  1、创建用于连接kafka的配置。
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.136.151:9092");
        // 配置消费者组，将若干个消费者组织到一起，，共同消费kafka中的topic数据。
        props.put("group.id", "test");
        // 关闭自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 2、创建Kafka消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        // 3、订阅要消费的主题
        kafkaConsumer.subscribe(Arrays.asList("first"));

        // 4、拉取数据
        while(true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));

            records.forEach(record->{
                // 获取主题
                String topic = record.topic();

                // 获取offset, 这条消息处于kafka分区中的哪个位置
                long offset = record.offset();

                // 获取key 和 value
                String key = record.key();
                String value = record.value();

                logger.info("topic: {}, offset: {}, key: {}, value: {}", topic, offset, key, value);
            });

            // 异步提交
            kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition,
                                        OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        System.err.println("Commit failed for" +
                                offsets);
                    }
                }
            });
        }
    }
}
