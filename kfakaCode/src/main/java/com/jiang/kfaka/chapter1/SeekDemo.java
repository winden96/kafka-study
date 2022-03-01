package com.jiang.kfaka.chapter1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;

/**
 * 指定位移消费
 */
public class SeekDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.poll(Duration.ofMillis(2000));
        // 获取消费者所分配到到分区
        Set<TopicPartition> assignment = consumer.assignment();
        System.out.println(assignment);
        for (TopicPartition topicPartition : assignment) {
            // 参数 partition 表示分区，offset 表示指定从分区到哪个位置开始消费
            consumer.seek(topicPartition, 10);
        }
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.offset() + ": " + record.value());
            }
        }
    }
}
