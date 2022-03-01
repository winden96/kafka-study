package com.jiang.kfaka.chapter1;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @author jiang
 * @date 2022/3/1 2:55 PM
 * @describe
 */
public class ConsumerIntercepter implements ConsumerInterceptor {


    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords records) {

        System.out.println("before: " + records);
        long now = System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String, String>>> map = new HashMap<>();

        Set<TopicPartition> partitions = records.partitions();
        for (TopicPartition partition : partitions) {
            List<ConsumerRecord<String, String>> tpRecords = records.records(partition);
            List<ConsumerRecord<String, String>> newTpRecords = new ArrayList<>();
            for (ConsumerRecord<String, String> record : tpRecords) {
                if (now - record.timestamp() < 3000) {
                    newTpRecords.add(record);
                }
            }

            if (!newTpRecords.isEmpty()) {
                map.put(partition, newTpRecords);
            }
        }

        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void onCommit(Map offsets) {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
