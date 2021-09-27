package com.example.demo;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @descriptions: 统计发送消息成功和发送失败消息数，并在 producer 关闭时打印这两个计数器
 * @author: zhangfaquan
 * @date: 2021/9/23 17:01
 * @version: 1.0
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

    private volatile AtomicInteger errorCounter = new AtomicInteger(0);
    private volatile AtomicInteger successCounter = new AtomicInteger(0);

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // 统计成功和失败的次数
        if (exception == null) {
            successCounter.incrementAndGet();
        } else {
            errorCounter.incrementAndGet();
        }
    }

    @Override
    public void close() {
        // 保存结果
        System.out.println("Successful sent: " + successCounter.get());
        System.out.println("Failed sent: " + errorCounter.get());
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
