package com.jiang.kfaka.springkafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SendMessage {

    private static final String topic = "heima";

    @Autowired
    private KafkaTemplate kafkaTemplate;

    /**
     * 消息的发送
     * @param message
     * @return
     */
    @GetMapping("/send/{message}")
    public String index(@PathVariable String message) {
        this.kafkaTemplate.send(topic, message);

        // 事务的支持
        kafkaTemplate.executeInTransaction(t -> {
            t.send(topic, message);
            if ("error".equals(message)) {
                throw new RuntimeException("input is error");
            }
            t.send(topic, message + " another");
            return true;
        });
        return "hello" + message;
    }

    @GetMapping("/send/{message}")
    @Transactional(rollbackFor = RuntimeException.class)
    public String index2(@PathVariable String message) {
        this.kafkaTemplate.send(topic, message);

        // 事务的支持
            kafkaTemplate.send(topic, message);
            if ("error".equals(message)) {
                throw new RuntimeException("input is error");
            }
            kafkaTemplate.send(topic, message + " another");
        return "hello" + message;
    }

    /**
     * 消息的接收
     */
    @KafkaListener(id = "", topics = topic, groupId = "group.demo")
    public void listener(String input) {
        System.out.println("message reserve: " + input);
    }
}
