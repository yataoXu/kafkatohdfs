package com.evan.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

/**
 * @Description
 * @ClassName ForwardListener
 * @Author Evan
 * @date 2019.10.15 10:46
 */
@Slf4j
@Component
public class ForwardListener {

    @KafkaListener(id = "forward", topics = "first_top4")
    @SendTo("first_top2")
    public String forward(String data) {
        log.info("接收到消息数量：{}",data);
        return "send msg : " + data;
    }
}
