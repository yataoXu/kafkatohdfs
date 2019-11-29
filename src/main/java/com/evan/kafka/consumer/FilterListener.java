package com.evan.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * @Description
 * @ClassName FilterListener
 * @Author Evan
 * @date 2019.10.15 10:46
 */
@Slf4j
@Component
public class FilterListener {

    @KafkaListener(topics = {"filter_topic"},containerFactory="filterContainerFactory")
    public void consumerBatch(ConsumerRecord<?, ?> record){
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (kafkaMessage.isPresent()) {
            Object message = kafkaMessage.get();
            log.info("record =" + record);
            log.info("接收到消息数量：{}",message);
        }

    }
}
