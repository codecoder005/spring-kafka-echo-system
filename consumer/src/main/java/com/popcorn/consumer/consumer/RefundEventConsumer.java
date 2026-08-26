package com.popcorn.consumer.consumer;

import avro.com.popcorn.refund.RefundEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class RefundEventConsumer {
    @KafkaListener(topics = "${topic.name}")
    public void read(ConsumerRecord<String, RefundEvent> consumerRecord) {
        String key = consumerRecord.key();
        RefundEvent refundEvent = consumerRecord.value();
        log.info("Avro message received for key : {} value : {}", key, refundEvent.toString());
    }
}
