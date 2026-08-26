package com.popcorn.producer.service;

import avro.com.popcorn.refund.RefundEvent;
import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventPublishService {
    @Value("${topic.name}")
    private String TOPIC_NAME;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Gson jsonHelper;

    public void publish(RefundEvent refundEvent) {
        log.info("Publishing event to Kafka");
        String uniqueId = UUID.randomUUID().toString();
        long timestamp = System.currentTimeMillis();

        Message<RefundEvent> message = MessageBuilder
                .withPayload(refundEvent)
                .setHeader(KafkaHeaders.TOPIC, TOPIC_NAME)
                .setHeader(KafkaHeaders.KEY, uniqueId)
                .setHeader(KafkaHeaders.TIMESTAMP, timestamp)
//                .setHeader("version", RefundEvent.getClassSchema().getObjectProp("version"))
                .build();

        try {
            log.info("kafka message before sending {}", jsonHelper.toJson(message));
            CompletableFuture<SendResult<String, Object>> send = kafkaTemplate.send(message);
            log.info("kafka message after sending {}", jsonHelper.toJson(send.get()));
        } catch (InterruptedException | ExecutionException iex) {
            log.error("Error occurred while publishing event to Kafka ", iex);
        } catch (RuntimeException re) {
            log.error("Unexpected error occurred ", re);
        }
    }
}
