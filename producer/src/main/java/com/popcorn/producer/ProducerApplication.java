package com.popcorn.producer;

import avro.com.popcorn.refund.RefundEvent;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

@SpringBootApplication
@Slf4j
public class ProducerApplication {

    private static final String TOPIC_NAME = "com-popcorn-refund";

    static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    @Bean
    public Gson jsonHelper() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        return gsonBuilder.disableJdkUnsafe().setPrettyPrinting().create();
    }

    @Bean
    public CommandLineRunner commandLineRunner(KafkaTemplate<String, Object> kafkaTemplate, Gson jsonHelper) {
        return args -> {
            RefundEvent refundEvent = RefundEvent.newBuilder()
                    .setFirstName("John")
                    .setLastName("Doe")
                    .build();
            CompletableFuture<SendResult<String, Object>> send = kafkaTemplate.send(TOPIC_NAME, refundEvent);
            log.info(jsonHelper.toJson(send.get()));
        };
    }

}
