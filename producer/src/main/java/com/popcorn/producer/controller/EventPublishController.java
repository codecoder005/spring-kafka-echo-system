package com.popcorn.producer.controller;

import avro.com.popcorn.refund.RefundEvent;
import com.popcorn.producer.service.EventPublishService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
@Slf4j
public class EventPublishController {
    private final EventPublishService eventPublishService;

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> publishEvent(@RequestBody RefundEvent refundEvent) {
        log.info("Publishing event to Kafka");
        eventPublishService.publish(refundEvent);
        return ResponseEntity.ok().build();
    }
}
