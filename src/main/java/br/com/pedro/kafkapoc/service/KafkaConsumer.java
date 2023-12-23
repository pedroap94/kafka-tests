package br.com.pedro.kafkapoc.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = "${spring.kafka.consumer.topic}", concurrency = "1")
    @RetryableTopic(attempts = "5", backoff = @Backoff(value = 900L, multiplier = 2), include = RuntimeException.class)
    public void consume(@Payload String message, Acknowledgment ack) {
        log.info("message {}", message);
        if ("fail".equals(message)) {
            throw new RuntimeException("Failed");
        }
        ack.acknowledge();
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

