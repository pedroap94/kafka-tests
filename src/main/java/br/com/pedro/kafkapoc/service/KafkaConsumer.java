package br.com.pedro.kafkapoc.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = "${spring.kafka.consumer.topic}")
    public void consume(@Payload List<String> messages, Acknowledgment ack) {
        log.info(messages.toString());
        messages.parallelStream().forEach(message -> {
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
        );

    }
}
