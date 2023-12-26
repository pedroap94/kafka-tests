package br.com.pedro.kafkapoc.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@AllArgsConstructor
public class KafkaConsumer {
    private RestService restService;

    @KafkaListener(topics = "${spring.kafka.consumer.topic}", concurrency = "10")
//    @RetryableTopic(attempts = "5", backoff = @Backoff(value = 900L, multiplier = 2), include = RuntimeException.class)
    public void consume(@Payload String message) {
        log.info("mensagem a ser enviada: {}", message);
        if ("fail".equals(message)) {
            throw new RuntimeException("Falha ao enviar dados");
        }
        restService.sleep(message);
        log.info("finalizando: {}", message);
    }
//        String response = restService.sendMessage(message);
//        log.info("resposta: {}", response);
//        restService.sleep();
//        log.info("finalizando mensagem: {}", message);

}

