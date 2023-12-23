package br.com.pedro.kafkapoc.controller;

import br.com.pedro.kafkapoc.service.KafkaProducer;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka/v1")
@AllArgsConstructor
public class KafkaController {
    private KafkaProducer kafkaProducer;

    @PostMapping("/{message}")
    public ResponseEntity<Void> addInTopic(@PathVariable String message) {
        kafkaProducer.addKafkaTopic(message);
        return new ResponseEntity<>(HttpStatus.OK);
    }
}
