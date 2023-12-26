package br.com.pedro.kafkapoc.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.URISyntaxException;

@Service
@AllArgsConstructor
@Slf4j
public class RestService {
    private RestTemplate restTemplate;

    public String sendMessage(String message) {
        try {
            ResponseEntity<String> stringResponseEntity = restTemplate.postForEntity(new URI("http://localhost:5000"), message, String.class);
            return stringResponseEntity.getBody();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void sleep(String mensagem){
        try {
            log.info("Mensagem que está sendo enviada: {}", mensagem);
            Thread.sleep(8000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
