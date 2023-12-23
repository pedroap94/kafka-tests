package br.com.pedro.kafkapoc.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.Map;

@EnableKafka
@Configuration
public class KafkaProducerConfig<K, V> {

    @Value("${spring.kafka.bootstrap-servers}")
    private String producerServer;

    @Value("${spring.kafka.producer.transaction-id}")
    private String transactionId;

    @Bean
    public ProducerFactory<K, V> factoryProducer() {

        return new DefaultKafkaProducerFactory(
                Map.of(
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerServer,
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId,
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
                )
        );
    }

    @Bean
    public KafkaTemplate<K, V> kafkaTemplate() {
        return new KafkaTemplate<>(factoryProducer());
    }

    @Bean
    public KafkaTransactionManager<K,V> kafkaTransactionManager() {
        return new KafkaTransactionManager<>(factoryProducer());
    }
}
