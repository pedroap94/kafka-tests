package br.com.pedro.kafkapoc.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class KafkaConsumerConfig<K, V> {

    @Autowired
    KafkaTransactionManager<K,V> kafkaTransactionManager;
    @Value("${spring.kafka.bootstrap-servers}")
    private String consumerServer;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public ConsumerFactory<K, V> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerServer,
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(),
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.GROUP_ID_CONFIG, groupId,
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"
                )
        );
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<K, V>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory());

        factory.getContainerProperties().setMissingTopicsFatal(false);

        factory.getContainerProperties().setSyncCommits(Boolean.TRUE);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
//        factory.setBatchListener(Boolean.TRUE);
//        factory.setConcurrency(5);
        factory.setCommonErrorHandler(errorHandler());
        factory.getContainerProperties().setTransactionManager(kafkaTransactionManager);

        return factory;
    }

    @Bean
    public DefaultErrorHandler errorHandler() {
//        ExponentialBackOff backoff = new ExponentialBackOff(1000L, 2);
//        backoff.setMaxAttempts(5);
        return new DefaultErrorHandler((consumerRecord, e) -> log.info("Fail to retry message"));
    }
}
