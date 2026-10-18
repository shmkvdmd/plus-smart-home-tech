package ru.yandex.practicum.collector.kafka.config;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaProducerConfiguration {
    @Bean
    public ProducerFactory<String, SpecificRecord> collectorProducerFactory(
            KafkaProperties kafkaProperties,
            SslBundles sslBundles
    ) {
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties(sslBundles));
    }

    @Bean
    public Producer<String, SpecificRecord> collectorProducer(
            ProducerFactory<String, SpecificRecord> collectorProducerFactory
    ) {
        return collectorProducerFactory.createProducer();
    }
}
