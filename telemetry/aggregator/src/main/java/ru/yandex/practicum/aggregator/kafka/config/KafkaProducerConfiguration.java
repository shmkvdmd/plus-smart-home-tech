package ru.yandex.practicum.aggregator.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.AvroSpecificRecordSerializer;

@Configuration(proxyBeanMethods = false)
public class KafkaProducerConfiguration {

    @Bean(destroyMethod = "")
    public Producer<String, SpecificRecord> aggregatorProducer(
            AggregatorKafkaProperties kafkaProperties
    ) {
        Map<String, Object> producerProperties = new HashMap<>();

        producerProperties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaProperties.bootstrapServers()
        );
        producerProperties.put(
                ProducerConfig.CLIENT_ID_CONFIG,
                kafkaProperties.producer().clientId()
        );
        producerProperties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );
        producerProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                AvroSpecificRecordSerializer.class
        );

        return new KafkaProducer<>(producerProperties);
    }
}