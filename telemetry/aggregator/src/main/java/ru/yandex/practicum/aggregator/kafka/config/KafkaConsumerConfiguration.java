package ru.yandex.practicum.aggregator.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import ru.yandex.practicum.kafka.telemetry.SensorEventDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Configuration(proxyBeanMethods = false)
public class KafkaConsumerConfiguration {

    @Bean(destroyMethod = "")
    public Consumer<String, SensorEventAvro> aggregatorConsumer(
            AggregatorKafkaProperties kafkaProperties
    ) {
        Map<String, Object> consumerProperties = new HashMap<>();

        consumerProperties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaProperties.bootstrapServers()
        );
        consumerProperties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                kafkaProperties.consumer().groupId()
        );
        consumerProperties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                kafkaProperties.consumer().autoOffsetReset()
        );
        consumerProperties.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                kafkaProperties.consumer().enableAutoCommit()
        );
        consumerProperties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );
        consumerProperties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                SensorEventDeserializer.class
        );

        return new KafkaConsumer<>(consumerProperties);
    }
}