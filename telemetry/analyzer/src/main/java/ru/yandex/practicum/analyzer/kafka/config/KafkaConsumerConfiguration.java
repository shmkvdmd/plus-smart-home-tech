package ru.yandex.practicum.analyzer.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.HubEventDeserializer;
import ru.yandex.practicum.kafka.telemetry.SensorsSnapshotDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfiguration {

    @Bean(destroyMethod = "")
    public KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer(AnalyzerKafkaProperties properties) {
        Map<String, Object> consumerProperties = createConsumerProperties(
                properties,
                properties.consumers().snapshots()
        );

        return new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new SensorsSnapshotDeserializer());
    }

    @Bean(destroyMethod = "")
    public KafkaConsumer<String, HubEventAvro> hubEventConsumer(AnalyzerKafkaProperties properties) {
        Map<String, Object> consumerProperties = createConsumerProperties(
                properties,
                properties.consumers().hubEvents()
        );

        return new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new HubEventDeserializer());
    }

    private Map<String, Object> createConsumerProperties(
            AnalyzerKafkaProperties properties,
            AnalyzerKafkaProperties.Consumer consumer
    ) {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumer.groupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumer.autoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, consumer.enableAutoCommit());

        return props;
    }
}