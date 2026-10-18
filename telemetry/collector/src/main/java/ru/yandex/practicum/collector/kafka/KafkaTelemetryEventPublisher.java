package ru.yandex.practicum.collector.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.kafka.config.CollectorKafkaProperties;
import ru.yandex.practicum.collector.kafka.mapper.HubEventAvroMapper;
import ru.yandex.practicum.collector.kafka.mapper.SensorEventAvroMapper;
import ru.yandex.practicum.collector.model.CollectHubEventRequest;
import ru.yandex.practicum.collector.model.CollectSensorEventRequest;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaTelemetryEventPublisher implements TelemetryEventPublisher {
    private final Producer<String, SpecificRecord> producer;
    private final CollectorKafkaProperties kafkaProperties;
    private final SensorEventAvroMapper sensorEventAvroMapper;
    private final HubEventAvroMapper hubEventAvroMapper;

    @Override
    public void publishSensorEvent(CollectSensorEventRequest event) {
        SensorEventAvro avroEvent = sensorEventAvroMapper.toAvro(event);

        send(
                kafkaProperties.topics().sensors(),
                avroEvent.getId(),
                avroEvent
        );
    }

    @Override
    public void publishHubEvent(CollectHubEventRequest event) {
        HubEventAvro avroEvent = hubEventAvroMapper.toAvro(event);

        send(
                kafkaProperties.topics().hubs(),
                avroEvent.getHubId(),
                avroEvent
        );
    }

    private void send(String topic, String key, SpecificRecord event) {
        ProducerRecord<String, SpecificRecord> record =
                new ProducerRecord<>(topic, key, event);

        try {
            producer.send(record, (metadata, exception) ->
                    handleSendResult(event, topic, key, metadata, exception)
            );
        } catch (RuntimeException exception) {
            log.error(
                    "Failed to submit event to Kafka: topic={}, key={}",
                    topic,
                    key,
                    exception
            );
            throw exception;
        }
    }

    private void handleSendResult(
            SpecificRecord event,
            String topic,
            String key,
            RecordMetadata metadata,
            Exception exception
    ) {
        if (exception != null) {
            log.error(
                    "Failed to send event to Kafka: topic={}, key={}, event={}",
                    topic,
                    key,
                    event,
                    exception
            );
            return;
        }

        log.debug(
                "Event sent to Kafka: topic={}, key={}, partition={}, offset={}",
                topic,
                key,
                metadata.partition(),
                metadata.offset()
        );
    }
}
