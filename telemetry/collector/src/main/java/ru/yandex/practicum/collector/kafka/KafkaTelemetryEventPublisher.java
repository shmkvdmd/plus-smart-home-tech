package ru.yandex.practicum.collector.kafka;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.practicum.collector.kafka.config.CollectorKafkaProperties;
import ru.yandex.practicum.collector.kafka.mapper.HubEventAvroMapper;
import ru.yandex.practicum.collector.kafka.mapper.SensorEventAvroMapper;
import ru.yandex.practicum.collector.model.event.HubEvent;
import ru.yandex.practicum.collector.model.event.SensorEvent;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Component
public class KafkaTelemetryEventPublisher implements TelemetryEventPublisher {
    private static final Logger log = LoggerFactory.getLogger(KafkaTelemetryEventPublisher.class);
    private final Producer<String, SpecificRecord> producer;
    private final CollectorKafkaProperties kafkaProperties;
    private final SensorEventAvroMapper sensorEventAvroMapper;
    private final HubEventAvroMapper hubEventAvroMapper;

    public KafkaTelemetryEventPublisher(
            Producer<String, SpecificRecord> producer,
            CollectorKafkaProperties kafkaProperties,
            SensorEventAvroMapper sensorEventAvroMapper,
            HubEventAvroMapper hubEventAvroMapper
    ) {
        this.producer = producer;
        this.kafkaProperties = kafkaProperties;
        this.sensorEventAvroMapper = sensorEventAvroMapper;
        this.hubEventAvroMapper = hubEventAvroMapper;
    }

    @Override
    public void publishSensorEvent(SensorEvent event) {
        SensorEventAvro avroEvent = sensorEventAvroMapper.toAvro(event);

        send(kafkaProperties.topics().sensors(), avroEvent.getId(), avroEvent);
    }

    @Override
    public void publishHubEvent(HubEvent event) {
        HubEventAvro avroEvent = hubEventAvroMapper.toAvro(event);

        send(kafkaProperties.topics().hubs(), avroEvent.getHubId(), avroEvent);
    }

    private void send(String topic, String key, SpecificRecord event) {
        ProducerRecord<String, SpecificRecord> record = new ProducerRecord<>(topic, key, event);

        try {
            producer.send(record, (metadata, exception) ->
                    handleSendResult(event, topic, key, metadata, exception)
            );
        } catch (RuntimeException exception) {
            log.error("Failed to submit event to Kafka: topic={}, key={}", topic, key, exception);
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
            log.error("Failed to send event to Kafka: topic={}, key={}, event={}", topic, key, event, exception);
            return;
        }

        log.debug("Event sent to Kafka: topic={}, key={}, partition={}, offset={}", topic, key,
                metadata.partition(),
                metadata.offset()
        );
    }
}
