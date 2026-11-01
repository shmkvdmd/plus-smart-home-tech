package ru.yandex.practicum.aggregator.service;

import jakarta.annotation.PreDestroy;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.aggregator.kafka.config.AggregatorKafkaProperties;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Component
public class AggregationStarter {
    private static final Logger log = LoggerFactory.getLogger(AggregationStarter.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);

    private final Consumer<String, SensorEventAvro> consumer;
    private final Producer<String, SpecificRecord> producer;
    private final AggregatorKafkaProperties kafkaProperties;

    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    private volatile boolean running = true;

    public AggregationStarter(Consumer<String, SensorEventAvro> consumer,
                              Producer<String, SpecificRecord> producer,
                              AggregatorKafkaProperties kafkaProperties) {
        this.consumer = consumer;
        this.producer = producer;
        this.kafkaProperties = kafkaProperties;
    }

    public void start() {
        boolean processingFailed = false;

        try {
            consumer.subscribe(Set.of(kafkaProperties.topics().sensors()));

            while (running) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(POLL_TIMEOUT);

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    processSensorEvent(record.value());
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }

        } catch (WakeupException exception) {
            if (running) {
                processingFailed = true;
                throw exception;
            }
        } catch (RuntimeException exception) {
            processingFailed = true;
            log.error("Error while processing sensor events", exception);
            throw exception;
        } finally {
            running = false;
            shutdown(processingFailed);
        }
    }

    private void processSensorEvent(SensorEventAvro event) {
        if (event == null) {
            log.warn("Received empty sensor event");
            return;
        }

        updateState(event).ifPresent(this::publishSnapshot);
    }

    private Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        String hubId = event.getHubId();
        String sensorId = event.getId();

        SensorsSnapshotAvro currentSnapshot = snapshots.get(hubId);

        Map<String, SensorStateAvro> sensorsState = new HashMap<>();

        if (currentSnapshot != null) {
            sensorsState.putAll(currentSnapshot.getSensorsState());

            SensorStateAvro oldState = sensorsState.get(sensorId);

            if (oldState != null) {
                boolean eventIsOlder = oldState.getTimestamp().isAfter(event.getTimestamp());

                boolean stateIsUnchanged = Objects.equals(oldState.getData(), event.getPayload());

                if (eventIsOlder || stateIsUnchanged) {
                    return Optional.empty();
                }
            }
        }

        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        sensorsState.put(sensorId, newState);

        SensorsSnapshotAvro updatedSnapshot = SensorsSnapshotAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setSensorsState(sensorsState)
                .build();

        return Optional.of(updatedSnapshot);
    }

    private void publishSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        String topic = kafkaProperties.topics().snapshots();

        ProducerRecord<String, SpecificRecord> record = new ProducerRecord<>(topic, hubId, snapshot);

        try {
            producer.send(record).get();
            snapshots.put(hubId, snapshot);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending snapshot to Kafka", exception);
        } catch (ExecutionException exception) {
            throw new IllegalStateException("Failed to send snapshot to Kafka: hubId=" + hubId, exception.getCause());
        }
    }

    @PreDestroy
    public void stop() {
        if (running) {
            running = false;
            consumer.wakeup();
        }
    }

    private void shutdown(boolean processingFailed) {
        log.info("Flushing Kafka producer");

        try {
            producer.flush();

            if (!processingFailed) consumer.commitSync();

        } catch (RuntimeException exception) {
            log.error("Error while flushing producer or committing offsets", exception);
        } finally {
            try {
                log.info("Closing Kafka consumer");
                consumer.close();
            } catch (RuntimeException exception) {
                log.error("Failed to close Kafka consumer", exception);
            } finally {
                log.info("Closing Kafka producer");
                producer.close();
            }
        }

        log.info("Aggregator stopped");
    }
}