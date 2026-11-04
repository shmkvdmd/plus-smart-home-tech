package ru.yandex.practicum.analyzer.processor;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.kafka.config.AnalyzerKafkaProperties;
import ru.yandex.practicum.analyzer.service.ScenarioService;
import ru.yandex.practicum.analyzer.service.SensorService;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
public class HubEventProcessor implements Runnable {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private final KafkaConsumer<String, HubEventAvro> consumer;
    private final String topic;
    private final SensorService sensorService;
    private final ScenarioService scenarioService;

    private final AtomicBoolean running = new AtomicBoolean(true);

    public HubEventProcessor(
            @Qualifier("hubEventConsumer")
            KafkaConsumer<String, HubEventAvro> consumer,
            AnalyzerKafkaProperties properties,
            SensorService sensorService,
            ScenarioService scenarioService
    ) {
        this.consumer = consumer;
        this.topic = properties.topics().hubs();
        this.sensorService = sensorService;
        this.scenarioService = scenarioService;
    }

    @Override
    public void run() {
        log.info("HubEventProcessor started, topic: {}", topic);

        try {
            consumer.subscribe(List.of(topic));

            while (running.get()) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(POLL_TIMEOUT);

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    processRecord(record);
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }

        } catch (WakeupException e) {
            if (running.get()) {
                log.error("Unexpected consumer wakeup", e);
            } else {
                log.info("HubEventProcessor stopped");
            }
        } catch (Exception e) {
            log.error("HubEventProcessor failed", e);
        } finally {
            running.set(false);
            consumer.close();
            log.info("Hub event consumer closed");
        }
    }

    private void processRecord(ConsumerRecord<String, HubEventAvro> record) {
        HubEventAvro event = record.value();
        String hubId = event.getHubId();

        Object payload = event.getPayload();

        try {
            switch (payload) {
                case DeviceAddedEventAvro deviceAdded -> sensorService.registerSensor(deviceAdded.getId(), hubId);
                case DeviceRemovedEventAvro deviceRemoved ->
                        sensorService.deleteSensorByIdAndHubId(deviceRemoved.getId(), hubId);

                case ScenarioAddedEventAvro scenarioAdded -> scenarioService.saveOrUpdateScenario(hubId, scenarioAdded);

                case ScenarioRemovedEventAvro scenarioRemoved -> scenarioService.deleteScenarioByHubIdAndName(
                        hubId,
                        scenarioRemoved.getName()
                );
                case null, default -> log.warn("Unknown hub event payload: {}", payload);
            }

        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to process hub event. Hub: %s, offset: %d".formatted(hubId, record.offset()), e
            );
        }
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        consumer.wakeup();
    }
}