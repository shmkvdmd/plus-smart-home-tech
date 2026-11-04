package ru.yandex.practicum.analyzer.processor;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.kafka.config.AnalyzerKafkaProperties;
import ru.yandex.practicum.analyzer.model.*;
import ru.yandex.practicum.analyzer.service.ScenarioService;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
public class SnapshotProcessor {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private final KafkaConsumer<String, SensorsSnapshotAvro> consumer;
    private final String topic;
    private final ScenarioService scenarioService;
    private final HubRouterProcessor hubRouterProcessor;

    private final AtomicBoolean running = new AtomicBoolean(true);

    public SnapshotProcessor(
            @Qualifier("snapshotConsumer")
            KafkaConsumer<String, SensorsSnapshotAvro> consumer,
            AnalyzerKafkaProperties properties,
            ScenarioService scenarioService,
            HubRouterProcessor hubRouterProcessor
    ) {
        this.consumer = consumer;
        this.topic = properties.topics().snapshots();
        this.scenarioService = scenarioService;
        this.hubRouterProcessor = hubRouterProcessor;
    }

    public void start() {
        log.info("SnapshotProcessor started, topic: {}", topic);

        try {
            consumer.subscribe(List.of(topic));

            while (running.get()) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(POLL_TIMEOUT);

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    handleSnapshot(record);
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }

        } catch (WakeupException e) {
            if (running.get()) {
                log.error("Unexpected snapshot consumer wakeup", e);
            } else {
                log.info("SnapshotProcessor stopped");
            }
        } catch (Exception e) {
            log.error("SnapshotProcessor failed", e);
        } finally {
            running.set(false);
            consumer.close();
            log.info("Snapshot consumer closed");
        }
    }

    private void handleSnapshot(ConsumerRecord<String, SensorsSnapshotAvro> record) {
        SensorsSnapshotAvro snapshot = record.value();
        String hubId = snapshot.getHubId();

        List<Scenario> scenarios = scenarioService.findAllByHubId(hubId);

        scenarios.stream()
                .filter(scenario -> areAllConditionsSatisfied(snapshot, scenario))
                .forEach(this::executeScenario);
    }

    private boolean areAllConditionsSatisfied(SensorsSnapshotAvro snapshot, Scenario scenario) {
        if (scenario.getConditions().isEmpty()) {
            return false;
        }

        return scenario.getConditions().stream()
                .allMatch(binding -> {
                    String sensorId = binding.getSensor().getId();

                    SensorStateAvro sensorState = snapshot.getSensorsState().get(sensorId);

                    if (sensorState == null) {
                        return false;
                    }

                    Object sensorData = sensorState.getData();

                    if (!(sensorData instanceof SpecificRecordBase avroData)) {
                        return false;
                    }

                    return isConditionSatisfied(avroData, binding.getCondition());
                });
    }

    private boolean isConditionSatisfied(SpecificRecordBase sensorData, Condition condition) {
        Integer sensorValue = extractSensorValue(sensorData, condition.getType());

        if (sensorValue == null || condition.getValue() == null) {
            return false;
        }

        return switch (condition.getOperation()) {
            case EQUALS -> sensorValue.equals(condition.getValue());
            case GREATER_THAN -> sensorValue > condition.getValue();
            case LOWER_THAN -> sensorValue < condition.getValue();
        };
    }

    private Integer extractSensorValue(SpecificRecordBase sensorData, ConditionType type) {
        return switch (type) {
            case TEMPERATURE -> {
                if (sensorData instanceof ClimateSensorAvro climate) {
                    yield climate.getTemperatureC();
                }
                if (sensorData instanceof TemperatureSensorAvro temperature) {
                    yield temperature.getTemperatureC();
                }
                yield null;
            }

            case HUMIDITY -> {
                if (sensorData instanceof ClimateSensorAvro climate) {
                    yield climate.getHumidity();
                }
                yield null;
            }

            case CO2LEVEL -> {
                if (sensorData instanceof ClimateSensorAvro climate) {
                    yield climate.getCo2Level();
                }
                yield null;
            }

            case LUMINOSITY -> {
                if (sensorData instanceof LightSensorAvro light) {
                    yield light.getLuminosity();
                }
                yield null;
            }

            case MOTION -> {
                if (sensorData instanceof MotionSensorAvro motion) {
                    yield motion.getMotion() ? 1 : 0;
                }
                yield null;
            }

            case SWITCH -> {
                if (sensorData instanceof SwitchSensorAvro switchSensor) {
                    yield switchSensor.getState() ? 1 : 0;
                }
                yield null;
            }
        };
    }

    private void executeScenario(Scenario scenario) {
        for (ScenarioAction binding : scenario.getActions()) {
            hubRouterProcessor.handleAction(scenario, binding);
        }
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        consumer.wakeup();
    }
}