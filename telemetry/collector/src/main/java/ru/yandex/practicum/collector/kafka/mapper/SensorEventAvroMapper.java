package ru.yandex.practicum.collector.kafka.mapper;

import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.model.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Objects;

@Component
public class SensorEventAvroMapper {

    public SensorEventAvro toAvro(CollectSensorEventRequest event) {
        Objects.requireNonNull(event, "Sensor event must not be null");

        if (event instanceof ClimateSensorEvent sensor) {
            return createSensorEvent(
                    sensor.getId(),
                    sensor.getHubId(),
                    toInstant(sensor.getTimestamp()),
                    ClimateSensorAvro.newBuilder()
                            .setTemperatureC(requiredInt(
                                    sensor.getTemperatureC(), "temperatureC"))
                            .setHumidity(requiredInt(
                                    sensor.getHumidity(), "humidity"))
                            .setCo2Level(requiredInt(
                                    sensor.getCo2Level(), "co2Level"))
                            .build()
            );
        }

        if (event instanceof LightSensorEvent sensor) {
            return createSensorEvent(
                    sensor.getId(),
                    sensor.getHubId(),
                    toInstant(sensor.getTimestamp()),
                    LightSensorAvro.newBuilder()
                            .setLinkQuality(requiredInt(
                                    sensor.getLinkQuality(), "linkQuality"))
                            .setLuminosity(requiredInt(
                                    sensor.getLuminosity(), "luminosity"))
                            .build()
            );
        }

        if (event instanceof MotionSensorEvent sensor) {
            return createSensorEvent(
                    sensor.getId(),
                    sensor.getHubId(),
                    toInstant(sensor.getTimestamp()),
                    MotionSensorAvro.newBuilder()
                            .setLinkQuality(requiredInt(
                                    sensor.getLinkQuality(), "linkQuality"))
                            .setMotion(requiredBoolean(
                                    sensor.getMotion(), "motion"))
                            .setVoltage(requiredInt(
                                    sensor.getVoltage(), "voltage"))
                            .build()
            );
        }

        if (event instanceof SwitchSensorEvent sensor) {
            return createSensorEvent(
                    sensor.getId(),
                    sensor.getHubId(),
                    toInstant(sensor.getTimestamp()),
                    SwitchSensorAvro.newBuilder()
                            .setState(requiredBoolean(
                                    sensor.getState(), "state"))
                            .build()
            );
        }

        if (event instanceof TemperatureSensorEvent sensor) {
            Instant timestamp = toInstant(sensor.getTimestamp());
            TemperatureSensorAvro payload =
                    TemperatureSensorAvro.newBuilder()
                            .setId(requiredText(sensor.getId(), "id"))
                            .setHubId(requiredText(sensor.getHubId(), "hubId"))
                            .setTimestamp(timestamp)
                            .setTemperatureC(requiredInt(
                                    sensor.getTemperatureC(), "temperatureC"))
                            .setTemperatureF(requiredInt(
                                    sensor.getTemperatureF(), "temperatureF"))
                            .build();

            return createSensorEvent(
                    sensor.getId(),
                    sensor.getHubId(),
                    timestamp,
                    payload
            );
        }

        throw new IllegalArgumentException(
                "Unsupported sensor event class: " + event.getClass().getName()
        );
    }

    private SensorEventAvro createSensorEvent(
            String id,
            String hubId,
            Instant timestamp,
            SpecificRecord payload
    ) {
        return SensorEventAvro.newBuilder()
                .setId(requiredText(id, "id"))
                .setHubId(requiredText(hubId, "hubId"))
                .setTimestamp(timestamp)
                .setPayload(payload)
                .build();
    }

    private String requiredText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "Required field is missing: " + fieldName
            );
        }
        return value;
    }

    private int requiredInt(Integer value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(
                    "Required field is missing: " + fieldName
            );
        }
        return value;
    }

    private boolean requiredBoolean(Boolean value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(
                    "Required field is missing: " + fieldName
            );
        }
        return value;
    }

    private Instant toInstant(OffsetDateTime timestamp) {
        return timestamp == null
                ? Instant.now()
                : timestamp.toInstant();
    }
}