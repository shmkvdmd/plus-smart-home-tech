package ru.yandex.practicum.collector.model.event;

import java.time.Instant;

public record ClimateSensorEvent(String id, String hubId, Instant timestamp, int temperatureC, int humidity,
                                 int co2Level) implements SensorEvent {
}
