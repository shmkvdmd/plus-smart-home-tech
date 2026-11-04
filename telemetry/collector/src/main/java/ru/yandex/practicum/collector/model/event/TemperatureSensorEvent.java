package ru.yandex.practicum.collector.model.event;

import java.time.Instant;

public record TemperatureSensorEvent(String id, String hubId, Instant timestamp, int temperatureC,
                                     int temperatureF) implements SensorEvent {
}
