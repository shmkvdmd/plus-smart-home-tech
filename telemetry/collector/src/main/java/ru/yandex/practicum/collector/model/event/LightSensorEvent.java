package ru.yandex.practicum.collector.model.event;

import java.time.Instant;

public record LightSensorEvent(String id, String hubId, Instant timestamp, int linkQuality,
                               int luminosity) implements SensorEvent {
}
