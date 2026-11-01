package ru.yandex.practicum.collector.model.event;

import java.time.Instant;

public record SwitchSensorEvent(String id, String hubId, Instant timestamp, boolean state) implements SensorEvent {
}
