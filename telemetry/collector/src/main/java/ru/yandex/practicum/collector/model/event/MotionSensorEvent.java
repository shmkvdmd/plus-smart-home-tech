package ru.yandex.practicum.collector.model.event;

import java.time.Instant;

public record MotionSensorEvent(String id, String hubId, Instant timestamp, int linkQuality, boolean motion,
                                int voltage) implements SensorEvent {
}
