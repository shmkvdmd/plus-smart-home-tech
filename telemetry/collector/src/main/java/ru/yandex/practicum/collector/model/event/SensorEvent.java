package ru.yandex.practicum.collector.model.event;

import java.time.Instant;

public interface SensorEvent {
    String id();

    String hubId();

    Instant timestamp();
}
