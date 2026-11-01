package ru.yandex.practicum.collector.model.event;

import java.time.Instant;

public interface HubEvent {
    String hubId();

    Instant timestamp();
}
