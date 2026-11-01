package ru.yandex.practicum.collector.model.event;

import java.time.Instant;

public record DeviceAddedEvent(String hubId, Instant timestamp, String id, String deviceType) implements HubEvent {
}
