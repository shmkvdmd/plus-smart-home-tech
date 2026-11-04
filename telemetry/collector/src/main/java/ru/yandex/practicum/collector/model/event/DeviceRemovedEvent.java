package ru.yandex.practicum.collector.model.event;

import java.time.Instant;

public record DeviceRemovedEvent(String hubId, Instant timestamp, String id) implements HubEvent {
}
