package ru.yandex.practicum.collector.model.event;

import java.time.Instant;

public record ScenarioRemovedEvent(String hubId, Instant timestamp, String name) implements HubEvent {
}
