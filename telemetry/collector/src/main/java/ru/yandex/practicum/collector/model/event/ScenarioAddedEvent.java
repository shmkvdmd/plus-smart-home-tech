package ru.yandex.practicum.collector.model.event;

import java.time.Instant;
import java.util.List;

public record ScenarioAddedEvent(String hubId, Instant timestamp, String name, List<ScenarioCondition> conditions,
                                 List<DeviceAction> actions) implements HubEvent {
}
