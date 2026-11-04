package ru.yandex.practicum.collector.kafka;

import ru.yandex.practicum.collector.model.event.HubEvent;
import ru.yandex.practicum.collector.model.event.SensorEvent;

public interface TelemetryEventPublisher {
    void publishSensorEvent(SensorEvent event);

    void publishHubEvent(HubEvent event);
}
