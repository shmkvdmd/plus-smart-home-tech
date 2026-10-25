package ru.yandex.practicum.collector.kafka;

import ru.yandex.practicum.collector.model.CollectHubEventRequest;
import ru.yandex.practicum.collector.model.CollectSensorEventRequest;

public interface TelemetryEventPublisher {
    void publishSensorEvent(CollectSensorEventRequest event);

    void publishHubEvent(CollectHubEventRequest event);
}
