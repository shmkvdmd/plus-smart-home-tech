package ru.yandex.practicum.collector.application;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.kafka.TelemetryEventPublisher;
import ru.yandex.practicum.collector.model.CollectHubEventRequest;
import ru.yandex.practicum.collector.model.CollectSensorEventRequest;

@Service
@RequiredArgsConstructor
public class CollectorService {

    private final TelemetryEventPublisher telemetryEventPublisher;

    public void collectSensorEvent(CollectSensorEventRequest event) {
        telemetryEventPublisher.publishSensorEvent(event);
    }

    public void collectHubEvent(CollectHubEventRequest event) {
        telemetryEventPublisher.publishHubEvent(event);
    }
}