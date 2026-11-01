package ru.yandex.practicum.collector.application;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.kafka.TelemetryEventPublisher;
import ru.yandex.practicum.collector.model.event.HubEvent;
import ru.yandex.practicum.collector.model.event.SensorEvent;

@Service
public class CollectorService {

    private final TelemetryEventPublisher telemetryEventPublisher;

    public CollectorService(TelemetryEventPublisher telemetryEventPublisher) {
        this.telemetryEventPublisher = telemetryEventPublisher;
    }

    public void collectSensorEvent(SensorEvent event) {
        telemetryEventPublisher.publishSensorEvent(event);
    }

    public void collectHubEvent(HubEvent event) {
        telemetryEventPublisher.publishHubEvent(event);
    }
}
