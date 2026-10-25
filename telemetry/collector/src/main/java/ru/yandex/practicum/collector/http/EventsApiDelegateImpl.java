package ru.yandex.practicum.collector.http;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.api.EventsApiDelegate;
import ru.yandex.practicum.collector.application.CollectorService;
import ru.yandex.practicum.collector.model.CollectHubEventRequest;
import ru.yandex.practicum.collector.model.CollectSensorEventRequest;

@Service
@RequiredArgsConstructor
public class EventsApiDelegateImpl implements EventsApiDelegate {
    private final CollectorService collectorService;

    @Override
    public ResponseEntity<Void> collectHubEvent(CollectHubEventRequest collectHubEventRequest) {
        collectorService.collectHubEvent(collectHubEventRequest);
        return ResponseEntity.ok().build();
    }

    @Override
    public ResponseEntity<Void> collectSensorEvent(CollectSensorEventRequest collectSensorEventRequest) {
        collectorService.collectSensorEvent(collectSensorEventRequest);
        return ResponseEntity.ok().build();
    }
}
