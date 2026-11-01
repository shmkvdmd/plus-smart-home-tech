package ru.yandex.practicum.collector.controller.http;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.api.EventsApiDelegate;
import ru.yandex.practicum.collector.application.CollectorService;
import ru.yandex.practicum.collector.model.CollectHubEventRequest;
import ru.yandex.practicum.collector.model.CollectSensorEventRequest;
import ru.yandex.practicum.collector.mapper.HttpEventMapper;

@Service
public class EventsApiDelegateImpl implements EventsApiDelegate {
    private final CollectorService collectorService;
    private final HttpEventMapper mapper;

    public EventsApiDelegateImpl(CollectorService collectorService, HttpEventMapper mapper) {
        this.collectorService = collectorService;
        this.mapper = mapper;
    }

    @Override
    public ResponseEntity<Void> collectHubEvent(CollectHubEventRequest collectHubEventRequest) {
        collectorService.collectHubEvent(mapper.toHubEvent(collectHubEventRequest));
        return ResponseEntity.ok().build();
    }

    @Override
    public ResponseEntity<Void> collectSensorEvent(CollectSensorEventRequest collectSensorEventRequest) {
        collectorService.collectSensorEvent(mapper.toSensorEvent(collectSensorEventRequest));
        return ResponseEntity.ok().build();
    }
}
