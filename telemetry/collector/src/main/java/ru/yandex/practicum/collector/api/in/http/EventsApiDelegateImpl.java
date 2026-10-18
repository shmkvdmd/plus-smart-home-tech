package ru.yandex.practicum.collector.api.in.http;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.api.EventsApiDelegate;
import ru.yandex.practicum.collector.model.CollectHubEventRequest;
import ru.yandex.practicum.collector.model.CollectSensorEventRequest;

@Service
public class EventsApiDelegateImpl implements EventsApiDelegate {

    public EventsApiDelegateImpl() {

    }

    @Override
    public ResponseEntity<Void> collectHubEvent(CollectHubEventRequest collectHubEventRequest) {
        return EventsApiDelegate.super.collectHubEvent(collectHubEventRequest);
    }

    @Override
    public ResponseEntity<Void> collectSensorEvent(CollectSensorEventRequest collectSensorEventRequest) {
        return EventsApiDelegate.super.collectSensorEvent(collectSensorEventRequest);
    }


}
