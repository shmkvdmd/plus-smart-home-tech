package ru.yandex.practicum.collector.handler.hub.impl;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.application.CollectorService;
import ru.yandex.practicum.collector.mapper.ProtoEventTimeMapper;
import ru.yandex.practicum.collector.handler.hub.HubEventHandler;
import ru.yandex.practicum.collector.model.event.DeviceAddedEvent;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

@Component
public class DeviceAddedEventHandler implements HubEventHandler {
    private final CollectorService collectorService;

    public DeviceAddedEventHandler(CollectorService collectorService) {
        this.collectorService = collectorService;
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }

    @Override
    public void handle(HubEventProto event) {
        var payload = event.getDeviceAdded();
        collectorService.collectHubEvent(new DeviceAddedEvent(
                event.getHubId(), ProtoEventTimeMapper.toInstant(event.getTimestamp()),
                payload.getId(), payload.getType().name()));
    }
}
