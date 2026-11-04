package ru.yandex.practicum.collector.handler.hub.impl;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.application.CollectorService;
import ru.yandex.practicum.collector.mapper.ProtoEventTimeMapper;
import ru.yandex.practicum.collector.handler.hub.HubEventHandler;
import ru.yandex.practicum.collector.model.event.DeviceRemovedEvent;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

@Component
public class DeviceRemovedEventHandler implements HubEventHandler {
    private final CollectorService collectorService;

    public DeviceRemovedEventHandler(CollectorService collectorService) {
        this.collectorService = collectorService;
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_REMOVED;
    }

    @Override
    public void handle(HubEventProto event) {
        collectorService.collectHubEvent(new DeviceRemovedEvent(
                event.getHubId(), ProtoEventTimeMapper.toInstant(event.getTimestamp()),
                event.getDeviceRemoved().getId()));
    }
}
