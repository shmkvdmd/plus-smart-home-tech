package ru.yandex.practicum.collector.handler.hub.impl;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.application.CollectorService;
import ru.yandex.practicum.collector.mapper.ProtoEventTimeMapper;
import ru.yandex.practicum.collector.handler.hub.HubEventHandler;
import ru.yandex.practicum.collector.model.event.ScenarioRemovedEvent;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

@Component
public class ScenarioRemovedEventHandler implements HubEventHandler {
    private final CollectorService collectorService;

    public ScenarioRemovedEventHandler(CollectorService collectorService) {
        this.collectorService = collectorService;
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_REMOVED;
    }

    @Override
    public void handle(HubEventProto event) {
        collectorService.collectHubEvent(new ScenarioRemovedEvent(
                event.getHubId(), ProtoEventTimeMapper.toInstant(event.getTimestamp()),
                event.getScenarioRemoved().getName()));
    }
}
