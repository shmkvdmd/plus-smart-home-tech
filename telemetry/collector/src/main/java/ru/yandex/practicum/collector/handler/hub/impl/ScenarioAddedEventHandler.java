package ru.yandex.practicum.collector.handler.hub.impl;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.application.CollectorService;
import ru.yandex.practicum.collector.mapper.ProtoEventTimeMapper;
import ru.yandex.practicum.collector.handler.hub.HubEventHandler;
import ru.yandex.practicum.collector.model.event.DeviceAction;
import ru.yandex.practicum.collector.model.event.ScenarioAddedEvent;
import ru.yandex.practicum.collector.model.event.ScenarioCondition;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;

@Component
public class ScenarioAddedEventHandler implements HubEventHandler {
    private final CollectorService collectorService;

    public ScenarioAddedEventHandler(CollectorService collectorService) {
        this.collectorService = collectorService;
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    public void handle(HubEventProto event) {
        var payload = event.getScenarioAdded();
        collectorService.collectHubEvent(new ScenarioAddedEvent(
                event.getHubId(), ProtoEventTimeMapper.toInstant(event.getTimestamp()),
                payload.getName(),
                payload.getConditionList().stream().map(this::toCondition).toList(),
                payload.getActionList().stream().map(action -> new DeviceAction(
                        action.getSensorId(), action.getType().name(),
                        action.hasValue() ? action.getValue() : null)).toList()));
    }

    private ScenarioCondition toCondition(ScenarioConditionProto condition) {
        Object value = switch (condition.getValueCase()) {
            case BOOL_VALUE -> condition.getBoolValue();
            case INT_VALUE -> condition.getIntValue();
            case VALUE_NOT_SET -> null;
        };
        return new ScenarioCondition(condition.getSensorId(), condition.getType().name(),
                condition.getOperation().name(), value);
    }
}
