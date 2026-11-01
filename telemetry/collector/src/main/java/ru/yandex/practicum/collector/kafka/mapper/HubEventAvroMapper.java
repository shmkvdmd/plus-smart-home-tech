package ru.yandex.practicum.collector.kafka.mapper;

import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.model.event.DeviceAction;
import ru.yandex.practicum.collector.model.event.DeviceAddedEvent;
import ru.yandex.practicum.collector.model.event.DeviceRemovedEvent;
import ru.yandex.practicum.collector.model.event.HubEvent;
import ru.yandex.practicum.collector.model.event.ScenarioAddedEvent;
import ru.yandex.practicum.collector.model.event.ScenarioCondition;
import ru.yandex.practicum.collector.model.event.ScenarioRemovedEvent;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

@Component
public class HubEventAvroMapper {
    public HubEventAvro toAvro(HubEvent event) {
        SpecificRecord payload = switch (event) {
            case DeviceAddedEvent deviceAdded -> DeviceAddedEventAvro.newBuilder()
                    .setId(deviceAdded.id())
                    .setType(DeviceTypeAvro.valueOf(deviceAdded.deviceType()))
                    .build();
            case DeviceRemovedEvent deviceRemoved -> DeviceRemovedEventAvro.newBuilder()
                    .setId(deviceRemoved.id())
                    .build();
            case ScenarioAddedEvent scenarioAdded -> ScenarioAddedEventAvro.newBuilder()
                    .setName(scenarioAdded.name())
                    .setConditions(scenarioAdded.conditions().stream().map(this::toAvro).toList())
                    .setActions(scenarioAdded.actions().stream().map(this::toAvro).toList())
                    .build();
            case ScenarioRemovedEvent scenarioRemoved -> ScenarioRemovedEventAvro.newBuilder()
                    .setName(scenarioRemoved.name())
                    .build();
            default -> throw new IllegalArgumentException(
                    "Unsupported hub event: " + event.getClass().getName());
        };

        return HubEventAvro.newBuilder()
                .setHubId(event.hubId())
                .setTimestamp(event.timestamp())
                .setPayload(payload)
                .build();
    }

    private ScenarioConditionAvro toAvro(ScenarioCondition condition) {
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.sensorId())
                .setType(ConditionTypeAvro.valueOf(condition.type()))
                .setOperation(ConditionOperationAvro.valueOf(condition.operation()))
                .setValue(condition.value())
                .build();
    }

    private DeviceActionAvro toAvro(DeviceAction action) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(action.sensorId())
                .setType(ActionTypeAvro.valueOf(action.type()))
                .setValue(action.value())
                .build();
    }
}
