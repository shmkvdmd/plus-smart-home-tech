package ru.yandex.practicum.collector.kafka.mapper;

import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.model.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;

@Component
public class HubEventAvroMapper {

    public HubEventAvro toAvro(CollectHubEventRequest event) {
        Objects.requireNonNull(event, "Hub event must not be null");

        if (event instanceof DeviceAddedEvent added) {
            DeviceAddedEventAvro payload =
                    DeviceAddedEventAvro.newBuilder()
                            .setId(requiredText(added.getId(), "id"))
                            .setType(DeviceTypeAvro.valueOf(
                                    requiredEnum(
                                            added.getDeviceType() == null
                                                    ? null
                                                    : added.getDeviceType().getValue(),
                                            "deviceType"
                                    )
                            ))
                            .build();

            return createHubEvent(
                    added.getHubId(), added.getTimestamp(), payload
            );
        }

        if (event instanceof DeviceRemovedEvent removed) {
            DeviceRemovedEventAvro payload =
                    DeviceRemovedEventAvro.newBuilder()
                            .setId(requiredText(removed.getId(), "id"))
                            .build();

            return createHubEvent(
                    removed.getHubId(), removed.getTimestamp(), payload
            );
        }

        if (event instanceof ScenarioAddedEvent added) {
            List<ScenarioConditionAvro> conditions =
                    Objects.requireNonNull(
                                    added.getConditions(), "conditions must not be null"
                            )
                            .stream()
                            .map(this::toAvro)
                            .toList();

            List<DeviceActionAvro> actions =
                    Objects.requireNonNull(
                                    added.getActions(), "actions must not be null"
                            )
                            .stream()
                            .map(this::toAvro)
                            .toList();

            ScenarioAddedEventAvro payload =
                    ScenarioAddedEventAvro.newBuilder()
                            .setName(requiredText(added.getName(), "name"))
                            .setConditions(conditions)
                            .setActions(actions)
                            .build();

            return createHubEvent(
                    added.getHubId(), added.getTimestamp(), payload
            );
        }

        if (event instanceof ScenarioRemovedEvent removed) {
            ScenarioRemovedEventAvro payload =
                    ScenarioRemovedEventAvro.newBuilder()
                            .setName(requiredText(removed.getName(), "name"))
                            .build();

            return createHubEvent(
                    removed.getHubId(), removed.getTimestamp(), payload
            );
        }

        throw new IllegalArgumentException(
                "Unsupported hub event class: " + event.getClass().getName()
        );
    }

    private ScenarioConditionAvro toAvro(ScenarioCondition condition) {
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(requiredText(condition.getSensorId(), "sensorId"))
                .setType(ConditionTypeAvro.valueOf(
                        requiredEnum(
                                condition.getType() == null
                                        ? null
                                        : condition.getType().getValue(),
                                "condition.type"
                        )
                ))
                .setOperation(ConditionOperationAvro.valueOf(
                        requiredEnum(
                                condition.getOperation() == null
                                        ? null
                                        : condition.getOperation().getValue(),
                                "condition.operation"
                        )
                ))
                .setValue(condition.getValue())
                .build();
    }

    private DeviceActionAvro toAvro(DeviceAction action) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(requiredText(action.getSensorId(), "sensorId"))
                .setType(ActionTypeAvro.valueOf(
                        requiredEnum(
                                action.getType() == null
                                        ? null
                                        : action.getType().getValue(),
                                "action.type"
                        )
                ))
                .setValue(action.getValue())
                .build();
    }

    private HubEventAvro createHubEvent(
            String hubId,
            OffsetDateTime timestamp,
            SpecificRecord payload
    ) {
        return HubEventAvro.newBuilder()
                .setHubId(requiredText(hubId, "hubId"))
                .setTimestamp(toInstant(timestamp))
                .setPayload(payload)
                .build();
    }

    private String requiredText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "Required field is missing: " + fieldName
            );
        }
        return value;
    }

    private String requiredEnum(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "Required enum is missing: " + fieldName
            );
        }
        return value;
    }

    private Instant toInstant(OffsetDateTime timestamp) {
        return timestamp == null
                ? Instant.now()
                : timestamp.toInstant();
    }
}