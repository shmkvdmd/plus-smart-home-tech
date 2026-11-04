package ru.yandex.practicum.collector.mapper;

import java.time.Instant;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.model.CollectHubEventRequest;
import ru.yandex.practicum.collector.model.CollectSensorEventRequest;
import ru.yandex.practicum.collector.model.DeviceAction;
import ru.yandex.practicum.collector.model.DeviceAddedEvent;
import ru.yandex.practicum.collector.model.DeviceRemovedEvent;
import ru.yandex.practicum.collector.model.event.HubEvent;
import ru.yandex.practicum.collector.model.event.SensorEvent;
import ru.yandex.practicum.collector.model.ScenarioAddedEvent;
import ru.yandex.practicum.collector.model.ScenarioCondition;
import ru.yandex.practicum.collector.model.ScenarioRemovedEvent;

@Component
public class HttpEventMapper {
    public SensorEvent toSensorEvent(CollectSensorEventRequest event) {
        Instant timestamp = sensorTimestamp(event);
        return switch (event) {
            case ru.yandex.practicum.collector.model.ClimateSensorEvent climateEvent -> new ru.yandex.practicum.collector.model.event.ClimateSensorEvent(climateEvent.getId(), climateEvent.getHubId(), timestamp, climateEvent.getTemperatureC(), climateEvent.getHumidity(), climateEvent.getCo2Level());
            case ru.yandex.practicum.collector.model.LightSensorEvent lightEvent -> new ru.yandex.practicum.collector.model.event.LightSensorEvent(lightEvent.getId(), lightEvent.getHubId(), timestamp, lightEvent.getLinkQuality(), lightEvent.getLuminosity());
            case ru.yandex.practicum.collector.model.MotionSensorEvent motionEvent -> new ru.yandex.practicum.collector.model.event.MotionSensorEvent(motionEvent.getId(), motionEvent.getHubId(), timestamp, motionEvent.getLinkQuality(), motionEvent.getMotion(), motionEvent.getVoltage());
            case ru.yandex.practicum.collector.model.SwitchSensorEvent switchEvent -> new ru.yandex.practicum.collector.model.event.SwitchSensorEvent(switchEvent.getId(), switchEvent.getHubId(), timestamp, switchEvent.getState());
            case ru.yandex.practicum.collector.model.TemperatureSensorEvent temperatureEvent -> new ru.yandex.practicum.collector.model.event.TemperatureSensorEvent(temperatureEvent.getId(), temperatureEvent.getHubId(), timestamp, temperatureEvent.getTemperatureC(), temperatureEvent.getTemperatureF());
            default -> throw new IllegalArgumentException("Unsupported sensor event: " + event.getClass().getName());
        };
    }

    public HubEvent toHubEvent(CollectHubEventRequest event) {
        Instant timestamp = hubTimestamp(event);
        return switch (event) {
            case DeviceAddedEvent deviceAdded -> new ru.yandex.practicum.collector.model.event.DeviceAddedEvent(deviceAdded.getHubId(), timestamp, deviceAdded.getId(), deviceAdded.getDeviceType().getValue());
            case DeviceRemovedEvent deviceRemoved -> new ru.yandex.practicum.collector.model.event.DeviceRemovedEvent(deviceRemoved.getHubId(), timestamp, deviceRemoved.getId());
            case ScenarioAddedEvent scenarioAdded -> new ru.yandex.practicum.collector.model.event.ScenarioAddedEvent(scenarioAdded.getHubId(), timestamp, scenarioAdded.getName(), scenarioAdded.getConditions().stream().map(this::toCondition).toList(), scenarioAdded.getActions().stream().map(this::toAction).toList());
            case ScenarioRemovedEvent scenarioRemoved -> new ru.yandex.practicum.collector.model.event.ScenarioRemovedEvent(scenarioRemoved.getHubId(), timestamp, scenarioRemoved.getName());
            default -> throw new IllegalArgumentException("Unsupported hub event: " + event.getClass().getName());
        };
    }

    private ru.yandex.practicum.collector.model.event.ScenarioCondition toCondition(ScenarioCondition condition) {
        return new ru.yandex.practicum.collector.model.event.ScenarioCondition(condition.getSensorId(), condition.getType().getValue(), condition.getOperation().getValue(), condition.getValue());
    }

    private ru.yandex.practicum.collector.model.event.DeviceAction toAction(DeviceAction action) {
        return new ru.yandex.practicum.collector.model.event.DeviceAction(action.getSensorId(), action.getType().getValue(), action.getValue());
    }

    private Instant sensorTimestamp(CollectSensorEventRequest event) {
        return switch (event) {
            case ru.yandex.practicum.collector.model.ClimateSensorEvent climateEvent ->
                    climateEvent.getTimestamp().toInstant();
            case ru.yandex.practicum.collector.model.LightSensorEvent lightEvent ->
                    lightEvent.getTimestamp().toInstant();
            case ru.yandex.practicum.collector.model.MotionSensorEvent motionEvent ->
                    motionEvent.getTimestamp().toInstant();
            case ru.yandex.practicum.collector.model.SwitchSensorEvent switchEvent ->
                    switchEvent.getTimestamp().toInstant();
            case ru.yandex.practicum.collector.model.TemperatureSensorEvent temperatureEvent ->
                    temperatureEvent.getTimestamp().toInstant();
            default -> throw new IllegalArgumentException("Unsupported sensor event: " + event.getClass().getName());
        };
    }

    private Instant hubTimestamp(CollectHubEventRequest event) {
        return switch (event) {
            case DeviceAddedEvent deviceAdded -> deviceAdded.getTimestamp().toInstant();
            case DeviceRemovedEvent deviceRemoved -> deviceRemoved.getTimestamp().toInstant();
            case ScenarioAddedEvent scenarioAdded -> scenarioAdded.getTimestamp().toInstant();
            case ScenarioRemovedEvent scenarioRemoved -> scenarioRemoved.getTimestamp().toInstant();
            default -> throw new IllegalArgumentException("Unsupported hub event: " + event.getClass().getName());
        };
    }
}
