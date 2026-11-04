package ru.yandex.practicum.analyzer.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.model.*;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;

import java.util.Map;

@Component
public class ScenarioMapper {

    public void updateScenario(
            Scenario scenario,
            String hubId,
            ScenarioAddedEventAvro event,
            Map<String, Sensor> sensors
    ) {
        scenario.setHubId(hubId);
        scenario.setName(event.getName());

        scenario.getConditions().clear();
        scenario.getActions().clear();

        for (ScenarioConditionAvro conditionEvent : event.getConditions()) {
            scenario.addCondition(toScenarioCondition(conditionEvent, sensors));
        }

        for (DeviceActionAvro actionEvent : event.getActions()) {
            scenario.addAction(toScenarioAction(actionEvent, sensors));
        }
    }

    private ScenarioCondition toScenarioCondition(ScenarioConditionAvro source, Map<String, Sensor> sensors) {
        String sensorId = source.getSensorId();
        Sensor sensor = getSensor(sensorId, sensors);

        Condition condition = Condition.builder()
                .type(ConditionType.valueOf(source.getType().toString()))
                .operation(ConditionOperation.valueOf(source.getOperation().toString()))
                .value(toIntegerValue(source.getValue()))
                .build();

        ScenarioCondition binding = new ScenarioCondition();
        binding.setSensor(sensor);
        binding.setCondition(condition);

        return binding;
    }

    private ScenarioAction toScenarioAction(DeviceActionAvro source, Map<String, Sensor> sensors) {
        String sensorId = source.getSensorId();
        Sensor sensor = getSensor(sensorId, sensors);

        Action action = Action.builder()
                .type(ActionType.valueOf(source.getType().toString()))
                .value(toIntegerValue(source.getValue()))
                .build();

        ScenarioAction binding = new ScenarioAction();
        binding.setSensor(sensor);
        binding.setAction(action);

        return binding;
    }

    private Sensor getSensor(String sensorId, Map<String, Sensor> sensors) {
        Sensor sensor = sensors.get(sensorId);

        if (sensor == null) {
            throw new IllegalStateException("Sensor %s is not registered in the hub".formatted(sensorId));
        }

        return sensor;
    }

    private Integer toIntegerValue(Object value) {
        return switch (value) {
            case null -> null;
            case Boolean booleanValue -> booleanValue ? 1 : 0;
            case Number number -> number.intValue();
            default -> throw new IllegalArgumentException(
                    "Unsupported condition/action value type: "
                            + value.getClass().getName()
            );
        };

    }
}