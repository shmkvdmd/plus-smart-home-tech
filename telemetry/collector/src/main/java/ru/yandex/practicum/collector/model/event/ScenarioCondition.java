package ru.yandex.practicum.collector.model.event;

public record ScenarioCondition(String sensorId, String type, String operation, Object value) {
}
