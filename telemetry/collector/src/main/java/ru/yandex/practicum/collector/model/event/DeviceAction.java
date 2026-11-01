package ru.yandex.practicum.collector.model.event;

public record DeviceAction(String sensorId, String type, Integer value) {
}
