package ru.yandex.practicum.collector.handler.sensor.impl;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.application.CollectorService;
import ru.yandex.practicum.collector.mapper.ProtoEventTimeMapper;
import ru.yandex.practicum.collector.handler.sensor.SensorEventHandler;
import ru.yandex.practicum.collector.model.event.SwitchSensorEvent;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Component
public class SwitchSensorEventHandler implements SensorEventHandler {
    private final CollectorService collectorService;

    public SwitchSensorEventHandler(CollectorService collectorService) {
        this.collectorService = collectorService;
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.SWITCH_SENSOR;
    }

    @Override
    public void handle(SensorEventProto event) {
        collectorService.collectSensorEvent(new SwitchSensorEvent(
                event.getId(), event.getHubId(), ProtoEventTimeMapper.toInstant(event.getTimestamp()),
                event.getSwitchSensor().getState()));
    }
}
