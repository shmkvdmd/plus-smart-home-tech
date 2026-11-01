package ru.yandex.practicum.collector.handler.sensor.impl;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.application.CollectorService;
import ru.yandex.practicum.collector.mapper.ProtoEventTimeMapper;
import ru.yandex.practicum.collector.handler.sensor.SensorEventHandler;
import ru.yandex.practicum.collector.model.event.MotionSensorEvent;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Component
public class MotionSensorEventHandler implements SensorEventHandler {
    private final CollectorService collectorService;

    public MotionSensorEventHandler(CollectorService collectorService) {
        this.collectorService = collectorService;
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR;
    }

    @Override
    public void handle(SensorEventProto event) {
        var payload = event.getMotionSensor();
        collectorService.collectSensorEvent(new MotionSensorEvent(
                event.getId(), event.getHubId(), ProtoEventTimeMapper.toInstant(event.getTimestamp()),
                payload.getLinkQuality(), payload.getMotion(), payload.getVoltage()));
    }
}
