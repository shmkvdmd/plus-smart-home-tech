package ru.yandex.practicum.collector.handler.sensor.impl;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.application.CollectorService;
import ru.yandex.practicum.collector.mapper.ProtoEventTimeMapper;
import ru.yandex.practicum.collector.handler.sensor.SensorEventHandler;
import ru.yandex.practicum.collector.model.event.ClimateSensorEvent;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Component
public class ClimateSensorEventHandler implements SensorEventHandler {
    private final CollectorService collectorService;

    public ClimateSensorEventHandler(CollectorService collectorService) {
        this.collectorService = collectorService;
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.CLIMATE_SENSOR;
    }

    @Override
    public void handle(SensorEventProto event) {
        var payload = event.getClimateSensor();
        collectorService.collectSensorEvent(new ClimateSensorEvent(
                event.getId(), event.getHubId(), ProtoEventTimeMapper.toInstant(event.getTimestamp()),
                payload.getTemperatureC(), payload.getHumidity(), payload.getCo2Level()));
    }
}
