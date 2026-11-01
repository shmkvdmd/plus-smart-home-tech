package ru.yandex.practicum.collector.kafka.mapper;

import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.model.event.ClimateSensorEvent;
import ru.yandex.practicum.collector.model.event.LightSensorEvent;
import ru.yandex.practicum.collector.model.event.MotionSensorEvent;
import ru.yandex.practicum.collector.model.event.SensorEvent;
import ru.yandex.practicum.collector.model.event.SwitchSensorEvent;
import ru.yandex.practicum.collector.model.event.TemperatureSensorEvent;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

@Component
public class SensorEventAvroMapper {
    public SensorEventAvro toAvro(SensorEvent event) {
        SpecificRecord payload = switch (event) {
            case ClimateSensorEvent climateEvent -> ClimateSensorAvro.newBuilder()
                    .setTemperatureC(climateEvent.temperatureC())
                    .setHumidity(climateEvent.humidity())
                    .setCo2Level(climateEvent.co2Level())
                    .build();
            case LightSensorEvent lightEvent -> LightSensorAvro.newBuilder()
                    .setLinkQuality(lightEvent.linkQuality())
                    .setLuminosity(lightEvent.luminosity())
                    .build();
            case MotionSensorEvent motionEvent -> MotionSensorAvro.newBuilder()
                    .setLinkQuality(motionEvent.linkQuality())
                    .setMotion(motionEvent.motion())
                    .setVoltage(motionEvent.voltage())
                    .build();
            case SwitchSensorEvent switchEvent -> SwitchSensorAvro.newBuilder()
                    .setState(switchEvent.state())
                    .build();
            case TemperatureSensorEvent temperatureEvent -> TemperatureSensorAvro.newBuilder()
                    .setId(temperatureEvent.id())
                    .setHubId(temperatureEvent.hubId())
                    .setTimestamp(temperatureEvent.timestamp())
                    .setTemperatureC(temperatureEvent.temperatureC())
                    .setTemperatureF(temperatureEvent.temperatureF())
                    .build();
            default -> throw new IllegalArgumentException(
                    "Unsupported sensor event: " + event.getClass().getName());
        };

        return SensorEventAvro.newBuilder()
                .setId(event.id())
                .setHubId(event.hubId())
                .setTimestamp(event.timestamp())
                .setPayload(payload)
                .build();
    }
}
