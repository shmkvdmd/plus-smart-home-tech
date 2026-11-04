package ru.yandex.practicum.collector.controller.grpc;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.handler.sensor.SensorEventHandler;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Component
public class SensorEventDispatcher {
    private final Map<SensorEventProto.PayloadCase, SensorEventHandler> handlers;

    public SensorEventDispatcher(Set<SensorEventHandler> handlers) {
        this.handlers = handlers.stream()
                .collect(Collectors.toUnmodifiableMap(
                        SensorEventHandler::getMessageType,
                        Function.identity(),
                        (firstHandler, secondHandler) -> {
                            throw new IllegalStateException(
                                    "More than one handler is registered for "
                                            + firstHandler.getMessageType());
                        }));
    }

    public void dispatch(SensorEventProto event) {
        SensorEventHandler handler = handlers.get(event.getPayloadCase());
        if (handler == null) {
            throw new IllegalArgumentException(
                    "Unsupported sensor event: " + event.getPayloadCase());
        }

        handler.handle(event);
    }
}
