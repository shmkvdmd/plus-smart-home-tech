package ru.yandex.practicum.collector.controller.grpc;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.handler.hub.HubEventHandler;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

@Component
public class HubEventDispatcher {
    private final Map<HubEventProto.PayloadCase, HubEventHandler> handlers;

    public HubEventDispatcher(Set<HubEventHandler> handlers) {
        this.handlers = handlers.stream()
                .collect(Collectors.toUnmodifiableMap(
                        HubEventHandler::getMessageType,
                        Function.identity(),
                        (firstHandler, secondHandler) -> {
                            throw new IllegalStateException(
                                    "More than one handler is registered for "
                                            + firstHandler.getMessageType());
                        }));
    }

    public void dispatch(HubEventProto event) {
        HubEventHandler handler = handlers.get(event.getPayloadCase());
        if (handler == null) {
            throw new IllegalArgumentException(
                    "Unsupported hub event: " + event.getPayloadCase());
        }

        handler.handle(event);
    }
}
