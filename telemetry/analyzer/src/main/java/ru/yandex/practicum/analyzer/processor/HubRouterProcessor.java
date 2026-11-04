package ru.yandex.practicum.analyzer.processor;

import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.analyzer.mapper.DeviceActionMapper;
import ru.yandex.practicum.analyzer.model.Scenario;
import ru.yandex.practicum.analyzer.model.ScenarioAction;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;

import java.time.Instant;

@Service
@Slf4j
public class HubRouterProcessor {

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    private final DeviceActionMapper deviceActionMapper;

    public HubRouterProcessor(DeviceActionMapper deviceActionMapper) {
        this.deviceActionMapper = deviceActionMapper;
    }

    public void handleAction(Scenario scenario, ScenarioAction binding) {
        DeviceActionRequest request = deviceActionMapper.toDeviceActionRequest(
                scenario,
                binding.getSensor(),
                binding.getAction(),
                Instant.now()
        );

        try {
            hubRouterClient.handleDeviceAction(request);
        } catch (StatusRuntimeException exception) {
            switch (exception.getStatus().getCode()) {
                case UNAVAILABLE ->
                        log.warn("Hub Router is unavailable. Hub: {}, scenario: {}", scenario.getHubId(), scenario.getName(), exception);
                case DEADLINE_EXCEEDED ->
                        log.warn("Hub Router request timed out. Hub: {}, scenario: {}", scenario.getHubId(), scenario.getName(), exception);
                case INVALID_ARGUMENT ->
                        log.error("Invalid device action. Sensor: {}, scenario: {}", binding.getSensor().getId(), scenario.getName(), exception);
                default -> log.error("gRPC error while sending action. Status: {}", exception.getStatus(), exception);
            }
            throw new IllegalStateException(
                    "Failed to send device action to hub-router. Hub: %s, scenario: %s, sensor: %s"
                            .formatted(scenario.getHubId(), scenario.getName(), binding.getSensor().getId()), exception);
        }
    }
}