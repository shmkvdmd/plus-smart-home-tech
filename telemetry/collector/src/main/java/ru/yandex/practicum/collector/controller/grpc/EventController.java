package ru.yandex.practicum.collector.controller.grpc;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;


@GrpcService
public class EventController extends CollectorControllerGrpc.CollectorControllerImplBase {
    private static final Logger log = LoggerFactory.getLogger(EventController.class);
    private final SensorEventDispatcher sensorEventDispatcher;
    private final HubEventDispatcher hubEventDispatcher;

    public EventController(
            SensorEventDispatcher sensorEventDispatcher,
            HubEventDispatcher hubEventDispatcher
    ) {
        this.sensorEventDispatcher = sensorEventDispatcher;
        this.hubEventDispatcher = hubEventDispatcher;
    }

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            sensorEventDispatcher.dispatch(request);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error collecting sensor event for {}", request.getPayloadCase(), e);
            responseObserver.onError(new StatusRuntimeException(Status.fromThrowable(e)));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            hubEventDispatcher.dispatch(request);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error collecting hub event for {}", request.getPayloadCase(), e);
            responseObserver.onError(new StatusRuntimeException(Status.fromThrowable(e)));
        }
    }
}
