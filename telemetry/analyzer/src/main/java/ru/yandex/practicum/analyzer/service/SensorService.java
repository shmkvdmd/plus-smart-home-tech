package ru.yandex.practicum.analyzer.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.model.Sensor;
import ru.yandex.practicum.analyzer.repository.ScenarioActionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioConditionRepository;
import ru.yandex.practicum.analyzer.repository.SensorRepository;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Transactional(readOnly = true)
public class SensorService {

    private final SensorRepository sensorRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;

    public SensorService(
            SensorRepository sensorRepository,
            ScenarioConditionRepository scenarioConditionRepository,
            ScenarioActionRepository scenarioActionRepository
    ) {
        this.sensorRepository = sensorRepository;
        this.scenarioConditionRepository = scenarioConditionRepository;
        this.scenarioActionRepository = scenarioActionRepository;
    }

    @Transactional
    public Sensor registerSensor(String sensorId, String hubId) {
        return sensorRepository.findById(sensorId)
                .map(sensor -> {
                    if (!sensor.getHubId().equals(hubId)) {
                        throw new IllegalStateException(
                                "Датчик %s уже зарегистрирован в другом хабе".formatted(sensorId)
                        );
                    }

                    return sensor;
                })
                .orElseGet(() -> sensorRepository.save(
                        Sensor.builder()
                                .id(sensorId)
                                .hubId(hubId)
                                .build()
                ));
    }

    @Transactional
    public void deleteSensorByIdAndHubId(String sensorId, String hubId) {
        if (!sensorRepository.existsByIdAndHubId(sensorId, hubId)) {
            return;
        }

        scenarioConditionRepository.deleteBySensorIdAndSensorHubId(sensorId, hubId);
        scenarioActionRepository.deleteBySensorIdAndSensorHubId(sensorId, hubId);
        sensorRepository.deleteByIdAndHubId(sensorId, hubId);
    }

    public Map<String, Sensor> findSensorsByIdsAndHubId(Collection<String> sensorIds, String hubId) {
        if (sensorIds.isEmpty()) {
            return Map.of();
        }

        return sensorRepository.findAllByIdInAndHubId(sensorIds, hubId)
                .stream()
                .collect(Collectors.toMap(
                        Sensor::getId,
                        Function.identity()
                ));
    }
}