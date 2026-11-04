package ru.yandex.practicum.analyzer.service;

import org.hibernate.Hibernate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.mapper.ScenarioMapper;
import ru.yandex.practicum.analyzer.model.Scenario;
import ru.yandex.practicum.analyzer.model.Sensor;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Transactional(readOnly = true)
public class ScenarioService {

    private final ScenarioRepository scenarioRepository;
    private final SensorService sensorService;
    private final ScenarioMapper scenarioMapper;


    public ScenarioService(
            ScenarioRepository scenarioRepository,
            SensorService sensorService,
            ScenarioMapper scenarioMapper
    ) {
        this.scenarioRepository = scenarioRepository;
        this.sensorService = sensorService;
        this.scenarioMapper = scenarioMapper;
    }

    public List<Scenario> findAllByHubId(String hubId) {
        List<Scenario> scenarios = scenarioRepository.findAllByHubId(hubId);

        for (Scenario scenario : scenarios) {
            Hibernate.initialize(scenario.getConditions());
            Hibernate.initialize(scenario.getActions());

            scenario.getConditions().forEach(binding -> {
                Hibernate.initialize(binding.getSensor());
                Hibernate.initialize(binding.getCondition());
            });

            scenario.getActions().forEach(binding -> {
                Hibernate.initialize(binding.getSensor());
                Hibernate.initialize(binding.getAction());
            });
        }

        return scenarios;
    }

    @Transactional
    public void deleteScenarioByHubIdAndName(String hubId, String name) {
        scenarioRepository.deleteByHubIdAndName(hubId, name);
    }

    @Transactional
    public Scenario saveOrUpdateScenario(
            String hubId,
            ScenarioAddedEventAvro event
    ) {
        Set<String> sensorIds = Stream.concat(
                        event.getConditions().stream().map(ScenarioConditionAvro::getSensorId),
                        event.getActions().stream().map(DeviceActionAvro::getSensorId)
                )
                .collect(Collectors.toSet());

        Map<String, Sensor> sensors = sensorService.findSensorsByIdsAndHubId(sensorIds, hubId);

        if (sensors.size() != sensorIds.size()) {
            Set<String> missingSensors = new HashSet<>(sensorIds);
            missingSensors.removeAll(sensors.keySet());

            throw new IllegalStateException(
                    "Some sensors are not registered in hub %s: %s".formatted(hubId, missingSensors)
            );
        }

        String scenarioName = event.getName();

        Scenario scenario = scenarioRepository
                .findByHubIdAndName(hubId, scenarioName)
                .orElseGet(() -> Scenario.builder()
                        .hubId(hubId)
                        .name(scenarioName)
                        .build()
                );

        scenarioMapper.updateScenario(scenario, hubId, event, sensors);

        return scenarioRepository.save(scenario);
    }
}