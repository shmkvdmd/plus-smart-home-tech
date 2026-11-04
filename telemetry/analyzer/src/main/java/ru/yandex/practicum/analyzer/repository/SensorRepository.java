package ru.yandex.practicum.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.analyzer.model.Sensor;

import java.util.Collection;
import java.util.List;

public interface SensorRepository extends JpaRepository<Sensor, String> {

    boolean existsByIdAndHubId(String id, String hubId);

    void deleteByIdAndHubId(String sensorId, String hubId);

    List<Sensor> findAllByIdInAndHubId(Collection<String> ids, String hubId);
}