package ru.yandex.practicum.collector.kafka.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka")
public record CollectorKafkaProperties(
        @NotNull @Valid Topics topics
) {
    public record Topics(
            @NotBlank String sensors,
            @NotBlank String hubs
    ) {
    }
}