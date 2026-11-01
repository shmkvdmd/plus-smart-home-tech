package ru.yandex.practicum.aggregator.kafka.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties(prefix = "app.kafka")
@Validated
public record AggregatorKafkaProperties(
        @NotBlank String bootstrapServers,
        @NotNull @Valid Consumer consumer,
        @NotNull @Valid Producer producer,
        @NotNull @Valid Topics topics
) {

    public record Consumer(
            @NotBlank String groupId,
            @NotBlank String autoOffsetReset,
            boolean enableAutoCommit
    ) {
    }

    public record Producer(
            @NotBlank String clientId
    ) {
    }

    public record Topics(
            @NotBlank String sensors,
            @NotBlank String snapshots
    ) {
    }
}