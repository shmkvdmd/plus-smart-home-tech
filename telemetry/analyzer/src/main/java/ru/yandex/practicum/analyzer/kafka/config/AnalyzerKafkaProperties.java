package ru.yandex.practicum.analyzer.kafka.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties(prefix = "app.kafka")
@Validated
public record AnalyzerKafkaProperties(
        @NotBlank String bootstrapServers,
        @NotNull @Valid Topics topics,
        @NotNull @Valid Consumers consumers
) {

    public record Topics(
            @NotBlank String snapshots,
            @NotBlank String hubs
    ) {
    }

    public record Consumers(
            @NotNull @Valid Consumer snapshots,
            @NotNull @Valid Consumer hubEvents
    ) {
    }

    public record Consumer(
            @NotBlank String groupId,
            @NotBlank String autoOffsetReset,
            boolean enableAutoCommit
    ) {
    }
}