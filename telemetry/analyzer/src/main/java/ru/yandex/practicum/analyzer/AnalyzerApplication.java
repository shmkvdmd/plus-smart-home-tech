package ru.yandex.practicum.analyzer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.analyzer.kafka.config.AnalyzerKafkaProperties;
import ru.yandex.practicum.analyzer.processor.HubEventProcessor;
import ru.yandex.practicum.analyzer.processor.SnapshotProcessor;

@SpringBootApplication
@EnableConfigurationProperties(AnalyzerKafkaProperties.class)
public class AnalyzerApplication {
    public static void main(String[] args) {
        ConfigurableApplicationContext context =
                SpringApplication.run(AnalyzerApplication.class, args);

        HubEventProcessor hubEventProcessor =
                context.getBean(HubEventProcessor.class);

        SnapshotProcessor snapshotProcessor =
                context.getBean(SnapshotProcessor.class);

        Thread hubEventThread = new Thread(hubEventProcessor, "HubEventProcessor");

        hubEventThread.start();

        snapshotProcessor.start();
    }
}