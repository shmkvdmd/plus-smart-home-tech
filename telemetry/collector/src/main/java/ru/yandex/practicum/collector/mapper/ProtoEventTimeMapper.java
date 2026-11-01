package ru.yandex.practicum.collector.mapper;

import com.google.protobuf.Timestamp;

import java.time.Instant;

public final class ProtoEventTimeMapper {
    private ProtoEventTimeMapper() {
    }

    public static Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}
