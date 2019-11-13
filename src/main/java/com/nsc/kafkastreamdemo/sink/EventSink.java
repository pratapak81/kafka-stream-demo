package com.nsc.kafkastreamdemo.sink;

import com.nsc.kafkastreamdemo.model.CustomEvent;
import com.nsc.kafkastreamdemo.model.Event;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;

public interface EventSink {
    @Input("inputTable")
    KTable<?, ?> inputTable();
}
