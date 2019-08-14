package com.nsc.kafkastreamdemo.sink;

import com.nsc.kafkastreamdemo.model.CustomEvent;
import com.nsc.kafkastreamdemo.model.Event;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;

public interface EventSink {
    String EVENT_INPUT = "eventIn";

    String CUSTOM_EVENT_INPUT = "customEventIn";

    @Input(EVENT_INPUT)
    KStream<String, Event> eventInput();

    @Input(CUSTOM_EVENT_INPUT)
    KStream<String, CustomEvent> customEventInput();
}
