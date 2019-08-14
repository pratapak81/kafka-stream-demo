package com.nsc.kafkastreamdemo.source;

import com.nsc.kafkastreamdemo.model.CustomEvent;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface EventSource {
    String EVENT_SOURCE = "eventOut";

    String CUSTOM_EVENT_OUTPUT = "customEventOut";

    @Output(EVENT_SOURCE)
    MessageChannel eventOutput();

    @Output(CUSTOM_EVENT_OUTPUT)
    KStream<String, CustomEvent> customEventOutput();
}
