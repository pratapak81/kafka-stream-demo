package com.nsc.kafkastreamdemo.source;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface EventSource {
    String EVENT_SOURCE = "eventOut";

    String CUSTOM_EVENT_OUTPUT = "customEventOut";

    @Output(EVENT_SOURCE)
    MessageChannel eventOutput();

    @Output(CUSTOM_EVENT_OUTPUT)
    MessageChannel customEventOutput();
}
