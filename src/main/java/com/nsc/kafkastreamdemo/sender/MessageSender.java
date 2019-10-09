package com.nsc.kafkastreamdemo.sender;

import com.nsc.kafkastreamdemo.model.Event;
import com.nsc.kafkastreamdemo.source.EventSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

@EnableBinding(EventSource.class)
public class MessageSender {

    private static int count = 0;

    @Autowired
    EventSource eventSource;

    public void send(int value) {
        Event event = Event.builder()
                .name("HEART_BEAT")
                .value(value)
                .build();

        Message<Event> message = MessageBuilder
                .withPayload(event)
                .setHeader(KafkaHeaders.MESSAGE_KEY, event.getName().getBytes())
                .build();

        eventSource.eventOutput().send(message);
    }
}
