package com.nsc.kafkastreamdemo.sender;

import com.nsc.kafkastreamdemo.model.Event;
import com.nsc.kafkastreamdemo.source.EventSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@EnableBinding(EventSource.class)
public class MessageSender {

    private static int count = 0;

    private int[] testValuesX = {
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
            31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45
    };
    /*private int[] testValuesX = {
            6, 6, 6, 6, 6, 6, 6, 5, 6, 6, 7, 8, 5, 5, 4,
            4, 4, 2, 1, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
            3, 4, 4, 4, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0
    };*/

    private int[] testValuesY = {
            9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
            9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
            9, 9, 9, 9, 9, 9, 3, 3, 9, 9, 9, 9, 9, 9, 9
    };

    @Autowired
    EventSource eventSource;

    public void send(int value) {
        Event event = Event.builder()
                .tenantId("APOLLO")
                .location("Whitefield")
                .value(value)
                .build();

        Message<Event> message = MessageBuilder
                .withPayload(event)
                .setHeader(KafkaHeaders.MESSAGE_KEY, (event.getTenantId()+"-"+event.getLocation()).getBytes())
                .build();

        eventSource.eventOutput().send(message);
    }

    public void send() {

        Runnable runnable = () -> {
            Event eventWhitefield = Event.builder()
                    .tenantId("APOLLO")
                    .location("Whitefield")
                    .value(testValuesX[count])
                    .build();

            Message<Event> messageWhitefield = MessageBuilder
                    .withPayload(eventWhitefield)
                    .setHeader(KafkaHeaders.MESSAGE_KEY, eventWhitefield.getLocation().getBytes())
                    .build();

            /*Event eventJayanagar = Event.builder()
                    .tenantId("APOLLO")
                    .location("Jayanagar")
                    .value(testValuesY[count])
                    .build();

            Message<Event> messageJayanagar = MessageBuilder
                    .withPayload(eventJayanagar)
                    .setHeader(KafkaHeaders.MESSAGE_KEY, (eventJayanagar.getTenantId()+"-"+eventJayanagar.getLocation()).getBytes())
                    .build();*/

            eventSource.eventOutput().send(messageWhitefield);
            //eventSource.eventOutput().send(messageJayanagar);

            count++;
        };

        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(runnable, 0, 4000, TimeUnit.MILLISECONDS);
    }
}
