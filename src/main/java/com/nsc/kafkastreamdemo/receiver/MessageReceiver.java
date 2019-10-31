package com.nsc.kafkastreamdemo.receiver;

import com.nsc.kafkastreamdemo.model.CustomEvent;
import com.nsc.kafkastreamdemo.model.Event;
import com.nsc.kafkastreamdemo.phasedetection.PhaseDetectionService;
import com.nsc.kafkastreamdemo.sink.EventSink;
import com.nsc.kafkastreamdemo.source.EventSource;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@EnableBinding(EventSink.class)
public class MessageReceiver {

    @Autowired
    private PhaseDetectionService phaseDetectionService;

    @StreamListener(target = EventSink.EVENT_INPUT)
    @SendTo(EventSource.CUSTOM_EVENT_OUTPUT)
    public KStream<String, List<CustomEvent>> process(KStream<String, Event> eventKStream) {

        JsonSerde<ArrayList<CustomEvent>> listOfIntegerJsonSerde = new JsonSerde<>(ArrayList.class);
        JsonSerde<Event> eventJsonSerdeJsonSerde = new JsonSerde<>(Event.class);

        return eventKStream
                .selectKey((key, event) -> event.getTenantId() + "-" + event.getLocation())
                .groupByKey(Serialized.with(Serdes.String(), eventJsonSerdeJsonSerde))
                .windowedBy(TimeWindows.of(60000))
                .aggregate(
                        ArrayList::new,
                        (key, event, eventList) -> {
                            Optional<CustomEvent> customEventOptional = Optional.ofNullable(phaseDetectionService.process(event));
                            customEventOptional.ifPresent(eventList::add);
                            return eventList;
                        },
                        Materialized.with(Serdes.String(), listOfIntegerJsonSerde))
                .toStream((key, value) -> key.key())
                .filter(((key, value) -> !value.isEmpty()))
                .map(KeyValue::pair);
    }

    @StreamListener(target = EventSink.CUSTOM_EVENT_INPUT)
    public void processSecondLevel(KStream<String, List<CustomEvent>> eventKStream) {
        eventKStream.foreach((key, value) -> {
            System.out.println("key = " + key);
            System.out.println("value = " + value);
        });
    }
}
