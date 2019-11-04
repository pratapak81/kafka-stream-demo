package com.nsc.kafkastreamdemo.receiver;

import com.nsc.kafkastreamdemo.model.CustomEvent;
import com.nsc.kafkastreamdemo.model.Event;
import com.nsc.kafkastreamdemo.phasedetection.PhaseDetectionService;
import com.nsc.kafkastreamdemo.sink.EventSink;
import com.nsc.kafkastreamdemo.source.EventSource;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.List;

@EnableBinding(EventSink.class)
public class MessageReceiver {

    @StreamListener(target = EventSink.EVENT_INPUT)
    @SendTo(EventSource.CUSTOM_EVENT_OUTPUT)
    public KStream<String, List<CustomEvent>> process(KStream<String, Event> eventKStream) {

        JsonSerde<PhaseDetectionService> phaseDetectionServiceJsonSerde = new JsonSerde<>(PhaseDetectionService.class);

        TimeWindowedKStream<String, Event> timeWindowedKStream = eventKStream
                .groupByKey()
                .windowedBy(TimeWindows.of(60000).advanceBy(30000));

        KTable<Windowed<String>, PhaseDetectionService> kTable = timeWindowedKStream
                .aggregate(
                        PhaseDetectionService::new,
                        ((key, value, phaseDetectionService) -> phaseDetectionService.add(value)),
                        Materialized.with(Serdes.String(), phaseDetectionServiceJsonSerde)
                );

        /*kTable.toStream((key, value) -> key.key())
                .foreach((key, value) -> System.out.println(value.getMachineData()));*/

        return kTable.toStream((key, value) -> key.key())
                .mapValues(PhaseDetectionService::getCustomEvents)
                .filter(((key, value) -> !value.isEmpty()));
    }

    @StreamListener(target = EventSink.CUSTOM_EVENT_INPUT)
    public void processSecondLevel(KStream<String, List<CustomEvent>> eventKStream) {
        eventKStream.foreach((key, value) -> {
            System.out.println("key = " + key);
            System.out.println("value = " + value);
        });
    }
}
