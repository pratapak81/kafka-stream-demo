package com.nsc.kafkastreamdemo.receiver;

import com.nsc.kafkastreamdemo.model.CustomEvent;
import com.nsc.kafkastreamdemo.model.Event;
import com.nsc.kafkastreamdemo.model.EventType;
import com.nsc.kafkastreamdemo.phasedetection.PhaseDetectionService;
import com.nsc.kafkastreamdemo.sink.EventSink;
import com.nsc.kafkastreamdemo.source.EventSource;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@EnableBinding(EventSink.class)
public class MessageReceiver {

    @Autowired
    private InteractiveQueryService interactiveQueryService;

    @StreamListener
    //@SendTo("output")
    public void process(@Input("inputTable") KTable<String, Event> KTable) {

        /*TimeWindowedKStream<String, Event> timeWindowedKStream = eventKStream
                .groupByKey()
                .windowedBy(TimeWindows.of(60000).advanceBy(30000));

        JsonSerde<ArrayList<Integer>> jsonSerde = new JsonSerde<>(ArrayList.class);

        timeWindowedKStream
                .aggregate(
                        ArrayList::new,
                        ((key, value, eventList) -> {
                            eventList.add(value.getValue());
                            return eventList;
                        }),
                        Materialized.with(Serdes.String(), jsonSerde)
                ).toStream((key, value) -> key.key())
                .foreach((key, value) -> System.out.println(value));*/

        KTable.toStream()
                .foreach((key, value) -> System.out.println(value));
    }
}
