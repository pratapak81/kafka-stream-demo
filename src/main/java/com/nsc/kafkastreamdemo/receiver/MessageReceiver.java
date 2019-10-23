package com.nsc.kafkastreamdemo.receiver;

import com.nsc.kafkastreamdemo.model.CustomEvent;
import com.nsc.kafkastreamdemo.model.Event;
import com.nsc.kafkastreamdemo.sink.EventSink;
import com.nsc.kafkastreamdemo.source.EventSource;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.ArrayList;

@EnableBinding(EventSink.class)
public class MessageReceiver {

    @StreamListener(target = EventSink.EVENT_INPUT)
    @SendTo(EventSource.CUSTOM_EVENT_OUTPUT)
    public KStream<String, CustomEvent> process(KStream<String, Event> eventKStream) {
        // return eventKStream.map((key, value) -> KeyValue.pair(key, new CustomEvent(value.getName(), value.getValue(), "Bengaluru")));

        /*return eventKStream
                .filter((key, value) -> value.getValue() > 10)
                .groupByKey()
                .windowedBy(TimeWindows.of(15000))
                .count()
                .toStream((key, value) -> key.key())
                .map((key, value) -> KeyValue.pair(key, new CustomEvent(key, value, "Pratap")));*/

        JsonSerde<ArrayList<Event>> listOfEventJsonSerde = new JsonSerde<>(ArrayList.class);

        return eventKStream
                .groupByKey()
                .windowedBy(TimeWindows.of(20000))
                .aggregate(
                        ArrayList::new,
                        (key, event, eventList) -> {
                            eventList.add(event);
                            return eventList;
                        },
                        Materialized.with(Serdes.String(), listOfEventJsonSerde))
                .toStream((key, value) -> key.key())
                .map((key, value) -> KeyValue.pair(key, new CustomEvent(key, 10L, "Pratap", value)));
    }

    @StreamListener(target = EventSink.CUSTOM_EVENT_INPUT)
    public void processSecondLevel(KStream<String, CustomEvent> eventKStream) {
        eventKStream.foreach((key, value) -> {
            System.out.println("key = " + key);
            System.out.println("value = " + value.getEventList());
        });
    }
}
