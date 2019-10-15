package com.nsc.kafkastreamdemo.receiver;

import com.nsc.kafkastreamdemo.model.CustomEvent;
import com.nsc.kafkastreamdemo.model.Event;
import com.nsc.kafkastreamdemo.sink.EventSink;
import com.nsc.kafkastreamdemo.source.EventSource;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;

@EnableBinding(EventSink.class)
public class MessageReceiver {

    @StreamListener(target = EventSink.EVENT_INPUT)
    @SendTo(EventSource.CUSTOM_EVENT_OUTPUT)
    public KStream<String, CustomEvent> process(KStream<String, Event> eventKStream) {
        // return eventKStream.map((key, value) -> KeyValue.pair(key, new CustomEvent(value.getName(), value.getValue(), "Bengaluru")));

        return eventKStream
                .filter((key, value) -> value.getValue() > 10)
                .groupByKey()
                .windowedBy(TimeWindows.of(15000))
                .count()
                .toStream((key, value) -> key.key())
                .map((key, value) -> KeyValue.pair(key, new CustomEvent(key, value, "Pratap")));
    }

    @StreamListener(target = EventSink.CUSTOM_EVENT_INPUT)
    public void processSecondLevel(KStream<String, CustomEvent> eventKStream) {
        eventKStream.foreach((key, value) -> {
            System.out.println("----------- After Process ----------");
            System.out.println(String.format("Patient crossed threshold %d times in the last 15 seconds", value.getValue()));
        });
    }
}
