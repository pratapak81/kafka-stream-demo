package com.nsc.kafkastreamdemo.receiver;

import com.nsc.kafkastreamdemo.model.CustomEvent;
import com.nsc.kafkastreamdemo.model.Event;
import com.nsc.kafkastreamdemo.sink.EventSink;
import com.nsc.kafkastreamdemo.source.EventSource;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;

@EnableBinding(EventSink.class)
public class MessageReceiver {

    @StreamListener(target = EventSink.EVENT_INPUT)
    @SendTo(EventSource.CUSTOM_EVENT_OUTPUT)
    public KStream<String, CustomEvent> process(KStream<String, Event> eventKStream) {
       /* eventKStream.foreach((key, value) -> {
            System.out.println("-------- Received Before Processing ---------");
            System.out.println(value.getName());
        });*/
        return eventKStream.map((key, value) -> KeyValue.pair(key, new CustomEvent(value.getName().toUpperCase(), "Bengaluru")));
    }

    @StreamListener(target = EventSink.CUSTOM_EVENT_INPUT)
    public void processSecondLevel(KStream<String, CustomEvent> eventKStream) {
        eventKStream.foreach((key, value) -> {
            System.out.println("----------- After Process ----------");
            System.out.println(value.getName() + " " + value.getPlace());
        });
    }
}
