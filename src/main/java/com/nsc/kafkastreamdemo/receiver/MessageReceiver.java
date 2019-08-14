package com.nsc.kafkastreamdemo.receiver;

import com.nsc.kafkastreamdemo.model.Event;
import com.nsc.kafkastreamdemo.sink.EventSink;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;

@EnableBinding(EventSink.class)
public class MessageReceiver {

    @StreamListener(target = EventSink.EVENT_INPUT)
    public void process(KStream<String, Event> eventKStream) {
        eventKStream.foreach((key, value) -> {
            System.out.println("-------- Received ---------");
            System.out.println(value.getName());
        });
    }
}
