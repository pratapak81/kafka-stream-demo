package com.nsc.kafkastreamdemo.phasedetection;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.nsc.kafkastreamdemo.model.CustomEvent;
import com.nsc.kafkastreamdemo.model.Event;
import com.nsc.kafkastreamdemo.model.EventType;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Predicate;

@Service
public class PhaseDetectionService {

    // https://stackoverflow.com/questions/52390040/serializing-hashmap-in-kafka-streams
    // https://stackoverflow.com/questions/18043587/why-im-not-able-to-unwrap-and-serialize-a-java-map-using-the-jackson-java-libra
    @JsonUnwrapped
    private FIFOMap<String, Integer> machineData = new FIFOMap<>(4);
    private List<CustomEvent> customEventList = new ArrayList<>();

    private Predicate<Integer> predicateGreaterThanEqualTo = (n) -> n >= 5;
    private Predicate<Integer> predicateLesserThan = (n) -> n < 5;

    private List<Predicate<Integer>> predicateList = new LinkedList<>(Arrays.asList(predicateGreaterThanEqualTo,
            predicateGreaterThanEqualTo, predicateLesserThan, predicateLesserThan));

    public PhaseDetectionService add(Event event) {
        machineData.put(new Date().toString()+"-"+event.getLocation(), event.getValue());
        //System.out.println(machineData);
        detectPhase();
        return this;
    }

    public FIFOMap<String, Integer> getMachineData() {
        return machineData;
    }

    public List<CustomEvent> getCustomEvents() {
        return customEventList;
    }

    private void detectPhase() {
        if (machineData.size() == 4) {
            int index = 0;
            for (Map.Entry<String, Integer> entry: machineData.entrySet()) {
                if (!predicateList.get(index++).test(entry.getValue())) {
                    return;
                }
            }
            CustomEvent customEvent = CustomEvent.builder()
                    .eventType(EventType.MAINTENANCE)
                    .build();
            customEventList.add(customEvent);
        }
    }
}
