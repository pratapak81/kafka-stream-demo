package com.nsc.kafkastreamdemo.phasedetection;

import com.nsc.kafkastreamdemo.model.CustomEvent;
import com.nsc.kafkastreamdemo.model.Event;
import com.nsc.kafkastreamdemo.model.EventType;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;

@Service
public class PhaseDetectionService {

    private List<Integer> eventList = new ArrayList<>();
    private List<CustomEvent> customEventList = new ArrayList<>();

    private Predicate<Integer> predicateGreaterThanEqualTo = (n) -> n >= 5;
    private Predicate<Integer> predicateLesserThan = (n) -> n < 5;

    private List<Predicate<Integer>> predicateList = new LinkedList<>(Arrays.asList(predicateGreaterThanEqualTo,
            predicateGreaterThanEqualTo, predicateLesserThan, predicateLesserThan));

    public List<Integer> getEventList() {
        return eventList;
    }

    public void setEventList(List<Integer> eventList) {
        this.eventList = eventList;
    }

    public PhaseDetectionService add(Event event) {
        if (eventList.size() >= 4) {
            Integer[] temp = new Integer[4];
            for (int i = 1; i < eventList.size(); i++) {
                temp[i - 1] = eventList.get(i);
            }
            temp[3] = event.getValue();
            eventList = Arrays.asList(temp);
        } else {
            eventList.add(event.getValue());
        }

        System.out.println(eventList);
        detectPhase();
        return this;
    }

    public List<CustomEvent> getCustomEvents() {
        return customEventList;
    }

    private void detectPhase() {
        if (eventList.size() == 4) {
            for (int i = 0; i < eventList.size(); i++) {
                if (!predicateList.get(i).test(eventList.get(i))) {
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
