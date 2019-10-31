package com.nsc.kafkastreamdemo.phasedetection;

import com.nsc.kafkastreamdemo.model.CustomEvent;
import com.nsc.kafkastreamdemo.model.Event;
import com.nsc.kafkastreamdemo.model.EventType;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Predicate;

@Service
public class PhaseDetectionService {

    private static class FIFOMap<K, V> extends LinkedHashMap<K, V> {

        private int maxSize;

        FIFOMap(int maxSize) {
            super(maxSize);
            this.maxSize = maxSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > maxSize;
        }
    }

    private Map<String, FIFOMap<String, Integer>> machineData = new HashMap<>();

    private Predicate<Integer> predicateGreaterThanEqualTo = (n) -> n >= 5;
    private Predicate<Integer> predicateLesserThan = (n) -> n < 5;

    private List<Predicate<Integer>> predicateList;

    public PhaseDetectionService() {
        predicateList = new LinkedList<>(Arrays.asList(predicateGreaterThanEqualTo, predicateGreaterThanEqualTo,
                predicateLesserThan, predicateLesserThan));
    }

    public CustomEvent process(Event event) {
        String key = event.getTenantId() + "-" + event.getLocation();
        if (machineData.containsKey(key)) {
            machineData.get(key).put(new Date().toString(), event.getValue());
        } else {
            FIFOMap<String, Integer> rawData = new FIFOMap<>(4);
            rawData.put(new Date().toString(), event.getValue());
            machineData.put(key, rawData);
        }
        return detectPhase(key);
    }

    private CustomEvent detectPhase(String key) {
        FIFOMap<String, Integer> rawData = machineData.get(key);
        if (rawData == null || rawData.size() < 4) {
            return null;
        }
        int index = 0;
        for (Map.Entry<String, Integer> entry : rawData.entrySet()) {
            if (!predicateList.get(index++).test(entry.getValue())) {
                return null;
            }
        }
        return generateEvent();
    }

    private CustomEvent generateEvent() {
        return CustomEvent.builder()
                .eventType(EventType.MAINTENANCE)
                .build();
    }
}
