package com.nsc.kafkastreamdemo.phasedetection;

import com.nsc.kafkastreamdemo.model.CustomEvent;
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

    private FIFOMap<String, Integer> rawData = new FIFOMap<>(4);

    private Predicate<Integer> predicateGreaterThanEqualTo = (n) -> n >= 5;
    private Predicate<Integer> predicateLesserThan = (n) -> n < 5;

    private List<Predicate<Integer>> predicateList;

    PhaseDetectionService() {
        predicateList = new LinkedList<>(Arrays.asList(predicateGreaterThanEqualTo, predicateGreaterThanEqualTo,
                predicateLesserThan, predicateLesserThan));
    }

    public CustomEvent process(int value) {
        rawData.put(new Date().toString(), value);
        return detectPhase();
    }

    private CustomEvent detectPhase() {
        if (rawData.size() < 4) {
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
