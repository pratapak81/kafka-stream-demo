package com.nsc.kafkastreamdemo.phasedetection;

import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.LinkedHashMap;
import java.util.Map;

public class FIFOMap<K, V> extends LinkedHashMap<K, V> {

    FIFOMap() {
        super(4);
    }

    public FIFOMap(int size) {
        super(size);
    }

    @Override
    @JsonAnySetter
    public V put(K key, V value) {
        return super.put(key, value);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > 4;
    }
}
