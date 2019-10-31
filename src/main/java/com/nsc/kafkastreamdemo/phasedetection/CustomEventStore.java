package com.nsc.kafkastreamdemo.phasedetection;

import java.util.ArrayList;

public class CustomEventStore<E> extends ArrayList<E> {
    public PhaseDetectionService phaseDetectionService = new PhaseDetectionService();
}
