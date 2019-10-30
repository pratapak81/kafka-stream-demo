package com.nsc.kafkastreamdemo.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CustomEvent {
    private String tenantId;
    private String location;
    private EventType eventType;
}
