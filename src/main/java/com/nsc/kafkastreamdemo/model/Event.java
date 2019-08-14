package com.nsc.kafkastreamdemo.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor // needed for kafka stream, otherwise deserialize from Object value (no delegate- or property-based Creator
@AllArgsConstructor
public class Event {
    private String name;
}
