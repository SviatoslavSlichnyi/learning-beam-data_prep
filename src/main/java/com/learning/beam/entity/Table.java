package com.learning.beam.entity;

public interface Table {

    String getType();

    String get(String fieldName);

    String getFieldType(String fieldName);
}
