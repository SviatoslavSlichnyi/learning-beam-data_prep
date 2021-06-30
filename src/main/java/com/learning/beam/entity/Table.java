package com.learning.beam.entity;

import java.io.Serializable;
import java.util.Map;

public interface Table extends Serializable {

    String getType();

    String get(String fieldName);

    Map<String, String> getRows();

    String getFieldType(String fieldName);

    Map<String, String> getFieldsType();

    boolean containsField(String fieldName);

    boolean containsFieldType(String fieldName);
}
