package com.learning.beam.entity;

public interface ISchemaFieldConfig {
    String getMappingValue();
    void setMappingValue(String mappingValue);
    String getFieldName();
    String getFieldType();

    boolean isField();
    boolean isObject();

    ISchemaFieldConfig getField();
    ISchemaFieldConfig getChildFieldConfig();
}
