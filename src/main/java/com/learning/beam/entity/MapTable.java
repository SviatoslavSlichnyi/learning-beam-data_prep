package com.learning.beam.entity;

import java.util.Map;

public class MapTable implements Table {

    String type;

    /**
     * key: field name
     * value: row value
     */
    Map<String, String> rows;

    /**
     * key: field name
     * value: field type
     */
    Map<String, String> fieldsType;

    public MapTable(String type, Map<String, String> rows, Map<String, String> fieldsType) {
        this.type = type;
        this.rows = rows;
        this.fieldsType = fieldsType;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public String get(String fieldName) {
        return rows.get(fieldName);
    }

    @Override
    public String getFieldType(String fieldName) {
        return fieldsType.get(fieldName);
    }

    @Override
    public boolean containsField(String fieldName) {
        return rows.containsKey(fieldName);
    }

    @Override
    public boolean containsFieldType(String fieldName) {
        return fieldsType.containsKey(fieldName);
    }
}
