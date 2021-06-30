package com.learning.beam.entity;

public class SchemaFieldConfig implements ISchemaFieldConfig {

    private String mappingValue;
    private final String fieldType;

    public SchemaFieldConfig(String mappingValue, String fieldType) {
        this.mappingValue = mappingValue;
        this.fieldType = fieldType;
    }


    @Override
    public String getMappingValue() {
        return mappingValue;
    }

    @Override
    public void setMappingValue(String mappingValue) {
        this.mappingValue = mappingValue;
    }

    @Override
    public String getFieldName() {
        int dotIndex = mappingValue.indexOf('.');
        if (dotIndex == -1) return mappingValue;

        else return mappingValue.substring(0, dotIndex);
    }

    @Override
    public String getFieldType() {
        return fieldType;
    }

    @Override
    public boolean isField() {
        return !isObject();
    }

    @Override
    public boolean isObject() {
        return mappingValue.contains(".");
    }

    @Override
    public SchemaFieldConfig getField() {
        String mappingVal;

        int dotIndex = mappingValue.indexOf('.');
        if (dotIndex == -1) mappingVal = mappingValue;
        else mappingVal = mappingValue.substring(0, dotIndex);

        return new SchemaFieldConfig(mappingVal, fieldType);
    }

    @Override
    public SchemaFieldConfig getChildFieldConfig() {
        if (this.isField())
            throw new RuntimeException("it is not Object. It is field. ChildFiledConfig can not be made from field)");

        int dotIndex = mappingValue.indexOf('.');
        String mappingVal = mappingValue.substring(dotIndex + 1);
        return new SchemaFieldConfig(mappingVal, fieldType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaFieldConfig that = (SchemaFieldConfig) o;

        return mappingValue != null ? mappingValue.equals(that.mappingValue) : that.mappingValue == null;
    }

    @Override
    public int hashCode() {
        return mappingValue != null ? mappingValue.hashCode() : 0;
    }
}
