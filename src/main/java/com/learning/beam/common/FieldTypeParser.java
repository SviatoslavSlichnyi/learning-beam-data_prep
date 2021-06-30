package com.learning.beam.common;

public class FieldTypeParser {
    public static Object parseField(String fieldValue, String fieldType) {
        switch (fieldType) {
            case "byte": return Byte.valueOf(fieldValue);
            case "short": return Short.valueOf(fieldValue);
            case "int": return Integer.valueOf(fieldValue);
            case "long": return Long.valueOf(fieldValue);
            case "float": return Float.valueOf(fieldValue);
            case "double": return Double.valueOf(fieldValue);
            case "boolean": return Boolean.valueOf(fieldValue);
            case "char": return fieldValue.charAt(0);
            case "string": return fieldValue;
            default: throw new RuntimeException(fieldType + " is not supported");
        }
    }
}
