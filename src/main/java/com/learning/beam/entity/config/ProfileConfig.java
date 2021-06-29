package com.learning.beam.entity.config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.avro.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public final class ProfileConfig implements Serializable {

    private ProfileConfig() {}

    @AllArgsConstructor
    @Getter
    @ToString
    public static class FieldTypes implements Serializable {
        private final Map<String, String> types;
    }

    @AllArgsConstructor
    @Getter
    @ToString
    public static class Action implements Serializable {
        private final List<ValidationAction> validationActions;
        private final List<GroupByAction> groupByActions;
        private final List<MapToAvroAction> mapToAvroActions;

        @AllArgsConstructor
        @Getter
        @ToString
        public static class ValidationAction implements Serializable {
            private final String type;
            private final String recordType;
            private final String field;
            private final String constraint;
        }

        @AllArgsConstructor
        @Getter
        @ToString
        public static class GroupByAction implements Serializable {
            private final List<String> recordTypes;
            private final String gropingKey;
            private final String resultRecordName;
        }

        @AllArgsConstructor
        @Getter
        @ToString
        public static class MapToAvroAction implements Serializable {
            private final String sourceLayout;
            private final String targetSchema;
            private final Schema schema;
        }
    }
}
