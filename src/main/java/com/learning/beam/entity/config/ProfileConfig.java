package com.learning.beam.entity.config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Getter
@ToString
public class ProfileConfig {
    private final List<Layout> layouts;
    private final Action action;

    @AllArgsConstructor
    @Getter
    @ToString
    public static class Layout {
        private final String layoutType;
        private final Map<String, String> fieldTypes;
    }

    @AllArgsConstructor
    @Getter
    @ToString
    public static class Action {
        private final List<ValidationAction> validationActions;
        private final List<GroupByAction> groupByActions;
        private final List<MapToAvroAction> mapToAvroActions;

        @AllArgsConstructor
        @Getter
        @ToString
        public static class ValidationAction {
            private final String type;
            private final String recordType;
            private final String field;
            private final String constraint;
        }

        @AllArgsConstructor
        @Getter
        @ToString
        public static class GroupByAction {
            private final List<String> recordTypes;
            private final String gropingKey;
            private final String resultRecordName;
        }

        @AllArgsConstructor
        @Getter
        @ToString
        public static class MapToAvroAction {
            private final String sourceLayout;
            private final String targetSchema;
            private final Map<String, String> mapping;
        }
    }
}
