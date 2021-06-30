package com.learning.beam.common;

import com.learning.beam.entity.ISchemaFieldConfig;
import com.learning.beam.entity.SchemaFieldConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MappingSchemaBuilder {

    private static final String ENTITY_PACKAGE = "com.learning.beam.entity";

    public static Schema parse(String recordName, Map<String, String> mappingProfileConfigMap, Map<String, String> fieldTypes) {
        List<SchemaFieldConfig> mappings = mappingProfileConfigMap.entrySet().stream()
                .map(mappingEntry -> {
                    String mapping = mappingEntry.getValue();
                    String type = fieldTypes.get(mappingEntry.getKey());
                    return new SchemaFieldConfig(mapping, type);
                })
                .collect(Collectors.toList());

        return parseSchemaByPaths(recordName, mappings);
    }

    private static Schema parseSchemaByPaths(String recordName, List<SchemaFieldConfig> mappings) {
        SchemaBuilder.RecordBuilder<Schema> recordSchemaBuilder = SchemaBuilder.record(recordName).namespace(ENTITY_PACKAGE);
        SchemaBuilder.FieldAssembler<Schema> recordFields = recordSchemaBuilder.fields();

        // key: field name ("user" or "account"...)
        // value: paths for sub fields ("public.accountId", "private.balance"...)
        Map<SchemaFieldConfig, List<SchemaFieldConfig>> fieldsWithValues = groupByFirstWordBeforeDot(mappings);

        for (Map.Entry<SchemaFieldConfig, List<SchemaFieldConfig>> field : fieldsWithValues.entrySet()) {
            ISchemaFieldConfig fieldConfig = field.getKey();
            String fieldName = fieldConfig.getFieldName();

            if (isField(field)) {
                // is field is string: string (like "name": "Ivan")
                recordFields = recordFields
                        .name(fieldName)
                        .type(fieldConfig.getFieldType())
                        .noDefault();
            } else {
                // is object is string: {...} (like "user": {...})
                Schema fieldSchema = parseSchemaByPaths(fieldName, field.getValue());
                recordFields = recordFields.name(fieldName).type(fieldSchema).noDefault();
            }

        }

        return recordFields.endRecord();
    }

    private static boolean isField(Map.Entry<SchemaFieldConfig, List<SchemaFieldConfig>> field) {
        return field.getValue() == null || field.getValue().isEmpty();
    }

    private static Map<SchemaFieldConfig, List<SchemaFieldConfig>> groupByFirstWordBeforeDot(List<SchemaFieldConfig> jsonFieldPaths) {
        Map<SchemaFieldConfig, List<SchemaFieldConfig>> result = new HashMap<>();

        for (SchemaFieldConfig fieldConfig : jsonFieldPaths) {
            SchemaFieldConfig field = fieldConfig.getField();

            if (fieldConfig.isField()) {
                result.put(field, null);
            } else {
                List<SchemaFieldConfig> paths = result.computeIfAbsent(field, k -> new ArrayList<>());
                SchemaFieldConfig childFieldConfig = fieldConfig.getChildFieldConfig();
                paths.add(childFieldConfig);
            }
        }

        return result;
    }

}
