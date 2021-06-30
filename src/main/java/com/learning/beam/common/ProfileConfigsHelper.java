package com.learning.beam.common;

import com.learning.beam.entity.config.ProfileConfig;
import com.learning.beam.option.DataPrepOptions;
import org.apache.avro.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProfileConfigsHelper {

    private static Map<String, ProfileConfig.FieldTypes> layouts;
    private static ProfileConfig.Action actions;

    /**
     * Article: https://www.baeldung.com/java-snake-yaml
     *
     * @param options
     * @throws IOException
     */
    public static void initWithOptions(DataPrepOptions options) throws IOException {
        Map<String, Object> profileConfigsMap = SnakeYamlReader.readYamlFile(options.getProfile());
        layouts = parseLayouts((Map<String, Map<String, String>>) profileConfigsMap.get("layouts"));
        actions = parseAction((List<Map<String, ?>>) profileConfigsMap.get("actions"));
    }

    private static Map<String, ProfileConfig.FieldTypes> parseLayouts(Map<String, Map<String, String>> layouts) {
        return layouts.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> new ProfileConfig.FieldTypes(entry.getValue())));
    }

    private static ProfileConfig.Action parseAction(List<Map<String, ?>> actions) {
        List<ProfileConfig.Action.ValidationAction> validationActions = parseActionValidationList(actions);
        List<ProfileConfig.Action.GroupByAction> groupByActionList = parseActionGroupByList(actions);
        List<ProfileConfig.Action.MapToAvroAction> mapToAvroActions = parseActionMapToAvroList(actions);

        return new ProfileConfig.Action(validationActions, groupByActionList, mapToAvroActions);
    }

    private static List<ProfileConfig.Action.ValidationAction> parseActionValidationList(List<Map<String, ?>> actions) {
        return actions.stream()
                .filter(act -> act.get("type").equals("validate"))
                .map(act -> parseActionValidation((Map<String, String>) act))
                .collect(Collectors.toList());
    }

    private static ProfileConfig.Action.ValidationAction parseActionValidation(Map<String, String> act) {
        String type = act.get("type");
        String recordType = act.get("recordType");
        String field = act.get("field");
        String constraint = act.get("constraint");

        return new ProfileConfig.Action.ValidationAction(type, recordType, field, constraint);
    }

    private static List<ProfileConfig.Action.GroupByAction> parseActionGroupByList(List<Map<String, ?>> actions) {
        return actions.stream()
                .filter(act -> act.get("type").equals("groupBy"))
                .map(act -> parseActionGroupBy((Map<String, Object>) act))
                .collect(Collectors.toList());
    }

    private static ProfileConfig.Action.GroupByAction parseActionGroupBy(Map<String, Object> act) {
        List<String> recordTypes = (List<String>) act.get("recordTypes");
        String gropingKey = (String) act.get("gropingKey");
        String resultRecordName = (String) act.get("resultRecordName");

        return new ProfileConfig.Action.GroupByAction(recordTypes, gropingKey, resultRecordName);
    }

    private static List<ProfileConfig.Action.MapToAvroAction> parseActionMapToAvroList(List<Map<String, ?>> actions) {
        return actions.stream()
                .filter(act -> act.get("type").equals("mapToAvro"))
                .map(act -> parseActionMapToAvro((Map<String, Object>) act))
                .collect(Collectors.toList());
    }

    private static ProfileConfig.Action.MapToAvroAction parseActionMapToAvro(Map<String, Object> act) {
        String sourceLayout = (String) act.get("sourceLayout");
        String targetSchema = (String) act.get("targetSchema");
        Map<String, String> mapping = (Map<String, String>) act.get("mapping");
        Schema avroSchema = parseActionMapMappingToSchema(act, mapping);
        org.apache.beam.sdk.schemas.Schema beamSchema = AvroUtils.toBeamSchema(avroSchema);

        return new ProfileConfig.Action.MapToAvroAction(sourceLayout, targetSchema, mapping, avroSchema, beamSchema);
    }

    private static Schema parseActionMapMappingToSchema(Map<String, Object> act, Map<String, String> mapping) {
        if (layouts == null) throw new RuntimeException("layouts must be initialize before actions");

        Map<String, String> fieldTypes = layouts.get((String) act.get("sourceLayout")).getTypes();
        String targetSchema = (String) act.get("targetSchema");
        String recordName = targetSchema.substring(0, targetSchema.indexOf('.'));

        return MappingSchemaBuilder.parse(recordName, mapping, fieldTypes);
    }

    public static Map<String, ProfileConfig.FieldTypes> getLayouts() {
        return layouts;
    }

    public static ProfileConfig.Action getActions() {
        return actions;
    }
}
