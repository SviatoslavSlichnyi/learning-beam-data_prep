package com.learning.beam.transform.actions;

import com.learning.beam.common.OptionalHelper;
import com.learning.beam.common.ProfileConfigsHelper;
import com.learning.beam.entity.Table;
import com.learning.beam.entity.config.ProfileConfig;
import com.learning.beam.option.DataPrepOptions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringDelegateCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MapToAvroActionTransform extends PTransform<PCollection<Table>, PCollection<Table>> {

    private final DataPrepOptions options;

    public MapToAvroActionTransform() {
        options = OptionalHelper.getOptions();
    }

    @Override
    public PCollection<Table> expand(PCollection<Table> input) {
        // List<> "mapToAvro" actions
        List<ProfileConfig.Action.MapToAvroAction> mapToAvroActions =
                ProfileConfigsHelper.getActions().getMapToAvroActions();

        // forEach actions
        for (ProfileConfig.Action.MapToAvroAction action : mapToAvroActions) {
            String jsonSchema = action.getAvroSchema().toString();
            String[] naming = action.getTargetSchema().split("[.]");
            String sourceLayout = action.getSourceLayout();

            // filter input
            PCollection<Table> filteredTables = input.apply(Filter.by((SerializableFunction<Table, Boolean>)
                    table -> table.getType().equals(sourceLayout)));

            // map Table -> GenericRecord
            // key: recordType
            // value: GenericRecord
            PCollection<KV<String, GenericRecord>> records = filteredTables.apply(
                    MapElements.via(mapTableToKVFn(action.getAvroSchema().toString(), action.getMapping())))
                    .setCoder(KvCoder.of(StringDelegateCoder.of(String.class), AvroCoder.of(GenericRecord.class, action.getAvroSchema())));

            // write into file
            records.apply("Writing to Files",
                FileIO.<String, KV<String, GenericRecord>>writeDynamic()
                    .withDestinationCoder(StringUtf8Coder.of())
                    .by((SerializableFunction<KV<String, GenericRecord>, String>) KV::getKey)
                    .via(
                        Contextful.fn((SerializableFunction<KV<String, GenericRecord>, GenericRecord>) KV::getValue),
                        AvroIO.sink(jsonSchema)
                    )
                    .withNaming(key -> FileIO.Write.defaultNaming(naming[0], "." + naming[1]))
                    .to(options.getOutput())
            );

        }


        return input;
    }

    public static SimpleFunction<Table, KV<String, GenericRecord>> mapTableToKVFn(String avroSchema, Map<String, String> mapping) {
        return new SimpleFunction<>() {
            @Override
            public KV<String, GenericRecord> apply(Table table) { return mapTableToKV(table, avroSchema, mapping); }
        };
    }

    public static KV<String, GenericRecord> mapTableToKV(Table table, String avroSchema, Map<String, String> mapping) {
        Schema schema = new Schema.Parser().parse(avroSchema);
        org.apache.beam.sdk.schemas.Schema beamSchema = AvroUtils.toBeamSchema(schema);

        Map<String, Object> fieldValues = mapping.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getValue,
                        entry -> {
                            String fieldName = entry.getKey();
                            return parseField(table.get(fieldName), table.getFieldType(fieldName));
                        })
                );

        Row row = Row.withSchema(beamSchema).withFieldValues(fieldValues).build();
        GenericRecord record = AvroUtils.toGenericRecord(row, schema);
        return KV.of(table.getType(), record);
    }

    private static Object parseField(String fieldValue, String fieldType) {
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
