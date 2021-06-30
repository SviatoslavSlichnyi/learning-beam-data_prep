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
            String jsonSchema = action.getSchema().toString();
            String targetSchema = action.getTargetSchema();
            String sourceLayout = action.getSourceLayout();

            // filter input
            PCollection<Table> filteredTables = input.apply(Filter.by((SerializableFunction<Table, Boolean>)
                    table -> table.getType().equals(sourceLayout)));

            // map Table -> GenericRecord
            // key: recordType
            // value: GenericRecord
            PCollection<KV<String, GenericRecord>> records = filteredTables.apply(
                    MapElements.via(mapTableToKVFn(action.getSchema().toString())))
                    .setCoder(KvCoder.of(StringDelegateCoder.of(String.class), AvroCoder.of(GenericRecord.class, action.getSchema())));

            // write into file
            records.apply("Writing to Files",
                FileIO.<String, KV<String, GenericRecord>>writeDynamic()
                    .withDestinationCoder(StringUtf8Coder.of())
                    .by((SerializableFunction<KV<String, GenericRecord>, String>) KV::getKey)
                    .via(
                        Contextful.fn((SerializableFunction<KV<String, GenericRecord>, GenericRecord>) KV::getValue),
                        AvroIO.sink(jsonSchema)
                    )
                    .withNaming(key -> FileIO.Write.defaultNaming(targetSchema, ".avro"))
                    .to(options.getOutput())
            );

        }


        return input;
    }

    public static SimpleFunction<Table, KV<String, GenericRecord>> mapTableToKVFn(String avroSchema) {
        return new SimpleFunction<>() {
            @Override
            public KV<String, GenericRecord> apply(Table table) { return mapTableToKV(table, avroSchema); }
        };
    }

    public static KV<String, GenericRecord> mapTableToKV(Table table, String avroSchema) {
        Map<String, String> rows = table.getRows();

        Schema schema = new Schema.Parser().parse(avroSchema);
        org.apache.beam.sdk.schemas.Schema beamSchema = AvroUtils.toBeamSchema(schema);
        Row row = Row.withSchema(beamSchema)
                .withFieldValue("user.name", table.get("name"))
                .withFieldValue("user.age", Long.valueOf(table.get("age")))
                .withFieldValue("account.number", table.get("accountId"))
                .withFieldValue("account.description", table.get("description"))
                .withFieldValue("account.balance", Long.valueOf(table.get("balance")))
                .build();
        GenericRecord record = AvroUtils.toGenericRecord(row, schema);

        return KV.of(table.getType(), record);
    }
}
