package com.learning.beam.transform.actions;

import com.learning.beam.common.OptionalHelper;
import com.learning.beam.common.ProfileConfigsHelper;
import com.learning.beam.entity.Table;
import com.learning.beam.entity.config.ProfileConfig;
import com.learning.beam.option.DataPrepOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.coders.StringDelegateCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.util.List;
import java.util.Map;

public class MapToAvroActionTransform extends PTransform<PCollection<Table>, PDone> {

    private final DataPrepOptions options;

    public MapToAvroActionTransform() {
        options = OptionalHelper.getOptions();
    }

    @Override
    public PDone expand(PCollection<Table> input) {
        // List<> "mapToAvro" actions
        List<ProfileConfig.Action.MapToAvroAction> mapToAvroActions =
                ProfileConfigsHelper.getActions().getMapToAvroActions();

        // forEach actions
        for (ProfileConfig.Action.MapToAvroAction action : mapToAvroActions) {

            // filter input
            PCollection<Table> filteredTables = input.apply(Filter.by((SerializableFunction<Table, Boolean>)
                    table -> table.getType().equals(action.getSourceLayout())));

            // map Table -> GenericRecord
            // key: recordType
            // value: GenericRecord
            PCollection<KV<String, GenericRecord>> records = filteredTables.apply(
                    MapElements.via(mapTableToKVFn()));

            // write into file
            records.apply("Writing to Files",
                FileIO.<String, KV<String, GenericRecord>>writeDynamic()
                    .withDestinationCoder(StringDelegateCoder.of(String.class))
                    .by((SerializableFunction<KV<String, GenericRecord>, String>) KV::getKey)
                    .via(
                        Contextful.fn((SerializableFunction<KV<String, GenericRecord>, IndexedRecord>) KV::getValue),
                        AvroIO.sink(action.getSchema())
                    )
                    .withNaming(FileIO.Write.defaultNaming(action.getTargetSchema(), ".avro"))
                    .to(options.getOutput())
            );

        }

        return PDone.in(input.getPipeline());
    }

    public static SimpleFunction<Table, KV<String, GenericRecord>> mapTableToKVFn() {
        return new SimpleFunction<>() {
            @Override
            public KV<String, GenericRecord> apply(Table table) { return mapTableToKV(table); }
        };
    }

    public static KV<String, GenericRecord> mapTableToKV(Table table) {
        Map<String, String> rows = table.getRows();

        // todo: map rows -> GenericRecord
        GenericRecord record = null;

        return KV.of(table.getType(), record);
    }
}
