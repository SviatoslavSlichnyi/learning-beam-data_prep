package com.learning.beam.transform.actions;

import com.learning.beam.entity.Table;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GroupByActionTransform extends PTransform<PCollection<Table>, PCollection<Table>> {
    @Override
    public PCollection<Table> expand(PCollection<Table> input) {
        List<PCollection<Table>> mappedTables = new ArrayList<>();

        // get from ProfileConfigs List<> layouts which contains "_AND_"
//        Map<String, Map<String, String>> layouts = ProfileConfigs.getLayouts();
        Map<String, Map<String, String>> layouts = null;

        // forEach of layouts
        for (Map.Entry<String, Map<String, String>> layout : layouts.entrySet()) {
            // split layout<USER_AND_BALANCE> into List<> types "USER", "BALANCE"...
            List<String> recordTypes = Arrays.asList(layout.getKey().split("_AND_"));

            // filter input: contains at least one of List<> types
            PCollection<Table> filteredTables = input.apply(Filter.by(
                    (SerializableFunction<Table, Boolean>) table -> recordTypes.contains(table.getType())));

            // groupBy "<gropingKey>" from ProfileConfigs | example: "accountId" field
            // KV<gropingKey, Table>
            Map<String, String> layoutConfigs = layout.getValue();

            filteredTables.apply(WithKeys.of((SerializableFunction<Table, Object>) table -> table.get(null)));

            // make KV<gropingKey, Iterable<Table>>

            // map KV<gropingKey, Iterable<Table>> => Table<USER_AND_BALANCE>
        }

        PCollectionList<Table> mappedTablePCollectionList = PCollectionList.of(mappedTables);
        return mappedTablePCollectionList.apply(Flatten.pCollections());
    }
}
