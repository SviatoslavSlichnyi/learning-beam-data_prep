package com.learning.beam.transform.actions;

import com.learning.beam.common.ProfileConfigsHelper;
import com.learning.beam.entity.MapTable;
import com.learning.beam.entity.Table;
import com.learning.beam.entity.config.ProfileConfig.Action.GroupByAction;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringDelegateCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.*;

public class GroupByActionTransform extends PTransform<PCollection<Table>, PCollection<Table>> {
    @Override
    public PCollection<Table> expand(PCollection<Table> input) {
        List<PCollection<Table>> mappedTables = new ArrayList<>();

        // get from ProfileConfigs List<> actions with "groupBy"
        List<GroupByAction> groupByActions = ProfileConfigsHelper.getProfileConfig().getAction().getGroupByActions();

        // forEach of GroupBy actions
        for (GroupByAction action : groupByActions) {
            // List<> types "USER", "BALANCE"...
             List<String> recordTypes = action.getRecordTypes();

            // filter input: contains at least one of List<> types
            PCollection<Table> filteredTables = input.apply(Filter.by(
                    (SerializableFunction<Table, Boolean>) table -> recordTypes.contains(table.getType())));

            // groupBy "<gropingKey>" from ProfileConfigs | example: "accountId" field
            // KV<gropingKey, Table>
            String gropingKey = action.getGropingKey();
            PCollection<KV<String, Table>> convertTableToKVWithGroupingKey = filteredTables.apply(WithKeys.of(
                    (SerializableFunction<Table, String>) table -> table.get(gropingKey)))
                    .setCoder(KvCoder.of(StringDelegateCoder.of(String.class), SerializableCoder.of(Table.class)));

            // make KV<gropingKey, Iterable<Table>>
            PCollection<KV<String, Iterable<Table>>> groupedTables = convertTableToKVWithGroupingKey.apply(GroupByKey.create())
                    .setCoder(KvCoder.of(StringDelegateCoder.of(String.class), IterableCoder.of(SerializableCoder.of(Table.class))));

            // map KV<gropingKey, Iterable<Table>> => Table<USER_AND_BALANCE>
            PCollection<Table> tablePCollection = groupedTables.apply(MapElements.into(TypeDescriptor.of(Table.class)).via(groupedTableKV -> {
                Map<String, String> rows = new HashMap<>();
                Map<String, String> fieldTypes = new HashMap<>();

                Objects.requireNonNull(groupedTableKV.getValue()).forEach(table -> {
                    rows.putAll(table.getRows());
                    fieldTypes.putAll(table.getFieldsType());
                });

                return new MapTable(action.getResultRecordName(), rows, fieldTypes);
            }));

            mappedTables.add(tablePCollection);
        }

        PCollectionList<Table> mappedTablePCollectionList = PCollectionList.of(mappedTables);
        return mappedTablePCollectionList.apply(Flatten.pCollections());
    }
}
