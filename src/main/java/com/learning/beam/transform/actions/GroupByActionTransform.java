package com.learning.beam.transform.actions;

import com.learning.beam.common.ProfileConfigsHelper;
import com.learning.beam.entity.MapTable;
import com.learning.beam.entity.Table;
import com.learning.beam.entity.config.ProfileConfig;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.*;
import java.util.stream.Collectors;

public class GroupByActionTransform extends PTransform<PCollection<Table>, PCollection<Table>> {
    @Override
    public PCollection<Table> expand(PCollection<Table> input) {
        List<PCollection<Table>> mappedTables = new ArrayList<>();

        // get from ProfileConfigs List<> layouts which contains "_AND_"
        Map<String, ProfileConfig.FieldTypes> layoutList = ProfileConfigsHelper.getProfileConfig().getLayouts();
        Set<String> layoutTypes = layoutList.keySet();
        List<String> andLayoutTypes = layoutTypes.stream()
                .filter(layoutType -> layoutType.equals("_AND_"))
                .collect(Collectors.toList());

        // forEach of layouts
        for (String layoutType : andLayoutTypes) {
            // split layout<USER_AND_BALANCE> into List<> types "USER", "BALANCE"...
            List<String> recordTypes = Arrays.asList(layoutType.split("_AND_"));

            // filter input: contains at least one of List<> types
            PCollection<Table> filteredTables = input.apply(Filter.by(
                    (SerializableFunction<Table, Boolean>) table -> recordTypes.contains(table.getType())));

            // groupBy "<gropingKey>" from ProfileConfigs | example: "accountId" field
            // KV<gropingKey, Table>
            Map<String, String> layoutConfigs = layoutList.get(layoutType).getTypes();
            String gropingKey = layoutConfigs.get("gropingKey");
            PCollection<KV<String, Table>> convertTableToKVWithGroupingKey = filteredTables.apply(WithKeys.of(
                    (SerializableFunction<Table, String>) table -> table.get(gropingKey)));

            // make KV<gropingKey, Iterable<Table>>
            PCollection<KV<String, Iterable<Table>>> groupedTables = convertTableToKVWithGroupingKey.apply(GroupByKey.create());

            // map KV<gropingKey, Iterable<Table>> => Table<USER_AND_BALANCE>
            groupedTables.apply(MapElements.into(TypeDescriptor.of(Table.class)).via(groupedTableKV -> {
                Map<String, String> rows = new HashMap<>();
                Map<String, String> fieldTypes = new HashMap<>();

                Objects.requireNonNull(groupedTableKV.getValue()).forEach(table -> {
                    rows.putAll(table.getRows());
                    fieldTypes.putAll(table.getFieldsType());
                });

                return new MapTable(layoutType, rows, fieldTypes);
            }));

        }

        PCollectionList<Table> mappedTablePCollectionList = PCollectionList.of(mappedTables);
        return mappedTablePCollectionList.apply(Flatten.pCollections());
    }
}
