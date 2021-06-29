package com.learning.beam.transform;

import com.learning.beam.common.ProfileConfigsHelper;
import com.learning.beam.entity.MapTable;
import com.learning.beam.entity.Table;
import com.learning.beam.entity.config.ProfileConfig;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.*;
import java.util.stream.Collectors;

public class ConvertCsvToTableTransform extends PTransform<PCollection<String>, PCollection<Table>> {

    private final Map<String, ProfileConfig.FieldTypes> fieldTypesGroupedByRecordType;

    public ConvertCsvToTableTransform() {
        this.fieldTypesGroupedByRecordType = initFieldTypes();
    }

    private Map<String, ProfileConfig.FieldTypes> initFieldTypes() {
        return ProfileConfigsHelper.getLayouts();
    }

    @Override
    public PCollection<Table> expand(PCollection<String> input) {
        // convert a line => new Table();
        return input.apply(MapElements.into(TypeDescriptor.of(Table.class)).via(line -> {
            Queue<String> columns = Arrays.stream(line.split("[,]"))
                    .map(String::trim)
                    .collect(Collectors.toCollection(LinkedList::new));

            String type = columns.poll();
            Map<String, String> fieldsTypes = fieldTypesGroupedByRecordType.get(type).getTypes();
            Map<String, String> rows = combineFieldNamesAndValuesIntoMap(fieldsTypes, columns);

            return new MapTable(type, rows, fieldsTypes);
        }));
    }

    private Map<String, String> combineFieldNamesAndValuesIntoMap(Map<String, String> fieldsTypes,
                                                                  Queue<String> columns) {
        Set<String> keySet = fieldsTypes.keySet();
        if (keySet.size() != columns.size())
            throw new IllegalArgumentException();

        Map<String, String> rows = new HashMap<>();
        keySet.forEach(fieldName -> rows.put(fieldName, columns.poll()));

        return rows;
    }
}
