package com.learning.beam.tranform;

import com.learning.beam.entity.MapTable;
import com.learning.beam.entity.Table;
import com.learning.beam.tranform.actions.GroupByActionTransform;
import com.learning.beam.tranform.actions.MapToAvroActionTransform;
import com.learning.beam.tranform.actions.ValidationActionTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ApplyActionsTransform extends PTransform<PCollection<Table>, PDone> {

    /**
     * key: recordType
     * value: type item from action list
     */
    private final Map<String, List<Map<String, String>>> actionsByRecordType;

    /**
     * key: actions.type
     * value: a class which realize needed functionality
     */
    private final Map<String, PTransform<PCollection<Table>, ? extends POutput>> actionsTransforms;

    public ApplyActionsTransform(List<Map<String, String>> actions) {
        this.actionsByRecordType = groupActionsByType(actions);
        this.actionsTransforms = initializeActionTypes();
    }

    private Map<String, List<Map<String, String>>> groupActionsByType(List<Map<String, String>> actions) {
        return actions.stream()
                .collect(Collectors.groupingBy(o -> o.get("recordType")));
    }

    private Map<String, PTransform<PCollection<Table>, ? extends POutput>> initializeActionTypes() {
        Map<String, PTransform<PCollection<Table>, ? extends POutput>> map = new HashMap<>();

        map.put("validate", new ValidationActionTransform());
        map.put("groupBy", new GroupByActionTransform());
        map.put("mapToAvro", new MapToAvroActionTransform());

        return map;
    }

    @Override
    public PDone expand(PCollection<Table> input) {
        Table table = new MapTable(null, null, null);
        String recordType = table.getType();
        List<Map<String, String>> actionsToExecute = actionsByRecordType.get(recordType);
        actionsToExecute.forEach(acts -> {
            PTransform<PCollection<Table>, ? extends POutput> pTransform = actionsTransforms.get(acts.get("type"));
            input.apply(pTransform);
        });

        return PDone.in(input.getPipeline());
    }
}
