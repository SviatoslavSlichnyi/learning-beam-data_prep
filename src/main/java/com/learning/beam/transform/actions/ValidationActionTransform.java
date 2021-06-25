package com.learning.beam.transform.actions;

import com.learning.beam.common.ProfileConfigs;
import com.learning.beam.entity.Table;
import com.learning.beam.transform.actions.validation.NonNull;
import com.learning.beam.transform.actions.validation.Positive;
import com.learning.beam.transform.actions.validation.ValidationConstraint;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ValidationActionTransform extends DoFn<Table, Table> {

    private final Map<String, ValidationConstraint> constraintList;
    private final Map<String, List<Map<String, String>>> actionsByRecordType;

    public ValidationActionTransform() {
        this.constraintList = initValidationRules();
        List<Map<String, String>> actions = ProfileConfigs.getActions();
        this.actionsByRecordType = groupActionsByRecordType(actions);
    }

    private Map<String, List<Map<String, String>>> groupActionsByRecordType(List<Map<String, String>> actions) {
        return actions.stream()
                .collect(Collectors.groupingBy(act -> act.get("recordType")));
    }

    private static Map<String, ValidationConstraint> initValidationRules() {
        Map<String, ValidationConstraint> constraints = new HashMap<>();

        constraints.put("com.learning.beam.transform.actions.validation.NonNull", new NonNull());
        constraints.put("com.learning.beam.transform.actions.validation.Positive", new Positive());

        return constraints;
    }


    @ProcessElement
    public void processElement(@Element Table table, OutputReceiver<Table> receiver) {
        String recordType = table.getType();
        List<Map<String, String>> actionToApply = actionsByRecordType.get(recordType);

        actionToApply.forEach(act -> {
            String constraint = act.get("constraint");
            ValidationConstraint validationConstraint = constraintList.get(constraint);
            validationConstraint.validate(table, act);
        });

        receiver.output(table);
    }
}
