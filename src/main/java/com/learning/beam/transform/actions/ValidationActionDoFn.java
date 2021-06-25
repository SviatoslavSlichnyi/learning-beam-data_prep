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

public class ValidationActionDoFn extends DoFn<Table, Table> {

    private final Map<String, ValidationConstraint> constraintList;
    private final Map<String, List<Map<String, String>>> actionsByRecordType;

    public ValidationActionDoFn() {
        this.constraintList = initValidationRules();
        List<Map<String, String>> actions = ProfileConfigs.getActions();
        this.actionsByRecordType = groupValidationActionsByRecordType(actions);
    }

    private Map<String, List<Map<String, String>>> groupValidationActionsByRecordType(List<Map<String, String>> actions) {
        return actions.stream()
                .filter(act -> act.get("type").equals("validate"))
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
        String recordType = table.getType(); // BALANCE
        List<Map<String, String>> actionToApply = actionsByRecordType.get(recordType);

        boolean isValid = true;

        for (Map<String, String> act : actionToApply) {
            String constraint = act.get("constraint");
            ValidationConstraint validationConstraint = constraintList.get(constraint);
            boolean validationResult = validationConstraint.validate(table, act);
            if (!validationResult) {
                isValid = false;
                break;
            }
        }

        if (isValid) receiver.output(table);
    }
}