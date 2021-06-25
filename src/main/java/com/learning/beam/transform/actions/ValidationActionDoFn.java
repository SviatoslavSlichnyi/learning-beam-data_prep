package com.learning.beam.transform.actions;

import com.learning.beam.common.ProfileConfigsHelper;
import com.learning.beam.entity.Table;
import com.learning.beam.entity.config.ProfileConfig;
import com.learning.beam.entity.config.ProfileConfig.Action.ValidationAction;
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
    private final Map<String, List<ValidationAction>> actionsByRecordType;

    public ValidationActionDoFn() {
        this.constraintList = initValidationRules();
        this.actionsByRecordType = groupValidationActionsByRecordType(ProfileConfigsHelper.getProfileConfig().getAction());
    }

    private Map<String, List<ValidationAction>> groupValidationActionsByRecordType(ProfileConfig.Action actions) {
        return actions.getValidationActions().stream()
                .collect(Collectors.groupingBy(ValidationAction::getRecordType));
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
        List<ValidationAction> actionToApply = actionsByRecordType.get(recordType);

        boolean isValid = true;

        for (ValidationAction act : actionToApply) {
            String constraint = act.getConstraint();
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
