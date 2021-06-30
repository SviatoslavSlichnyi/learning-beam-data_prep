package com.learning.beam.transform.actions.validation;

import com.learning.beam.entity.Table;
import com.learning.beam.entity.config.ProfileConfig;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

public class Positive implements ValidationConstraint {
    @Override
    public boolean validate(Table table, ProfileConfig.Action.ValidationAction validationActionConfigs) {
        String fieldName = validationActionConfigs.getField(); // example: balance

        // validate: field exists
        boolean containsField = table.containsField(fieldName);
        if (!containsField) return false;

        // validate: fieldType exists
        boolean containsFieldType = table.containsFieldType(fieldName);
        if (!containsFieldType) return false;

        // validate: fieldType number
        String fieldType = table.getFieldType(fieldName);
        if (isNotNumberType(fieldType)) return false;

        String fieldValueString = table.get(fieldName);

        return isPositive(fieldValueString);
    }

    private boolean isNotNumberType(String fieldType) {
        return !isNumberType(fieldType);
    }

    private boolean isNumberType(String fieldType) {
        List<String> numberFieldTypes = Arrays.asList(
                "byte",
                "short",
                "int",
                "long",
                "float",
                "double"
        );

        return numberFieldTypes.contains(fieldType);
    }

    private boolean isPositive(String fieldValueString) {
        BigDecimal number = new BigDecimal(fieldValueString);
        return number.compareTo(BigDecimal.ZERO) > 0;
    }
}
