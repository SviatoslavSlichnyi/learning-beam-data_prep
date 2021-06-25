package com.learning.beam.transform.actions.validation;

import com.learning.beam.entity.Table;
import org.apache.logging.log4j.util.Strings;

import java.util.Map;

public class NonNull implements ValidationConstraint {

    @Override
    public boolean validate(Table table, Map<String, String> validationActionConfigs) {
        String fieldName = validationActionConfigs.get("field"); // example: accountId

        boolean containsField = table.containsField(fieldName);
        if (!containsField) return false;

        String fieldValue = table.get(fieldName);
        return Strings.isNotBlank(fieldValue);
    }
}
