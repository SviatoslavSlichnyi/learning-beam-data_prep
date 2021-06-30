package com.learning.beam.transform.actions.validation;

import com.learning.beam.entity.Table;
import com.learning.beam.entity.config.ProfileConfig;
import org.apache.logging.log4j.util.Strings;

public class NonNull implements ValidationConstraint {

    @Override
    public boolean validate(Table table, ProfileConfig.Action.ValidationAction validationActionConfigs) {
        String fieldName = validationActionConfigs.getField(); // example: accountId

        boolean containsField = table.containsField(fieldName);
        if (!containsField) return false;

        String fieldValue = table.get(fieldName);
        return Strings.isNotBlank(fieldValue);
    }
}
