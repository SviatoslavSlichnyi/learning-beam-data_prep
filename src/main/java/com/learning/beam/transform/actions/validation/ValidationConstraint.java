package com.learning.beam.transform.actions.validation;

import com.learning.beam.entity.Table;
import com.learning.beam.entity.config.ProfileConfig;

public interface ValidationConstraint {

    boolean validate(Table table, ProfileConfig.Action.ValidationAction validationActionConfigs);
}
