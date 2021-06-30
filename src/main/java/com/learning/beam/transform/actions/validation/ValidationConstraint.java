package com.learning.beam.transform.actions.validation;

import com.learning.beam.entity.Table;
import com.learning.beam.entity.config.ProfileConfig;

import java.io.Serializable;

public interface ValidationConstraint extends Serializable {

    boolean validate(Table table, ProfileConfig.Action.ValidationAction validationActionConfigs);
}
