package com.learning.beam.transform.actions.validation;

import com.learning.beam.entity.Table;

import java.util.Map;

public interface ValidationConstraint {

    boolean validate(Table table, Map<String, String> validationActionConfigs);
}
