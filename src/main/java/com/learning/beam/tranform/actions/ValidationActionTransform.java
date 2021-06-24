package com.learning.beam.tranform.actions;

import com.learning.beam.entity.Table;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class ValidationActionTransform extends PTransform<PCollection<Table>, PCollection<Table>> {
    @Override
    public PCollection<Table> expand(PCollection<Table> input) {
        // Pattern: command
        // key: com.example.validation.Positive
        // value: new Positive
        return null;
    }
}
