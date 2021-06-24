package com.learning.beam.tranform.actions;

import com.learning.beam.entity.Table;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class GroupByActionTransform extends PTransform<PCollection<Table>, PCollection<Table>> {
    @Override
    public PCollection<Table> expand(PCollection<Table> input) {
        return null;
    }
}
