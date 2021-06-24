package com.learning.beam.tranform.actions;

import com.learning.beam.entity.Table;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class MapToAvroActionTransform extends PTransform<PCollection<Table>, PDone> {
    @Override
    public PDone expand(PCollection<Table> input) {
        return null;
    }
}
