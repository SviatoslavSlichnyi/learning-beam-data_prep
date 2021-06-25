package com.learning.beam.transform;

import com.learning.beam.entity.Table;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class ConvertCsvToTableTransform extends PTransform<PCollection<String>, PCollection<Table>> {

    public ConvertCsvToTableTransform() {
    }

    @Override
    public PCollection<Table> expand(PCollection<String> input) {
        // convert a line => new Table();

        return null;
    }
}
