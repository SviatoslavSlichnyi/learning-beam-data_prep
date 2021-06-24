package com.learning.beam.tranform;

import com.learning.beam.entity.Table;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import java.util.Map;

public class ConvertCsvToTableTransform extends PTransform<PCollection<String>, PCollection<Table>> {

    private final Map<String, Object> layouts;

    public ConvertCsvToTableTransform(Map<String, Object> layouts) {
        this.layouts = layouts;
    }

    @Override
    public PCollection<Table> expand(PCollection<String> input) {
        // convert a line => new Table();

        return null;
    }
}
