package com.learning.beam.transform;

import org.apache.beam.sdk.coders.StringDelegateCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class ReadCsvTransform extends PTransform<PBegin, PCollection<String>> {

    private final String inputFilePaths;

    public ReadCsvTransform(String inputFilePaths) {
        this.inputFilePaths = inputFilePaths;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
        return input.apply(TextIO.read().from(inputFilePaths))
                .setCoder(StringDelegateCoder.of(String.class));
    }
}
