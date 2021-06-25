package com.learning.beam.transform;

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
        // https://medium.com/@mohamed.t.esmat/apache-beam-bites-10b8ded90d4c

        return null;
    }
}
