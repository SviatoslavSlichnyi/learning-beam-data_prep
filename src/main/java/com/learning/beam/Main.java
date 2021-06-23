package com.learning.beam;

import com.learning.beam.option.DataPrepOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class Main {
    public static void main(String[] args) {
        DataPrepOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataPrepOptions.class);

        runDataPrepAnalyzer(options);
    }

    private static void runDataPrepAnalyzer(DataPrepOptions options) {
        Pipeline p = Pipeline.create(options);



        p.run().waitUntilFinish();
    }
}
