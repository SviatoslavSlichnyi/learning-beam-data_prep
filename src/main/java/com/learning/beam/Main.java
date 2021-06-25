package com.learning.beam;

import com.learning.beam.common.ProfileConfigsHelper;
import com.learning.beam.entity.Table;
import com.learning.beam.option.DataPrepOptions;
import com.learning.beam.transform.ApplyActionsTransform;
import com.learning.beam.transform.ConvertCsvToTableTransform;
import com.learning.beam.transform.ReadCsvTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        DataPrepOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataPrepOptions.class);

        runDataPrepAnalyzer(options);
    }

    private static void runDataPrepAnalyzer(DataPrepOptions options) throws IOException {
        Pipeline p = Pipeline.create(options);

        ProfileConfigsHelper.initWithOptions(options);

        // read lines from csv files
        PCollection<String> lines = p.apply(new ReadCsvTransform(options.getInputFiles()));

        // convert lines into PCollection<Table>
        PCollection<Table> tables = lines.apply(new ConvertCsvToTableTransform());

        // make some actions on data
        tables.apply(new ApplyActionsTransform());

        p.run().waitUntilFinish();
    }
}
