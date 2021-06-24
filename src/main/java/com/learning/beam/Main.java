package com.learning.beam;

import com.learning.beam.common.Constrains;
import com.learning.beam.common.SnakeYamlReader;
import com.learning.beam.entity.Table;
import com.learning.beam.option.DataPrepOptions;
import com.learning.beam.tranform.ApplyActionsTransform;
import com.learning.beam.tranform.ConvertCsvToTableTransform;
import com.learning.beam.tranform.ReadCsvTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException {
        DataPrepOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataPrepOptions.class);

        runDataPrepAnalyzer(options);
    }

    private static void runDataPrepAnalyzer(DataPrepOptions options) throws IOException {
        Pipeline p = Pipeline.create(options);

        // read configs from yaml file into Map<> profileConfigs
        // https://www.baeldung.com/java-snake-yaml
        Map<String, Object> profileConfigs = SnakeYamlReader.readYamlFile(options.getProfile());
        Map<String, Object> layouts = (Map<String, Object>) profileConfigs.get(Constrains.ProfileYaml.LAYOUTS);
        List<Map<String, String>> actions = (List<Map<String, String>>) profileConfigs.get(Constrains.ProfileYaml.ACTIONS);

        // read lines from csv files
        PCollection<String> lines = p.apply(new ReadCsvTransform(options.getInputFiles()));

        // convert lines into PCollection<Table>
        PCollection<Table> tables = lines.apply(new ConvertCsvToTableTransform(layouts));

        // make some actions on data
        tables.apply(new ApplyActionsTransform(actions));

        p.run().waitUntilFinish();
    }
}
