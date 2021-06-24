package com.learning.beam.option;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

// --inputPath (GCS шлях по якому лежатимуть source .csv файли )
//--profile (GCS шлях по якому лежить profile.yaml з конфігурацією )
//--outputPath (GCS шлях куди має записатися .avro результат )
public interface DataPrepOptions extends PipelineOptions {

    @Description("Path of the file to configure the behavior of processing data")
    @Default.String("src/main/resources/profiles/profile.yaml")
    String getProfile();

    void setProfile(String value);

    @Description("Path of the file to read form")
    @Default.String("src/main/resources/input/*.csv")
    String getInputFiles();

    void setInputFiles(String value);

    @Description("Path of the file to write to")
    @Default.String("src/main/resources/output")
    String getOutput();

    void setOutput(String value);
}
