package com.learning.beam;

import com.learning.beam.common.ProfileConfigsHelper;
import com.learning.beam.option.DataPrepOptions;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class ReadAvro {
    public static void main(String[] args) throws IOException {
        DataPrepOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(DataPrepOptions.class);
        ProfileConfigsHelper.initWithOptions(options);
        Schema schema = ProfileConfigsHelper.getActions().getMapToAvroActions().get(0).getAvroSchema();
        File avroPath = Paths.get("C:\\Users\\Sviatoslav_Slichnyi\\Documents\\epam\\Project\\EQFX\\Tasks\\data-prep\\src\\main\\resources\\output\\CreditFile.avro-00000-of-00001.avro")
                .toFile();

        // Deserialize users from disk
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(avroPath, datumReader);
        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            // Reuse record object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            record = dataFileReader.next(record);
            System.out.println(record);
        }
    }
}
