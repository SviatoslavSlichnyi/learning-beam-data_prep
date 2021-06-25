package com.learning.beam.transform.actions;

import com.learning.beam.entity.Table;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PDone;

public class MapToAvroActionTransform extends DoFn<Table, PDone> {
    @ProcessElement
    public void processElement(@Element Table table, PDone done) {


    }
}
