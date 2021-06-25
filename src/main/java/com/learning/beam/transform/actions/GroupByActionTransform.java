package com.learning.beam.transform.actions;

import com.learning.beam.entity.Table;
import org.apache.beam.sdk.transforms.DoFn;

public class GroupByActionTransform extends DoFn<Table, Table> {
    @ProcessElement
    public void processElement(@Element Table table, OutputReceiver<Table> receiver) {

        receiver.output(table);
    }
}
