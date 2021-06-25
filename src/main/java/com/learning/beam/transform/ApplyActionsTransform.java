package com.learning.beam.transform;

import com.learning.beam.entity.Table;
import com.learning.beam.transform.actions.GroupByActionTransform;
import com.learning.beam.transform.actions.MapToAvroActionTransform;
import com.learning.beam.transform.actions.ValidationActionDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class ApplyActionsTransform extends PTransform<PCollection<Table>, PDone> {

    @Override
    public PDone expand(PCollection<Table> input) {

        PCollection<Table> validTables = input.apply(ParDo.of(new ValidationActionDoFn()));

        PCollection<Table> groupedTables = validTables.apply(new GroupByActionTransform());

        groupedTables.apply(new MapToAvroActionTransform());

        return PDone.in(input.getPipeline());
    }
}
