package com.learning.beam.common;

import com.learning.beam.option.DataPrepOptions;

public class OptionalHelper {
    private static DataPrepOptions dataPrepOptions;

    public static void init(DataPrepOptions options) {
        dataPrepOptions = options;
    }

    public static DataPrepOptions getOptions() {
        return dataPrepOptions;
    }
}
