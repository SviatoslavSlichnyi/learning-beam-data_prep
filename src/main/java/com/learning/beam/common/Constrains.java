package com.learning.beam.common;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Constrains {

    public static class DirectoryPaths {
        public static final Path resourcePath = Paths.get("src", "main", "resources");
        public static final Path avroDirPath = Paths.get(resourcePath.toString(), "avro");
        public static final Path avscDirPath = Paths.get(Constrains.DirectoryPaths.resourcePath.toString(), "avsc");
    }

    public static class ProfileYaml {
        public static final String LAYOUTS = "layouts";
        public static final String ACTIONS = "actions";
    }
}
