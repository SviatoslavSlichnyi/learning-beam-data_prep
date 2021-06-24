package com.learning.beam.common;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class SnakeYamlReader {
    public static Map<String, Object> readYamlFile(String path) throws IOException {
        Yaml yaml = new Yaml();
        String yamlFileString = Files.readString(Paths.get(path));
        return yaml.load(yamlFileString);
    }
}
