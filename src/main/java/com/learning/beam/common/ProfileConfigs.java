package com.learning.beam.common;

import com.learning.beam.option.DataPrepOptions;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ProfileConfigs {

    private static Map<String, Object> profileConfigs = null;

    public static void initWithOptions(DataPrepOptions options) throws IOException {
        // read configs from yaml file into Map<> profileConfigs
        // https://www.baeldung.com/java-snake-yaml
        profileConfigs = SnakeYamlReader.readYamlFile(options.getProfile());
    }

    public static Map<String, Object> getLayouts() {
        return  (Map<String, Object>) profileConfigs.get(Constrains.ProfileYaml.LAYOUTS);
    }

    public static List<Map<String, String>> getActions() {
        return (List<Map<String, String>>) profileConfigs.get(Constrains.ProfileYaml.ACTIONS);
    }
}
