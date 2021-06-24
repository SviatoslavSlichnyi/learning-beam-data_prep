package com.learning.beam.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtil {

    public static void saveAvscToFile(String data, String filename) throws IOException {
        if (Files.notExists(Constrains.DirectoryPaths.avscDirPath)) Files.createDirectory(Constrains.DirectoryPaths.avscDirPath);

        Path filePath = Paths.get(Constrains.DirectoryPaths.avscDirPath.toString(), filename + ".avsc");
        if (Files.notExists(filePath)) Files.createFile(filePath);

        Files.writeString(filePath, data);
    }
}
