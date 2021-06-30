package com.learning.beam.common;

public class StringUtils {
    public static String getFirstWordBeforeDot(String str) {
        int dotIndex = str.indexOf('.');
        if (dotIndex == -1) return str;

        else return str.substring(0, dotIndex);
    }

    public static String getWordAfterDot(String str) {
        int dotIndex = str.indexOf('.');
        if (dotIndex == -1) return null;

        return str.substring(dotIndex + 1);
    }
}
