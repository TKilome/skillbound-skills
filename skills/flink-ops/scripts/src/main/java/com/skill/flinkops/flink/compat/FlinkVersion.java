package com.skill.flinkops.flink.compat;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FlinkVersion {
    private static final Pattern VERSION_PATTERN = Pattern.compile("(\\d+\\.\\d+)(?:\\.\\d+)?");

    private final String raw;
    private final String majorMinor;

    private FlinkVersion(String raw, String majorMinor) {
        this.raw = raw;
        this.majorMinor = majorMinor;
    }

    public static FlinkVersion parse(String value) {
        String raw = value == null || value.trim().isEmpty() ? "unknown" : value.trim();
        Matcher matcher = VERSION_PATTERN.matcher(raw);
        String majorMinor = matcher.find() ? matcher.group(1) : raw;
        return new FlinkVersion(raw, majorMinor);
    }

    public String raw() {
        return raw;
    }

    public String majorMinor() {
        return majorMinor;
    }
}
