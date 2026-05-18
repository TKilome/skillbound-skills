package com.skill.flinkops;

import com.skill.flinkops.common.CommandContext;
import com.skill.flinkops.flink.compat.FlinkVersion;
import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FlinkVersionAdapters {
    private static final Pattern VERSION_PATTERN = Pattern.compile("(\\d+\\.\\d+)(?:\\.\\d+)?");

    private FlinkVersionAdapters() {
    }

    public static FlinkVersionAdapter resolve(CommandContext context) {
        String explicit = context.option("flink-version");
        String version = explicit == null || explicit.trim().isEmpty()
                ? detectFromHome(context.option("flink-home"))
                : explicit.trim();
        String major = majorVersion(version);
        return new FlinkVersionAdapter(version, major);
    }

    public static String majorVersion(String version) {
        return FlinkVersion.parse(version).majorMinor();
    }

    private static String detectFromHome(String flinkHome) {
        if (flinkHome == null || flinkHome.trim().isEmpty()) {
            return "unknown";
        }
        File lib = new File(flinkHome, "lib");
        File[] jars = lib.listFiles((dir, name) -> name.startsWith("flink-dist") && name.endsWith(".jar"));
        if (jars == null || jars.length == 0) {
            return "unknown";
        }
        Matcher matcher = VERSION_PATTERN.matcher(jars[0].getName());
        if (matcher.find()) {
            return matcher.group();
        }
        return "unknown";
    }
}
