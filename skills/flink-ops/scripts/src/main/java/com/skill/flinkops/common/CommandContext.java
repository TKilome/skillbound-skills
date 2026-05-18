package com.skill.flinkops.common;

import com.skill.flinkops.common.errors.ValidationException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class CommandContext {
    public final String command;
    public final Map<String, String> options;
    public final boolean confirm;
    public final boolean dryRun;

    private CommandContext(String command, Map<String, String> options) {
        this.command = command;
        this.options = Collections.unmodifiableMap(options);
        this.confirm = options.containsKey("confirm");
        this.dryRun = options.containsKey("dry-run");
    }

    public static CommandContext parse(String[] args) {
        if (args.length == 0) {
            return new CommandContext("", new LinkedHashMap<String, String>());
        }

        Map<String, String> options = new LinkedHashMap<String, String>();
        String command = args[0];
        for (int i = 1; i < args.length; i++) {
            String token = args[i];
            if (!token.startsWith("--")) {
                continue;
            }
            String name = token.substring(2);
            if ("confirm".equals(name) || "dry-run".equals(name) || "report".equals(name) || "verify".equals(name)) {
                options.put(name, "true");
                continue;
            }
            if (i + 1 >= args.length) {
                options.put(name, "");
                continue;
            }
            options.put(name, args[++i]);
        }
        return new CommandContext(command, options);
    }

    public String option(String name) {
        return options.get(name);
    }

    public boolean hasFlag(String name) {
        return options.containsKey(name);
    }

    public String require(String name) {
        String value = options.get(name);
        if (value == null || value.trim().isEmpty()) {
            throw new ValidationException("Parameter '--" + name + "' is required.");
        }
        return value;
    }

    public int intOption(String name, int defaultValue) {
        String value = options.get(name);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new ValidationException("Parameter '--" + name + "' must be an integer.");
        }
    }

    public boolean booleanOption(String name, boolean defaultValue) {
        String value = options.get(name);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }
}
