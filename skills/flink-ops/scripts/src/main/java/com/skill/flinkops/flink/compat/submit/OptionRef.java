package com.skill.flinkops.flink.compat.submit;

public final class OptionRef {
    private final String key;
    private final String className;
    private final String fieldName;

    private OptionRef(String key, String className, String fieldName) {
        this.key = key;
        this.className = className;
        this.fieldName = fieldName;
    }

    public static OptionRef key(String key) {
        return new OptionRef(key, null, null);
    }

    public static OptionRef field(String key, String className, String fieldName) {
        return new OptionRef(key, className, fieldName);
    }

    public String key() {
        return key;
    }

    public String className() {
        return className;
    }

    public String fieldName() {
        return fieldName;
    }

    public boolean hasField() {
        return className != null && fieldName != null;
    }
}
