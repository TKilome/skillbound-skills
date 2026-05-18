package com.skill.flinkops.flink.compat.submit;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class FlinkConfigApplier {
    private final Object flinkConfig;

    public FlinkConfigApplier(Object flinkConfig) {
        this.flinkConfig = flinkConfig;
    }

    public void set(OptionRef option, Object value) throws Exception {
        if (value == null || flinkConfig == null) {
            return;
        }
        if (option.hasField()) {
            try {
                Class<?> configOptionClass = Class.forName("org.apache.flink.configuration.ConfigOption");
                Method setMethod = flinkConfig.getClass().getMethod("set", configOptionClass, Object.class);
                Class<?> optionClass = Class.forName(option.className());
                Field field = optionClass.getField(option.fieldName());
                setMethod.invoke(flinkConfig, field.get(null), value);
                return;
            } catch (NoSuchFieldException ignored) {
                setString(option.key(), String.valueOf(value));
                return;
            }
        }
        setString(option.key(), String.valueOf(value));
    }

    public void setString(String key, String value) throws Exception {
        if (value == null || flinkConfig == null) {
            return;
        }
        Method setString = flinkConfig.getClass().getMethod("setString", String.class, String.class);
        setString.invoke(flinkConfig, key, value);
    }
}
