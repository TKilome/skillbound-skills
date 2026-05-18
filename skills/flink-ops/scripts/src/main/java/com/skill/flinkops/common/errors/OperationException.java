package com.skill.flinkops.common.errors;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class OperationException extends RuntimeException {
    private final String code;
    private final Map<String, Object> data;

    public OperationException(String code, String message) {
        this(code, message, new LinkedHashMap<String, Object>());
    }

    public OperationException(String code, String message, Throwable cause) {
        this(code, message, new LinkedHashMap<String, Object>(), cause);
    }

    public OperationException(String code, String message, Map<String, Object> data) {
        this(code, message, data, null);
    }

    public OperationException(String code, String message, Map<String, Object> data, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.data = Collections.unmodifiableMap(new LinkedHashMap<String, Object>(data));
    }

    public String code() {
        return code;
    }

    public Map<String, Object> data() {
        return data;
    }
}
