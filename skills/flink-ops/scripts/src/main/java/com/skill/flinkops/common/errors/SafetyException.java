package com.skill.flinkops.common.errors;

public final class SafetyException extends RuntimeException {
    public SafetyException(String message) {
        super(message);
    }
}
