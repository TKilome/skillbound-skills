package com.skill.flinkops.common.errors;

public final class ValidationException extends RuntimeException {
    public ValidationException(String message) {
        super(message);
    }
}
