package com.skill.flinkops.workflow.report;

final class Finding {
    private final String message;

    Finding(String message) {
        this.message = message;
    }

    String message() {
        return message;
    }
}
