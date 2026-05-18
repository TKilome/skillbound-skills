package com.skill.flinkops.workflow.report;

final class ReportSection {
    private final String name;
    private final Object value;

    ReportSection(String name, Object value) {
        this.name = name;
        this.value = value;
    }

    String name() {
        return name;
    }

    Object value() {
        return value;
    }
}
