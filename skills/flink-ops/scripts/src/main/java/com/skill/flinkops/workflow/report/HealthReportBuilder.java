package com.skill.flinkops.workflow.report;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class HealthReportBuilder {
    public Map<String, Object> fromEvidence(Map<String, Object> evidence, String summary) {
        String joined = evidenceText(evidence);
        List<String> findings = new ArrayList<String>();
        List<String> rootCauseCandidates = new ArrayList<String>();
        List<String> recommendations = new ArrayList<String>();
        String healthStatus = "healthy";
        String riskLevel = "low";

        boolean hasNonNullRootException = joined.contains("\"root-exception\":\"") || joined.contains("\"root-exception\":{");
        if (joined.contains("\"state\":\"failed\"") || joined.contains("\"status\":\"failed\"") || hasNonNullRootException) {
            healthStatus = "critical";
            riskLevel = "high";
            findings.add("Job evidence contains failure or exception signals.");
            rootCauseCandidates.add("Runtime exception or failed job state.");
            recommendations.add("Inspect the exception payload and correlate it with recent deployment or dependency changes.");
        }
        if (hasCheckpointFailureSignal(joined)) {
            healthStatus = "critical";
            riskLevel = "high";
            findings.add("Checkpoint evidence contains timeout or failed checkpoint signals.");
            rootCauseCandidates.add("Checkpoint timeout, state backend pressure, slow sink, or unstable storage.");
            recommendations.add("Review checkpoint timeout, state size, sink throughput, and external storage latency.");
        }
        if (joined.contains("\"slots-available\":0") || joined.contains("\"slots-available\":\"0\"")) {
            if (!"critical".equals(healthStatus)) {
                healthStatus = "warning";
                riskLevel = "medium";
            }
            findings.add("No available slots were reported by the JobManager overview.");
            rootCauseCandidates.add("Cluster slot pressure or insufficient TaskManager capacity.");
            recommendations.add("Check TaskManager capacity, slot configuration, and job parallelism before starting more work.");
        }
        if (joined.contains("\"blocked\":true") || joined.contains("\"blocked\":\"true\"")) {
            if (!"critical".equals(healthStatus)) {
                healthStatus = "warning";
                riskLevel = "medium";
            }
            findings.add("TaskManager evidence reports a blocked TaskManager.");
            rootCauseCandidates.add("TaskManager heartbeat, JVM, network, or resource pressure.");
            recommendations.add("Inspect the blocked TaskManager and compare it with recent resource or network changes.");
        }
        if ((joined.contains("\"numrestarts\"") && !joined.contains("\"value\":\"0\""))
                || (joined.contains("\"fullrestarts\"") && !joined.contains("\"value\":\"0\""))) {
            if (!"critical".equals(healthStatus)) {
                healthStatus = "warning";
                riskLevel = "medium";
            }
            findings.add("Job metrics indicate restart activity.");
            rootCauseCandidates.add("Recent failover, unstable dependency, or operator exception.");
            recommendations.add("Correlate restart metrics with exceptions, checkpoint failures, and deployment changes.");
        }
        if (findings.isEmpty()) {
            findings.add("No failure, exception, or checkpoint timeout signal was detected in collected REST evidence.");
            recommendations.add("Continue monitoring job state and checkpoint progress.");
        }

        Map<String, Object> report = new LinkedHashMap<String, Object>();
        report.put("summary", summary);
        report.put("healthStatus", healthStatus);
        report.put("riskLevel", riskLevel);
        report.put("rootCauseCandidates", rootCauseCandidates);
        report.put("findings", findings);
        report.put("recommendations", recommendations);
        return report;
    }

    public Map<String, Object> backpressureReport(Map<String, Object> evidence) {
        Map<String, Object> report = fromEvidence(evidence, "Backpressure diagnosis completed.");
        String joined = evidenceText(evidence);
        if (joined.contains("\"backpressure-level\":\"high\"") || joined.contains("\"backpressure-level\":\"deprecated\"") || joined.contains("\"ratio\":0.9")) {
            report.put("healthStatus", "critical");
            report.put("riskLevel", "high");
            @SuppressWarnings("unchecked")
            List<String> findings = (List<String>) report.get("findings");
            findings.add("Flink per-vertex backpressure REST evidence reports high backpressure.");
            @SuppressWarnings("unchecked")
            List<String> rootCauses = (List<String>) report.get("rootCauseCandidates");
            rootCauses.add("Downstream operator, sink throughput, or external system latency may be limiting upstream progress.");
            @SuppressWarnings("unchecked")
            List<String> recommendations = (List<String>) report.get("recommendations");
            recommendations.add("Inspect the highest backpressured vertex, compare busy/idle/backpressured metrics, then consider sink scaling, parallelism changes, or external dependency tuning.");
            return report;
        }
        if (!joined.contains("backpressure") && !joined.contains("busy") && !joined.contains("idle")) {
            report.put("healthStatus", "unknown");
            report.put("riskLevel", "unknown");
            @SuppressWarnings("unchecked")
            List<String> findings = (List<String>) report.get("findings");
            findings.add("Direct backpressure metrics were not present in the collected REST evidence.");
            @SuppressWarnings("unchecked")
            List<String> rootCauses = (List<String>) report.get("rootCauseCandidates");
            rootCauses.add("Insufficient metric-level evidence; inspect source/sink throughput and busy/idle/backpressured metrics when available.");
        }
        return report;
    }

    private String evidenceText(Map<String, Object> evidence) {
        StringBuilder builder = new StringBuilder();
        for (Object value : evidence.values()) {
            if (value != null) {
                builder.append(String.valueOf(value)).append('\n');
            }
        }
        return builder.toString().toLowerCase();
    }

    private boolean hasCheckpointFailureSignal(String evidenceText) {
        if (!evidenceText.contains("checkpoint")) {
            return false;
        }
        if (evidenceText.contains("timeout") || evidenceText.contains("failure_message")) {
            return true;
        }
        if (evidenceText.contains("\"latest\":{\"failed\":{") || evidenceText.contains("\"failed\":{\"id\"")) {
            return true;
        }
        java.util.regex.Matcher matcher = java.util.regex.Pattern
                .compile("\"failed\"\\s*:\\s*([1-9][0-9]*)")
                .matcher(evidenceText);
        return matcher.find();
    }
}
