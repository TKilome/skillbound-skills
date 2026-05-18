package com.skill.flinkops.common;

import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class ApiResponse {
    private ApiResponse() {
    }

    public static String success(String operation, Map<String, Object> data) {
        Map<String, Object> root = new LinkedHashMap<String, Object>();
        root.put("success", Boolean.TRUE);
        root.put("operation", operation);
        root.put("data", data);
        root.put("requestId", "");
        root.put("warnings", new String[0]);
        return Json.stringify(root);
    }

    public static String error(String operation, String code, String message) {
        return error(operation, code, message, null, null);
    }

    public static String error(String operation, String code, String message, Map<String, Object> data) {
        return error(operation, code, message, data, null);
    }

    public static String error(String operation, String code, String message, Throwable cause) {
        return error(operation, code, message, null, cause);
    }

    public static String error(String operation, String code, String message, Map<String, Object> data, Throwable cause) {
        Map<String, Object> root = new LinkedHashMap<String, Object>();
        Map<String, Object> error = new LinkedHashMap<String, Object>();
        Map<String, Object> errorData = new LinkedHashMap<String, Object>();
        error.put("code", code);
        error.put("message", message);
        if (data != null && !data.isEmpty()) {
            errorData.putAll(data);
        }
        List<Map<String, Object>> causes = causes(cause);
        if (!causes.isEmpty()) {
            errorData.put("causes", causes);
        }
        if (!errorData.isEmpty()) {
            error.put("data", errorData);
        }
        root.put("success", Boolean.FALSE);
        root.put("operation", operation);
        root.put("error", error);
        root.put("requestId", "");
        return Json.stringify(root);
    }

    private static List<Map<String, Object>> causes(Throwable throwable) {
        List<Map<String, Object>> causes = new ArrayList<Map<String, Object>>();
        Throwable current = throwable;
        int depth = 0;
        while (current != null && depth < 12) {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("type", current.getClass().getSimpleName());
            item.put("message", message(current));
            List<String> stackTrace = stackTrace(current);
            if (!stackTrace.isEmpty()) {
                item.put("stackTrace", stackTrace);
            }
            causes.add(item);
            current = current.getCause();
            depth++;
        }
        return causes;
    }

    private static String message(Throwable throwable) {
        String message = throwable.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return throwable.toString();
        }
        return message;
    }

    private static List<String> stackTrace(Throwable throwable) {
        List<String> frames = new ArrayList<String>();
        StackTraceElement[] stackTrace = throwable.getStackTrace();
        int limit = Math.min(stackTrace.length, 8);
        for (int i = 0; i < limit; i++) {
            frames.add(stackTrace[i].toString());
        }
        return frames;
    }
}
