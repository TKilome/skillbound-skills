package com.skill.flinkops.flink.rest;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class FlinkRestClient {
    public String get(String endpoint) throws IOException {
        return get(endpoint, null);
    }

    public String get(String endpoint, String hostHeader) throws IOException {
        return request("GET", endpoint, null, hostHeader);
    }

    public String patch(String endpoint) throws IOException {
        return patch(endpoint, null);
    }

    public String patch(String endpoint, String hostHeader) throws IOException {
        return request("PATCH", endpoint, null, hostHeader);
    }

    public String postJson(String endpoint, String body) throws IOException {
        return postJson(endpoint, body, null);
    }

    public String postJson(String endpoint, String body, String hostHeader) throws IOException {
        return request("POST", endpoint, body, hostHeader);
    }

    private String request(String method, String endpoint, String body, String hostHeader) throws IOException {
        if (hostHeader != null && !hostHeader.trim().isEmpty()) {
            System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
        }
        HttpURLConnection connection = (HttpURLConnection) new URL(endpoint).openConnection();
        connection.setRequestMethod(method);
        connection.setConnectTimeout(10000);
        connection.setReadTimeout(30000);
        if (hostHeader != null && !hostHeader.trim().isEmpty()) {
            connection.setRequestProperty("Host", hostHeader);
        }
        if (body != null) {
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));
        }
        int code = connection.getResponseCode();
        String text = readResponseBody(connection, code);
        if (code < 200 || code >= 300) {
            throw new IOException("Flink REST " + method + " " + endpoint + " failed with HTTP " + code + ": " + text);
        }
        return text;
    }

    private String readResponseBody(HttpURLConnection connection, int code) throws IOException {
        InputStream stream = code >= 200 && code < 300 ? connection.getInputStream() : connection.getErrorStream();
        if (stream == null) {
            return "";
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        StringBuilder responseBody = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            responseBody.append(line).append('\n');
        }
        return responseBody.toString().trim();
    }
}
