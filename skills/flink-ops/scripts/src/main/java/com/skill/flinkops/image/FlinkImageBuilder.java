package com.skill.flinkops.image;

import com.skill.flinkops.common.errors.ValidationException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class FlinkImageBuilder {
    public Map<String, Object> build(FlinkImageBuildSpec spec) throws Exception {
        File localJar = new File(spec.localJar);
        if (!localJar.isFile()) {
            throw new ValidationException("Parameter '--local-jar' must point to an existing jar file.");
        }

        File contextDir = Files.createTempDirectory("flink-skill-image-").toFile();
        try {
            File copiedJar = new File(contextDir, "job.jar");
            Files.copy(localJar.toPath(), copiedJar.toPath(), StandardCopyOption.REPLACE_EXISTING);
            writeDockerfile(new File(contextDir, "Dockerfile"), spec);

            List<String> command = new ArrayList<String>();
            command.add("docker");
            command.add("build");
            command.add("-t");
            command.add(spec.targetImage);
            command.add(contextDir.getAbsolutePath());

            CommandResult result = run(command);
            Map<String, Object> data = new LinkedHashMap<String, Object>();
            data.putAll(spec.plan());
            data.put("exitCode", Integer.valueOf(result.exitCode));
            data.put("stdout", result.stdout);
            data.put("stderr", result.stderr);
            if (result.exitCode != 0) {
                throw new RuntimeException("Docker image build failed with exit code " + result.exitCode + ": " + result.stderr);
            }
            return data;
        } finally {
            deleteRecursively(contextDir);
        }
    }

    private void writeDockerfile(File dockerfile, FlinkImageBuildSpec spec) throws IOException {
        FileOutputStream out = new FileOutputStream(dockerfile);
        try {
            out.write(("FROM " + spec.baseImage + "\n").getBytes(StandardCharsets.UTF_8));
            out.write(("COPY job.jar " + spec.targetJarPath + "\n").getBytes(StandardCharsets.UTF_8));
        } finally {
            out.close();
        }
    }

    private CommandResult run(List<String> command) throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        Process process = builder.start();
        String output = read(process.getInputStream());
        int exitCode = process.waitFor();
        return new CommandResult(exitCode, output, "");
    }

    private String read(java.io.InputStream stream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        StringBuilder text = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            text.append(line).append('\n');
        }
        return text.toString().trim();
    }

    private void deleteRecursively(File file) {
        if (file == null || !file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (int i = 0; i < children.length; i++) {
                    deleteRecursively(children[i]);
                }
            }
        }
        file.delete();
    }

    private static final class CommandResult {
        final int exitCode;
        final String stdout;
        final String stderr;

        CommandResult(int exitCode, String stdout, String stderr) {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }
    }
}
