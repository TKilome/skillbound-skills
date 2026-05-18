package com.skill.flinkops.image;

import com.skill.flinkops.common.CommandContext;
import com.skill.flinkops.common.errors.ValidationException;
import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

public final class FlinkImageBuildSpec {
    public final String baseImage;
    public final String localJar;
    public final String targetImage;
    public final String targetJarPath;

    private FlinkImageBuildSpec(String baseImage, String localJar, String targetImage, String targetJarPath) {
        this.baseImage = baseImage;
        this.localJar = localJar;
        this.targetImage = targetImage;
        this.targetJarPath = targetJarPath;
    }

    public static FlinkImageBuildSpec from(CommandContext context) {
        String baseImage = context.require("base-image");
        String localJar = context.require("local-jar");
        String targetImage = context.require("target-image");
        String targetJarPath = context.option("target-jar-path");
        if (targetJarPath == null || targetJarPath.trim().isEmpty()) {
            targetJarPath = defaultTargetJarPath(localJar);
        }
        if (!targetJarPath.startsWith("/")) {
            throw new ValidationException("Parameter '--target-jar-path' must be an absolute path inside the image.");
        }
        return new FlinkImageBuildSpec(baseImage, localJar, targetImage, targetJarPath);
    }

    private static String defaultTargetJarPath(String localJar) {
        String fileName = new File(localJar).getName();
        if (fileName == null || fileName.trim().isEmpty()) {
            throw new ValidationException("Parameter '--local-jar' must include a jar file name.");
        }
        return "/opt/flink/usrlib/" + fileName;
    }

    public Map<String, Object> plan() {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("baseImage", baseImage);
        data.put("localJar", localJar);
        data.put("targetImage", targetImage);
        data.put("targetJarPath", targetJarPath);
        data.put("resultingJarUri", "local://" + targetJarPath);
        data.put("dockerfile", "FROM " + baseImage + "\nCOPY job.jar " + targetJarPath + "\n");
        data.put("buildEngine", "FlinkOpsCli internal docker build");
        return data;
    }
}
