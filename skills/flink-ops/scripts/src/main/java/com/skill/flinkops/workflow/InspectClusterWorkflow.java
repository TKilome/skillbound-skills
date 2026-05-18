package com.skill.flinkops.workflow;

import com.skill.flinkops.common.CommandContext;

public final class InspectClusterWorkflow {
    public interface Support {
        String inspectCluster(CommandContext context) throws Exception;
    }

    private final Support support;

    public InspectClusterWorkflow(Support support) {
        this.support = support;
    }

    public String inspectCluster(CommandContext context) throws Exception {
        return support.inspectCluster(context);
    }
}
