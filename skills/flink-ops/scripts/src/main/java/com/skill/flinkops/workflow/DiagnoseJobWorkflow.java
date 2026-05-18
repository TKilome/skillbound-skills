package com.skill.flinkops.workflow;

import com.skill.flinkops.common.CommandContext;

public final class DiagnoseJobWorkflow {
    public interface Support {
        String diagnoseJob(CommandContext context) throws Exception;
        String diagnoseBackpressure(CommandContext context) throws Exception;
    }

    private final Support support;

    public DiagnoseJobWorkflow(Support support) {
        this.support = support;
    }

    public String diagnoseJob(CommandContext context) throws Exception {
        return support.diagnoseJob(context);
    }

    public String diagnoseBackpressure(CommandContext context) throws Exception {
        return support.diagnoseBackpressure(context);
    }
}
