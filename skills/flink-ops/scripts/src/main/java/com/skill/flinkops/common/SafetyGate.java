package com.skill.flinkops.common;

import com.skill.flinkops.common.errors.SafetyException;

public final class SafetyGate {
    private SafetyGate() {
    }

    public static void requireConfirm(CommandContext context, String operation) {
        if (!context.confirm) {
            throw new SafetyException(operation + " requires --confirm.");
        }
    }
}
