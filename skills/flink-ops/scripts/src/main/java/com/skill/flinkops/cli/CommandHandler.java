package com.skill.flinkops.cli;

import com.skill.flinkops.common.CommandContext;

public interface CommandHandler {
    String handle(CommandContext context) throws Exception;
}
