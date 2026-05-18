package com.skill.flinkops.cli;

import com.skill.flinkops.common.CommandContext;
import com.skill.flinkops.common.errors.ValidationException;
import java.util.LinkedHashMap;
import java.util.Map;

public final class CommandRouter {
    private final Map<String, CommandHandler> handlers = new LinkedHashMap<String, CommandHandler>();

    public CommandRouter register(String command, CommandHandler handler) {
        handlers.put(command, handler);
        return this;
    }

    public String dispatch(CommandContext context) throws Exception {
        if (context.command == null || context.command.trim().isEmpty()) {
            throw new ValidationException("Command is required.");
        }
        CommandHandler handler = handlers.get(context.command);
        if (handler == null) {
            throw new ValidationException("Unsupported command '" + context.command + "'.");
        }
        return handler.handle(context);
    }
}
