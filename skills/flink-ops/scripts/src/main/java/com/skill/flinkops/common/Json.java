package com.skill.flinkops.common;

import java.util.Iterator;
import java.util.Map;

public final class Json {
    private Json() {
    }

    public static String stringify(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String) {
            return "\"" + escape((String) value) + "\"";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return String.valueOf(value);
        }
        if (value instanceof Map) {
            StringBuilder builder = new StringBuilder();
            builder.append("{");
            Iterator<? extends Map.Entry<?, ?>> iterator = ((Map<?, ?>) value).entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<?, ?> entry = iterator.next();
                builder.append(stringify(String.valueOf(entry.getKey())));
                builder.append(":");
                builder.append(stringify(entry.getValue()));
                if (iterator.hasNext()) {
                    builder.append(",");
                }
            }
            builder.append("}");
            return builder.toString();
        }
        if (value instanceof Iterable) {
            StringBuilder builder = new StringBuilder();
            builder.append("[");
            Iterator<?> iterator = ((Iterable<?>) value).iterator();
            while (iterator.hasNext()) {
                builder.append(stringify(iterator.next()));
                if (iterator.hasNext()) {
                    builder.append(",");
                }
            }
            builder.append("]");
            return builder.toString();
        }
        if (value instanceof String[]) {
            String[] values = (String[]) value;
            StringBuilder builder = new StringBuilder();
            builder.append("[");
            for (int i = 0; i < values.length; i++) {
                builder.append(stringify(values[i]));
                if (i + 1 < values.length) {
                    builder.append(",");
                }
            }
            builder.append("]");
            return builder.toString();
        }
        return stringify(String.valueOf(value));
    }

    private static String escape(String value) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    builder.append("\\\"");
                    break;
                case '\\':
                    builder.append("\\\\");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    builder.append(c);
            }
        }
        return builder.toString();
    }
}
