/*
 * Kibana Workflows connector for ES|QL Data Federation.
 */
package org.elasticsearch.xpack.esql.datasource.workflow;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal JSON reader/writer for Kibana's responses (no dependency on Elasticsearch's parser
 * implementation, which unit tests can't load). A repeated key collects its values into a list.
 */
final class Json {

    private final String s;
    private int i;

    private Json(String s) {
        this.s = s;
    }

    static Object parse(String text) {
        Json p = new Json(text);
        p.ws();
        Object v = p.value();
        p.ws();
        if (p.i != p.s.length()) {
            throw p.error("trailing characters");
        }
        return v;
    }

    private Object value() {
        if (i >= s.length()) {
            throw error("unexpected end");
        }
        char c = s.charAt(i);
        return switch (c) {
            case '{' -> object();
            case '[' -> array();
            case '"' -> string();
            case 't' -> literal("true", Boolean.TRUE);
            case 'f' -> literal("false", Boolean.FALSE);
            case 'n' -> literal("null", null);
            default -> {
                if (c == '-' || (c >= '0' && c <= '9')) {
                    yield number();
                }
                throw error("unexpected character '" + c + "'");
            }
        };
    }

    private Map<String, Object> object() {
        Map<String, Object> m = new LinkedHashMap<>();
        i++;
        ws();
        if (peek() == '}') {
            i++;
            return m;
        }
        while (true) {
            ws();
            if (peek() != '"') {
                throw error("expected a key");
            }
            String k = string();
            ws();
            expect(':');
            ws();
            Object v = value();
            if (m.containsKey(k)) {
                Object prev = m.get(k);
                if (prev instanceof Multi multi) {
                    multi.add(v);
                } else {
                    Multi multi = new Multi();
                    multi.add(prev);
                    multi.add(v);
                    m.put(k, multi);
                }
            } else {
                m.put(k, v);
            }
            ws();
            char c = next();
            if (c == '}') {
                return m;
            }
            if (c != ',') {
                throw error("expected ',' or '}'");
            }
        }
    }

    /** Values of a repeated key; a plain list once parsing is done. */
    private static final class Multi extends ArrayList<Object> {}

    private List<Object> array() {
        List<Object> a = new ArrayList<>();
        i++;
        ws();
        if (peek() == ']') {
            i++;
            return a;
        }
        while (true) {
            ws();
            a.add(value());
            ws();
            char c = next();
            if (c == ']') {
                return a;
            }
            if (c != ',') {
                throw error("expected ',' or ']'");
            }
        }
    }

    private String string() {
        i++;
        StringBuilder b = null;
        int start = i;
        while (true) {
            if (i >= s.length()) {
                throw error("unterminated string");
            }
            char c = s.charAt(i);
            if (c == '"') {
                String out = b == null ? s.substring(start, i) : b.append(s, start, i).toString();
                i++;
                return out;
            }
            if (c == '\\') {
                if (b == null) {
                    b = new StringBuilder();
                }
                b.append(s, start, i);
                i++;
                char e = next();
                switch (e) {
                    case '"', '\\', '/' -> b.append(e);
                    case 'b' -> b.append('\b');
                    case 'f' -> b.append('\f');
                    case 'n' -> b.append('\n');
                    case 'r' -> b.append('\r');
                    case 't' -> b.append('\t');
                    case 'u' -> {
                        if (i + 4 > s.length()) {
                            throw error("bad \\u escape");
                        }
                        b.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                        i += 4;
                    }
                    default -> throw error("bad escape \\" + e);
                }
                start = i;
            } else {
                i++;
            }
        }
    }

    private Object number() {
        int start = i;
        boolean fraction = false;
        if (peek() == '-') {
            i++;
        }
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c >= '0' && c <= '9') {
                i++;
            } else if (c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') {
                fraction = true;
                i++;
            } else {
                break;
            }
        }
        String n = s.substring(start, i);
        try {
            if (fraction) {
                return Double.parseDouble(n);
            }
            BigInteger big = new BigInteger(n);
            return big.bitLength() < 64 ? (Object) big.longValue() : big.toString();
        } catch (NumberFormatException e) {
            throw error("bad number " + n);
        }
    }

    private Object literal(String word, Object v) {
        if (s.startsWith(word, i) == false) {
            throw error("unexpected token");
        }
        i += word.length();
        return v;
    }

    private void ws() {
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                i++;
            } else {
                break;
            }
        }
    }

    private char peek() {
        return i < s.length() ? s.charAt(i) : '\0';
    }

    private char next() {
        if (i >= s.length()) {
            throw error("unexpected end");
        }
        return s.charAt(i++);
    }

    private void expect(char c) {
        if (next() != c) {
            throw error("expected '" + c + "'");
        }
    }

    private IllegalArgumentException error(String msg) {
        return new IllegalArgumentException("invalid JSON at " + i + ": " + msg);
    }

    /** Serializes parsed values back to JSON (used when an event body is an object, not a string). */
    static String sha256(String s) {
        try {
            return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                .digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    static String write(Object v) {
        StringBuilder b = new StringBuilder();
        write(b, v);
        return b.toString();
    }

    private static void write(StringBuilder b, Object v) {
        if (v == null) {
            b.append("null");
        } else if (v instanceof String str) {
            b.append('"');
            for (int k = 0; k < str.length(); k++) {
                char c = str.charAt(k);
                switch (c) {
                    case '"' -> b.append("\\\"");
                    case '\\' -> b.append("\\\\");
                    case '\n' -> b.append("\\n");
                    case '\r' -> b.append("\\r");
                    case '\t' -> b.append("\\t");
                    default -> {
                        if (c < 0x20) {
                            b.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) c));
                        } else {
                            b.append(c);
                        }
                    }
                }
            }
            b.append('"');
        } else if (v instanceof Map<?, ?> m) {
            b.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (first == false) {
                    b.append(',');
                }
                first = false;
                write(b, String.valueOf(e.getKey()));
                b.append(':');
                write(b, e.getValue());
            }
            b.append('}');
        } else if (v instanceof List<?> l) {
            b.append('[');
            for (int k = 0; k < l.size(); k++) {
                if (k > 0) {
                    b.append(',');
                }
                write(b, l.get(k));
            }
            b.append(']');
        } else {
            b.append(v);
        }
    }
}
