/*
 * Kibana Workflows connector for ES|QL Data Federation.
 */
package org.elasticsearch.xpack.esql.datasource.workflow;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Turns a workflow run's output into a table.
 *
 * <p>Which output: the {@code step} setting, else the last step that produced one. Where in it the rows
 * are: the {@code path} setting (dots; numbers index lists), else found automatically:
 * <ul>
 *   <li>an ES|QL response ({@code columns} + {@code values}, e.g. an {@code elasticsearch.request} to
 *       {@code /_query}): its columns, with their ES|QL types;</li>
 *   <li>a list: one row per element;</li>
 *   <li>a search response ({@code hits.hits}): one row per hit's {@code _source}, plus {@code _id} and {@code _index};</li>
 *   <li>an object with exactly one list of objects in it (e.g. {@code {"people": [...]}}): that list;</li>
 *   <li>anything else: one row.</li>
 * </ul>
 * Nested objects are flattened with dots ({@code user.name}); lists of scalars become multi-values;
 * lists of objects become a JSON string. Types follow the values: long, double, boolean, keyword
 * (mixed types fall back to keyword), datetime for an ISO-8601 {@code @timestamp}.
 */
final class WorkflowOutput {

    record Column(String name, DataType type) {}

    record Table(List<Column> columns, List<Map<String, Object>> rows) {}

    private WorkflowOutput() {}

    /** The output of {@code step}, or of the last step that produced any. */
    static Object stepOutput(Map<String, Object> execution, String step) {
        Object steps = execution.get("stepExecutions");
        if (!(steps instanceof List<?> list)) {
            throw new IllegalStateException("the workflow run has no step executions");
        }
        Object found = null;
        List<String> withOutput = new ArrayList<>();
        for (Object o : list) {
            if (o instanceof Map<?, ?> s && s.get("output") != null) {
                String id = String.valueOf(s.get("stepId"));
                withOutput.add(id);
                if (step == null || step.equals(id)) {
                    found = s.get("output");    // the last one wins (loops run a step several times)
                }
            }
        }
        if (found == null) {
            throw new IllegalStateException(step == null
                ? "no step of the workflow produced output"
                : "step [" + step + "] produced no output; steps with output: " + withOutput);
        }
        return found;
    }

    static Table toTable(Object output, String path) {
        Object node = path == null ? output : navigate(output, path);
        if (node == null) {
            throw new IllegalStateException("[path] " + path + " is not in the step's output");
        }
        if (path == null) {
            Table esql = esqlTable(node);
            if (esql != null) {
                return esql;
            }
            node = autoRows(node);
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        if (node instanceof List<?> list) {
            for (Object o : list) {
                rows.add(row(o));
            }
        } else {
            rows.add(row(node));
        }
        return new Table(inferColumns(rows), rows);
    }

    /** {"columns":[{"name","type"}...],"values":[[...]...]}, the ES|QL response shape. */
    @SuppressWarnings("unchecked")
    static Table esqlTable(Object node) {
        if (node instanceof Map<?, ?> m && m.get("columns") instanceof List<?> cols && m.get("values") instanceof List<?> values) {
            List<Column> columns = new ArrayList<>();
            for (Object c : cols) {
                if (!(c instanceof Map<?, ?> cm) || cm.get("name") == null) {
                    return null;
                }
                columns.add(new Column(String.valueOf(cm.get("name")), esqlType(String.valueOf(cm.get("type")))));
            }
            List<Map<String, Object>> rows = new ArrayList<>(values.size());
            for (Object v : values) {
                if (!(v instanceof List<?> cells)) {
                    return null;
                }
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 0; i < columns.size() && i < cells.size(); i++) {
                    if (cells.get(i) != null) {
                        row.put(columns.get(i).name(), cells.get(i));
                    }
                }
                rows.add(row);
            }
            return new Table(columns, rows);
        }
        return null;
    }

    static DataType esqlType(String t) {
        return switch (t.toLowerCase(Locale.ROOT)) {
            case "long", "integer", "short", "byte", "counter_long", "counter_integer" -> DataType.LONG;
            case "double", "float", "half_float", "scaled_float", "counter_double" -> DataType.DOUBLE;
            case "boolean" -> DataType.BOOLEAN;
            case "date", "date_nanos", "datetime" -> DataType.DATETIME;
            default -> DataType.KEYWORD;
        };
    }

    private static Object autoRows(Object node) {
        if (node instanceof List<?>) {
            return node;
        }
        if (node instanceof Map<?, ?> m) {
            if (m.get("hits") instanceof Map<?, ?> h && h.get("hits") instanceof List<?> hits) {
                List<Object> rows = new ArrayList<>(hits.size());
                for (Object o : hits) {
                    if (o instanceof Map<?, ?> hit) {
                        Map<String, Object> r = new LinkedHashMap<>();
                        if (hit.get("_source") instanceof Map<?, ?> src) {
                            src.forEach((k, v) -> r.put(String.valueOf(k), v));
                        }
                        r.put("_id", hit.get("_id"));
                        r.put("_index", hit.get("_index"));
                        rows.add(r);
                    }
                }
                return rows;
            }
            Object only = null;
            int lists = 0;
            for (Object v : m.values()) {
                if (v instanceof List<?> l && l.isEmpty() == false && l.get(0) instanceof Map<?, ?>) {
                    only = v;
                    lists++;
                }
            }
            if (lists == 1) {
                return only;
            }
        }
        return node;
    }

    private static Object navigate(Object node, String path) {
        Object cur = node;
        for (String part : path.split("\\.")) {
            if (cur instanceof Map<?, ?> m) {
                cur = m.get(part);
            } else if (cur instanceof List<?> l) {
                try {
                    int i = Integer.parseInt(part);
                    cur = i >= 0 && i < l.size() ? l.get(i) : null;
                } catch (NumberFormatException e) {
                    return null;
                }
            } else {
                return null;
            }
        }
        return cur;
    }

    private static Map<String, Object> row(Object o) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (o instanceof Map<?, ?> m) {
            flatten("", m, out);
        } else if (o != null) {
            out.put("value", o);
        }
        return out;
    }

    private static void flatten(String prefix, Map<?, ?> m, Map<String, Object> out) {
        for (Map.Entry<?, ?> e : m.entrySet()) {
            String key = prefix + e.getKey();
            Object v = e.getValue();
            if (v instanceof Map<?, ?> nested) {
                flatten(key + ".", nested, out);
            } else if (v instanceof List<?> l) {
                boolean scalars = true;
                for (Object x : l) {
                    if (x instanceof Map<?, ?> || x instanceof List<?>) {
                        scalars = false;
                    }
                }
                if (l.isEmpty() == false) {
                    out.put(key, scalars ? l : Json.write(l));
                }
            } else if (v != null) {
                out.put(key, v);
            }
        }
    }

    /** First-seen column order; the type every non-null value supports. */
    static List<Column> inferColumns(List<Map<String, Object>> rows) {
        LinkedHashMap<String, DataType> types = new LinkedHashMap<>();
        for (Map<String, Object> r : rows) {
            for (Map.Entry<String, Object> e : r.entrySet()) {
                List<?> vals = e.getValue() instanceof List<?> l ? l : List.of(e.getValue());
                for (Object v : vals) {
                    DataType t = typeOf(e.getKey(), v);
                    types.merge(e.getKey(), t, WorkflowOutput::widen);
                }
            }
        }
        List<Column> out = new ArrayList<>(types.size());
        types.forEach((n, t) -> out.add(new Column(n, t)));
        return out;
    }

    private static DataType typeOf(String name, Object v) {
        if (v instanceof Boolean) {
            return DataType.BOOLEAN;
        }
        if (v instanceof Long || v instanceof Integer) {
            return DataType.LONG;
        }
        if (v instanceof Number) {
            return DataType.DOUBLE;
        }
        if (name.equals("@timestamp") && v instanceof String s && epochMillis(s) != null) {
            return DataType.DATETIME;
        }
        return DataType.KEYWORD;
    }

    private static DataType widen(DataType a, DataType b) {
        if (a == b) {
            return a;
        }
        if ((a == DataType.LONG && b == DataType.DOUBLE) || (a == DataType.DOUBLE && b == DataType.LONG)) {
            return DataType.DOUBLE;
        }
        return DataType.KEYWORD;
    }

    /** ISO-8601 (with Z or an offset) or epoch milliseconds. */
    static Long epochMillis(Object v) {
        if (v instanceof Number n) {
            return n.longValue();
        }
        if (v instanceof String s && s.isEmpty() == false) {
            try {
                return Instant.parse(s).toEpochMilli();
            } catch (DateTimeParseException e) {
                try {
                    return OffsetDateTime.parse(s).toInstant().toEpochMilli();
                } catch (DateTimeParseException e2) {
                    return null;
                }
            }
        }
        return null;
    }
}
