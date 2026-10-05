/*
 * Kibana Workflows connector for ES|QL Data Federation.
 */
package org.elasticsearch.xpack.esql.datasource.workflow;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A dataset's settings: which Kibana, which workflow, its inputs, and where in its output the rows are.
 *
 * <pre>
 *   resource    workflow://my-workflow          the workflow's name or id
 *   kibana_url  https://my-deployment.kb.us-east-1.aws.elastic-cloud.com
 *   inputs      {"index": "logs-*"}             the workflow's inputs (fixed per dataset)
 *   step        list_alerts                     whose output becomes rows (default: the last step with output)
 *   path        hits.hits                       where in that output the rows are (default: found automatically)
 * </pre>
 */
record WorkflowConfig(
    String location,
    String workflow,
    URI kibanaUrl,
    String space,
    Map<String, Object> inputs,
    String step,
    String path,
    boolean verifyTls,
    String caCert,
    Duration timeout
) {

    static final String TYPE = "workflow";
    static final String SCHEME = "workflow";
    static final String DATASOURCE_ENVELOPE_KEY = "_datasource";
    static final String LOCATION_KEY = "location";

    static final Set<String> CONFIG_KEYS = Set.copyOf(new LinkedHashSet<>(java.util.List.of(
        "kibana_url", "space", "inputs", "step", "path", "verify_tls", "ca_cert", "timeout"
    )));

    static boolean handles(String location) {
        return location != null && location.startsWith(SCHEME + "://") && location.length() > (SCHEME + "://").length();
    }

    /** The _datasource envelope flattened underneath dataset-level keys (which win); _-keys dropped. */
    static Map<String, Object> effective(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> out = new HashMap<>();
        if (config.get(DATASOURCE_ENVELOPE_KEY) instanceof Map<?, ?> ds) {
            for (Map.Entry<?, ?> e : ds.entrySet()) {
                out.put(String.valueOf(e.getKey()), e.getValue());
            }
        }
        for (Map.Entry<String, Object> e : config.entrySet()) {
            if (e.getKey().startsWith("_") == false) {
                out.put(e.getKey(), e.getValue());
            }
        }
        return out;
    }

    static WorkflowConfig parse(String location, Map<String, Object> rawConfig) {
        Map<String, Object> config = effective(rawConfig);
        if (location == null) {
            location = str(config, LOCATION_KEY, null);
        }
        if (handles(location) == false) {
            throw new IllegalArgumentException("workflow dataset resource must be workflow://<workflow name or id>, got [" + location + "]");
        }
        // names may hold spaces and other characters a URI host can't, so take the text as is
        String workflow = java.net.URLDecoder.decode(location.substring((SCHEME + "://").length()), java.nio.charset.StandardCharsets.UTF_8);
        if (workflow.endsWith("/")) {
            workflow = workflow.substring(0, workflow.length() - 1);
        }
        String url = str(config, "kibana_url", null);
        if (url == null) {
            throw new IllegalArgumentException("[kibana_url] is required, e.g. https://my-deployment.kb.us-east-1.aws.elastic-cloud.com");
        }
        URI kibana = URI.create(url.endsWith("/") ? url.substring(0, url.length() - 1) : url);
        if (kibana.getScheme() == null || kibana.getHost() == null || (kibana.getScheme().equals("http") == false
            && kibana.getScheme().equals("https") == false)) {
            throw new IllegalArgumentException("[kibana_url] must be an http(s) URL, got [" + url + "]");
        }
        Map<String, Object> inputs = new HashMap<>();
        Object in = config.get("inputs");
        if (in instanceof Map<?, ?> m) {
            m.forEach((k, v) -> inputs.put(String.valueOf(k), v));
        } else if (in instanceof String s && s.isBlank() == false) {
            if (Json.parse(s) instanceof Map<?, ?> m) {
                m.forEach((k, v) -> inputs.put(String.valueOf(k), v));
            } else {
                throw new IllegalArgumentException("[inputs] must be an object, got [" + s + "]");
            }
        } else if (in != null) {
            throw new IllegalArgumentException("[inputs] must be an object, got [" + in + "]");
        }
        long timeout = lng(config, "timeout", 300);
        return new WorkflowConfig(
            location,
            workflow,
            kibana,
            str(config, "space", "default"),
            Map.copyOf(inputs),
            str(config, "step", null),
            str(config, "path", null),
            bool(config, "verify_tls", true),
            str(config, "ca_cert", null),
            Duration.ofSeconds(Math.max(1, timeout))
        );
    }

    static Map<String, Object> resolved(String location, Map<String, Object> rawConfig) {
        Map<String, Object> config = effective(rawConfig);
        Map<String, Object> out = new HashMap<>();
        for (String key : CONFIG_KEYS) {
            Object v = config.get(key);
            if (v != null) {
                out.put(key, v);
            }
        }
        out.put(LOCATION_KEY, location);
        return out;
    }

    /** What changes the rows' shape (never credentials). */
    String cacheKey() {
        return kibanaUrl + "|" + space + "|" + workflow + "|" + inputs + "|" + step + "|" + path;
    }

    // Objects.toString: values may be SecureString or numbers; a (String) cast would CCE.
    private static String str(Map<String, Object> config, String key, String fallback) {
        String s = Objects.toString(config.get(key), null);
        return s == null || s.isEmpty() ? fallback : s;
    }

    private static boolean bool(Map<String, Object> config, String key, boolean fallback) {
        Object v = config.get(key);
        return v == null ? fallback : Boolean.TRUE.equals(v) || "true".equalsIgnoreCase(Objects.toString(v));
    }

    private static long lng(Map<String, Object> config, String key, long fallback) {
        Object v = config.get(key);
        if (v == null) {
            return fallback;
        }
        if (v instanceof Number n) {
            return n.longValue();
        }
        try {
            return Long.parseLong(v.toString().trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("[" + key + "] must be a number, got [" + v + "]");
        }
    }
}
