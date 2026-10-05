/*
 * Kibana Workflows connector for ES|QL Data Federation.
 */
package org.elasticsearch.xpack.esql.datasource.workflow;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * The Kibana Workflows API calls the connector makes (JDK HttpClient, TLS per dataset):
 * <ul>
 *   <li>{@code GET  /api/workflows/workflow/{id}} and {@code GET /api/workflows}: name → id</li>
 *   <li>{@code POST /api/workflows/workflow/{id}/run}: start a run with the dataset's inputs</li>
 *   <li>{@code GET  /api/workflows/executions/{id}?includeOutput=true}: status, then every step's output</li>
 *   <li>{@code GET  /api/workflows/workflow/{id}/executions?statuses=completed&size=1}: the latest run, for the schema</li>
 * </ul>
 */
final class KibanaClient implements AutoCloseable {

    static final Set<String> RUNNING = Set.of("pending", "running", "waiting", "waiting_for_input", "queued");

    /** Workflow name → id, per Kibana + space. */
    private static final Map<String, String> IDS = new ConcurrentHashMap<>();

    private final WorkflowConfig config;
    private final HttpClient http;
    private final String authorization;
    private final UserKey userKey;
    private final String apiBase;

    KibanaClient(WorkflowConfig config) {
        this.config = config;
        HttpClient.Builder b = HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(30)).followRedirects(HttpClient.Redirect.NEVER);
        if (config.kibanaUrl().getScheme().equals("https")) {
            b.sslContext(sslContext(config));
        }
        this.http = b.build();
        // Kibana is always called as the user running the query, with a key made for this call
        this.userKey = UserKey.create(config.timeout());
        this.authorization = userKey.authorization();
        this.apiBase = config.kibanaUrl() + (config.space().equals("default") ? "" : "/s/" + enc(config.space())) + "/api/workflows";
    }

    /** Invalidates the querying user's key. */
    @Override
    public void close() {
        userKey.close();
    }

    /** The workflow's id: the resource is tried as an id first, then matched against workflow names. */
    String workflowId() {
        String key = config.kibanaUrl() + "|" + config.space() + "|" + config.workflow();
        String cached = IDS.get(key);
        if (cached != null) {
            return cached;
        }
        HttpResponse<String> byId = get("/workflow/" + enc(config.workflow()));
        String id = null;
        if (byId.statusCode() == 200) {
            id = config.workflow();
        } else if (byId.statusCode() == 404) {
            for (int page = 1; page <= 50 && id == null; page++) {
                Map<String, Object> list = json(get("?size=100&page=" + page), "list workflows");
                List<?> results = list.get("results") instanceof List<?> l ? l : List.of();
                for (Object o : results) {
                    if (o instanceof Map<?, ?> w && config.workflow().equals(w.get("name"))) {
                        id = String.valueOf(w.get("id"));
                    }
                }
                if (results.size() < 100) {
                    break;
                }
            }
            if (id == null) {
                throw new IllegalArgumentException("no workflow named or with id [" + config.workflow() + "] in Kibana space ["
                    + config.space() + "]");
            }
        } else {
            throw error("get workflow", byId);
        }
        IDS.put(key, id);
        return id;
    }

    /** Starts a run; returns the execution id. */
    String run(String workflowId) {
        String body = Json.write(Map.of("inputs", config.inputs()));
        HttpResponse<String> resp = send(request(apiBase + "/workflow/" + enc(workflowId) + "/run")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build());
        Map<String, Object> m = json(resp, "run workflow");
        Object id = m.get("workflowExecutionId");
        if (id == null) {
            throw new IllegalStateException("Kibana didn't return an execution id: " + m);
        }
        return id.toString();
    }

    /** Polls until the run ends; returns it with every step's output. Throws if it didn't complete. */
    Map<String, Object> await(String executionId) {
        long deadline = System.nanoTime() + config.timeout().toNanos();
        long sleep = 100;
        while (true) {
            Map<String, Object> exec = json(get("/executions/" + enc(executionId) + "?includeOutput=true"), "get execution");
            String status = String.valueOf(exec.get("status"));
            if (RUNNING.contains(status) == false) {
                if (status.equals("completed") == false) {
                    throw new IllegalStateException("workflow [" + config.workflow() + "] run " + executionId + " ended " + status
                        + describeError(exec));
                }
                return exec;
            }
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("workflow [" + config.workflow() + "] run " + executionId + " still " + status + " after "
                    + config.timeout().toSeconds() + "s (raise [timeout])");
            }
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted waiting for workflow [" + config.workflow() + "]", e);
            }
            sleep = Math.min(sleep * 2, 500);
        }
    }

    /** The latest completed run with its outputs, or null when the workflow never completed. */
    Map<String, Object> latestCompleted(String workflowId) {
        Map<String, Object> list = json(get("/workflow/" + enc(workflowId) + "/executions?statuses=completed&size=1&omitStepRuns=true"),
            "list executions");
        if (list.get("results") instanceof List<?> l && l.isEmpty() == false && l.get(0) instanceof Map<?, ?> first) {
            return json(get("/executions/" + enc(String.valueOf(first.get("id"))) + "?includeOutput=true"), "get execution");
        }
        return null;
    }

    private static String describeError(Map<String, Object> exec) {
        StringBuilder b = new StringBuilder();
        if (exec.get("error") != null) {
            b.append(": ").append(errorText(exec.get("error")));
        }
        if (exec.get("stepExecutions") instanceof List<?> steps) {
            for (Object o : steps) {
                if (o instanceof Map<?, ?> s && s.get("error") != null) {
                    b.append(" [step ").append(s.get("stepId")).append(": ").append(errorText(s.get("error"))).append("]");
                }
            }
        }
        return b.toString();
    }

    private static String errorText(Object e) {
        if (e instanceof Map<?, ?> m && m.get("message") != null) {
            return String.valueOf(m.get("message"));
        }
        return String.valueOf(e);
    }

    private HttpResponse<String> get(String path) {
        return send(request(apiBase + path).GET().build());
    }

    private HttpRequest.Builder request(String url) {
        return HttpRequest.newBuilder(URI.create(url))
            .timeout(java.time.Duration.ofSeconds(60))
            .header("Authorization", authorization)
            .header("kbn-xsrf", "esql-datasource-workflow");
    }

    private HttpResponse<String> send(HttpRequest req) {
        try {
            return http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            String hint = "";
            for (Throwable t = e; t != null; t = t.getCause()) {
                if (t instanceof java.nio.channels.UnresolvedAddressException || t instanceof java.net.UnknownHostException) {
                    hint = " (host not found)";
                } else if (t instanceof javax.net.ssl.SSLHandshakeException) {
                    hint = " (TLS: Kibana's certificate isn't trusted; set [ca_cert], or [verify_tls] false)";
                }
            }
            throw new IllegalStateException("Kibana " + config.kibanaUrl() + " unreachable" + hint + ": " + e, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted calling Kibana " + config.kibanaUrl(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> json(HttpResponse<String> resp, String what) {
        if (resp.statusCode() != 200) {
            throw error(what, resp);
        }
        Object parsed = Json.parse(resp.body());
        if (parsed instanceof Map<?, ?> m) {
            return (Map<String, Object>) m;
        }
        throw new IllegalStateException("Kibana " + what + ": unexpected response " + abbreviate(resp.body()));
    }

    private IllegalStateException error(String what, HttpResponse<String> resp) {
        String hint = switch (resp.statusCode()) {
            case 401 -> " (the querying user's key was refused)";
            case 403 -> " (the key/user lacks the Workflows privileges in this space)";
            case 404 -> " (Workflows not available at this Kibana URL/space?)";
            default -> "";
        };
        String msg = resp.body();
        try {
            if (Json.parse(msg) instanceof Map<?, ?> m && m.get("message") != null) {
                msg = String.valueOf(m.get("message"));
            }
        } catch (IllegalArgumentException e) {
            // not JSON
        }
        return new IllegalStateException("Kibana " + what + " failed: HTTP " + resp.statusCode() + hint + ": " + abbreviate(msg));
    }

    private static String abbreviate(String s) {
        return s == null ? "" : s.length() > 500 ? s.substring(0, 500) + "…" : s;
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }

    static SSLContext sslContext(WorkflowConfig config) {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            if (config.verifyTls() == false) {
                ctx.init(null, new TrustManager[] { new TrustAll() }, null);
            } else if (config.caCert() != null) {
                KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
                ks.load(null, null);
                int i = 0;
                for (Certificate c : CertificateFactory.getInstance("X.509")
                    .generateCertificates(new ByteArrayInputStream(config.caCert().getBytes(StandardCharsets.US_ASCII)))) {
                    ks.setCertificateEntry("kibana-ca-" + i++, c);
                }
                if (i == 0) {
                    throw new IllegalArgumentException("[ca_cert] holds no PEM certificate");
                }
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(ks);
                ctx.init(null, tmf.getTrustManagers(), null);
            } else {
                ctx.init(null, null, null);
            }
            return ctx;
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalArgumentException("invalid TLS settings: " + e.getMessage(), e);
        }
    }

    /** Extended so the JDK skips its hostname check too. */
    private static final class TrustAll extends X509ExtendedTrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {}

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {}

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {}

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
