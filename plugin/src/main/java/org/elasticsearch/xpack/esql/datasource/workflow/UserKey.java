/*
 * Kibana Workflows connector for ES|QL Data Federation.
 */
package org.elasticsearch.xpack.esql.datasource.workflow;

import org.elasticsearch.rest.RestRequest;

import java.time.Duration;
import java.util.Map;

/**
 * A short-lived API key for the user running the query, so Kibana runs the workflow as them.
 *
 * <p>Made in memory through this node's {@code _security/api_key} handler, in the thread context that
 * already carries the user's authentication (as their own request would be), and invalidated by
 * {@link #close}. Without role descriptors the key gets the user's own privileges. Needs
 * {@code manage_own_api_key}. Elasticsearch won't give privileges to a key made by an API key, so a user
 * logged in with one gets an error saying to log in as a user.
 */
final class UserKey implements AutoCloseable {

    private static final Duration CALL_TIMEOUT = Duration.ofSeconds(30);

    private final String id;
    private final String encoded;

    private UserKey(String id, String encoded) {
        this.id = id;
        this.encoded = encoded;
    }

    /** A key that outlives a run of {@code runTimeout} by a minute (it's invalidated sooner, on close). */
    static UserKey create(Duration runTimeout) {
        if (WorkflowNode.identity() == null) {
            throw new IllegalStateException("no user to run the workflow as: Elasticsearch security must be on");
        }
        Map<?, ?> me = json(InternalRest.call(RestRequest.Method.GET, "/_security/_authenticate", null, CALL_TIMEOUT), "who is querying");
        if ("api_key".equals(me.get("authentication_type"))) {
            throw new IllegalStateException("you're logged in with an API key, and Elasticsearch won't let an API key make one with "
                + "privileges; log in as a user to run workflows from ES|QL");
        }
        String body = Json.write(Map.of(
            "name", "esql-datasource-workflow",
            "expiration", (runTimeout.toSeconds() + 60) + "s",
            "metadata", Map.of("created_by", "esql-datasource-workflow")
        ));
        Map<?, ?> key = json(InternalRest.call(RestRequest.Method.POST, "/_security/api_key", body, CALL_TIMEOUT),
            "create an API key for [" + me.get("username") + "] (needs manage_own_api_key)");
        return new UserKey(String.valueOf(key.get("id")), String.valueOf(key.get("encoded")));
    }

    String authorization() {
        return "ApiKey " + encoded;
    }

    /** Invalidates the key; it expires on its own if this fails. */
    @Override
    public void close() {
        try {
            InternalRest.call(RestRequest.Method.DELETE, "/_security/api_key",
                Json.write(Map.of("ids", java.util.List.of(id), "owner", true)), CALL_TIMEOUT);
        } catch (RuntimeException e) {
            // ponytail: best effort; the key expires a minute after the run's timeout anyway
        }
    }

    private static Map<?, ?> json(InternalRest.Response resp, String what) {
        Object parsed = resp.body().isEmpty() ? null : Json.parse(resp.body());
        if (resp.status() / 100 != 2 || parsed instanceof Map<?, ?> == false) {
            String reason = parsed instanceof Map<?, ?> m && m.get("error") instanceof Map<?, ?> err
                ? err.get("type") + ": " + err.get("reason")
                : resp.body();
            throw new IllegalStateException("couldn't " + what + ": HTTP " + resp.status() + " " + reason);
        }
        return (Map<?, ?>) parsed;
    }
}
