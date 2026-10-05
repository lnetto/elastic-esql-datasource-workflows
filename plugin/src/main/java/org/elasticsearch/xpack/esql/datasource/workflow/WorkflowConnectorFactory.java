/*
 * Kibana Workflows connector for ES|QL Data Federation.
 */
package org.elasticsearch.xpack.esql.datasource.workflow;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.spi.ConfigKeyValidator;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Resolves a workflow dataset's columns and opens connectors.
 *
 * <p>The columns come from the workflow's latest completed run, so planning a query never runs the
 * workflow. When it has never completed, resolution runs it once and keeps that run for the query
 * being planned ({@link #takeFresh}), so the workflow still runs once per query.
 */
final class WorkflowConnectorFactory implements ConnectorFactory {

    static final long FRESH_MILLIS = TimeUnit.SECONDS.toMillis(60);

    private record Fresh(Map<String, Object> execution, long expiresAt) {}

    /** A run made during resolution, handed to the query's execution instead of running again. */
    private static final Map<String, Fresh> FRESH = new ConcurrentHashMap<>();

    @Override
    public String type() {
        return WorkflowConfig.TYPE;
    }

    @Override
    public boolean canHandle(String location) {
        return WorkflowConfig.handles(location);
    }

    @Override
    public void validateConfig(String location, Map<String, Object> config) {
        ConfigKeyValidator.check(WorkflowConfig.effective(config), List.of(WorkflowConfig.CONFIG_KEYS));
        WorkflowConfig.parse(location, config);
    }

    @Override
    public SourceMetadata resolveMetadata(String location, Map<String, Object> rawConfig) {
        WorkflowConfig config = WorkflowConfig.parse(location, rawConfig);
        String id;
        Map<String, Object> exec;
        try (KibanaClient client = new KibanaClient(config)) {
            id = client.workflowId();
            exec = client.latestCompleted(id);
            if (exec == null) {
                exec = client.await(client.run(id));
                FRESH.put(freshKey(config), new Fresh(exec, System.currentTimeMillis() + FRESH_MILLIS));
            }
        }
        WorkflowOutput.Table table = WorkflowOutput.toTable(WorkflowOutput.stepOutput(exec, config.step()), config.path());
        if (table.columns().isEmpty()) {
            throw new IllegalStateException("workflow [" + config.workflow() + "]'s latest run produced no columns"
                + (config.step() == null ? "; pick the step with [step]" : "") + (config.path() == null ? " or the rows with [path]" : ""));
        }
        List<Attribute> attributes = new ArrayList<>(table.columns().size());
        for (WorkflowOutput.Column c : table.columns()) {
            attributes.add(new ReferenceAttribute(Source.EMPTY, null, c.name(), c.type(), Nullability.TRUE, null, false));
        }
        // sourceType must be the URI scheme: the operator registries key connectors by scheme.
        return new SimpleSourceMetadata(attributes, WorkflowConfig.SCHEME, location, null, null, Map.of("workflow_id", id),
            WorkflowConfig.resolved(location, rawConfig));
    }

    /** The run resolution just made for this dataset, if any (used once). */
    static Map<String, Object> takeFresh(WorkflowConfig config) {
        Fresh f = FRESH.remove(freshKey(config));
        return f != null && f.expiresAt() > System.currentTimeMillis() ? f.execution() : null;
    }

    /** Per dataset and per user: one user's run never answers another's query. */
    private static String freshKey(WorkflowConfig config) {
        return config.cacheKey() + "|" + Json.sha256(String.valueOf(WorkflowNode.identity()));
    }

    @Override
    public Connector open(Map<String, Object> rawConfig) {
        return new WorkflowConnector(WorkflowConfig.parse(null, rawConfig));
    }
}
