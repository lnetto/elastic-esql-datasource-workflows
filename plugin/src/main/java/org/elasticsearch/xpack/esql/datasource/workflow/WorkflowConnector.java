/*
 * Kibana Workflows connector for ES|QL Data Federation.
 */
package org.elasticsearch.xpack.esql.datasource.workflow;

import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.util.Map;

/** Runs the workflow (once per query), waits for it, and hands its output's rows to ES|QL. */
final class WorkflowConnector implements Connector {

    private final WorkflowConfig config;

    WorkflowConnector(WorkflowConfig config) {
        this.config = config;
    }

    @Override
    public ResultCursor execute(QueryRequest request, Split split) {
        Map<String, Object> exec = WorkflowConnectorFactory.takeFresh(config);
        if (exec == null) {
            try (KibanaClient client = new KibanaClient(config)) {
                exec = client.await(client.run(client.workflowId()));
            }
        }
        WorkflowOutput.Table table = WorkflowOutput.toTable(WorkflowOutput.stepOutput(exec, config.step()), config.path());
        int limit = request.rowLimit();
        return new WorkflowResultCursor(
            table.rows(),
            request.attributes(),
            request.blockFactory(),
            request.batchSize(),
            limit == FormatReader.NO_LIMIT || limit < 0 ? -1 : limit
        );
    }

    @Override
    public ResultCursor execute(QueryRequest request, ExternalSplit split) {
        return execute(request, Split.SINGLE);
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
        return "WorkflowConnector[" + config.kibanaUrl() + " " + config.workflow() + "]";
    }
}
