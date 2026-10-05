/*
 * Kibana Workflows connector for ES|QL Data Federation.
 */
package org.elasticsearch.xpack.esql.datasource.workflow;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Registers the Kibana Workflows connector for ES|QL: {@code FROM <dataset>} runs the dataset's workflow
 * through Kibana's Workflows API and returns its output as rows. Scheme {@code workflow://},
 * data source type {@code workflow}.
 *
 * <p>Requires {@code esql.federation.enabled: true} on every node.
 */
public class WorkflowDataSourcePlugin extends Plugin implements DataSourcePlugin, ActionPlugin {

    private static final Set<String> SCHEMES = Set.of(WorkflowConfig.SCHEME);

    @Override
    public Collection<?> createComponents(PluginServices services) {
        WorkflowNode.init(services.threadPool().getThreadContext(), services.xContentRegistry());
        return List.of();
    }

    /** Registers nothing: this is how a plugin gets the node's REST controller, to make the querying user's key. */
    @Override
    public Collection<RestHandler> getRestHandlers(
        RestHandlersServices services,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        WorkflowNode.init(services.restController());
        return List.of();
    }

    @Override
    public Set<String> supportedSchemes() {
        return SCHEMES;
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        return Map.of(WorkflowConfig.SCHEME, StorageProviderFactory.noConfigKeys(WorkflowStorageProvider::new));
    }

    @Override
    public Set<String> supportedConnectorSchemes() {
        return SCHEMES;
    }

    @Override
    public Map<String, ConnectorFactory> connectors(Settings settings) {
        return Map.of(WorkflowConfig.TYPE, new WorkflowConnectorFactory());
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        DataSourceValidator validator = new WorkflowDataSourceValidator();
        return Map.of(validator.type(), validator);
    }
}
