/*
 * Kibana Workflows connector for ES|QL Data Federation.
 */
package org.elasticsearch.xpack.esql.datasource.workflow;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.HashMap;
import java.util.Map;

/**
 * CRUD-time validator for the {@code workflow} data source type. Registering it is what makes
 * {@code PUT /_query/data_source} accept {@code "type": "workflow"}. Connection settings
 * (kibana_url, TLS) usually go on the data source, the workflow settings on each dataset; any key works on either.
 */
final class WorkflowDataSourceValidator implements DataSourceValidator {

    @Override
    public String type() {
        return WorkflowConfig.TYPE;
    }

    @Override
    public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
        if (datasourceSettings == null || datasourceSettings.isEmpty()) {
            return Map.of();
        }
        ValidationException errors = new ValidationException();
        Map<String, DataSourceSetting> out = new HashMap<>();
        for (Map.Entry<String, Object> e : datasourceSettings.entrySet()) {
            if (WorkflowConfig.CONFIG_KEYS.contains(e.getKey()) == false) {
                errors.addValidationError("unknown setting [" + e.getKey() + "] for data source type [workflow]; recognised: "
                    + WorkflowConfig.CONFIG_KEYS);
                continue;
            }
            // Not marked secret: on 9.5.4 secret settings reach connectors still encrypted (see README).
            out.put(e.getKey(), new DataSourceSetting(e.getValue(), false));
        }
        if (errors.validationErrors().isEmpty() == false) {
            throw errors;
        }
        return out;
    }

    @Override
    public Map<String, Object> validateDataset(
        Map<String, DataSourceSetting> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    ) {
        ValidationException errors = new ValidationException();
        if (WorkflowConfig.handles(resource) == false) {
            errors.addValidationError("dataset resource must be workflow://<workflow name or id>, got [" + resource + "]");
        }
        Map<String, Object> merged = new HashMap<>();
        if (datasourceSettings != null) {
            datasourceSettings.forEach((k, v) -> merged.put(k, v.rawValue()));
        }
        if (datasetSettings != null) {
            for (Map.Entry<String, Object> e : datasetSettings.entrySet()) {
                if (WorkflowConfig.CONFIG_KEYS.contains(e.getKey()) == false) {
                    errors.addValidationError("unknown dataset setting [" + e.getKey() + "] for data source type [workflow]; recognised: "
                        + WorkflowConfig.CONFIG_KEYS);
                }
                merged.put(e.getKey(), e.getValue());
            }
        }
        if (errors.validationErrors().isEmpty()) {
            try {
                WorkflowConfig.parse(resource, merged);
            } catch (IllegalArgumentException e) {
                errors.addValidationError(e.getMessage());
            }
        }
        if (errors.validationErrors().isEmpty() == false) {
            throw errors;
        }
        return datasetSettings == null ? Map.of() : Map.copyOf(datasetSettings);
    }
}
