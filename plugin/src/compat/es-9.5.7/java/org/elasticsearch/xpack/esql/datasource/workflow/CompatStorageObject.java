/*
 * Kibana Workflows connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.workflow;

import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

/**
 * {@link StorageObject} with the parts of the SPI that differ between Elasticsearch releases
 * (Elasticsearch 9.5.7 and later). Build.gradle picks this source set or es-pre-9.5.7 by esVersion.
 */
interface CompatStorageObject extends StorageObject {

    @Override
    default StorageIdentity storageIdentity() {
        // Workflow output changes on every run, so never share cached state.
        return StorageIdentity.unique();
    }
}
