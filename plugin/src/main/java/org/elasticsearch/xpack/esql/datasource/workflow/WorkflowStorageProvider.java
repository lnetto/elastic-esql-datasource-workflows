/*
 * Kibana Workflows connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.workflow;

import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

/**
 * Minimal {@link StorageProvider} for {@code workflow://}. Workflows are
 * run by the {@link WorkflowConnector}, not read as byte streams; this exists so the resolver can
 * register a FileList entry for the schemes (same role FlightStorageProvider plays for Flight).
 */
public final class WorkflowStorageProvider implements StorageProvider {

    @Override
    public StorageObject newObject(StoragePath path) {
        return newObject(path, 0L, null);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        return newObject(path, length, null);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        validateScheme(path);
        return new ApiObject(path, length, lastModified);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
        throw new UnsupportedOperationException("Workflows are run by the workflow connector");
    }

    @Override
    public boolean exists(StoragePath path) {
        validateScheme(path);
        // Reachability is checked by the connector at schema-resolution time.
        return true;
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of(WorkflowConfig.SCHEME);
    }

    @Override
    public boolean supportsStableMetadata() {
        // Workflow output changes on every run; never cache under a stale identity.
        return false;
    }

    @Override
    public void close() {}

    private static void validateScheme(StoragePath path) {
        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        if (supported(scheme) == false) {
            throw new IllegalArgumentException("WorkflowStorageProvider only supports workflow://, got: " + scheme);
        }
    }

    private static boolean supported(String scheme) {
        return WorkflowConfig.SCHEME.equals(scheme);
    }

    private record ApiObject(StoragePath path, long knownLength, Instant knownLastModified) implements StorageObject {

        @Override
        public InputStream newStream() throws IOException {
            throw notByteAddressable();
        }

        @Override
        public InputStream newStream(long position, long length) throws IOException {
            throw notByteAddressable();
        }

        private static IOException notByteAddressable() {
            return new IOException("Workflows are run by the workflow connector, not read as byte streams");
        }

        @Override
        public long length() {
            return knownLength;
        }

        @Override
        public Instant lastModified() {
            return knownLastModified;
        }

        @Override
        public boolean exists() {
            return true;
        }
    }
}
