/*
 * Kibana Workflows connector for ES|QL Data Federation.
 */
package org.elasticsearch.xpack.esql.datasource.workflow;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/** Pages over a workflow run's rows, converting each value to its column's ES|QL type (null when it can't). */
final class WorkflowResultCursor implements ResultCursor {

    static final int DEFAULT_BATCH = 1000;

    private final Iterator<Map<String, Object>> rows;
    private final List<Attribute> attributes;
    private final BlockFactory blockFactory;
    private final int batchSize;
    private final long rowLimit;
    private long emitted;

    WorkflowResultCursor(List<Map<String, Object>> rows, List<Attribute> attributes, BlockFactory blockFactory, int batchSize, long rowLimit) {
        this.rows = rows.iterator();
        this.attributes = attributes;
        this.blockFactory = blockFactory;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH;
        this.rowLimit = rowLimit < 0 ? Long.MAX_VALUE : rowLimit;
    }

    @Override
    public boolean hasNext() {
        return rows.hasNext() && emitted < rowLimit;
    }

    @Override
    public Page next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        Block.Builder[] builders = new Block.Builder[attributes.size()];
        try {
            for (int i = 0; i < builders.length; i++) {
                DataType t = attributes.get(i).dataType();
                builders[i] = t == DataType.LONG || t == DataType.DATETIME ? blockFactory.newLongBlockBuilder(batchSize)
                    : t == DataType.DOUBLE ? blockFactory.newDoubleBlockBuilder(batchSize)
                    : t == DataType.BOOLEAN ? blockFactory.newBooleanBlockBuilder(batchSize)
                    : blockFactory.newBytesRefBlockBuilder(batchSize);
            }
            int n = 0;
            while (n < batchSize && rows.hasNext() && emitted < rowLimit) {
                Map<String, Object> row = rows.next();
                for (int i = 0; i < builders.length; i++) {
                    append(builders[i], attributes.get(i).dataType(), row.get(attributes.get(i).name()));
                }
                n++;
                emitted++;
            }
            Block[] blocks = new Block[builders.length];
            try {
                for (int i = 0; i < builders.length; i++) {
                    blocks[i] = builders[i].build();
                }
            } catch (RuntimeException e) {
                Releasables.closeExpectNoException(blocks);
                throw e;
            }
            return new Page(n, blocks);
        } finally {
            Releasables.closeExpectNoException(builders);
        }
    }

    static void append(Block.Builder b, DataType type, Object value) {
        List<?> vals = value instanceof List<?> l ? l : value == null ? List.of() : List.of(value);
        List<Object> converted = new ArrayList<>(vals.size());
        for (Object v : vals) {
            Object c = convert(type, v);
            if (c != null) {
                converted.add(c);
            }
        }
        if (converted.isEmpty()) {
            b.appendNull();
            return;
        }
        if (converted.size() > 1) {
            b.beginPositionEntry();
        }
        for (Object c : converted) {
            switch (c) {
                case Long l when type == DataType.LONG || type == DataType.DATETIME -> ((LongBlock.Builder) b).appendLong(l);
                case Double d -> ((DoubleBlock.Builder) b).appendDouble(d);
                case Boolean bool -> ((BooleanBlock.Builder) b).appendBoolean(bool);
                default -> ((BytesRefBlock.Builder) b).appendBytesRef(new BytesRef(c.toString()));
            }
        }
        if (converted.size() > 1) {
            b.endPositionEntry();
        }
    }

    static Object convert(DataType type, Object v) {
        if (v == null) {
            return null;
        }
        if (type == DataType.LONG) {
            if (v instanceof Long || v instanceof Integer) {
                return ((Number) v).longValue();
            }
            try {
                return new BigDecimal(v.toString().trim()).longValueExact();
            } catch (ArithmeticException | NumberFormatException e) {
                return null;
            }
        }
        if (type == DataType.DOUBLE) {
            if (v instanceof Number n) {
                return n.doubleValue();
            }
            try {
                return Double.parseDouble(v.toString().trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        if (type == DataType.BOOLEAN) {
            if (v instanceof Boolean bool) {
                return bool;
            }
            String s = v.toString();
            return s.equalsIgnoreCase("true") ? Boolean.TRUE : s.equalsIgnoreCase("false") ? Boolean.FALSE : null;
        }
        if (type == DataType.DATETIME) {
            return WorkflowOutput.epochMillis(v);
        }
        return v instanceof String s ? s : v instanceof Map<?, ?> || v instanceof List<?> ? Json.write(v) : String.valueOf(v);
    }

    @Override
    public void close() {}
}
