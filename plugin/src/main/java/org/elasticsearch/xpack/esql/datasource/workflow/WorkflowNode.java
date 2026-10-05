/*
 * Kibana Workflows connector for ES|QL Data Federation.
 */
package org.elasticsearch.xpack.esql.datasource.workflow;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.xcontent.NamedXContentRegistry;

/**
 * This node's thread context (where ES|QL keeps the querying user's authentication) and its REST
 * controller (where the _security API handlers are). Set when the plugin's components and REST handlers are created.
 */
final class WorkflowNode {

    /** Where Elasticsearch security keeps the authenticated identity of the current request. */
    static final String AUTHENTICATION_HEADER = "_xpack_security_authentication";

    private static volatile ThreadContext threadContext;
    private static volatile RestController restController;
    private static volatile NamedXContentRegistry xContentRegistry = NamedXContentRegistry.EMPTY;

    private WorkflowNode() {}

    static void init(ThreadContext tc, NamedXContentRegistry registry) {
        threadContext = tc;
        xContentRegistry = registry;
    }

    /** Request bodies (e.g. a _search query) parse with the node's registry of query/aggregation types. */
    static NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    static void init(RestController rc) {
        restController = rc;
    }

    static ThreadContext threadContext() {
        ThreadContext tc = threadContext;
        if (tc == null) {
            throw new IllegalStateException("esql-datasource-workflow isn't initialised on this node");
        }
        return tc;
    }

    static RestController restController() {
        RestController rc = restController;
        if (rc == null) {
            throw new IllegalStateException("esql-datasource-workflow has no REST controller on this node");
        }
        return rc;
    }

    /** Who is calling (Elasticsearch's serialized authentication), or null with security off. */
    static String identity() {
        return threadContext().getHeader(AUTHENTICATION_HEADER);
    }
}
