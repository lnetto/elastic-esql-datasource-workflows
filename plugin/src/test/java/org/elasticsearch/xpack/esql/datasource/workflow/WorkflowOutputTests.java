/*
 * Kibana Workflows connector for ES|QL Data Federation.
 */
package org.elasticsearch.xpack.esql.datasource.workflow;

import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Against a real Kibana 9.5.4 workflow execution (src/test/resources, GET …/executions/{id}?includeOutput=true). */
class WorkflowOutputTests {

    @SuppressWarnings("unchecked")
    static Map<String, Object> execution() throws IOException {
        return (Map<String, Object>) Json.parse(Files.readString(Path.of("src/test/resources/execution-with-output.json")));
    }

    static List<String> names(WorkflowOutput.Table t) {
        return t.columns().stream().map(c -> c.name() + ":" + c.type().typeName()).toList();
    }

    @Test
    void lastStepListOfObjects() throws IOException {
        WorkflowOutput.Table t = WorkflowOutput.toTable(WorkflowOutput.stepOutput(execution(), null), null);
        assertEquals(List.of("name:keyword", "team:keyword", "score:double", "active:boolean"), names(t));
        assertEquals(3, t.rows().size());
        assertEquals("ann", t.rows().get(0).get("name"));
    }

    @Test
    void esqlStepKeepsItsTypes() throws IOException {
        WorkflowOutput.Table t = WorkflowOutput.toTable(WorkflowOutput.stepOutput(execution(), "cluster_indices"), null);
        assertEquals(List.of("docs:long", "_index:keyword"), names(t));
        assertEquals(5, t.rows().size());
        assertEquals(157L, t.rows().get(0).get("docs"));
    }

    @Test
    void explicitPathAndErrors() throws IOException {
        WorkflowOutput.Table t = WorkflowOutput.toTable(WorkflowOutput.stepOutput(execution(), "rows"), "people.1");
        assertEquals(1, t.rows().size());
        assertEquals("bob", t.rows().get(0).get("name"));
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> WorkflowOutput.stepOutput(execution(), "nope"));
        assertTrue(e.getMessage().contains("cluster_indices"), e.getMessage());
        assertThrows(IllegalStateException.class, () -> WorkflowOutput.toTable(Map.of("a", 1), "b.c"));
    }

    @Test
    void searchHitsNestingAndTypeWidening() {
        Map<String, Object> resp = Map.of("hits", Map.of("hits", List.of(
            Map.of("_id", "1", "_index", "logs", "_source", Map.of("@timestamp", "2026-10-01T20:43:41.584Z", "user", Map.of("name", "ann"),
                "n", 1, "tags", List.of("a", "b"))),
            Map.of("_id", "2", "_index", "logs", "_source", Map.of("@timestamp", "2026-10-01T20:44:00Z", "user", Map.of("name", "bob"),
                "n", 2.5, "tags", List.of(Map.of("k", "v")))))));
        WorkflowOutput.Table t = WorkflowOutput.toTable(resp, null);
        Map<String, DataType> types = new java.util.HashMap<>();
        t.columns().forEach(c -> types.put(c.name(), c.type()));
        assertEquals(DataType.DATETIME, types.get("@timestamp"));
        assertEquals(DataType.KEYWORD, types.get("user.name"));
        assertEquals(DataType.DOUBLE, types.get("n"));
        assertEquals(DataType.KEYWORD, types.get("_id"));
        assertEquals(List.of("a", "b"), t.rows().get(0).get("tags"));
        assertEquals("[{\"k\":\"v\"}]", t.rows().get(1).get("tags"));
    }

    @Test
    void scalarsAndSingleObjects() {
        assertEquals(List.of("value:long"), names(WorkflowOutput.toTable(List.of(1, 2, 3), null)));
        WorkflowOutput.Table one = WorkflowOutput.toTable(Map.of("status", "ok", "count", 4), null);
        assertEquals(1, one.rows().size());
    }

    @Test
    void conversions() {
        assertEquals(12L, WorkflowResultCursor.convert(DataType.LONG, "12"));
        assertNull(WorkflowResultCursor.convert(DataType.LONG, "12.5"));
        assertEquals(1790887421584L, WorkflowResultCursor.convert(DataType.DATETIME, "2026-10-01T20:43:41.584Z"));
        assertEquals(Boolean.TRUE, WorkflowResultCursor.convert(DataType.BOOLEAN, "true"));
        assertEquals("3.5", WorkflowResultCursor.convert(DataType.KEYWORD, 3.5));
    }

    @Test
    void config() {
        WorkflowConfig c = WorkflowConfig.parse("workflow://My%20Workflow", Map.of("kibana_url", "https://kb.example.com/",
            "inputs", Map.of("index", "logs-*"), "space", "secops"));
        assertEquals("My Workflow", c.workflow());
        assertEquals("https://kb.example.com", c.kibanaUrl().toString());
        assertEquals(Map.of("index", "logs-*"), c.inputs());
        assertThrows(IllegalArgumentException.class, () -> WorkflowConfig.parse("workflow://x", Map.of("space", "s")));
        assertThrows(IllegalArgumentException.class, () -> WorkflowConfig.parse("workflow://", Map.of("kibana_url", "https://kb")));
    }
}
