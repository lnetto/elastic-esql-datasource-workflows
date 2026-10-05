# elastic-esql-datasource-workflows

An **ES|QL Data Federation** connector that makes a **Kibana workflow** a data source.
`FROM <dataset>` runs the workflow **as the user running the query**, waits for it, and returns its output
as rows. No credentials to store.

```esql
FROM wf_tor_exits                               // runs the "tor-exit-nodes" workflow
| EVAL ip = TO_IP(value)
| WHERE CIDR_MATCH(ip, "185.220.0.0/16")
| STATS exits = COUNT(*)
```

Anything a workflow can reach (a URL via `http`, a connector, an Elasticsearch request, data built with
`data.set`) becomes queryable with `WHERE`, `EVAL`, `STATS`, `LOOKUP JOIN` and the rest.

> **Experimental.** ES|QL Data Federation is experimental in Elasticsearch 9.5 and this plugin uses its
> internal SPI. Each zip works with exactly one Elasticsearch version. Community project, not an official
> Elastic product.

## 1. Install the plugin

Use `dist/esql-datasource-workflow-0.2.0-es9.5.4.zip`, or build it:

```bash
./build-plugin.sh    # -> dist/esql-datasource-workflow-0.2.0-es9.5.4.zip
```

**Self-managed**, on every node, then restart them one by one:

```bash
bin/elasticsearch-plugin install file:///path/to/esql-datasource-workflow-0.2.0-es9.5.4.zip
echo 'esql.federation.enabled: true' >> config/elasticsearch.yml
```

**Docker**, with the plugin baked into the image:

```bash
docker build -f docker/Dockerfile -t elasticsearch-esql-workflow:9.5.4 .
```

Run it with `-e esql.federation.enabled=true`.

**Elastic Cloud Hosted:**

1. **Deployments → Extensions → Create extension**: type *plugin*, version `9.5.4`, upload the zip.
2. **Edit deployment → Elasticsearch → Manage plugins and extensions**: enable it.
3. **Elasticsearch user settings**: `esql.federation.enabled: true`. Save (rolling restart).

Needs a licence with ES|QL Data Federation (Enterprise or trial). Check with `GET _cat/plugins`.

## 2. Create the data source

It only needs your Kibana's URL (on Elastic Cloud, the deployment's Kibana endpoint; Elasticsearch must be
able to reach it). Kibana's Data Federation UI can't create this type, so use **Dev Tools**:

```
PUT _query/data_source/workflows
{"type":"workflow","settings":{"kibana_url":"https://my-deployment.kb.us-east-1.aws.elastic-cloud.com"}}
```

There's no key to store. For each query the plugin makes a short-lived API key for the person running it,
calls Kibana with it, and invalidates it straight after, so the workflow runs with *their* privileges and
Kibana records them as the one who ran it.

## 3. Create the sample workflows

Two samples: fetch a URL, maybe parse it, return rows. In **Kibana → Workflows**, create each one.

**`elasticsearch-eol`**: JSON. The `http` step parses a JSON response for you, so one step is enough.

```yaml
name: elasticsearch-eol
description: Elasticsearch release cycles and end-of-life dates, from endoflife.date
enabled: true
triggers:
  - type: manual
steps:
  - name: fetch
    type: http
    with:
      url: https://endoflife.date/api/elasticsearch.json
      method: GET
```

**`tor-exit-nodes`**: plain text. A text body arrives base64-encoded: decode it, then split it into lines.

```yaml
name: tor-exit-nodes
description: Tor exit node IPs, one row per IP
enabled: true
triggers:
  - type: manual
steps:
  - name: fetch
    type: http
    with:
      url: https://check.torproject.org/torbulkexitlist
      method: GET
  - name: parse
    type: data.set
    with:
      ips: "${{ steps.fetch.output.data | base64_decode | strip | split: '\n' }}"
```

## 4. Create the datasets

One dataset per workflow (and per set of inputs). Name the workflow, or give its id:

```
PUT _query/dataset/wf_es_eol
{"data_source":"workflows","resource":"workflow://elasticsearch-eol"}

PUT _query/dataset/wf_tor_exits
{"data_source":"workflows","resource":"workflow://tor-exit-nodes","settings":{"path":"ips"}}
```

`wf_tor_exits` sets `path` because that workflow's output is `{"ips": [...]}`.

| setting | default | |
|---|---|---|
| `resource` | | `workflow://<name or id>`, spaces as `%20` |
| `kibana_url` | required | Kibana's base URL |
| `space` | `default` | the Kibana space the workflow is in |
| `inputs` | `{}` | the workflow's inputs, fixed per dataset |
| `step` | last step with output | whose output becomes rows |
| `path` | found automatically | where the rows are, e.g. `ips` or `hits.hits` |
| `timeout` | `300` | seconds to wait for a run |
| `verify_tls`, `ca_cert` | `true` | Kibana TLS: trust a PEM CA, or nothing (`false`) |

Any setting can go on the data source or the dataset; the dataset wins.

Rows are found automatically: an ES|QL response gives its rows and types, a list one row per element
(scalars in a `value` column), a search one per hit, an object holding one list of objects that list;
anything else is one row. Nested objects become dotted columns. The columns come from the workflow's
latest completed run, so planning doesn't run it (a workflow that has never completed runs once, and that
run is the query's).

## 5. Query

```esql
FROM wf_es_eol                                  // 12 rows: cycle, latest, eol, lts, …
| WHERE eol != "false"
| EVAL eol_date = DATE_PARSE("yyyy-MM-dd", eol)
| WHERE eol_date < NOW()
| KEEP cycle, latest, eol_date | SORT eol_date DESC
```

```esql
FROM wf_tor_exits                               // one row per IP, in column `value`
| EVAL ip = TO_IP(value)
| WHERE CIDR_MATCH(ip, "185.220.101.0/24")
| STATS exits = COUNT(*)
```

## Things to know

* **Every query runs the workflow**, including each Discover refresh. Use workflows that only read.
* **About 3 s per query** plus the workflow's own time (Kibana's task manager cycle). Not for busy dashboards.
* `WHERE` and `LIMIT` run in ES|QL after the workflow. Filter inside the workflow, or with `inputs`.
* A failed run fails the query with the workflow's error.

## Repository

| path | |
|---|---|
| `plugin/` | the plugin (Java 21, ES 9.5.4) and its tests (a real Kibana 9.5.4 execution) |
| `docker/` | Elasticsearch image with the plugin baked in |
| `scripts/extract-es-jars.sh` | pulls the SPI jars out of the stock ES image |
