# Kestra Cassandra Plugin

## What

- Provides plugin components under `io.kestra.plugin.cassandra`.
- Includes classes such as `AstraDbSession`, `Trigger`, `Query`, `CassandraDbSession`.

## Why

- What user problem does this solve? Teams need to integrate Apache Cassandra into Kestra workflows for querying and event-driven triggers from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Apache Cassandra steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Apache Cassandra.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `cassandra`

Infrastructure dependencies (Docker Compose services):

- `cassandra`

### Key Plugin Classes

- `io.kestra.plugin.cassandra.AbstractQuery`
- `io.kestra.plugin.cassandra.astradb.Query`
- `io.kestra.plugin.cassandra.astradb.Trigger`
- `io.kestra.plugin.cassandra.standard.Query`
- `io.kestra.plugin.cassandra.standard.Trigger`

### Project Structure

```
plugin-cassandra/
├── src/main/java/io/kestra/plugin/cassandra/standard/
├── src/test/java/io/kestra/plugin/cassandra/standard/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
