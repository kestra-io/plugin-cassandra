# Kestra Cassandra Plugin

## What

- Provides plugin components under `io.kestra.plugin.cassandra`.
- Includes classes such as `AstraDbSession`, `Trigger`, `Query`, `CassandraDbSession`.

## Why

- This plugin integrates Kestra with Astra DB.
- It provides tasks that run CQL queries and triggers on DataStax Astra DB.

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
