# Kestra Cassandra Plugin

## What

Utilize Apache Cassandra in Kestra workflows for data management. Exposes 5 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Apache Cassandra, allowing orchestration of Apache Cassandra-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
