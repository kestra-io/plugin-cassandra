# How to use the Cassandra plugin

Run CQL queries against a self-managed Cassandra cluster or DataStax Astra DB from Kestra flows.

## Authentication

Connection details are configured via a `session` object on each task.

**Standard Cassandra**: set `endpoints` (a list of `hostname`/`port` objects), `localDatacenter`, and optionally `username` and `password` for plain-text auth. Enable SSL/TLS via `secureConnection` with `truststorePath` and `keystorePath`.

**Astra DB**: set `secureBundle` (a base64-encoded secure connect bundle ZIP available from the Astra console), `keyspace`, `clientId`, and `clientSecret`.

Store credentials in [secrets](https://kestra.io/docs/concepts/secret) and apply `session` globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults) when all tasks in a flow target the same cluster.

## Tasks

`standard.Query` and `astradb.Query` execute a CQL statement set in `cql`. Control output with `fetchType`: `FETCH_ONE` returns the first row, `FETCH` returns all rows, `STORE` streams rows to a file in internal storage for large result sets, and `NONE` discards results.

`standard.Trigger` and `astradb.Trigger` poll the database on a schedule and start one execution when the query returns rows — use them to react to new data arriving in a Cassandra table.
