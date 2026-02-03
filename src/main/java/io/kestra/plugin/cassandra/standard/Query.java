package io.kestra.plugin.cassandra.standard;

import com.datastax.oss.driver.api.core.CqlSession;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.cassandra.AbstractQuery;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run a CQL query on Cassandra",
    description = "Executes a CQL statement against Cassandra. Use fetchType to control result handling (FETCH, STORE, FETCH_ONE); NONE is the default and only records metrics."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a CQL query to a Cassandra database.",
            full = true,
            code = """
                id: cassandra_standard_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.cassandra.standard.Query
                    session:
                      endpoints:
                        - hostname: localhost
                      secureConnection:
                        truststorePath: path to .crt file
                        truststorePassword: truststore_password
                        keystorePath: path to .jks file
                        keystorePassword: keystore_password
                      username: cassandra_user
                      password: cassandra_passwd
                    cql: SELECT * FROM CQL_KEYSPACE.CQL_TABLE
                    fetchType: FETCH
                """
        ),
    },
    aliases = "io.kestra.plugin.cassandra.Query"
)
public class Query extends AbstractQuery {
    @Schema(
        title = "Cassandra session configuration",
        description = "Connection settings including endpoints, datacenter, auth, and optional TLS. Required to open the query session."
    )
    @PluginProperty
    @NotNull
    protected CassandraDbSession session;


    @Override
    public CqlSession cqlSession(RunContext runContext) throws IllegalVariableEvaluationException {
        return this.session.connect(runContext);
    }
}
