package io.kestra.plugin.cassandra.astradb;

import com.datastax.oss.driver.api.core.CqlSession;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.cassandra.AbstractQuery;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run a CQL query on Astra DB",
    description = "Executes a CQL statement against Astra DB. Use fetchType to control result handling (FETCH, STORE, FETCH_ONE); NONE is the default and only records metrics."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a CQL query to an Astra DB.",
            full = true,
            code = """
                id: cassandra_astradb_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.cassandra.astradb.Query
                    session:
                      secureBundle: /path/to/secureBundle.zip
                      keyspace: astradb_keyspace
                      clientId: astradb_clientId
                      clientSecret: astradb_clientSecret
                    cql: SELECT * FROM CQL_TABLE
                    fetchType: FETCH
                """
        ),
    },
    aliases = "io.kestra.plugin.astradb.Query"
)
public class Query extends AbstractQuery {
    @Schema(
        title = "Astra DB session configuration",
        description = "Connection settings including secure bundle or proxy (choose one), keyspace, and service credentials."
    )
    @PluginProperty(group = "main")
    @NotNull
    protected AstraDbSession session;

    @Override
    public CqlSession cqlSession(RunContext runContext) throws IllegalVariableEvaluationException {
        return this.session.connect(runContext);
    }
}
