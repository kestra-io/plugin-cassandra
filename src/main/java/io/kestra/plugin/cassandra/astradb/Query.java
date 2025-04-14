package io.kestra.plugin.cassandra.astradb;

import com.datastax.oss.driver.api.core.CqlSession;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
    title = "Query Astra DB with CQL."
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
        title = "The session connection properties."
    )
    @PluginProperty
    @NotNull
    protected AstraDbSession session;

    @Override
    public CqlSession cqlSession(RunContext runContext) throws IllegalVariableEvaluationException {
        return this.session.connect(runContext);
    }
}
