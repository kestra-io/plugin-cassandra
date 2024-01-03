package io.kestra.plugin.astradb;

import com.datastax.oss.driver.api.core.CqlSession;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.cassandra.AbstractQuery;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query an Astra DB with CQL."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a CQL query to an Astra DB.",
            code = {
                "session:",
                "  secureBundle: /path/to/secureBundle.zip",
                "  keyspace: astradb_keyspace",
                "  clientId: astradb_clientId",
                "  clientSecret: astradb_clientSecret",
                "cql: SELECT * FROM CQL_TABLE",
                "fetch: true",
            }
        ),
    }
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
