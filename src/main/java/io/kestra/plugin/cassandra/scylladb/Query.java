package io.kestra.plugin.cassandra.scylladb;

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
    title = "Query ScyllaDB with CQL."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a CQL query to ScyllaDB.",
            code = {
                "session:",
                "  endpoints:",
                "    - hostname: localhost",
                "  username: scylladb_user",
                "  password: scylladb_passwd",
                "cql: SELECT * FROM CQL_KEYSPACE.CQL_TABLE",
                "fetch: true",
            }
        )
    }
)
public class Query extends AbstractQuery {
    @Schema(
        title = "The session connection properties."
    )
    @PluginProperty
    @NotNull
    protected ScyllaDbSession session;


    @Override
    public CqlSession cqlSession(RunContext runContext) throws IllegalVariableEvaluationException {
        return this.session.connect(runContext);
    }
}
