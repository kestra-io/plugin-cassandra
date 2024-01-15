package io.kestra.plugin.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a Cassandra database with CQL."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a CQL query to a Cassandra database.",
            code = {
                "session:",
                "  endpoints:",
                "    - hostname: localhost",
                "  username: cassandra_user",
                "  password: cassandra_passwd",
                "cql: SELECT * FROM CQL_KEYSPACE.CQL_TABLE",
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
    protected CassandraDbSession session;


    @Override
    public CqlSession cqlSession(RunContext runContext) throws IllegalVariableEvaluationException {
        return this.session.connect(runContext);
    }
}
