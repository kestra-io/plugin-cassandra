package io.kestra.plugin.cassandra;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
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
            title = "Send a cql query to a Cassandra Database",
            code = {
                "host: localhost",
                "username: mysql_user",
                "password: mysql_passwd",
                "cql: SELECT * FROM CQL_KEYSPACE.CQL_TABLE",
            }
        ),
    }
)
public class Query {
    @Schema(
        title = "The session properties"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected CassandraDbSession session;

    @Schema(
        title = "Cql query to execute"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String cql;

    public Query.Output run(RunContext runContext) throws Exception {
        try (CqlSession session = this.session.connect(runContext)) {
            ResultSet rs = session.execute(runContext.render(cql));
            session.close();
            return Output.builder().success(true).build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "If the operation is success of the operation"
        )
        protected Boolean success;
    }
}
