package io.kestra.plugin.cassandra;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.tasks.scripts.BashService;
import io.micronaut.http.uri.UriBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import java.lang.invoke.MethodHandles;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a Cassandra database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a cql query to a Cassandra Database",
            code = {
                "host: localhost",
                "username: mysql_user",
                "password: mysql_passwd",
                "cqlQuery: SELECT * FROM CQL_KEYSPACE.CQL_TABLE",
            }
        ),
    }
)
public class Query {
    @Schema(
        title = "CassandraDbSession",
        description = "The session properties"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected CassandraDbSession session;

    @Schema(
        title = "cqlQuery",
        description = "Cql query to execute"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String cqlQuery;


    public Query.Output run(RunContext runContext) throws Exception {
        try (CqlSession session = this.session.connect(runContext)) {
            ResultSet rs = session.execute(cqlQuery);
            session.close();
            return Output.builder().success(true).build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "success",
            description = "Success of the operation"
        )
        protected Boolean success;
    }
}