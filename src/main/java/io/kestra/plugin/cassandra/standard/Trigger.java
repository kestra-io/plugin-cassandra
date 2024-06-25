package io.kestra.plugin.cassandra.standard;

import com.datastax.oss.driver.api.core.CqlSession;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.cassandra.AbstractCQLTrigger;
import io.kestra.plugin.cassandra.AbstractQuery;
import io.kestra.plugin.cassandra.QueryInterface;
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
    title = "Wait for a query to return results on a Cassandra database."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a CQL query to return results, and then iterate through rows.",
            full = true,
            code = {
                "id: cassandra-trigger",
                "namespace: io.kestra.tests",
                "tasks:",
                "  - id: each",
                "    type: io.kestra.core.tasks.flows.EachSequential",
                "    tasks:",
                "      - id: return",
                "        type: io.kestra.core.tasks.debugs.Return",
                "        format: \"{{ json(taskrun.value) }}\"",
                "    value: \"{{ trigger.rows }}\"",
                "",
                "triggers:",
                "  - id: watch",
                "    type: io.kestra.plugin.cassandra.Trigger",
                "    interval: \"PT5M\"",
                "    session:",
                "       endpoints:",
                "          - hostname: localhost",
                "       username: cassandra_user",
                "       password: cassandra_passwd",
                "    cql: \"SELECT * FROM CQL_KEYSPACE.CQL_TABLE\"",
                "    fetch: true",
            }
        )
    },
    aliases = "io.kestra.plugin.cassandra.Trigger"
)
public class Trigger extends AbstractCQLTrigger implements QueryInterface {

    @Override
    protected AbstractQuery.Output runQuery(RunContext runContext) throws Exception {
        var query = Query.builder()
                .id(this.id)
                .type(Query.class.getName())
                .session(this.getSession())
                .cql(this.getCql())
                .fetch(this.isFetch())
                .store(this.isStore())
                .fetchOne(this.isFetchOne())
                .build();
        return query.run(runContext);
    }

    @Schema(
        title = "The session connection properties"
    )
    @PluginProperty
    @NotNull
    protected CassandraDbSession session;

    @Override
    public CqlSession cqlSession(RunContext runContext) throws IllegalVariableEvaluationException {
        return this.session.connect(runContext);
    }
}


