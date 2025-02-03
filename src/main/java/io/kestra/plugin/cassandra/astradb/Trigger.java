package io.kestra.plugin.cassandra.astradb;

import com.datastax.oss.driver.api.core.CqlSession;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.cassandra.*;
import io.kestra.plugin.cassandra.astradb.Query;
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
    title = "Wait for a query to return results on Astra DB."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a CQL query to return results, and then iterate through rows.",
            full = true,
            code = """
                id: astra_trigger
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.core.tasks.flows.ForEach
                    values: "{{ trigger.rows }}"
                    tasks:
                      - id: return
                        type: io.kestra.core.tasks.debugs.Return
                        format: "{{ json(taskrun.value) }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.cassandra.astradb.Trigger
                    interval: "PT5M"
                    session:
                        secureBundle: /path/to/secureBundle.zip
                        keyspace: astradb_keyspace
                        clientId: astradb_clientId
                        clientSecret: astradb_clientSecret
                    cql: "SELECT * FROM CQL_KEYSPACE.CQL_TABLE"
                    fetchType: FETCH
                """
        )
    },
    aliases = "io.kestra.plugin.astradb.Trigger"
)
public class Trigger extends AbstractCQLTrigger implements QueryInterface {

    @Override
    protected AbstractQuery.Output runQuery(RunContext runContext) throws Exception {
        var query = io.kestra.plugin.cassandra.astradb.Query.builder()
            .id(this.id)
            .type(Query.class.getName())
            .session(this.getSession())
            .cql(this.getCql())
            .fetch(this.getFetch())
            .store(this.getStore())
            .fetchOne(this.getFetchOne())
            .fetchType(this.getFetchType())
            .build();
        return query.run(runContext);
    }

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