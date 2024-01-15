package io.kestra.plugin.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;

public interface QueryInterface {
    @Schema(
        title = "CQL query to execute."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getCql();

    @Schema(
        title = "Whether to fetch the data from the query result to the task output."
    )
    @PluginProperty
    boolean isFetch();

    @Schema(
        title = "Whether to store the data from the query result into an ion serialized data file."
    )
    @PluginProperty
    boolean isStore();

    @Schema(
        title = "Whether to fetch only one data row from the query result to the task output."
    )
    @PluginProperty
    boolean isFetchOne();

    CqlSession cqlSession(RunContext runContext) throws IllegalVariableEvaluationException;
}
