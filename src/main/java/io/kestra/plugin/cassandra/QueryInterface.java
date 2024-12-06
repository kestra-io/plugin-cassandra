package io.kestra.plugin.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface QueryInterface {
    @Schema(
        title = "CQL query to execute."
    )
    @NotNull
    Property<String> getCql();

    @Schema(
        title = "Whether to fetch the data from the query result to the task output."
    )
    Property<Boolean> getFetch();

    @Schema(
        title = "Whether to store the data from the query result into an ion serialized data file."
    )
    Property<Boolean> getStore();

    @Schema(
        title = "Whether to fetch only one data row from the query result to the task output."
    )
    Property<Boolean> getFetchOne();

    CqlSession cqlSession(RunContext runContext) throws IllegalVariableEvaluationException;
}
