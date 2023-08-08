package io.kestra.plugin.cassandra;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;

public interface QueryInterface {
    @Schema(
        title = "CQL query to execute"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getCql();

    @Schema(
        title = "Whether to Fetch the data from the query result to the task output"
    )
    @PluginProperty
    boolean isFetch();

    @Schema(
        title = "Whether to store the data from the query result into an ion serialized data file"
    )
    @PluginProperty
    boolean isStore();

    @Schema(
        title = "Whether to Fetch only one data row from the query result to the task output"
    )
    @PluginProperty
    boolean isFetchOne();
}
