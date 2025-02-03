package io.kestra.plugin.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface QueryInterface {
    @Schema(
        title = "CQL query to execute."
    )
    @NotNull
    Property<String> getCql();

    @Deprecated(since = "0.22.0", forRemoval = true)
    @Schema(
        title = "DEPRECATED, please use `fetchType: FETCH` instead." +
            "Whether to fetch the data from the query result to the task output."
    )
    Property<Boolean> getFetch();

    @Deprecated(since = "0.22.0", forRemoval = true)
    @Schema(
        title = "DEPRECATED, please use `fetchType: STORE` instead." +
            "Whether to store the data from the query result into an ion serialized data file."
    )
    Property<Boolean> getStore();

    @Deprecated(since = "0.22.0", forRemoval = true)
    @Schema(
        title = "DEPRECATED, please use `fetchType: FETCH_ONE` instead." +
            "Whether to fetch only one data row from the query result to the task output."
    )
    Property<Boolean> getFetchOne();

    @Schema(
        title = "The way you want to store data.",
        description = "FETCH_ONE - output the first row.\n"
            + "FETCH - output all rows as output variable.\n"
            + "STORE - store all rows to a file.\n"
            + "NONE - do nothing."
    )
    Property<FetchType> getFetchType();

    CqlSession cqlSession(RunContext runContext) throws IllegalVariableEvaluationException;
}
