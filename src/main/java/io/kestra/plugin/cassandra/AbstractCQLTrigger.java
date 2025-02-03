package io.kestra.plugin.cassandra;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractCQLTrigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<AbstractQuery.Output>, QueryInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    private String timeZoneId;

    @Schema(
        title = "CQL query."
    )
    @NotNull
    private Property<String> cql;

    @Deprecated(since = "0.22.0", forRemoval = true)
    @Builder.Default
    private Property<Boolean> store = Property.of(false);

    @Deprecated(since = "0.22.0", forRemoval = true)
    @Builder.Default
    private Property<Boolean> fetchOne = Property.of(false);

    @Deprecated(since = "0.22.0", forRemoval = true)
    @Builder.Default
    private Property<Boolean> fetch = Property.of(false);

    @Builder.Default
    protected Property<FetchType> fetchType = Property.of(FetchType.NONE);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient Map<String, Object> additionalVars = new HashMap<>();

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        var run = runQuery(runContext);

        logger.debug("Found '{}' rows from '{}'", run.getSize(), runContext.render(this.cql));

        if (Optional.ofNullable(run.getSize()).orElse(0L) == 0) {
            return Optional.empty();
        }

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, run);

        return Optional.of(execution);
    }

    protected abstract AbstractQuery.Output runQuery(RunContext runContext) throws Exception;
}
