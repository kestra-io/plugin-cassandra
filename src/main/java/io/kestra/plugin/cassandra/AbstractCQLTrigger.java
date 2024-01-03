package io.kestra.plugin.cassandra;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.ExecutionTrigger;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import jakarta.validation.constraints.NotNull;
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
    @PluginProperty
    @NotNull
    private String cql;

    @Builder.Default
    private boolean store = false;

    @Builder.Default
    private boolean fetchOne = false;

    @Builder.Default
    private boolean fetch = false;

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

        ExecutionTrigger executionTrigger = ExecutionTrigger.of(
                this,
                run
        );

        Execution execution = Execution.builder()
                .id(runContext.getTriggerExecutionId())
                .namespace(context.getNamespace())
                .flowId(context.getFlowId())
                .flowRevision(context.getFlowRevision())
                .state(new State())
                .trigger(executionTrigger)
                .build();

        return Optional.of(execution);
    }

    protected abstract AbstractQuery.Output runQuery(RunContext runContext) throws Exception;
}
