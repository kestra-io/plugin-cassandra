package io.kestra.plugin.cassandra;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.annotation.JsonIgnore;

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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractCQLTrigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<AbstractQuery.Output>, QueryInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(title = "Time zone id used to parse date/time values returned by the query")
    private String timeZoneId;

    @Schema(
        title = "CQL query"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> cql;

    @Deprecated(since = "0.22.0", forRemoval = true)
    @Builder.Default
    private Property<Boolean> store = Property.ofValue(false);

    @Deprecated(since = "0.22.0", forRemoval = true)
    @Builder.Default
    private Property<Boolean> fetchOne = Property.ofValue(false);

    @Deprecated(since = "0.22.0", forRemoval = true)
    @Builder.Default
    private Property<Boolean> fetch = Property.ofValue(false);

    @Builder.Default
    protected Property<FetchType> fetchType = Property.ofValue(FetchType.NONE);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient Map<String, Object> additionalVars = new HashMap<>();

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final transient Set<PollingQuery> runningQueries = ConcurrentHashMap.newKeySet();

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

    protected AbstractQuery.Output runQuery(RunContext runContext, AbstractQuery query) throws Exception {
        var pollingQuery = new PollingQuery();
        runningQueries.add(pollingQuery);
        var result = new FutureTask<AbstractQuery.Output>(() ->
        {
            try {
                pollingQuery.checkKilled();
                var output = query.run(runContext, session -> pollingQuery.register(session, runContext.logger()));
                pollingQuery.checkKilled();
                return output;
            } finally {
                runningQueries.remove(pollingQuery);
            }
        });
        Thread queryThread;
        try {
            // Driver waits ignore interrupts; keep the worker wait interruptible for the polling deadline.
            queryThread = Thread.ofVirtual().name("cassandra-poll-" + getId()).start(result);
        } catch (RuntimeException | Error e) {
            runningQueries.remove(pollingQuery);
            throw e;
        }
        try {
            return result.get();
        } catch (InterruptedException e) {
            pollingQuery.kill();
            queryThread.interrupt();
            throw e;
        } catch (ExecutionException e) {
            if (pollingQuery.killed.get()) {
                var cancellation = new CancellationException("Cassandra polling trigger was killed");
                cancellation.initCause(e.getCause());
                throw cancellation;
            }
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            if (e.getCause() instanceof Error cause) {
                throw cause;
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public void kill() {
        runningQueries.forEach(PollingQuery::kill);
    }

    @Override
    public void stop() {
        kill();
    }

    private static final class PollingQuery {
        private final AtomicBoolean killed = new AtomicBoolean();
        private final AtomicReference<Runnable> cancellation = new AtomicReference<>();

        private void register(CqlSession session, Logger logger) {
            cancellation.set(() ->
            {
                try {
                    session.forceCloseAsync().whenComplete((unused, error) ->
                    {
                        if (error != null) {
                            logger.warn("Failed to force-close Cassandra polling session", error);
                        }
                    });
                } catch (RuntimeException e) {
                    logger.warn("Failed to force-close Cassandra polling session", e);
                }
            });
            if (killed.get()) {
                cancel();
            }
            checkKilled();
        }

        private void kill() {
            killed.set(true);
            cancel();
        }

        private void cancel() {
            var action = cancellation.getAndSet(null);
            if (action != null) {
                action.run();
            }
        }

        private void checkKilled() {
            if (killed.get()) {
                throw new CancellationException("Cassandra polling trigger was killed");
            }
        }
    }
}
