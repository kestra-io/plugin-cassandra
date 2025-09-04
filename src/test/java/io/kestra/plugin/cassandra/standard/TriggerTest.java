package io.kestra.plugin.cassandra.standard;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Worker;
import io.kestra.scheduler.AbstractScheduler;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.micronaut.context.ApplicationContext;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TriggerTest {

    @Inject
    private RunContextFactory runContextFactory;

    private RunContext runContext;

    @BeforeAll
    public void startUp() throws Exception {
        CassandraTestHelper testHelper = new CassandraTestHelper();
        this.runContext = runContextFactory.of(ImmutableMap.of());
        testHelper.initServer(this.runContext);
    }

    @Inject
    protected ApplicationContext applicationContext;

    @Inject
    protected FlowListeners flowListenersService;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    protected QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    protected Execution triggerFlow(ClassLoader classLoader, String flowRepository, String flow) throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        try (Worker worker = applicationContext.createBean(Worker.class, IdUtils.create(), 8, null)) {
            try (
                    AbstractScheduler scheduler = new JdbcScheduler(
                        this.applicationContext,
                        this.flowListenersService
                    );
            ) {
                // wait for execution
                Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
                    queueCount.countDown();
                    assertThat(execution.getLeft().getFlowId(), is(flow));
                });

                worker.run();
                scheduler.run();
                repositoryLoader.load(Objects.requireNonNull(classLoader.getResource(flowRepository)));

                boolean await = queueCount.await(1, TimeUnit.MINUTES);
                assertThat(await, is(true));

                return receive.blockLast();
            }
        }
    }

    @Test
    public void testCassandraTrigger() throws Exception {
        var execution = triggerFlow(this.getClass().getClassLoader(), "flows","cassandra-listen");
        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(1));
    }
}
