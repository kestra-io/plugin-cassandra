package io.kestra.plugin.cassandra.standard;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import reactor.core.publisher.Flux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true, startScheduler = true)
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
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    protected QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    protected Execution triggerFlow(ClassLoader classLoader, String flowRepository, String flow) throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);
        Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
            queueCount.countDown();
            assertThat(execution.getLeft().getFlowId(), is(flow));
        });

        repositoryLoader.load(Objects.requireNonNull(classLoader.getResource(flowRepository)));

        boolean await = queueCount.await(1, TimeUnit.MINUTES);
        assertThat(await, is(true));

        return receive.blockLast();
    }

    @Test
    public void testCassandraTrigger() throws Exception {
        var execution = triggerFlow(this.getClass().getClassLoader(), "flows", "cassandra-listen");
        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(1));
    }
}
