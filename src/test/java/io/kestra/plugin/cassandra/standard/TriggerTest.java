package io.kestra.plugin.cassandra.standard;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;

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


    @Test
    @EvaluateTrigger(flow = "flows/cassandra-listen.yml", triggerId = "watch")
    public void testCassandraTrigger(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));
        Execution execution = optionalExecution.get();

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(1));
    }
}
