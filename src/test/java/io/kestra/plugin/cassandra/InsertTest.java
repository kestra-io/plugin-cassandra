package io.kestra.plugin.cassandra;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class InsertTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query query = Query.builder()
            .session(CassandraDbSession.builder()
                .host("localhost")
                .build())
            .cql("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 3 };")
            .build();

        Query.Output queryOutput = query.run(runContext);
        assertThat(queryOutput.success, is(true));

        query = Query.builder()
            .session(CassandraDbSession.builder()
                .host("localhost")
                .build())
            .cql("CREATE TABLE IF NOT EXISTS test.test (id text, name text, PRIMARY KEY (id, name));")
            .build();

        queryOutput = query.run(runContext);
        assertThat(queryOutput.success, is(true));


        query = Query.builder()
            .session(CassandraDbSession.builder()
                .host("localhost")
                .build())
            .cql("INSERT INTO test.test(id, name) VALUES ('1', 'Dusan');")
            .build();

        queryOutput = query.run(runContext);
        assertThat(queryOutput.success, is(true));
    }
}
