package io.kestra.plugin.cassandra.astradb;

import java.util.function.Supplier;

import com.datastax.oss.driver.api.core.CqlSession;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;

public final class TriggerTestSupport {
    private TriggerTestSupport() {
    }

    public static Trigger trigger(Supplier<CqlSession> sessions) {
        return Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .session(new AstraDbSession() {
                @Override
                CqlSession connect(RunContext runContext) {
                    return sessions.get();
                }
            })
            .cql(Property.ofValue("SELECT * FROM test.test_table"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
    }
}
