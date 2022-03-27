package io.kestra.plugin.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.net.InetSocketAddress;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@SuperBuilder
@NoArgsConstructor
@Getter
@Introspected
public class CassandraDbSession {
    @Schema(
        title = "The hostname of cassandra server"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    private String host;

    @Schema(
        title = "The hostname of cassandra port"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    @NotEmpty
    @Builder.Default
    private Integer port = 9042;

    CqlSession connect(RunContext runContext) throws IllegalVariableEvaluationException {
        return CqlSession.builder()
            .withLocalDatacenter("datacenter1")
            .addContactPoint(new InetSocketAddress(runContext.render(host), port))
            .build();
    }
}
