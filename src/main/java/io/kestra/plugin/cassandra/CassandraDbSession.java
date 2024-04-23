package io.kestra.plugin.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.SniEndPoint;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@NoArgsConstructor
@Getter
public class CassandraDbSession {
    @Schema(
        title = "List of contact endpoints to use for the initial connection to the cluster."
    )
    @PluginProperty(dynamic = true)
    private List<Endpoint> endpoints;

    @Schema(
        title = "Specifies the datacenter that is considered \"local\" by the load balancing policy."
    )
    @PluginProperty(dynamic = true)
    private String localDatacenter;

    @Schema(
        title = "Plaintext authentication username."
    )
    @PluginProperty(dynamic = true)
    private String username;

    @Schema(
        title = "Plaintext authentication password."
    )
    @PluginProperty(dynamic = true)
    private String password;

    @Schema(
        title = "The name of the application using the created session.",
        description = "It will be sent in the STARTUP protocol message, under the key `APPLICATION_NAME`, for each " +
            "new connection established by the driver. Currently, this information is used by Insights monitoring " +
            "(if the target cluster does not support Insights, the entry will be ignored by the server)."
    )
    @PluginProperty(dynamic = true)
    private String applicationName;

    CqlSession connect(RunContext runContext) throws IllegalVariableEvaluationException {
        CqlSessionBuilder cqlSessionBuilder = CqlSession.builder()
            .addContactEndPoints(this.endpoints
                .stream()
                .map(throwFunction(e -> {
                    InetSocketAddress inetSocketAddress = new InetSocketAddress(
                        runContext.render(e.getHostname()),
                        e.getPort()
                    );

                    if (e.getServerName() != null) {
                        return new SniEndPoint(
                            inetSocketAddress,
                            runContext.render(e.getServerName())
                        );
                    } else {
                        return new DefaultEndPoint(inetSocketAddress);
                    }

                }))
                .collect(Collectors.toList())
            );

        if (this.localDatacenter != null) {
            cqlSessionBuilder.withLocalDatacenter(runContext.render(this.localDatacenter));
        }

        if (this.username != null && this.password != null) {
            cqlSessionBuilder.withAuthCredentials(
                runContext.render(this.username),
                runContext.render(this.password)
            );
        }

        if (this.applicationName != null) {
            cqlSessionBuilder.withApplicationName(runContext.render(this.applicationName));
        }

        return cqlSessionBuilder.build();
    }

    @Getter
    @Builder
    public static class Endpoint {
        @Schema(
            title = "The hostname of the Cassandra server."
        )
        @PluginProperty(dynamic = true)
        @NotNull
        @NotEmpty
        String hostname;

        @Schema(
            title = "The port of the Cassandra server."
        )
        @PluginProperty
        @NotNull
        @NotEmpty
        @Builder.Default
        private Integer port = 9042;

        @Schema(
            title = "The SNI server name.",
            description = "In the context of Cloud, this is the string representation of the host ID."
        )
        @PluginProperty(dynamic = true)
        String serverName;
    }
}
