package io.kestra.plugin.cassandra.astradb;

import com.datastax.oss.driver.api.core.CqlSession;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.util.Base64;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@NoArgsConstructor
@Getter
public class AstraDbSession {
    @Schema(
        title = "The Astra DB secure bundle, base64 encoded.",
        description = "It must be the ZIP archive containing the secure bundle encoded in base64. Use it only when you are not using the proxy address."
    )
    private Property<String> secureBundle;

    @Schema(
        title = "The Astra DB proxy address.",
        description = " Use it only when you are not using the secure bundle."
    )
    @PluginProperty
    private ProxyAddress proxyAddress;

    @NotNull
    private Property<String> keyspace;

    @NotNull
    private Property<String> clientId;

    @NotNull
    private Property<String> clientSecret;

    CqlSession connect(RunContext runContext) throws IllegalVariableEvaluationException {
        if ((secureBundle != null && proxyAddress != null) || (secureBundle == null && proxyAddress == null)) {
            throw new IllegalArgumentException("Please use only one of secureBundle or proxyAddress");
        }

        var builder = CqlSession.builder()
            .withAuthCredentials(runContext.render(this.clientId).as(String.class).orElseThrow(),
                runContext.render(this.clientSecret).as(String.class).orElseThrow())
            .withKeyspace(runContext.render(this.keyspace).as(String.class).orElseThrow());

        if (secureBundle != null) {
            byte[] decoded = Base64.getDecoder().decode(runContext.render(this.secureBundle).as(String.class).orElseThrow());
            builder.withCloudSecureConnectBundle(new ByteArrayInputStream(decoded));
        }

        if(proxyAddress != null) {
            builder.withCloudProxyAddress(
                new InetSocketAddress(
                    runContext.render(this.proxyAddress.hostname),
                    runContext.render(this.proxyAddress.port).as(Integer.class).orElseThrow())
            );
        }

        return builder.build();
    }

    @Getter
    @Builder
    public static class ProxyAddress {
        @Schema(
            title = "The hostname of the Astra DB server."
        )
        @PluginProperty(dynamic = true)
        @NotNull
        @NotEmpty
        private String hostname;

        @Schema(
            title = "The port of the Astra DB server."
        )
        @NotNull
        @Builder.Default
        private Property<Integer> port = Property.of(9042);
    }
}
