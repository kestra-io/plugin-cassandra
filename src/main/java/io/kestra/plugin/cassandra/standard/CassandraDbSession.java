package io.kestra.plugin.cassandra.standard;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.List;
import java.util.stream.Collectors;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.SniEndPoint;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@NoArgsConstructor
@Getter
public class CassandraDbSession {
    @Schema(
        title = "Contact endpoints for cluster bootstrap",
        description = "List of hosts (and optional SNI names) used to open the initial connection. Port defaults to 9042 per endpoint unless overridden."
    )
    @PluginProperty(dynamic = true, group = "advanced")
    private List<Endpoint> endpoints;

    @Schema(
        title = "Local datacenter name",
        description = "Datacenter considered local by the load-balancing policy; recommended for multi-DC clusters."
    )
    @PluginProperty(group = "advanced")
    private Property<String> localDatacenter;

    @Schema(
        title = "Plaintext authentication username"
    )
    @PluginProperty(secret = true, group = "connection")
    private Property<String> username;

    @Schema(
        title = "Plaintext authentication password"
    )
    @PluginProperty(secret = true, group = "connection")
    private Property<String> password;

    @Schema(
        title = "Application name sent in startup",
        description = "It will be sent in the STARTUP protocol message, under the key `APPLICATION_NAME`, for each " +
            "new connection established by the driver. Currently, this information is used by Insights monitoring " +
            "(if the target cluster does not support Insights, the entry will be ignored by the server)."
    )
    @PluginProperty(group = "advanced")
    private Property<String> applicationName;

    @Schema(title = "Secure connection details")
    @PluginProperty(dynamic = true, group = "advanced")
    private SecureConnection secureConnection;

    CqlSession connect(RunContext runContext) throws IllegalVariableEvaluationException {
        CqlSessionBuilder cqlSessionBuilder = CqlSession.builder()
            .addContactEndPoints(
                this.endpoints
                    .stream()
                    .map(throwFunction(e ->
                    {
                        InetSocketAddress inetSocketAddress = new InetSocketAddress(
                            runContext.render(e.getHostname()),
                            runContext.render(e.getPort()).as(Integer.class).orElseThrow()
                        );

                        if (e.getServerName() != null) {
                            return new SniEndPoint(
                                inetSocketAddress,
                                runContext.render(e.getServerName()).as(String.class).orElseThrow()
                            );
                        } else {
                            return new DefaultEndPoint(inetSocketAddress);
                        }

                    }))
                    .collect(Collectors.toList())
            );

        if (this.localDatacenter != null) {
            cqlSessionBuilder.withLocalDatacenter(runContext.render(this.localDatacenter).as(String.class).orElseThrow());
        }

        if (this.username != null && this.password != null) {
            cqlSessionBuilder.withAuthCredentials(
                runContext.render(this.username).as(String.class).orElseThrow(),
                runContext.render(this.password).as(String.class).orElseThrow()
            );
        }

        if (this.applicationName != null) {
            cqlSessionBuilder.withApplicationName(runContext.render(this.applicationName).as(String.class).orElseThrow());
        }

        if (this.secureConnection != null) {
            this.secureConnection.configure(cqlSessionBuilder, runContext);
        }

        return cqlSessionBuilder.build();
    }

    @Getter
    @Builder
    public static class Endpoint {
        @Schema(
            title = "Cassandra hostname"
        )
        @PluginProperty(dynamic = true)
        @NotNull
        @NotEmpty
        String hostname;

        @Schema(
            title = "Cassandra port",
            description = "Defaults to 9042."
        )
        @NotNull
        @Builder.Default
        @PluginProperty(group = "main")
        private Property<Integer> port = Property.ofValue(9042);

        @Schema(
            title = "SNI server name",
            description = "In the context of Cloud, this is the string representation of the host ID."
        )
        Property<String> serverName;
    }

    @Getter
    @Builder
    public static class SecureConnection {
        @Schema(
            title = "Path to truststore file (JKS or PKCS12)"
        )
        @PluginProperty(group = "advanced")
        private Property<String> truststorePath;

        @Schema(
            title = "Password for the truststore file"
        )
        @PluginProperty(secret = true, group = "connection")
        private Property<String> truststorePassword;

        @Schema(
            title = "Path to keystore file (JKS or PKCS12)",
            description = "Only needed when the server requires client certificates (mutual TLS)."
        )
        @PluginProperty(group = "advanced")
        private Property<String> keystorePath;

        @Schema(
            title = "Password for the keystore file"
        )
        @PluginProperty(secret = true, group = "connection")
        private Property<String> keystorePassword;

        void configure(CqlSessionBuilder builder, RunContext runContext) throws IllegalVariableEvaluationException {
            try {
                var sslContext = SSLContext.getInstance("TLS");
                sslContext.init(keyManagers(runContext), trustManagers(runContext), null);

                builder.withSslContext(sslContext);
            } catch (Exception e) {
                throw new IllegalVariableEvaluationException("Failed to configure SSL", e);
            }
        }

        private KeyManager[] keyManagers(RunContext runContext) throws GeneralSecurityException, IOException, IllegalVariableEvaluationException {
            if (this.keystorePath == null) {
                return null;
            }

            var password = renderPassword(runContext, this.keystorePassword);
            var kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(loadStore(runContext, this.keystorePath, password), password);
            return kmf.getKeyManagers();
        }

        private TrustManager[] trustManagers(RunContext runContext) throws GeneralSecurityException, IOException, IllegalVariableEvaluationException {
            var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(loadStore(runContext, this.truststorePath, renderPassword(runContext, this.truststorePassword)));
            return tmf.getTrustManagers();
        }

        private static char[] renderPassword(RunContext runContext, Property<String> password) throws IllegalVariableEvaluationException {
            return runContext.render(password).as(String.class).map(String::toCharArray).orElse(null);
        }

        private static KeyStore loadStore(RunContext runContext, Property<String> path, char[] password) throws GeneralSecurityException, IOException, IllegalVariableEvaluationException {
            var store = KeyStore.getInstance(KeyStore.getDefaultType());
            try (var in = new FileInputStream(runContext.render(path).as(String.class).orElseThrow())) {
                store.load(in, password);
            }
            return store;
        }
    }
}
