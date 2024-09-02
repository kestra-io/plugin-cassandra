package io.kestra.plugin.cassandra.standard;

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

import java.io.FileInputStream;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

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

    @Schema(title = "Secure connection details.")
    @PluginProperty(dynamic = true)
    private SecureConnection secureConnection;

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
        
        if (this.secureConnection != null) {
            this.secureConnection.configure(cqlSessionBuilder, runContext);
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
        @Builder.Default
        private Integer port = 9042;

        @Schema(
            title = "The SNI server name.",
            description = "In the context of Cloud, this is the string representation of the host ID."
        )
        @PluginProperty(dynamic = true)
        String serverName;
    }
    
    @Getter
    @Builder
    public static class SecureConnection {
        @Schema(
            title = "Path to the truststore file. (.crt)"
        )
        @PluginProperty(dynamic = true)
        private String truststorePath;

        @Schema(
            title = "Password for the truststore file."
        )
        @PluginProperty(dynamic = true)
        private String truststorePassword;

        @Schema(
            title = "Path to the keystore file. (*.jks)"
        )
        @PluginProperty(dynamic = true)
        private String keystorePath;

        @Schema(
            title = "Password for the keystore file."
        )
        @PluginProperty(dynamic = true)
        private String keystorePassword;
        
        void configure(CqlSessionBuilder builder, RunContext runContext) throws IllegalVariableEvaluationException {
            try {
                KeyStore truststore = KeyStore.getInstance(KeyStore.getDefaultType());
                try (FileInputStream truststoreFis = new FileInputStream(runContext.render(this.truststorePath))) {
                    if(this.truststorePassword != null) {
                        truststore.load(truststoreFis, runContext.render(this.truststorePassword).toCharArray());
                    }else{
                        truststore.load(truststoreFis, null);
                    }
                }

                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(truststore);

                KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
                try (FileInputStream keystoreFis = new FileInputStream(runContext.render(this.keystorePath))) {
                    if(this.keystorePassword != null){
                        keystore.load(keystoreFis, runContext.render(this.keystorePassword).toCharArray());
                    }else{
                        keystore.load(keystoreFis, null);
                    }
                }

                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keystore, runContext.render(this.keystorePassword).toCharArray());

                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

                builder.withSslContext(sslContext);
            } catch (Exception e) {
                throw new IllegalVariableEvaluationException("Failed to configure SSL", e);
            }
        }
    }
}
