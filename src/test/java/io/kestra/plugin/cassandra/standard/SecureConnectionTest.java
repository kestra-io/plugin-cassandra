package io.kestra.plugin.cassandra.standard;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class SecureConnectionTest {
    private static final String PASSWORD = "changeit";

    @TempDir
    private static Path stores;

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeAll
    static void createStores() throws Exception {
        keytool("-genkeypair", "-alias", "server", "-keyalg", "RSA", "-dname", "CN=localhost", "-keystore", store("server.p12"));
        keytool("-genkeypair", "-alias", "other", "-keyalg", "RSA", "-dname", "CN=other", "-keystore", store("other.p12"));
        keytool("-genkeypair", "-alias", "client", "-keyalg", "RSA", "-dname", "CN=client", "-keystore", store("client.p12"));
        trust("server", "server.p12", "truststore.p12");
        trust("client", "client.p12", "server-truststore.p12");
    }

    @Test
    void truststoreOnly() throws Exception {
        var secureConnection = CassandraDbSession.SecureConnection.builder()
            .truststorePath(Property.ofValue(store("truststore.p12")))
            .truststorePassword(Property.ofValue(PASSWORD))
            .build();

        assertThat(handshake(configuredSslContext(secureConnection), serverSslContext("server.p12", null), false), is(true));
    }

    @Test
    void truststoreOnlyRejectsUnknownServer() throws Exception {
        var secureConnection = CassandraDbSession.SecureConnection.builder()
            .truststorePath(Property.ofValue(store("truststore.p12")))
            .truststorePassword(Property.ofValue(PASSWORD))
            .build();
        var client = configuredSslContext(secureConnection);
        var server = serverSslContext("other.p12", null);

        assertThrows(SSLHandshakeException.class, () -> handshake(client, server, false));
    }

    @Test
    void mutualTls() throws Exception {
        var secureConnection = CassandraDbSession.SecureConnection.builder()
            .truststorePath(Property.ofValue(store("truststore.p12")))
            .truststorePassword(Property.ofValue(PASSWORD))
            .keystorePath(Property.ofValue(store("client.p12")))
            .keystorePassword(Property.ofValue(PASSWORD))
            .build();

        assertThat(handshake(configuredSslContext(secureConnection), serverSslContext("server.p12", "server-truststore.p12"), true), is(true));
    }

    @Test
    void missingTruststoreNamesTheField() {
        var secureConnection = CassandraDbSession.SecureConnection.builder()
            .keystorePath(Property.ofValue(store("client.p12")))
            .keystorePassword(Property.ofValue(PASSWORD))
            .build();

        var exception = assertThrows(IllegalVariableEvaluationException.class, () -> configuredSslContext(secureConnection));

        assertThat(exception.getMessage(), containsString("secureConnection.truststorePath is missing or empty"));
    }

    @Test
    void blankTruststoreNamesTheField() {
        var secureConnection = CassandraDbSession.SecureConnection.builder()
            .truststorePath(Property.ofValue(" "))
            .build();

        var exception = assertThrows(IllegalVariableEvaluationException.class, () -> configuredSslContext(secureConnection));

        assertThat(exception.getMessage(), containsString("secureConnection.truststorePath is missing or empty"));
    }

    private SSLContext configuredSslContext(CassandraDbSession.SecureConnection secureConnection) throws Exception {
        var sslContext = new AtomicReference<SSLContext>();
        var builder = new CqlSessionBuilder() {
            @Override
            public CqlSessionBuilder withSslContext(SSLContext context) {
                sslContext.set(context);
                return super.withSslContext(context);
            }
        };

        secureConnection.configure(builder, runContextFactory.of(Map.of()));

        return sslContext.get();
    }

    private static SSLContext serverSslContext(String keystore, String clientTruststore) throws Exception {
        var kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(load(keystore), PASSWORD.toCharArray());

        var trustManagers = clientTruststore == null ? null : trustManagers(clientTruststore);
        var context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), trustManagers, null);
        return context;
    }

    private static TrustManager[] trustManagers(String truststore) throws Exception {
        var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(load(truststore));
        return tmf.getTrustManagers();
    }

    private static boolean handshake(SSLContext clientContext, SSLContext serverContext, boolean needClientAuth) throws Exception {
        var client = clientContext.createSSLEngine("localhost", 9042);
        client.setUseClientMode(true);
        var server = serverContext.createSSLEngine();
        server.setUseClientMode(false);
        server.setNeedClientAuth(needClientAuth);

        var size = Math.max(client.getSession().getPacketBufferSize(), client.getSession().getApplicationBufferSize()) * 2;
        var toServer = ByteBuffer.allocate(size);
        var toClient = ByteBuffer.allocate(size);
        var app = ByteBuffer.allocate(size);
        var empty = ByteBuffer.allocate(0);

        client.beginHandshake();
        server.beginHandshake();
        for (var i = 0; i < 100 && !(finished(client) && finished(server)); i++) {
            client.wrap(empty, toServer);
            server.wrap(empty, toClient);

            toServer.flip();
            server.unwrap(toServer, app);
            toServer.compact();

            toClient.flip();
            client.unwrap(toClient, app);
            toClient.compact();

            runTasks(client);
            runTasks(server);
            app.clear();
        }
        return finished(client) && finished(server);
    }

    private static boolean finished(SSLEngine engine) {
        var status = engine.getHandshakeStatus();
        return status == HandshakeStatus.NOT_HANDSHAKING || status == HandshakeStatus.FINISHED;
    }

    private static void runTasks(SSLEngine engine) {
        Runnable task;
        while ((task = engine.getDelegatedTask()) != null) {
            task.run();
        }
    }

    private static KeyStore load(String fileName) throws Exception {
        var store = KeyStore.getInstance("PKCS12");
        try (var in = new FileInputStream(store(fileName))) {
            store.load(in, PASSWORD.toCharArray());
        }
        return store;
    }

    private static void trust(String alias, String keystore, String truststore) throws Exception {
        var certificate = stores.resolve(alias + ".crt").toString();
        keytool("-exportcert", "-alias", alias, "-keystore", store(keystore), "-file", certificate);
        keytool("-importcert", "-noprompt", "-alias", alias, "-file", certificate, "-keystore", store(truststore));
    }

    private static String store(String fileName) {
        return stores.resolve(fileName).toString();
    }

    private static void keytool(String... args) throws Exception {
        var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        command.addAll(List.of(args));
        command.addAll(List.of("-storetype", "PKCS12", "-storepass", PASSWORD));
        var process = new ProcessBuilder(command).redirectErrorStream(true).start();
        var output = new String(process.getInputStream().readAllBytes());
        assertThat(output, process.waitFor(), is(0));
    }
}
