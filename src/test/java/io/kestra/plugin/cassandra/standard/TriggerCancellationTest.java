package io.kestra.plugin.cassandra.standard;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.cassandra.AbstractCQLTrigger;
import io.kestra.plugin.cassandra.astradb.TriggerTestSupport;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class TriggerCancellationTest {
    @Inject
    private RunContextFactory runContextFactory;

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void killUnblocksQueryAndCanBeRepeated(boolean astra) throws Exception {
        var client = new TestSession(true, false, false);
        var trigger = trigger(client, astra);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var poll = executor.submit(() -> evaluate(trigger));
            assertTrue(client.requestStarted.await(5, TimeUnit.SECONDS));

            trigger.kill();
            trigger.kill();

            assertTrue(client.forceClosed.await(5, TimeUnit.SECONDS));
            var error = assertThrows(ExecutionException.class, () -> poll.get(5, TimeUnit.SECONDS));
            assertInstanceOf(CancellationException.class, error.getCause());
            assertEquals(1, client.forcedCloses.get());
        } finally {
            client.release.countDown();
            stopExecutor(executor);
        }
        trigger.kill();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void stopUnblocksQueryAndCanBeRepeated(boolean astra) throws Exception {
        var client = new TestSession(true, false, false);
        var trigger = trigger(client, astra);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var poll = executor.submit(() -> evaluate(trigger));
            assertTrue(client.requestStarted.await(5, TimeUnit.SECONDS));

            trigger.stop();
            trigger.stop();

            var error = assertThrows(ExecutionException.class, () -> poll.get(5, TimeUnit.SECONDS));
            assertInstanceOf(CancellationException.class, error.getCause());
            assertEquals("Session closed", error.getCause().getCause().getMessage());
            assertEquals(1, client.forcedCloses.get());
        } finally {
            client.release.countDown();
            stopExecutor(executor);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void killUpgradesBlockedGracefulClose(boolean astra) throws Exception {
        var client = new TestSession(false, true, false);
        var trigger = trigger(client, astra);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var poll = executor.submit(() -> evaluate(trigger));
            assertTrue(client.closeStarted.await(5, TimeUnit.SECONDS));

            trigger.kill();

            assertTrue(client.forceClosed.await(5, TimeUnit.SECONDS));
            var error = assertThrows(ExecutionException.class, () -> poll.get(5, TimeUnit.SECONDS));
            assertInstanceOf(CancellationException.class, error.getCause());
        } finally {
            client.release.countDown();
            stopExecutor(executor);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void killWhileConnectingClosesLateSessionBeforeQuery(boolean astra) throws Exception {
        var client = new TestSession(false, false, true);
        var trigger = trigger(client, astra);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var poll = executor.submit(() -> evaluate(trigger));
            assertTrue(client.connectionStarted.await(5, TimeUnit.SECONDS));

            trigger.kill();
            client.connectionRelease.countDown();

            assertTrue(client.forceClosed.await(5, TimeUnit.SECONDS));
            var error = assertThrows(ExecutionException.class, () -> poll.get(5, TimeUnit.SECONDS));
            assertInstanceOf(CancellationException.class, error.getCause());
            assertEquals(0, client.requests.get());
        } finally {
            client.connectionRelease.countDown();
            client.release.countDown();
            stopExecutor(executor);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void interruptUnblocksQuery(boolean astra) throws Exception {
        var client = new TestSession(true, false, false);
        var trigger = trigger(client, astra);
        var waitingThread = new AtomicReference<Thread>();
        var executor = Executors.newSingleThreadExecutor();
        try {
            var poll = executor.submit(() ->
            {
                waitingThread.set(Thread.currentThread());
                return evaluate(trigger);
            });
            assertTrue(client.requestStarted.await(5, TimeUnit.SECONDS));

            waitingThread.get().interrupt();

            var error = assertThrows(ExecutionException.class, () -> poll.get(5, TimeUnit.SECONDS));
            assertInstanceOf(InterruptedException.class, error.getCause());
            assertTrue(client.forceClosed.await(5, TimeUnit.SECONDS));
            assertTrue(client.closeStarted.await(5, TimeUnit.SECONDS));
            assertEquals(1, client.forcedCloses.get());
        } finally {
            client.release.countDown();
            stopExecutor(executor);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void interruptUnblocksPageFetch(boolean astra) throws Exception {
        var client = new TestSession(false, false, false);
        client.blockRows = true;
        var trigger = trigger(client, astra);
        var waitingThread = new AtomicReference<Thread>();
        var executor = Executors.newSingleThreadExecutor();
        try {
            var poll = executor.submit(() ->
            {
                waitingThread.set(Thread.currentThread());
                return evaluate(trigger);
            });
            assertTrue(client.pageStarted.await(5, TimeUnit.SECONDS));

            waitingThread.get().interrupt();

            var error = assertThrows(ExecutionException.class, () -> poll.get(5, TimeUnit.SECONDS));
            assertInstanceOf(InterruptedException.class, error.getCause());
            assertTrue(client.forceClosed.await(5, TimeUnit.SECONDS));
            assertTrue(client.closeStarted.await(5, TimeUnit.SECONDS));
        } finally {
            client.release.countDown();
            stopExecutor(executor);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void interruptWhileConnectingClosesLateSessionBeforeQuery(boolean astra) throws Exception {
        var client = new TestSession(false, false, true);
        var trigger = trigger(client, astra);
        var waitingThread = new AtomicReference<Thread>();
        var executor = Executors.newSingleThreadExecutor();
        try {
            var poll = executor.submit(() ->
            {
                waitingThread.set(Thread.currentThread());
                return evaluate(trigger);
            });
            assertTrue(client.connectionStarted.await(5, TimeUnit.SECONDS));

            waitingThread.get().interrupt();

            var error = assertThrows(ExecutionException.class, () -> poll.get(5, TimeUnit.SECONDS));
            assertInstanceOf(InterruptedException.class, error.getCause());
            assertEquals(0, client.requests.get());
            assertEquals(0, client.forcedCloses.get());
            client.connectionRelease.countDown();
            assertTrue(client.forceClosed.await(5, TimeUnit.SECONDS));
            assertTrue(client.closeStarted.await(5, TimeUnit.SECONDS));
            assertEquals(0, client.requests.get());
            assertEquals(1, client.forcedCloses.get());
        } finally {
            client.connectionRelease.countDown();
            stopExecutor(executor);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void failedConnectionAfterKillDoesNotPoisonNextPoll(boolean astra) throws Exception {
        var connecting = new TestSession(false, false, true);
        connecting.connectionFailure = new IllegalStateException("Initialization failed");
        var next = new TestSession(false, false, false);
        var connections = new AtomicInteger();
        var trigger = trigger(() -> connections.getAndIncrement() == 0 ? connecting : next, astra);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var poll = executor.submit(() -> evaluate(trigger));
            assertTrue(connecting.connectionStarted.await(5, TimeUnit.SECONDS));

            assertTimeoutPreemptively(Duration.ofSeconds(1), trigger::kill);
            connecting.connectionRelease.countDown();

            var error = assertThrows(ExecutionException.class, () -> poll.get(5, TimeUnit.SECONDS));
            assertInstanceOf(CancellationException.class, error.getCause());
            assertSame(connecting.connectionFailure, error.getCause().getCause());
            trigger.kill();
            assertEquals(0, connecting.forcedCloses.get());
            assertEquals(0, connecting.requests.get());
            assertTrue(evaluate(trigger).isEmpty());
            assertEquals(1, next.closes.get());
            assertEquals(0, next.forcedCloses.get());
        } finally {
            connecting.connectionRelease.countDown();
            stopExecutor(executor);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void normalPollingKeepsResultsAndClosesEachSession(boolean astra) throws Exception {
        var client = new TestSession(false, false, false);
        var trigger = trigger(client, astra);

        trigger.kill();
        assertTrue(evaluate(trigger).isEmpty());
        assertTrue(evaluate(trigger).isEmpty());
        trigger.kill();

        assertEquals(2, client.requests.get());
        assertEquals(2, client.closes.get());
        assertEquals(0, client.forcedCloses.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void normalPollingCreatesExecutionWithFetchedRows(boolean astra) throws Exception {
        var client = new TestSession(false, false, false);
        client.rowCount = 1;
        var trigger = trigger(client, astra);

        var execution = evaluate(trigger).orElseThrow();

        assertEquals(List.of(Map.of("id", "sample")), execution.getTrigger().getVariables().get("rows"));
        assertEquals(1L, execution.getTrigger().getVariables().get("size"));
        assertEquals(1, client.closes.get());
        assertEquals(0, client.forcedCloses.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void killUnblocksPageFetch(boolean astra) throws Exception {
        var client = new TestSession(false, false, false);
        client.blockRows = true;
        var trigger = trigger(client, astra);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var poll = executor.submit(() -> evaluate(trigger));
            assertTrue(client.pageStarted.await(5, TimeUnit.SECONDS));

            trigger.kill();

            assertThrows(ExecutionException.class, () -> poll.get(5, TimeUnit.SECONDS));
            assertEquals(1, client.forcedCloses.get());
        } finally {
            client.release.countDown();
            stopExecutor(executor);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void killedQueryCannotReturnSuccessfulResult(boolean astra) throws Exception {
        var client = new TestSession(true, false, false);
        client.returnOnKill = true;
        client.rowCount = 1;
        var trigger = trigger(client, astra);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var poll = executor.submit(() -> evaluate(trigger));
            assertTrue(client.requestStarted.await(5, TimeUnit.SECONDS));

            trigger.kill();

            var error = assertThrows(ExecutionException.class, () -> poll.get(5, TimeUnit.SECONDS));
            assertInstanceOf(CancellationException.class, error.getCause());
        } finally {
            client.release.countDown();
            stopExecutor(executor);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void killDoesNotWaitForShutdownFuture(boolean astra) throws Exception {
        var client = new TestSession(true, false, false);
        client.closeFuture = new CompletableFuture<>();
        var trigger = trigger(client, astra);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var poll = executor.submit(() -> evaluate(trigger));
            assertTrue(client.requestStarted.await(5, TimeUnit.SECONDS));

            assertTimeoutPreemptively(Duration.ofSeconds(1), trigger::kill);

            assertFalse(client.closeFuture.isDone());
            assertThrows(ExecutionException.class, () -> poll.get(5, TimeUnit.SECONDS));
        } finally {
            client.closeFuture.complete(null);
            client.release.countDown();
            stopExecutor(executor);
        }
    }

    @Test
    void killDoesNotPropagateDriverCloseFailure() throws Exception {
        var client = new TestSession(true, false, false);
        client.closeFailure = new IllegalStateException("Close failed");
        var trigger = trigger(client, false);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var poll = executor.submit(() -> evaluate(trigger));
            assertTrue(client.requestStarted.await(5, TimeUnit.SECONDS));

            assertDoesNotThrow(trigger::kill);
            assertDoesNotThrow(trigger::kill);
            assertEquals(1, client.forcedCloses.get());

            client.release.countDown();
            assertThrows(ExecutionException.class, () -> poll.get(5, TimeUnit.SECONDS));
        } finally {
            client.release.countDown();
            stopExecutor(executor);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void killClosesAllOverlappingPollSessions(boolean astra) throws Exception {
        var first = new TestSession(true, false, false);
        var second = new TestSession(true, false, false);
        var connections = new AtomicInteger();
        var trigger = trigger(() -> connections.getAndIncrement() == 0 ? first : second, astra);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var firstPoll = executor.submit(() -> evaluate(trigger));
            assertTrue(first.requestStarted.await(5, TimeUnit.SECONDS));
            var secondPoll = executor.submit(() -> evaluate(trigger));
            assertTrue(second.requestStarted.await(5, TimeUnit.SECONDS));

            trigger.kill();

            assertThrows(ExecutionException.class, () -> firstPoll.get(5, TimeUnit.SECONDS));
            assertThrows(ExecutionException.class, () -> secondPoll.get(5, TimeUnit.SECONDS));
            assertEquals(1, first.forcedCloses.get());
            assertEquals(1, second.forcedCloses.get());
        } finally {
            first.release.countDown();
            second.release.countDown();
            stopExecutor(executor);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void canceledPollDoesNotPoisonNextPoll(boolean astra) throws Exception {
        var first = new TestSession(true, false, false);
        var next = new TestSession(false, false, false);
        var connections = new AtomicInteger();
        var trigger = trigger(() -> connections.getAndIncrement() == 0 ? first : next, astra);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var poll = executor.submit(() -> evaluate(trigger));
            assertTrue(first.requestStarted.await(5, TimeUnit.SECONDS));

            trigger.kill();

            assertThrows(ExecutionException.class, () -> poll.get(5, TimeUnit.SECONDS));
            assertTrue(evaluate(trigger).isEmpty());
            trigger.kill();
            assertEquals(0, next.forcedCloses.get());
            assertEquals(1, next.closes.get());
        } finally {
            first.release.countDown();
            stopExecutor(executor);
        }
    }

    private static void stopExecutor(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    private Optional<Execution> evaluate(AbstractCQLTrigger trigger) throws Exception {
        var flow = Flow.builder()
            .id("cassandra_test")
            .namespace("io.kestra.tests")
            .revision(1)
            .triggers(List.of(trigger))
            .build();
        var runContext = runContextFactory.of(flow, trigger);
        var triggerContext = TriggerContext.builder()
            .namespace(flow.getNamespace())
            .flowId(flow.getId())
            .triggerId(trigger.getId())
            .date(ZonedDateTime.now())
            .build();
        runContextFactory.initializer().forScheduler((DefaultRunContext) runContext, triggerContext, trigger);
        return trigger.evaluate(ConditionContext.builder().flow(flow).runContext(runContext).build(), triggerContext);
    }

    private AbstractCQLTrigger trigger(TestSession client, boolean astra) {
        return trigger(() -> client, astra);
    }

    private AbstractCQLTrigger trigger(Supplier<TestSession> clients, boolean astra) {
        if (astra) {
            return TriggerTestSupport.trigger(() -> clients.get().connect());
        }
        return Trigger.builder()
            .id("watch")
            .type(Trigger.class.getName())
            .session(new CassandraDbSession() {
                @Override
                CqlSession connect(RunContext runContext) {
                    return clients.get().connect();
                }
            })
            .cql(Property.ofValue("SELECT * FROM test.test_table"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static class TestSession {
        private final CountDownLatch requestStarted = new CountDownLatch(1);
        private final CountDownLatch pageStarted = new CountDownLatch(1);
        private final CountDownLatch closeStarted = new CountDownLatch(1);
        private final CountDownLatch forceClosed = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final CountDownLatch connectionStarted = new CountDownLatch(1);
        private final CountDownLatch connectionRelease = new CountDownLatch(1);
        private final AtomicInteger requests = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private final AtomicInteger forcedCloses = new AtomicInteger();
        private final boolean blockConnect;
        private final CqlSession session;
        private boolean blockRows;
        private boolean returnOnKill;
        private int rowCount;
        private RuntimeException connectionFailure;
        private RuntimeException closeFailure;
        private CompletableFuture<Void> closeFuture = CompletableFuture.completedFuture(null);

        private CqlSession connect() {
            connectionStarted.countDown();
            if (blockConnect) {
                awaitUninterruptibly(connectionRelease);
            }
            if (connectionFailure != null) {
                throw connectionFailure;
            }
            return session;
        }

        @SuppressWarnings("unchecked")
        private TestSession(boolean blockQuery, boolean blockClose, boolean blockConnect) {
            this.blockConnect = blockConnect;
            ColumnDefinition column = proxy(ColumnDefinition.class, (method, args) -> switch (method) {
                case "getName" -> CqlIdentifier.fromInternal("id");
                case "getType" -> DataTypes.TEXT;
                default -> throw new UnsupportedOperationException(method);
            });
            ColumnDefinitions columns = proxy(ColumnDefinitions.class, (method, args) -> switch (method) {
                case "size" -> 1;
                case "get" -> column;
                default -> throw new UnsupportedOperationException(method);
            });
            Row row = proxy(Row.class, (method, args) -> "sample");
            ExecutionInfo executionInfo = proxy(ExecutionInfo.class, (method, args) -> 0);
            ResultSet result = proxy(ResultSet.class, (method, args) -> switch (method) {
                case "getColumnDefinitions" -> columns;
                case "getExecutionInfo" -> executionInfo;
                case "forEach" -> {
                    if (blockRows) {
                        pageStarted.countDown();
                        awaitUninterruptibly(release);
                        throw new CancellationException("Page fetch aborted by forced close");
                    }
                    for (int i = 0; i < rowCount; i++) {
                        ((Consumer<Row>) args[0]).accept(row);
                    }
                    yield null;
                }
                default -> throw new UnsupportedOperationException(method);
            });
            session = proxy(CqlSession.class, (method, args) -> switch (method) {
                case "execute" -> {
                    requests.incrementAndGet();
                    requestStarted.countDown();
                    if (blockQuery) {
                        awaitUninterruptibly(release);
                        if (!returnOnKill) {
                            throw new IllegalStateException("Session closed");
                        }
                    }
                    yield result;
                }
                case "forceCloseAsync" -> {
                    forcedCloses.incrementAndGet();
                    if (closeFailure != null) {
                        throw closeFailure;
                    }
                    forceClosed.countDown();
                    release.countDown();
                    yield closeFuture;
                }
                case "close" -> {
                    closes.incrementAndGet();
                    closeStarted.countDown();
                    if (blockClose) {
                        awaitUninterruptibly(release);
                    }
                    yield null;
                }
                default -> throw new UnsupportedOperationException(method);
            });
        }
    }

    private interface Invocation {
        Object invoke(String method, Object[] args);
    }

    private static <T> T proxy(Class<T> type, Invocation invocation) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] { type }, (proxy, method, args) -> invocation.invoke(method.getName(), args)));
    }
}
