package io.kestra.plugin.cassandra.standard;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QueryTest {
    @Inject
    private RunContextFactory runContextFactory;

    private RunContext runContext;

    @BeforeAll
    public void startUp() throws Exception {
        runContext = runContextFactory.of(ImmutableMap.of());
        CassandraTestHelper testHelper = new CassandraTestHelper();
        testHelper.initServer(runContext);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void run(boolean useFetchOne) throws Exception {
        Query query = Query.builder()
            .session(CassandraDbSession.builder()
                .endpoints(List.of(CassandraDbSession.Endpoint.builder().hostname("localhost").build()))
                .localDatacenter(Property.ofValue("datacenter1"))
                .build())
            .cql(Property.ofValue("SELECT * FROM test.test_table;"))
            .fetchType(useFetchOne ? null : Property.ofValue(FetchType.FETCH_ONE))
            .fetchOne(Property.ofValue(useFetchOne))
            .build();
        Query.Output queryOutput = query.run(runContext);

        assertThat(queryOutput.getRow().get("id"), is("1"));
        assertThat(queryOutput.getRow().get("name"), is("Dusan"));
        assertThat(queryOutput.getRow().get("c_ascii"), is("ascii"));
        assertThat(queryOutput.getRow().get("c_bigint"), is(9223372036854775807L));
        assertThat(queryOutput.getRow().get("c_blob"), is(Longs.toByteArray(3L)));
        assertThat(queryOutput.getRow().get("c_boolean"), is(true));
        assertThat(queryOutput.getRow().get("c_date"), is(LocalDate.parse("2011-02-03")));
        assertThat(queryOutput.getRow().get("c_decimal"), is(BigDecimal.valueOf(12345.12345D)));
        assertThat(queryOutput.getRow().get("c_double"), is(12345.12345D));
        assertThat(queryOutput.getRow().get("c_duration"), is(Duration.parse("PT89H8M53S")));
        assertThat(queryOutput.getRow().get("c_float"), is(12345.12345F));
        assertThat(queryOutput.getRow().get("c_inet"), is("/1.2.3.4"));
        assertThat(queryOutput.getRow().get("c_int"), is(2147483647));
        assertThat(queryOutput.getRow().get("c_smallint"), is(Short.valueOf("32767")));
        assertThat(queryOutput.getRow().get("c_text"), is("text"));
        assertThat(queryOutput.getRow().get("c_time"), is(LocalTime.parse("08:12:54")));
        assertThat(queryOutput.getRow().get("c_timestamp"), is(Instant.parse("2011-02-03T02:05:00.000Z")));
        assertThat(queryOutput.getRow().get("c_timeuuid"), is(UUID.fromString("e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f").toString()));
        assertThat(queryOutput.getRow().get("c_tinyint"), is(Byte.valueOf("3")));
        assertThat(queryOutput.getRow().get("c_uuid"), is("123e4567-e89b-12d3-a456-426614174000"));
        assertThat(queryOutput.getRow().get("c_varchar"), is("c_varchar"));
        assertThat(queryOutput.getRow().get("c_varint"), is(BigInteger.valueOf(2147483647)));
        assertThat(queryOutput.getRow().get("c_map"), is(Map.of("key", 1, "value", 2)));
        assertThat(queryOutput.getRow().get("c_set"), is(Set.of(1, 2, 3)));
        assertThat(queryOutput.getRow().get("c_list"), is(List.of("a", "b", "c")));
        assertThat(queryOutput.getRow().get("c_tuple"), is(List.of(3, "hours")));
    }
}
