package io.kestra.plugin.cassandra;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

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
import static org.hamcrest.Matchers.nullValue;

@MicronautTest
class QueryTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Query query = Query.builder()
            .session(CassandraDbSession.builder()
                .endpoints(List.of(CassandraDbSession.Endpoint.builder().hostname("localhost").build()))
                .localDatacenter("datacenter1")
                .build())
            .cql("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 3 };")
            .build();

        Query.Output queryOutput = query.run(runContext);
        assertThat(queryOutput.getSize(), is(nullValue()));

        String table = "t" + IdUtils.create();

        query = Query.builder()
            .session(CassandraDbSession.builder()
                .endpoints(List.of(CassandraDbSession.Endpoint.builder().hostname("localhost").build()))
                .localDatacenter("datacenter1")
                .build())
            .cql("CREATE TABLE IF NOT EXISTS test." + table + " (" +
                "  id text, " +
                "  name text, " +
                "  c_ascii ascii, " +
                "  c_bigint bigint, " +
                "  c_blob blob, " +
                "  c_boolean boolean, " +
                // "  c_counter counter, " +
                "  c_date date, " +
                "  c_decimal decimal, " +
                "  c_double double, " +
                "  c_duration duration, " +
                "  c_float float, " +
                "  c_inet inet, " +
                "  c_int int, " +
                "  c_smallint smallint, " +
                "  c_text text, " +
                "  c_time time, " +
                "  c_timestamp timestamp, " +
                "  c_timeuuid timeuuid, " +
                "  c_tinyint tinyint, " +
                "  c_uuid uuid, " +
                "  c_varchar varchar, " +
                "  c_varint varint, " +
                "  c_map map<text, int>, " +
                "  c_set set<int>, " +
                "  c_list list<text>, " +
                "  c_tuple tuple<int, text>, " +
                "  PRIMARY KEY (id)" +
                ");"
            )
            .build();

        queryOutput = query.run(runContext);
        assertThat(queryOutput.getSize(), is(nullValue()));


        query = Query.builder()
            .session(CassandraDbSession.builder()
                .endpoints(List.of(CassandraDbSession.Endpoint.builder().hostname("localhost").build()))
                .localDatacenter("datacenter1")
                .build())
            .cql("INSERT INTO test." + table + "" +
                "(" +
                    "  id, " +
                    "  name, " +
                    "  c_ascii, " +
                    "  c_bigint, " +
                    "  c_blob, " +
                    "  c_boolean, " +
                    // "  c_counter, " +
                    "  c_date, " +
                    "  c_decimal, " +
                    "  c_double, " +
                    "  c_duration, " +
                    "  c_float, " +
                    "  c_inet, " +
                    "  c_int, " +
                    "  c_smallint, " +
                    "  c_text, " +
                    "  c_time, " +
                    "  c_timestamp, " +
                    "  c_timeuuid, " +
                    "  c_tinyint, " +
                    "  c_uuid, " +
                    "  c_varchar, " +
                    "  c_varint, " +
                    "  c_map, " +
                    "  c_set, " +
                    "  c_list, " +
                    "  c_tuple " +
                ") " +
                "VALUES " +
                "(" +
                "  '1', " +
                "  'Dusan'," +
                "  'ascii'," +
                "  9223372036854775807," +
                "  bigintAsBlob(3)," +
                "  true," +
                // "  'c_counter'," +
                "  '2011-02-03'," +
                "  12345.12345," +
                "  12345.12345," +
                "  PT89H8M53S," +
                "  12345.12345," +
                "  '1.2.3.4'," +
                "  2147483647," +
                "  32767," +
                "  'text'," +
                "  '08:12:54'," +
                "  '2011-02-03 04:05:00.000+0200'," +
                "  maxTimeuuid('2013-01-01 00:05+0000')," +
                "  3," +
                "  123e4567-e89b-12d3-a456-426614174000," +
                "  'c_varchar'," +
                "  2147483647," +
                "  {'key' : 1, 'value': 2}," +
                "  {1, 2, 3}," +
                "  ['a', 'b', 'c']," +
                "  (3, 'hours')" +
                ");")
            .build();
        queryOutput = query.run(runContext);
        assertThat(queryOutput.getSize(), is(nullValue()));


        query = Query.builder()
            .session(CassandraDbSession.builder()
                .endpoints(List.of(CassandraDbSession.Endpoint.builder().hostname("localhost").build()))
                .localDatacenter("datacenter1")
                .build())
            .cql("SELECT * FROM test." + table + ";")
            .fetchOne(true)
            .build();
        queryOutput = query.run(runContext);

        assertThat(queryOutput.getRow().get("id"), is("1"));
        assertThat(queryOutput.getRow().get("name"), is("Dusan"));
        assertThat(queryOutput.getRow().get("c_ascii"), is("ascii"));
        assertThat(queryOutput.getRow().get("c_bigint"), is(9223372036854775807L));
        assertThat(queryOutput.getRow().get("c_blob"), is(Longs.toByteArray(3L)));
        assertThat(queryOutput.getRow().get("c_boolean"), is(true));
        //assertThat(queryOutput.getRow().get("c_counter"), is(""));
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
