package io.kestra.plugin.cassandra.standard;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class CassandraTestHelper {
    public void initServer(RunContext runContext) throws Exception {

        Query query = Query.builder()
                .session(CassandraDbSession.builder()
                        .endpoints(List.of(CassandraDbSession.Endpoint.builder().hostname("localhost").build()))
                        .localDatacenter(Property.of("datacenter1"))
                        .build())
                .cql(Property.of("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 3 };"))
                .build();

        Query.Output queryOutput = query.run(runContext);
        assertThat(queryOutput.getSize(), is(nullValue()));

        query = Query.builder()
                .session(CassandraDbSession.builder()
                        .endpoints(List.of(CassandraDbSession.Endpoint.builder().hostname("localhost").build()))
                        .localDatacenter(Property.of("datacenter1"))
                        .build())
                .cql(Property.of("CREATE TABLE IF NOT EXISTS test.test_table (" +
                        "  id text, " +
                        "  name text, " +
                        "  c_ascii ascii, " +
                        "  c_bigint bigint, " +
                        "  c_blob blob, " +
                        "  c_boolean boolean, " +
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
                ))
                .build();

        queryOutput = query.run(runContext);

        query = Query.builder()
                .session(CassandraDbSession.builder()
                        .endpoints(List.of(CassandraDbSession.Endpoint.builder().hostname("localhost").build()))
                        .localDatacenter(Property.of("datacenter1"))
                        .build())
                .cql(Property.of("SELECT * from test.test_table"))
                .build();
        queryOutput = query.run(runContext);

        if(queryOutput.getSize() == null) {
            query = Query.builder()
                    .session(CassandraDbSession.builder()
                            .endpoints(List.of(CassandraDbSession.Endpoint.builder().hostname("localhost").build()))
                            .localDatacenter(Property.of("datacenter1"))
                            .build())
                    .cql(Property.of("INSERT INTO test.test_table" +
                            "(" +
                            "  id, " +
                            "  name, " +
                            "  c_ascii, " +
                            "  c_bigint, " +
                            "  c_blob, " +
                            "  c_boolean, " +
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
                            ");"))
                    .build();
            query.run(runContext);
        }
    }
}
