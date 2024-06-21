package io.kestra.plugin.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.GettableByIndex;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractQuery extends Task implements RunnableTask<AbstractQuery.Output>, QueryInterface {
    @Builder.Default
    private boolean fetch = false;

    @Builder.Default
    private boolean store = false;

    @Builder.Default
    private boolean fetchOne = false;

    protected String cql;

    public AbstractQuery.Output run(RunContext runContext) throws Exception {
        try (CqlSession session = this.cqlSession(runContext)) {
            ResultSet rs = session.execute(runContext.render(cql));
            ColumnDefinitions columnDefinitions = rs.getColumnDefinitions();

            Output.OutputBuilder outputBuilder = Output.builder()
                .bytes(rs.getExecutionInfo().getResponseSizeInBytes());

            if (this.fetchOne) {
                outputBuilder
                    .row(convertRow(rs.one(), columnDefinitions))
                    .size(1L);
            } else if (this.store) {
                File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile));
                AtomicLong count = new AtomicLong();
                try (OutputStream outputStream = new FileOutputStream(tempFile)) {
                    rs.forEach(throwConsumer(row -> {
                        count.getAndIncrement();
                        FileSerde.write(outputStream, convertRow(row, columnDefinitions));
                    }));
                }

                fileWriter.flush();
                fileWriter.close();

                outputBuilder
                    .uri(runContext.storage().putFile(tempFile))
                    .size(count.get());
            } else if (this.fetch) {
                List<Map<String, Object>> maps = new ArrayList<>();
                rs.forEach(row -> maps.add(convertRow(row, columnDefinitions)));

                outputBuilder
                    .rows(maps)
                    .size((long) maps.size());
            }

            Output output = outputBuilder.build();

            if (output.getSize() != null) {
                runContext.metric(Counter.of("fetch.size", output.getSize()));
            }

            if (output.getBytes() != null) {
                runContext.metric(Counter.of("fetch.size", output.getBytes()));
            }

            return output;
        }
    }

    private Map<String, Object> convertRow(Row row, ColumnDefinitions columnDefinitions) {
        Map<String, Object> map = new LinkedHashMap<>();

        for (int index = 0; index < columnDefinitions.size(); index++) {
            ColumnDefinition columnDefinition = columnDefinitions.get(index);

            map.put(
                columnDefinition.getName().asInternal(),
                convertCell(columnDefinition, row, index)
            );
        }

        return map;
    }

    private Object convertCell(ColumnDefinition columnDefinition, GettableByIndex row, int index) {
        switch (columnDefinition.getType().getProtocolCode()) {
            case ProtocolConstants.DataType.COUNTER:
            case ProtocolConstants.DataType.BIGINT:
                return row.getLong(index);

            case ProtocolConstants.DataType.BLOB:
                ByteBuffer bytes = row.getBytesUnsafe(index);
                return bytes == null ? null : bytes.array();

            case ProtocolConstants.DataType.BOOLEAN:
                return row.getBoolean(index);

            case ProtocolConstants.DataType.DECIMAL:
                return row.getBigDecimal(index);

            case ProtocolConstants.DataType.DOUBLE:
                return row.getDouble(index);

            case ProtocolConstants.DataType.FLOAT:
                return row.getFloat(index);

            case ProtocolConstants.DataType.SMALLINT:
                return row.getShort(index);

            case ProtocolConstants.DataType.TINYINT:
                return row.getByte(index);

            case ProtocolConstants.DataType.INT:
                return row.getInt(index);

            case ProtocolConstants.DataType.VARINT:
                return row.getBigInteger(index);

            case ProtocolConstants.DataType.TIMESTAMP:
                return row.getInstant(index);

            case ProtocolConstants.DataType.TIMEUUID:
            case ProtocolConstants.DataType.UUID:
                UUID uuid = row.getUuid(index);
                return uuid == null ? null : uuid.toString();

            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.VARCHAR:
                return row.getString(index);

            case ProtocolConstants.DataType.INET:
                InetAddress inetAddress = row.getInetAddress(index);
                return inetAddress == null ? null : inetAddress.toString();

            case ProtocolConstants.DataType.DATE:
                return row.getLocalDate(index);

            case ProtocolConstants.DataType.TIME:
                return row.getLocalTime(index);

            case ProtocolConstants.DataType.DURATION:
                CqlDuration cqlDuration = row.getCqlDuration(index);
                return cqlDuration == null ? null : Duration.ofNanos(cqlDuration.getNanoseconds());

            case ProtocolConstants.DataType.LIST:
            case ProtocolConstants.DataType.MAP:
            case ProtocolConstants.DataType.SET:
                return row.getObject(index);

            case ProtocolConstants.DataType.TUPLE:
                TupleValue tupleValue = row.getTupleValue(index);

                if (tupleValue == null) {
                    return null;
                }

                List<Object> list = new ArrayList<>();
                for (int i = 0; i < tupleValue.size(); i++) {
                    list.add(tupleValue.getObject(i));
                }
                return list;

            case ProtocolConstants.DataType.CUSTOM:
            case ProtocolConstants.DataType.UDT:
        }

        throw new IllegalArgumentException("Invalid datatype '" + columnDefinition.getType() + '"');
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Map containing the first row of fetched data",
            description = "Only populated if 'fetchOne' parameter is set to true."
        )
        private final Map<String, Object> row;

        @Schema(
            title = "Lit of map containing rows of fetched data",
            description = "Only populated if 'fetch' parameter is set to true."
        )
        private final List<Map<String, Object>> rows;

        @Schema(
            title = "The url of the result file on kestra storage (.ion file / Amazon Ion text format)",
            description = "Only populated if 'store' is set to true."
        )
        private final URI uri;

        @Schema(
            title = "The size of the fetched rows",
            description = "Only populated if 'store' or 'fetch' parameter is set to true."
        )
        private final Long size;

        @Schema(
            title = "The size of the binary response in bytes."
        )
        private final Integer bytes;
    }
}
