/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.starrocks;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public class StarrocksJdbcPageSource
        implements ConnectorPageSource
{
    private static final int BATCH_SIZE = 1000;

    private final Connection connection;
    private final Statement statement;
    private final ResultSet resultSet;
    private final List<StarrocksColumnHandle> columns;
    private final List<Type> types;

    private boolean finished;
    private long completedBytes;
    private long readTimeNanos;

    public StarrocksJdbcPageSource(
            Connection connection,
            String sql,
            List<StarrocksColumnHandle> columns,
            StarrocksTypeMapper typeMapper)
    {
        this.connection = requireNonNull(connection, "connection is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.types = columns.stream()
                .map(col -> typeMapper.toTrinoType(col.getType(), col.getColumnType(), col.getColumnSize(), col.getDecimalDigits()))
                .collect(toImmutableList());
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            this.resultSet = stmt.executeQuery(requireNonNull(sql, "sql is null"));
            this.statement = stmt;
        }
        catch (SQLException e) {
            if (stmt != null) {
                try {
                    stmt.close();
                }
                catch (SQLException ignored) {
                }
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to execute aggregate query: " + e.getMessage(), e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished) {
            return null;
        }

        long start = System.nanoTime();

        try {
            BlockBuilder[] builders = new BlockBuilder[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                builders[i] = types.get(i).createBlockBuilder(null, BATCH_SIZE);
            }

            int rowCount = 0;
            while (rowCount < BATCH_SIZE && resultSet.next()) {
                for (int col = 0; col < columns.size(); col++) {
                    appendValue(builders[col], types.get(col), resultSet, columns.get(col).getColumnName());
                }
                rowCount++;
            }

            if (rowCount == 0) {
                finished = true;
                return null;
            }

            readTimeNanos += System.nanoTime() - start;
            Block[] blocks = Arrays.stream(builders).map(BlockBuilder::build).toArray(Block[]::new);
            return SourcePage.create(new Page(rowCount, blocks));
        }
        catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read aggregate result row: " + e.getMessage(), e);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        try {
            resultSet.close();
        }
        catch (SQLException ignored) {
        }
        try {
            statement.close();
        }
        catch (SQLException ignored) {
        }
        try {
            connection.close();
        }
        catch (SQLException ignored) {
        }
    }

    private void appendValue(BlockBuilder builder, Type type, ResultSet rs, String colName)
            throws SQLException
    {
        if (type instanceof BigintType) {
            long val = rs.getLong(colName);
            if (rs.wasNull()) {
                builder.appendNull();
            }
            else {
                BigintType.BIGINT.writeLong(builder, val);
                completedBytes += Long.BYTES;
            }
        }
        else if (type instanceof IntegerType) {
            int val = rs.getInt(colName);
            if (rs.wasNull()) {
                builder.appendNull();
            }
            else {
                IntegerType.INTEGER.writeLong(builder, val);
                completedBytes += Integer.BYTES;
            }
        }
        else if (type instanceof SmallintType) {
            short val = rs.getShort(colName);
            if (rs.wasNull()) {
                builder.appendNull();
            }
            else {
                SmallintType.SMALLINT.writeLong(builder, val);
                completedBytes += Short.BYTES;
            }
        }
        else if (type instanceof TinyintType) {
            byte val = rs.getByte(colName);
            if (rs.wasNull()) {
                builder.appendNull();
            }
            else {
                TinyintType.TINYINT.writeLong(builder, val);
                completedBytes += Byte.BYTES;
            }
        }
        else if (type instanceof BooleanType) {
            boolean val = rs.getBoolean(colName);
            if (rs.wasNull()) {
                builder.appendNull();
            }
            else {
                BooleanType.BOOLEAN.writeBoolean(builder, val);
                completedBytes += 1;
            }
        }
        else if (type instanceof DoubleType) {
            double val = rs.getDouble(colName);
            if (rs.wasNull()) {
                builder.appendNull();
            }
            else {
                DoubleType.DOUBLE.writeDouble(builder, val);
                completedBytes += Double.BYTES;
            }
        }
        else if (type instanceof RealType) {
            float val = rs.getFloat(colName);
            if (rs.wasNull()) {
                builder.appendNull();
            }
            else {
                RealType.REAL.writeLong(builder, floatToRawIntBits(val));
                completedBytes += Float.BYTES;
            }
        }
        else if (type instanceof DecimalType decimalType) {
            BigDecimal val = rs.getBigDecimal(colName);
            if (val == null || rs.wasNull()) {
                builder.appendNull();
            }
            else {
                Decimals.writeBigDecimal(decimalType, builder, val.setScale(decimalType.getScale(), java.math.RoundingMode.HALF_UP));
                completedBytes += decimalType.isShort() ? Long.BYTES : 16;
            }
        }
        else if (type instanceof VarcharType) {
            String val = rs.getString(colName);
            if (val == null || rs.wasNull()) {
                builder.appendNull();
            }
            else {
                type.writeSlice(builder, Slices.utf8Slice(val));
                completedBytes += val.length();
            }
        }
        else if (type instanceof VarbinaryType) {
            byte[] val = rs.getBytes(colName);
            if (val == null || rs.wasNull()) {
                builder.appendNull();
            }
            else {
                type.writeSlice(builder, Slices.wrappedBuffer(val));
                completedBytes += val.length;
            }
        }
        else if (type instanceof DateType) {
            java.sql.Date val = rs.getDate(colName);
            if (val == null || rs.wasNull()) {
                builder.appendNull();
            }
            else {
                DateType.DATE.writeLong(builder, val.toLocalDate().toEpochDay());
                completedBytes += Long.BYTES;
            }
        }
        else if (type instanceof TimestampType) {
            java.sql.Timestamp val = rs.getTimestamp(colName);
            if (val == null || rs.wasNull()) {
                builder.appendNull();
            }
            else {
                TimestampType.TIMESTAMP_MILLIS.writeLong(builder, val.getTime() * 1000);
                completedBytes += Long.BYTES;
            }
        }
        else {
            String val = rs.getString(colName);
            if (val == null || rs.wasNull()) {
                builder.appendNull();
            }
            else {
                type.writeSlice(builder, Slices.utf8Slice(val));
                completedBytes += val.length();
            }
        }
    }
}
