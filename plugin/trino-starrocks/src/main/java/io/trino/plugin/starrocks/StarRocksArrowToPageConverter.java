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

import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.multiplyExact;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public class StarRocksArrowToPageConverter
{
    private static final JsonCodec<Object> JSON_CODEC = jsonCodec(Object.class);
    private static final long MAX_SECONDS_MAGNITUDE = 10_000_000_000L;
    private static final long MAX_MILLIS_MAGNITUDE = 10_000_000_000_000L;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .toFormatter();

    public Page convert(PageBuilder pageBuilder, List<StarRocksColumnHandle> columns, VectorSchemaRoot root)
    {
        requireNonNull(pageBuilder, "pageBuilder is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(root, "root is null");

        int positionCount = root.getRowCount();
        pageBuilder.declarePositions(positionCount);

        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            StarRocksColumnHandle column = columns.get(columnIndex);
            FieldVector vector = root.getVector(column.columnName());
            if (vector == null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Missing StarRocks Arrow field for column '%s'".formatted(column.columnName()));
            }
            writeColumn(pageBuilder.getBlockBuilder(columnIndex), column.columnType(), vector, positionCount);
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    private void writeColumn(BlockBuilder output, Type type, FieldVector vector, int positionCount)
    {
        try {
            for (int index = 0; index < positionCount; index++) {
                if (vector.isNull(index)) {
                    output.appendNull();
                    continue;
                }

                Class<?> javaType = type.getJavaType();
                if (javaType == boolean.class) {
                    type.writeBoolean(output, readBoolean(vector, index));
                }
                else if (javaType == long.class) {
                    writeLongValue(output, type, vector, index);
                }
                else if (javaType == double.class) {
                    type.writeDouble(output, readDouble(vector, index));
                }
                else if (javaType == Slice.class) {
                    writeSliceValue(output, type, vector, index);
                }
                else if (javaType == Int128.class) {
                    writeLongDecimal(output, (DecimalType) type, vector, index);
                }
                else {
                    throw unsupportedType(type, vector);
                }
            }
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Failed to convert StarRocks Arrow field '%s' into Trino type '%s'".formatted(vector.getName(), type),
                    e);
        }
    }

    private void writeLongValue(BlockBuilder output, Type type, FieldVector vector, int index)
    {
        if (type == TINYINT) {
            type.writeLong(output, readTinyint(vector, index));
            return;
        }
        if (type == SMALLINT) {
            type.writeLong(output, readSmallint(vector, index));
            return;
        }
        if (type == INTEGER) {
            type.writeLong(output, readInteger(vector, index));
            return;
        }
        if (type == BIGINT) {
            type.writeLong(output, readBigint(vector, index));
            return;
        }
        if (type == REAL) {
            type.writeLong(output, floatToRawIntBits(readFloat(vector, index)));
            return;
        }
        if (type == DATE) {
            type.writeLong(output, readDateValue(vector, index));
            return;
        }
        if (type instanceof TimestampType timestampType && timestampType.isShort()) {
            type.writeLong(output, readTimestampMicros(vector, index, timestampType.getPrecision()));
            return;
        }
        if (type instanceof DecimalType decimalType && decimalType.isShort()) {
            decimalType.writeLong(output, encodeShortScaledValue(coerceDecimalValue(vector.getName(), readDecimalValue(vector, index), decimalType), decimalType.getScale()));
            return;
        }

        throw unsupportedType(type, vector);
    }

    private void writeLongDecimal(BlockBuilder output, DecimalType decimalType, FieldVector vector, int index)
    {
        decimalType.writeObject(output, Decimals.encodeScaledValue(coerceDecimalValue(vector.getName(), readDecimalValue(vector, index), decimalType), decimalType.getScale()));
    }

    private void writeSliceValue(BlockBuilder output, Type type, FieldVector vector, int index)
    {
        if (type == VARBINARY) {
            type.writeSlice(output, wrappedBuffer(readBinaryValue(vector, index)));
            return;
        }
        if (type.getBaseName().equals(JSON)) {
            type.writeSlice(output, utf8Slice(readJsonTextValue(vector, index)));
            return;
        }

        Slice value = utf8Slice(readTextValue(vector, index));
        if (type instanceof CharType charType) {
            type.writeSlice(output, truncateToLengthAndTrimSpaces(value, charType));
            return;
        }
        type.writeSlice(output, value);
    }

    private static boolean readBoolean(FieldVector vector, int index)
    {
        if (vector instanceof BitVector bitVector) {
            return bitVector.get(index) == 1;
        }
        if (vector instanceof TinyIntVector tinyIntVector) {
            return tinyIntVector.get(index) != 0;
        }
        return Boolean.parseBoolean(readTextValue(vector, index));
    }

    private static long readTinyint(FieldVector vector, int index)
    {
        if (vector instanceof TinyIntVector tinyIntVector) {
            return tinyIntVector.get(index);
        }
        if (vector instanceof BitVector bitVector) {
            return bitVector.get(index);
        }
        return Long.parseLong(readTextValue(vector, index));
    }

    private static long readSmallint(FieldVector vector, int index)
    {
        if (vector instanceof SmallIntVector smallIntVector) {
            return smallIntVector.get(index);
        }
        if (vector instanceof TinyIntVector tinyIntVector) {
            return tinyIntVector.get(index);
        }
        if (vector instanceof BitVector bitVector) {
            return bitVector.get(index);
        }
        return Long.parseLong(readTextValue(vector, index));
    }

    private static long readInteger(FieldVector vector, int index)
    {
        if (vector instanceof IntVector intVector) {
            return intVector.get(index);
        }
        if (vector instanceof SmallIntVector smallIntVector) {
            return smallIntVector.get(index);
        }
        if (vector instanceof TinyIntVector tinyIntVector) {
            return tinyIntVector.get(index);
        }
        if (vector instanceof BitVector bitVector) {
            return bitVector.get(index);
        }
        return Long.parseLong(readTextValue(vector, index));
    }

    private static long readBigint(FieldVector vector, int index)
    {
        if (vector instanceof BigIntVector bigIntVector) {
            return bigIntVector.get(index);
        }
        if (vector instanceof IntVector intVector) {
            return intVector.get(index);
        }
        return Long.parseLong(readTextValue(vector, index));
    }

    private static float readFloat(FieldVector vector, int index)
    {
        if (vector instanceof Float4Vector float4Vector) {
            return float4Vector.get(index);
        }
        return Float.parseFloat(readTextValue(vector, index));
    }

    private static double readDouble(FieldVector vector, int index)
    {
        if (vector instanceof Float8Vector float8Vector) {
            return float8Vector.get(index);
        }
        if (vector instanceof Float4Vector float4Vector) {
            return float4Vector.get(index);
        }
        return Double.parseDouble(readTextValue(vector, index));
    }

    private static long readDateValue(FieldVector vector, int index)
    {
        if (vector instanceof DateDayVector dateDayVector) {
            return dateDayVector.get(index);
        }
        return parseDateText(readTextValue(vector, index)).toEpochDay();
    }

    private static long readTimestampMicros(FieldVector vector, int index, int precision)
    {
        long epochMicros;
        if (vector instanceof TimeStampVector timestampVector) {
            epochMicros = readArrowTimestampMicros(timestampVector, index);
        }
        else {
            epochMicros = toTrinoTimestampMicros(parseTimestampText(readTextValue(vector, index)));
        }

        if (precision == TIMESTAMP_MICROS.getPrecision()) {
            return epochMicros;
        }
        return round(epochMicros, TIMESTAMP_MICROS.getPrecision() - precision);
    }

    private static long readArrowTimestampMicros(TimeStampVector timestampVector, int index)
    {
        long rawValue = timestampVector.get(index);
        long normalizedValue = normalizeTimestamp(timestampVector, rawValue);
        String timeZone = arrowTimestampTimeZone(timestampVector);
        if (normalizedValue != rawValue) {
            if (timeZone != null && !timeZone.isBlank()) {
                return toTrinoTimestampMicros(timestampMicrosToLocalDateTime(normalizedValue, timeZone));
            }
            return normalizedValue;
        }

        Object timestampValue = timestampVector.getObject(index);
        if (timestampValue instanceof LocalDateTime localDateTime) {
            return toTrinoTimestampMicros(localDateTime);
        }
        if (timestampValue instanceof OffsetDateTime offsetDateTime) {
            return toTrinoTimestampMicros(offsetDateTime.toLocalDateTime());
        }
        if (timeZone != null && !timeZone.isBlank()) {
            return toTrinoTimestampMicros(timestampMicrosToLocalDateTime(normalizedValue, timeZone));
        }
        return normalizedValue;
    }

    private static BigDecimal readDecimalValue(FieldVector vector, int index)
    {
        if (vector instanceof Decimal256Vector decimal256Vector) {
            return decimal256Vector.getObject(index);
        }
        if (vector instanceof DecimalVector decimalVector) {
            return decimalVector.getObject(index);
        }
        if (vector instanceof FixedSizeBinaryVector fixedSizeBinaryVector) {
            return new BigDecimal(decodeLittleEndianSignedInteger(fixedSizeBinaryVector.get(index)));
        }
        if (vector instanceof BigIntVector bigIntVector) {
            return BigDecimal.valueOf(bigIntVector.get(index));
        }
        if (vector instanceof IntVector intVector) {
            return BigDecimal.valueOf(intVector.get(index));
        }
        return new BigDecimal(readTextValue(vector, index));
    }

    private static byte[] readBinaryValue(FieldVector vector, int index)
    {
        if (vector instanceof VarBinaryVector varBinaryVector) {
            return varBinaryVector.get(index);
        }
        if (vector instanceof LargeVarBinaryVector largeVarBinaryVector) {
            return largeVarBinaryVector.get(index);
        }
        if (vector instanceof FixedSizeBinaryVector fixedSizeBinaryVector) {
            return fixedSizeBinaryVector.get(index);
        }
        return readTextValue(vector, index).getBytes(StandardCharsets.UTF_8);
    }

    private static String readJsonTextValue(FieldVector vector, int index)
    {
        if (vector instanceof VarCharVector || vector instanceof LargeVarCharVector) {
            return readTextValue(vector, index);
        }

        Object value = vector.getObject(index);
        if (value instanceof Slice slice) {
            return slice.toStringUtf8();
        }
        if (value instanceof CharSequence) {
            return value.toString();
        }
        return JSON_CODEC.toJson(toJsonCompatibleValue(value));
    }

    private static Object toJsonCompatibleValue(Object value)
    {
        if (value == null ||
                value instanceof Boolean ||
                value instanceof Number ||
                value instanceof CharSequence) {
            return value;
        }
        if (value instanceof Slice slice) {
            return slice.toStringUtf8();
        }
        if (value instanceof Map<?, ?> map) {
            return map.entrySet().stream()
                    .collect(java.util.stream.Collectors.toMap(
                            entry -> entry.getKey().toString(),
                            entry -> toJsonCompatibleValue(entry.getValue())));
        }
        if (value instanceof Iterable<?> iterable) {
            java.util.ArrayList<Object> values = new java.util.ArrayList<>();
            for (Object element : iterable) {
                values.add(toJsonCompatibleValue(element));
            }
            return values;
        }
        return value.toString();
    }

    private static String readTextValue(FieldVector vector, int index)
    {
        if (vector instanceof VarCharVector varCharVector) {
            return new String(varCharVector.get(index), StandardCharsets.UTF_8);
        }
        if (vector instanceof LargeVarCharVector largeVarCharVector) {
            return new String(largeVarCharVector.get(index), StandardCharsets.UTF_8);
        }
        if (vector instanceof FixedSizeBinaryVector fixedSizeBinaryVector) {
            return decodeLittleEndianSignedInteger(fixedSizeBinaryVector.get(index)).toString();
        }
        if (vector instanceof Decimal256Vector decimal256Vector) {
            return decimal256Vector.getObject(index).toPlainString();
        }
        if (vector instanceof DecimalVector decimalVector) {
            return decimalVector.getObject(index).toPlainString();
        }
        if (vector instanceof DateDayVector dateDayVector) {
            return LocalDate.ofEpochDay(dateDayVector.get(index)).format(DATE_FORMATTER);
        }
        if (vector instanceof TimeStampVector) {
            return SqlTimestamp.newInstance(6, readTimestampMicros(vector, index, 6), 0).toString();
        }

        Object value = vector.getObject(index);
        if (value != null) {
            return value.toString();
        }
        throw unsupportedVector(vector);
    }

    private static long normalizeTimestamp(TimeStampVector timestampVector, long value)
    {
        if (timestampVector.getField().getType() instanceof ArrowType.Timestamp timestampType) {
            return switch (timestampType.getUnit()) {
                case SECOND -> multiplyExact(value, MICROSECONDS_PER_SECOND);
                case MILLISECOND -> multiplyExact(value, MICROSECONDS_PER_MILLISECOND);
                case MICROSECOND -> value;
                case NANOSECOND -> Math.floorDiv(value, 1_000L);
            };
        }
        return normalizeTimestampHeuristically(value);
    }

    private static long normalizeTimestampHeuristically(long value)
    {
        if (value > -MAX_SECONDS_MAGNITUDE && value < MAX_SECONDS_MAGNITUDE) {
            return multiplyExact(value, MICROSECONDS_PER_SECOND);
        }
        if (value > -MAX_MILLIS_MAGNITUDE && value < MAX_MILLIS_MAGNITUDE) {
            return multiplyExact(value, MICROSECONDS_PER_MILLISECOND);
        }
        return value;
    }

    private static long toTrinoTimestampMicros(LocalDateTime timestamp)
    {
        return multiplyExact(timestamp.toEpochSecond(UTC), MICROSECONDS_PER_SECOND) + (timestamp.getLong(ChronoField.NANO_OF_SECOND) / 1_000);
    }

    private static LocalDateTime timestampMicrosToLocalDateTime(long epochMicros, String timeZone)
    {
        long epochSeconds = Math.floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
        long microsOfSecond = Math.floorMod(epochMicros, MICROSECONDS_PER_SECOND);
        return Instant.ofEpochSecond(epochSeconds, microsOfSecond * 1_000L)
                .atZone(ZoneId.of(timeZone))
                .toLocalDateTime();
    }

    private static String arrowTimestampTimeZone(TimeStampVector timestampVector)
    {
        if (timestampVector.getField().getType() instanceof ArrowType.Timestamp timestampType) {
            return timestampType.getTimezone();
        }
        return null;
    }

    private static LocalDate parseDateText(String value)
    {
        String normalized = value.trim();
        if (normalized.contains(" ") || normalized.contains("T")) {
            return parseTimestampText(normalized).toLocalDate();
        }
        return LocalDate.parse(normalized, DATE_FORMATTER);
    }

    private static LocalDateTime parseTimestampText(String value)
    {
        String normalized = value.trim().replace('T', ' ');
        if (normalized.length() == 10) {
            normalized = normalized + " 00:00:00";
        }
        return LocalDateTime.parse(normalized, TIMESTAMP_FORMATTER);
    }

    private static BigDecimal coerceDecimalValue(String columnName, BigDecimal value, DecimalType decimalType)
    {
        BigDecimal scaledValue;
        try {
            scaledValue = value.setScale(decimalType.getScale(), UNNECESSARY);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "StarRocks value '%s' for column '%s' cannot be represented as %s without rounding".formatted(value.toPlainString(), columnName, decimalType),
                    e);
        }

        if (overflows(scaledValue, decimalType.getPrecision())) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "StarRocks value '%s' for column '%s' exceeds Trino type %s".formatted(value.toPlainString(), columnName, decimalType));
        }
        return scaledValue;
    }

    private static BigInteger decodeLittleEndianSignedInteger(byte[] littleEndianBytes)
    {
        byte[] bigEndianBytes = littleEndianBytes.clone();
        for (int left = 0, right = bigEndianBytes.length - 1; left < right; left++, right--) {
            byte temp = bigEndianBytes[left];
            bigEndianBytes[left] = bigEndianBytes[right];
            bigEndianBytes[right] = temp;
        }
        return new BigInteger(bigEndianBytes);
    }

    private static IllegalArgumentException unsupportedVector(FieldVector vector)
    {
        return new IllegalArgumentException("Unsupported StarRocks Arrow vector: " + vector.getClass().getSimpleName());
    }

    private static IllegalArgumentException unsupportedType(Type type, FieldVector vector)
    {
        return new IllegalArgumentException("Unsupported Trino type %s for StarRocks Arrow vector %s".formatted(type, vector.getClass().getSimpleName()));
    }
}
