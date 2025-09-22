package com.databricks.jdbc.api.impl.converters;

import static com.databricks.jdbc.api.impl.converters.ArrowToJavaObjectConverter.convert;
import static com.databricks.jdbc.api.impl.converters.ArrowToJavaObjectConverter.getZoneIdFromTimeZoneOpt;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.VARIANT;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksArray;
import com.databricks.jdbc.api.impl.DatabricksStruct;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ArrowToJavaObjectConverterTest {
  @Mock IDatabricksConnectionContext connectionContext;
  private final BufferAllocator bufferAllocator;

  ArrowToJavaObjectConverterTest() {
    this.bufferAllocator = new RootAllocator();
  }

  @Test
  public void testConvert_Interval() throws SQLException {
    // 1200 months → "100-0"
    IntervalYearVector yv = new IntervalYearVector("iv", bufferAllocator);
    yv.allocateNewSafe();
    yv.setSafe(0, 1200);
    yv.setValueCount(1);
    ColumnInfo intervalColumnInfo = new ColumnInfo();

    Object out =
        ArrowToJavaObjectConverter.convert(
            yv, 0, ColumnInfoTypeName.INTERVAL, "INTERVAL YEAR TO MONTH", intervalColumnInfo);
    assertEquals("100-0", out);

    // build a Duration of 200h13m50.3s → -200:13:50.3
    IntervalDayVector dv = new IntervalDayVector("dv", bufferAllocator);
    dv.allocateNewSafe();
    // Arrow's IntervalDayVector takes (days, milliseconds)
    Duration d = Duration.ofHours(200).plusMinutes(13).plusSeconds(50).plusMillis(300);
    long days = d.toDays();
    int millis = (int) (d.minusDays(days).toMillis());
    dv.setSafe(0, (int) days, millis);
    dv.setValueCount(1);

    out =
        ArrowToJavaObjectConverter.convert(
            dv, 0, ColumnInfoTypeName.INTERVAL, "INTERVAL HOUR TO SECOND", new ColumnInfo());
    assertEquals("8 08:13:50.300000000", out);

    // null metadata throws DatabricksValidation Exception
    assertThrows(
        DatabricksValidationException.class,
        () -> {
          ArrowToJavaObjectConverter.convert(
              dv, 0, ColumnInfoTypeName.INTERVAL, null, new ColumnInfo());
        });
  }

  @Test
  public void testNullObjectConversion() throws SQLException {
    TinyIntVector tinyIntVector = new TinyIntVector("tinyIntVector", this.bufferAllocator);
    tinyIntVector.allocateNew(1);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            tinyIntVector, 0, ColumnInfoTypeName.BYTE, "BYTE", new ColumnInfo());
    assertNull(convertedObject);
  }

  @Test
  public void testNullHandlingInVarCharVector() throws Exception {
    // Create a VarCharVector with 3 values: non-null, null, and empty string
    disableArrowNullChecking();
    VarCharVector vector = new VarCharVector("varCharVector", this.bufferAllocator);
    vector.allocateNew(4);

    // Set first value: "hello"
    vector.set(0, "hello".getBytes());

    // Second value: null (don't set it, which makes it null by default)

    // Third value: empty string (explicitly set as "")
    vector.set(2, "".getBytes());

    // Fourth value: set to null
    vector.setNull(3);

    // Set vector value count
    vector.setValueCount(4);

    // Case 1: Non-null value
    assertFalse(vector.isNull(0));
    assertEquals(
        "hello", convert(vector, 0, ColumnInfoTypeName.STRING, "STRING", new ColumnInfo()));

    // Case 2: Null value
    assertTrue(vector.isNull(1));
    assertNull(convert(vector, 1, ColumnInfoTypeName.STRING, "STRING", new ColumnInfo()));

    // Case 3: Empty string (should not be treated as null)
    assertFalse(vector.isNull(2));
    assertEquals(
        "",
        convert(
            vector,
            2,
            ColumnInfoTypeName.STRING,
            "STRING",
            new ColumnInfo())); // Empty string should be empty, not null

    // Case 4: Explicitly set to null
    assertTrue(vector.isNull(3));
    String valueWithoutCheck =
        (String) convert(vector, 3, ColumnInfoTypeName.STRING, "STRING", new ColumnInfo());
    // This assertion is expected to fail - it shows the problem when isNull check is removed
    assertNull(valueWithoutCheck);

    enableArrowNullChecking();
  }

  private void disableArrowNullChecking() {
    System.setProperty("arrow.enable_null_check_for_get", "false");
  }

  private void enableArrowNullChecking() {
    System.setProperty("arrow.enable_null_check_for_get", "true");
  }

  @Test
  public void testByteVectorWithNullChecks() throws Exception {
    TinyIntVector vector = new TinyIntVector("tinyIntVector", this.bufferAllocator);
    vector.allocateNew(3);

    // First value: explicitly set to null
    vector.setNull(0);

    // Second value: skip setting it, which makes it null by default

    // Third value: set to 0
    vector.set(2, 0);

    vector.setValueCount(3);

    // Test our converter with proper null handling
    assertTrue(vector.isNull(0));
    assertNull(convert(vector, 0, ColumnInfoTypeName.BYTE, "BYTE", new ColumnInfo()));

    assertTrue(vector.isNull(1));
    assertNull(convert(vector, 1, ColumnInfoTypeName.BYTE, "BYTE", new ColumnInfo()));

    // The zero value should still be correctly identified as 0, not null
    assertFalse(vector.isNull(2));
    assertEquals((byte) 0, convert(vector, 2, ColumnInfoTypeName.BYTE, "BYTE", new ColumnInfo()));
  }

  @Test
  public void testByteConversion() throws SQLException {
    TinyIntVector tinyIntVector = new TinyIntVector("tinyIntVector", this.bufferAllocator);
    tinyIntVector.allocateNew(1);
    tinyIntVector.set(0, 65);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            tinyIntVector, 0, ColumnInfoTypeName.BYTE, "BYTE", new ColumnInfo());

    assertInstanceOf(Byte.class, convertedObject);
    assertEquals((byte) 65, convertedObject);
  }

  @Test
  public void testVariantConversion() throws SQLException, JsonProcessingException {
    VarCharVector varCharVector = new VarCharVector("varCharVector", this.bufferAllocator);
    varCharVector.allocateNew(3);

    // Test null
    Object nullObject =
        ArrowToJavaObjectConverter.convert(varCharVector, 0, null, VARIANT, new ColumnInfo());
    assertNull(nullObject);

    // Test integer
    varCharVector.set(1, "1".getBytes());
    Object intObject =
        ArrowToJavaObjectConverter.convert(varCharVector, 1, null, VARIANT, new ColumnInfo());
    assertNotNull(intObject);
    assertInstanceOf(String.class, intObject, "Expected result to be a String");
    assertEquals("1", intObject, "The integer should be converted to a string.");

    // Test map
    Map<String, String> map = new HashMap<>();
    map.put("key", "value");
    varCharVector.set(2, map.toString().getBytes());
    Object mapObject =
        ArrowToJavaObjectConverter.convert(varCharVector, 2, null, VARIANT, new ColumnInfo());
    assertNotNull(mapObject);
    assertInstanceOf(String.class, mapObject, "Expected result to be a String");
    assertEquals(mapObject.toString(), mapObject, "The map should be converted to a JSON string.");
  }

  @Test
  public void testShortConversion() throws SQLException {
    SmallIntVector smallIntVector = new SmallIntVector("smallIntVector", this.bufferAllocator);
    smallIntVector.allocateNew(1);
    smallIntVector.set(0, 4);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            smallIntVector, 0, ColumnInfoTypeName.SHORT, "SHORT", new ColumnInfo());

    assertInstanceOf(Short.class, convertedObject);
    assertEquals((short) 4, convertedObject);
  }

  @Test
  public void testTimestampNTZConversion() throws SQLException {
    long timestamp = 1704054601234567L;

    TimeStampMicroVector timestampMicroVector =
        new TimeStampMicroVector("timestampMicroVector", this.bufferAllocator);
    timestampMicroVector.allocateNew(1);
    timestampMicroVector.set(0, timestamp);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            timestampMicroVector,
            0,
            ColumnInfoTypeName.TIMESTAMP,
            "TIMESTAMP_NTZ",
            new ColumnInfo());

    assertInstanceOf(Timestamp.class, convertedObject);
    assertEquals(getTimestampAdjustedToTimeZone(timestamp, "UTC"), convertedObject);
  }

  @Test
  public void testIntConversion() throws SQLException {
    IntVector intVector = new IntVector("intVector", this.bufferAllocator);
    intVector.allocateNew(1);
    intVector.set(0, 1111111111);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            intVector, 0, ColumnInfoTypeName.INT, "INT", new ColumnInfo());

    assertInstanceOf(Integer.class, convertedObject);
    assertEquals(1111111111, convertedObject);
  }

  @Test
  public void testLongConversion() throws SQLException {
    BigIntVector bigIntVector = new BigIntVector("bigIntVector", this.bufferAllocator);
    bigIntVector.allocateNew(1);
    bigIntVector.set(0, 1111111111111111111L);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            bigIntVector, 0, ColumnInfoTypeName.LONG, "LONG", new ColumnInfo());

    assertInstanceOf(Long.class, convertedObject);
    assertEquals(1111111111111111111L, convertedObject);
  }

  @Test
  public void testFloatConversion() throws SQLException {
    Float4Vector float4Vector = new Float4Vector("float4Vector", this.bufferAllocator);
    float4Vector.allocateNew(1);
    float4Vector.set(0, 4.2f);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            float4Vector, 0, ColumnInfoTypeName.FLOAT, "FLOAT", new ColumnInfo());

    assertInstanceOf(Float.class, convertedObject);
    assertEquals(4.2f, convertedObject);
  }

  @Test
  public void testDoubleConversion() throws SQLException {
    Float8Vector float8Vector = new Float8Vector("float8Vector", this.bufferAllocator);
    float8Vector.allocateNew(1);
    float8Vector.set(0, 4.11111111);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            float8Vector, 0, ColumnInfoTypeName.DOUBLE, "DOUBLE", new ColumnInfo());

    assertInstanceOf(Double.class, convertedObject);
    assertEquals(4.11111111, convertedObject);
  }

  @Test
  public void testBigDecimalConversion() throws SQLException {
    DecimalVector decimalVector = new DecimalVector("decimalVector", this.bufferAllocator, 30, 10);
    decimalVector.allocateNew(1);
    decimalVector.set(0, BigDecimal.valueOf(4.1111111111));
    ColumnInfo decimalColumnInfo = new ColumnInfo().setTypeScale(10L).setTypePrecision(30L);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            decimalVector, 0, ColumnInfoTypeName.DECIMAL, "DECIMAL(30,10)", decimalColumnInfo);

    assertInstanceOf(BigDecimal.class, convertedObject);
    assertEquals(BigDecimal.valueOf(4.1111111111), convertedObject);
  }

  @Test
  public void testByteArrayConversion() throws SQLException {
    VarBinaryVector varBinaryVector = new VarBinaryVector("varBinaryVector", this.bufferAllocator);
    varBinaryVector.allocateNew(1);
    varBinaryVector.set(0, new byte[] {65, 66, 67});
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            varBinaryVector, 0, ColumnInfoTypeName.BINARY, "BINARY", new ColumnInfo());

    assertInstanceOf(byte[].class, convertedObject);
    assertArrayEquals("ABC".getBytes(), (byte[]) convertedObject);
  }

  @Test
  public void testBooleanConversion() throws SQLException {
    BitVector bitVector = new BitVector("bitVector", this.bufferAllocator);
    bitVector.allocateNew(2);
    bitVector.set(0, 0);
    bitVector.set(1, 1);
    Object convertedFalseObject =
        ArrowToJavaObjectConverter.convert(
            bitVector, 0, ColumnInfoTypeName.BOOLEAN, "BOOLEAN", new ColumnInfo());
    Object convertedTrueObject =
        ArrowToJavaObjectConverter.convert(
            bitVector, 1, ColumnInfoTypeName.BOOLEAN, "BOOLEAN", new ColumnInfo());

    assertInstanceOf(Boolean.class, convertedTrueObject);
    assertInstanceOf(Boolean.class, convertedFalseObject);
    assertEquals(false, convertedFalseObject);
    assertEquals(true, convertedTrueObject);
  }

  @Test
  public void testCharConversion() throws SQLException {
    VarCharVector varCharVector = new VarCharVector("varCharVector", this.bufferAllocator);
    varCharVector.allocateNew(1);
    varCharVector.set(0, new byte[] {65});
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            varCharVector, 0, ColumnInfoTypeName.CHAR, "CHAR", new ColumnInfo());

    assertInstanceOf(Character.class, convertedObject);
    assertEquals('A', convertedObject);
  }

  @Test
  public void testStringConversion() throws SQLException {
    VarCharVector varCharVector = new VarCharVector("varCharVector", this.bufferAllocator);
    varCharVector.allocateNew(1);
    varCharVector.set(0, new byte[] {65, 66, 67});
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            varCharVector, 0, ColumnInfoTypeName.STRING, "STRING", new ColumnInfo());

    assertInstanceOf(String.class, convertedObject);
    assertEquals("ABC", convertedObject);
  }

  @Test
  public void testDateConversion() throws SQLException {
    DateDayVector dateDayVector = new DateDayVector("dateDayVector", this.bufferAllocator);
    dateDayVector.allocateNew(1);
    dateDayVector.set(0, 19598); // 29th August 2023
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            dateDayVector, 0, ColumnInfoTypeName.DATE, "DATE", new ColumnInfo());

    assertInstanceOf(Date.class, convertedObject);
    assertEquals(Date.valueOf("2023-08-29"), convertedObject);
  }

  @Test
  public void testTimestampConversion() throws SQLException {
    long timestamp = 1704054601234567L;
    String timeZone = "Asia/Tokyo";
    TimeStampMicroTZVector timeStampMicroTZVector =
        new TimeStampMicroTZVector("timeStampMicroTzVector", this.bufferAllocator, timeZone);
    timeStampMicroTZVector.allocateNew(1);
    timeStampMicroTZVector.set(0, timestamp);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            timeStampMicroTZVector, 0, ColumnInfoTypeName.TIMESTAMP, "TIMESTAMP", new ColumnInfo());

    assertInstanceOf(Timestamp.class, convertedObject);
    assertEquals(getTimestampAdjustedToTimeZone(timestamp, timeZone), convertedObject);
  }

  private static Timestamp getTimestampAdjustedToTimeZone(long timestampMicro, String timeZone) {
    Instant instant = Instant.ofEpochMilli(timestampMicro / 1000);
    LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.of(timeZone));
    var ts = Timestamp.valueOf(localDateTime);
    ts.setNanos((int) (timestampMicro % 1000_000) * 1000);
    return ts;
  }

  @Test
  public void testStructConversion() throws SQLException {
    VarCharVector varCharVector = new VarCharVector("varCharVector", this.bufferAllocator);
    varCharVector.allocateNew(1);
    varCharVector.set(0, "{\"k\": 10}".getBytes());
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            varCharVector,
            0,
            ColumnInfoTypeName.STRUCT,
            "STRUCT<key: STRING, value: INT>",
            new ColumnInfo());
    assertInstanceOf(DatabricksStruct.class, convertedObject);
  }

  @Test
  public void testArrayConversion() throws SQLException {
    VarCharVector varCharVector = new VarCharVector("varCharVector", this.bufferAllocator);
    varCharVector.allocateNew(1);
    varCharVector.set(0, "[\"A\", \"B\"]".getBytes());
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            varCharVector, 0, ColumnInfoTypeName.STRING, "ARRAY<STRING>", new ColumnInfo());
    assertInstanceOf(DatabricksArray.class, convertedObject);
  }

  @Test
  public void testConvertToDecimal() throws DatabricksValidationException {
    // Test with Text object
    Text textObject = new Text("123.456");
    ColumnInfo columnInfo = new ColumnInfo().setTypeScale(3L).setTypePrecision(10L);
    columnInfo.setTypeName(ColumnInfoTypeName.DECIMAL);
    columnInfo.setTypeText("DECIMAL(10,3)");
    BigDecimal result = ArrowToJavaObjectConverter.convertToDecimal(textObject, columnInfo);
    assertEquals(new BigDecimal("123.456"), result);

    // Test with Number object and valid metadata
    Double numberObject = 123.456;
    columnInfo = new ColumnInfo().setTypeScale(2L).setTypePrecision(10L);
    columnInfo.setTypeName(ColumnInfoTypeName.DECIMAL);
    columnInfo.setTypeText("DECIMAL(10,2)");
    result = ArrowToJavaObjectConverter.convertToDecimal(numberObject, columnInfo);
    assertEquals(new BigDecimal("123.46"), result); // Rounded to 2 decimal places

    numberObject = 123.45;
    result = ArrowToJavaObjectConverter.convertToDecimal(numberObject, columnInfo);
    assertEquals(new BigDecimal("123.45"), result); // No rounding

    // Test with Number object and invalid metadata
    columnInfo = new ColumnInfo().setTypePrecision(10L);
    columnInfo.setTypeName(ColumnInfoTypeName.DECIMAL);
    columnInfo.setTypeText("DECIMAL(10,invalid)");
    result = ArrowToJavaObjectConverter.convertToDecimal(numberObject, columnInfo);
    assertEquals(new BigDecimal("123.45"), result); // No scale should not be applied

    // Test with unsupported object type
    assertThrows(
        DatabricksValidationException.class,
        () -> {
          ColumnInfo errorColumnInfo = new ColumnInfo().setTypeScale(2L).setTypePrecision(10L);
          errorColumnInfo.setTypeName(ColumnInfoTypeName.DECIMAL);
          errorColumnInfo.setTypeText("DECIMAL(10,2)");
          ArrowToJavaObjectConverter.convertToDecimal(new Object(), errorColumnInfo);
        });

    // Test with rounding
    numberObject = 123.456789;
    columnInfo = new ColumnInfo().setTypeScale(4L).setTypePrecision(10L);
    columnInfo.setTypeName(ColumnInfoTypeName.DECIMAL);
    columnInfo.setTypeText("DECIMAL(10,4)");
    result = ArrowToJavaObjectConverter.convertToDecimal(numberObject, columnInfo);
    assertEquals(new BigDecimal("123.4568"), result); // Rounded to 4 decimal places
  }

  @Test
  public void testGetZoneIdFromTimeZoneOpt_StandardTimeZones() {
    assertEquals(
        ZoneId.of("America/New_York"), getZoneIdFromTimeZoneOpt(Optional.of("America/New_York")));

    assertEquals(
        ZoneId.of("Europe/London"), getZoneIdFromTimeZoneOpt(Optional.of("Europe/London")));

    assertEquals(ZoneId.of("Asia/Kolkata"), getZoneIdFromTimeZoneOpt(Optional.of("Asia/Kolkata")));

    assertEquals(
        ZoneId.of("Australia/Sydney"), getZoneIdFromTimeZoneOpt(Optional.of("Australia/Sydney")));
  }

  @Test
  public void testGetZoneIdFromTimeZoneOpt_PositiveOffsets() {
    ZoneId expected = ZoneOffset.ofHoursMinutes(4, 30);
    assertEquals(expected, getZoneIdFromTimeZoneOpt(Optional.of("+4:30")));

    expected = ZoneOffset.ofHoursMinutes(1, 0);
    assertEquals(expected, getZoneIdFromTimeZoneOpt(Optional.of("+1:00")));

    expected = ZoneOffset.ofHoursMinutes(5, 45);
    assertEquals(expected, getZoneIdFromTimeZoneOpt(Optional.of("+5:45")));

    expected = ZoneOffset.ofHoursMinutes(12, 0);
    assertEquals(expected, getZoneIdFromTimeZoneOpt(Optional.of("+12:00")));
  }

  @Test
  public void testGetZoneIdFromTimeZoneOpt_NegativeOffsets() {
    ZoneId expected = ZoneOffset.ofHoursMinutes(-3, 0);
    assertEquals(expected, getZoneIdFromTimeZoneOpt(Optional.of("-3:00")));

    expected = ZoneOffset.ofHoursMinutes(-9, -30);
    assertEquals(expected, getZoneIdFromTimeZoneOpt(Optional.of("-9:30")));

    expected = ZoneOffset.ofHoursMinutes(-11, -45);
    assertEquals(expected, getZoneIdFromTimeZoneOpt(Optional.of("-11:45")));
  }

  @Test
  public void testGetZoneIdFromTimeZoneOpt_EmptyOptional() {
    assertEquals(ZoneId.systemDefault(), getZoneIdFromTimeZoneOpt(Optional.empty()));
  }

  @Test
  public void testGetZoneIdFromTimeZoneOpt_InvalidTimeZones() {
    assertThrows(
        DateTimeException.class, () -> getZoneIdFromTimeZoneOpt(Optional.of("Invalid/TimeZone")));

    assertThrows(
        DateTimeException.class,
        () -> getZoneIdFromTimeZoneOpt(Optional.of("+25:00"))); // Hours out of range
    assertThrows(
        DateTimeException.class,
        () -> getZoneIdFromTimeZoneOpt(Optional.of("+12:60"))); // Minutes out of range
    assertThrows(
        DateTimeException.class,
        () -> getZoneIdFromTimeZoneOpt(Optional.of("5:30"))); // Missing sign
  }
}
