// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

// Generates single-value-vectors.json: what Iceberg Java itself writes and accepts for the binary
// (Appendix D) and JSON single-value serializations. EngineeredWood's codecs are tested against it.
//
// Run with JDK 17+ and the Iceberg runtime jar the corpus is generated with:
//
//   java -cp iceberg-spark-runtime-4.1_2.13-1.12.0.jar SingleValueOracle.java > single-value-vectors.json
//
// Every case names an Iceberg type, as schema JSON spells it, and either a Java value (encode
// cases) or a JSON text (parse cases). Encode cases record Java's binary bytes and JSON text;
// parse cases record the binary of what Java parsed, or the error it raised.

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class SingleValueOracle {
  private static final List<String> out = new ArrayList<>();

  public static void main(String[] args) {
    // -- Encode: a Java value of each type --
    enc("boolean", true);
    enc("boolean", false);
    for (int v : new int[] {0, 1, -1, 34, Integer.MAX_VALUE, Integer.MIN_VALUE}) enc("int", v);
    for (long v : new long[] {0L, 1L, -1L, 34L, Long.MAX_VALUE, Long.MIN_VALUE}) enc("long", v);
    for (float v : new float[] {0f, -0f, 1f, -1.5f, 3.4028235e38f, 1.4e-45f, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NaN, 0.1f})
      enc("float", v);
    for (double v : new double[] {0d, -0d, 1d, -1.5d, Double.MAX_VALUE, Double.MIN_VALUE, Double.POSITIVE_INFINITY, Double.NaN, 0.1d, 1e300})
      enc("double", v);
    enc("decimal(9, 2)", new BigDecimal("14.20"));
    enc("decimal(9, 2)", new BigDecimal("-14.20"));
    enc("decimal(9, 2)", new BigDecimal("0.00"));
    enc("decimal(9, 2)", new BigDecimal("1.28"));
    enc("decimal(9, 2)", new BigDecimal("-1.28"));
    enc("decimal(9, 2)", new BigDecimal("1.27"));
    enc("decimal(9, 2)", new BigDecimal("-1.29"));
    enc("decimal(9, 0)", new BigDecimal("255"));
    enc("decimal(9, 0)", new BigDecimal("-256"));
    enc("decimal(38, 10)", new BigDecimal("9999999999999999999999999999.9999999999"));
    enc("decimal(38, 10)", new BigDecimal("-9999999999999999999999999999.9999999999"));
    enc("decimal(38, 0)", new BigDecimal("1E+20").setScale(0));
    for (int v : new int[] {0, 1, -1, 17486, -719162, 2932896}) enc("date", v);
    for (long v : new long[] {0L, 1L, 81068123456L, 86399999999L, 60000000L, 100000L}) enc("time", v);
    for (long v : new long[] {0L, 1L, -1L, 1510871468123456L, -62135596800000000L, 253402300799999999L, 1510871468000000L, 1510871468100000L})
      enc("timestamp", v);
    for (long v : new long[] {0L, 1L, -1L, 1510871468123456L}) enc("timestamptz", v);
    for (long v : new long[] {0L, 1L, -1L, 1510871468123456789L, Long.MAX_VALUE, Long.MIN_VALUE, 1510871468000000000L})
      enc("timestamp_ns", v);
    for (long v : new long[] {0L, -1L, 1510871468123456789L}) enc("timestamptz_ns", v);
    enc("string", "");
    enc("string", "iceberg");
    enc("string", new String(new int[] {0x63, 0x61, 0x66, 0xe9, 0x20, 0x6c34, 0x20, 0x1f9ca}, 0, 8));
    enc("uuid", UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7"));
    enc("uuid", UUID.fromString("00000000-0000-0000-0000-000000000000"));
    enc("uuid", UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"));
    enc("fixed[4]", ByteBuffer.wrap(new byte[] {0, 1, 2, (byte) 0xff}));
    enc("binary", ByteBuffer.wrap(new byte[] {0, 1, 2, (byte) 0xff, (byte) 0xab}));
    enc("binary", ByteBuffer.wrap(new byte[0]));

    // -- Parse: JSON texts Java may or may not accept --
    parse("boolean", "true");
    parse("boolean", "\"true\"");
    parse("boolean", "1");
    parse("int", "34");
    parse("int", "34.0");
    parse("int", "\"34\"");
    parse("int", "2147483648");
    parse("long", "9223372036854775807");
    parse("long", "9223372036854775808");
    parse("long", "1e3");
    parse("float", "1");
    parse("float", "1.0");
    parse("float", "1e40");
    parse("float", "\"NaN\"");
    parse("double", "1");
    parse("double", "-0.0");
    parse("double", "\"NaN\"");
    parse("double", "\"Infinity\"");
    parse("decimal(9, 2)", "\"14.20\"");
    parse("decimal(9, 2)", "\"14.2\"");
    parse("decimal(9, 2)", "\"14.200\"");
    parse("decimal(9, 2)", "14.20");
    parse("decimal(9, 2)", "\"1420E-2\"");
    parse("decimal(9, 2)", "\"12345678.90\"");
    parse("decimal(9, 2)", "\"-0.00\"");
    parse("decimal(9, 2)", "\"+1.00\"");
    parse("decimal(38, 0)", "\"2E+20\"");
    parse("decimal(38, 0)", "\"200000000000000000000\"");
    parse("date", "\"2017-11-16\"");
    parse("date", "\"+10000-01-01\"");
    parse("date", "\"2017-11-16T00:00:00\"");
    parse("date", "\"2017-02-29\"");
    parse("date", "\"17-11-16\"");
    parse("time", "\"22:31:08.123456\"");
    parse("time", "\"22:31:08\"");
    parse("time", "\"22:31\"");
    parse("time", "\"22:31:08.1\"");
    parse("time", "\"22:31:08.123456789\"");
    parse("time", "\"24:00:00\"");
    parse("timestamp", "\"2017-11-16T22:31:08.123456\"");
    parse("timestamp", "\"2017-11-16T22:31:08\"");
    parse("timestamp", "\"2017-11-16T22:31\"");
    parse("timestamp", "\"2017-11-16T22:31:08.123456789\"");
    parse("timestamp", "\"2017-11-16T22:31:08.123456+00:00\"");
    parse("timestamp", "\"2017-11-16 22:31:08.123456\"");
    parse("timestamp", "\"1969-12-31T23:59:59.9999999\"");
    parse("timestamptz", "\"2017-11-16T22:31:08.123456+00:00\"");
    parse("timestamptz", "\"2017-11-16T22:31:08.123456Z\"");
    parse("timestamptz", "\"2017-11-16T22:31:08.123456-00:00\"");
    parse("timestamptz", "\"2017-11-16T22:31:08.123456+01:00\"");
    parse("timestamptz", "\"2017-11-16T22:31:08.123456\"");
    parse("timestamp_ns", "\"2017-11-16T22:31:08.123456789\"");
    parse("timestamp_ns", "\"2017-11-16T22:31:08.1\"");
    parse("timestamp_ns", "\"2017-11-16T22:31:08.123456789+00:00\"");
    parse("timestamp_ns", "\"1677-09-21T00:12:43.145224192\"");
    parse("timestamp_ns", "\"1677-09-21T00:12:43.145224191\"");
    parse("timestamp_ns", "\"2262-04-11T23:47:16.854775807\"");
    parse("timestamp_ns", "\"2262-04-11T23:47:16.854775808\"");
    parse("timestamptz_ns", "\"2017-11-16T22:31:08.123456789+00:00\"");
    parse("timestamptz_ns", "\"2017-11-16T22:31:08.123456789Z\"");
    parse("timestamptz_ns", "\"2017-11-16T22:31:08.123456789\"");
    parse("string", "\"iceberg\"");
    parse("string", "34");
    parse("uuid", "\"f79c3e09-677c-4bbd-a479-3f349cb785e7\"");
    parse("uuid", "\"F79C3E09-677C-4BBD-A479-3F349CB785E7\"");
    parse("uuid", "\"f79c3e09677c4bbda4793f349cb785e7\"");
    parse("fixed[4]", "\"000102ff\"");
    parse("fixed[4]", "\"000102FF\"");
    parse("fixed[4]", "\"000102\"");
    parse("fixed[4]", "\"000102ff00\"");
    parse("binary", "\"000102ffab\"");
    parse("binary", "\"\"");
    parse("binary", "\"abc\"");
    parse("binary", "\"zz\"");

    System.out.println("{");
    System.out.println("  \"generator\": \"SingleValueOracle.java\",");
    System.out.println("  \"iceberg_version\": \"" + org.apache.iceberg.IcebergBuild.version() + "\",");
    System.out.println("  \"java_version\": \"" + System.getProperty("java.version") + "\",");
    System.out.println("  \"cases\": [");
    for (int i = 0; i < out.size(); i++)
      System.out.println("    " + out.get(i) + (i + 1 < out.size() ? "," : ""));
    System.out.println("  ]");
    System.out.println("}");
  }

  private static Type type(String name) {
    return Types.fromPrimitiveString(name);
  }

  private static void enc(String typeName, Object value) {
    Type t = type(typeName);
    String json;
    String jsonError = null;
    try {
      json = SingleValueParser.toJson(t, value);
    } catch (RuntimeException e) {
      json = null;
      jsonError = e.getClass().getSimpleName() + ": " + e.getMessage();
    }
    String bin = hex(Conversions.toByteBuffer(t, value));
    StringBuilder sb = new StringBuilder();
    sb.append("{\"op\": \"encode\", \"type\": ").append(q(typeName));
    sb.append(", \"java_value\": ").append(q(String.valueOf(value instanceof ByteBuffer ? hex((ByteBuffer) value) : value)));
    sb.append(", \"binary\": ").append(q(bin));
    if (json != null) sb.append(", \"json\": ").append(q(json));
    if (jsonError != null) sb.append(", \"json_error\": ").append(q(jsonError));
    // Java's own round trip of the binary, so a reader can check it decodes to the same thing.
    Object back = Conversions.fromByteBuffer(t, Conversions.toByteBuffer(t, value));
    sb.append(", \"binary_round_trips\": ").append(String.valueOf(equalValues(value, back)));
    sb.append("}");
    out.add(sb.toString());
  }

  private static void parse(String typeName, String jsonText) {
    Type t = type(typeName);
    StringBuilder sb = new StringBuilder();
    sb.append("{\"op\": \"parse\", \"type\": ").append(q(typeName)).append(", \"json\": ").append(q(jsonText));
    try {
      Object v = SingleValueParser.fromJson(t, jsonText);
      sb.append(", \"binary\": ").append(q(hex(Conversions.toByteBuffer(t, v))));
      sb.append(", \"rewritten\": ").append(q(SingleValueParser.toJson(t, v)));
    } catch (RuntimeException e) {
      sb.append(", \"error\": ").append(q(e.getClass().getSimpleName() + ": " + e.getMessage()));
    }
    sb.append("}");
    out.add(sb.toString());
  }

  private static boolean equalValues(Object a, Object b) {
    if (a instanceof Float && b instanceof Float) return Float.compare((Float) a, (Float) b) == 0;
    if (a instanceof Double && b instanceof Double) return Double.compare((Double) a, (Double) b) == 0;
    if (a instanceof CharSequence && b instanceof CharSequence) return a.toString().equals(b.toString());
    return a.equals(b);
  }

  private static String hex(ByteBuffer buffer) {
    ByteBuffer b = buffer.duplicate();
    StringBuilder sb = new StringBuilder();
    while (b.hasRemaining()) sb.append(String.format("%02x", b.get() & 0xff));
    return sb.toString();
  }

  private static String q(String s) {
    StringBuilder sb = new StringBuilder("\"");
    for (char c : s.toCharArray()) {
      if (c == '"' || c == '\\') sb.append('\\').append(c);
      else if (c < 0x20 || c > 0x7e) sb.append(String.format("\\u%04x", (int) c));
      else sb.append(c);
    }
    return sb.append('"').toString();
  }
}
