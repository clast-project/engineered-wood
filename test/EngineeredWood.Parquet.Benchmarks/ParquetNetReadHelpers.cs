// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER
using Parquet;
using Parquet.Schema;

namespace EngineeredWood.Benchmarks;

internal static class ParquetNetReadHelpers
{
    public static async Task DrainColumnV6Async(
        ParquetRowGroupReader rg, DataField field, int rowCount)
    {
        Type t = field.ClrType;
        bool nullable = field.IsNullable;

        if (t == typeof(int))
        {
            if (nullable) { var b = new int?[rowCount]; await rg.ReadAsync<int>(field, b.AsMemory()).ConfigureAwait(false); _ = b.Length; }
            else { var b = new int[rowCount]; await rg.ReadAsync<int>(field, b.AsMemory()).ConfigureAwait(false); _ = b.Length; }
        }
        else if (t == typeof(long))
        {
            if (nullable) { var b = new long?[rowCount]; await rg.ReadAsync<long>(field, b.AsMemory()).ConfigureAwait(false); _ = b.Length; }
            else { var b = new long[rowCount]; await rg.ReadAsync<long>(field, b.AsMemory()).ConfigureAwait(false); _ = b.Length; }
        }
        else if (t == typeof(float))
        {
            if (nullable) { var b = new float?[rowCount]; await rg.ReadAsync<float>(field, b.AsMemory()).ConfigureAwait(false); _ = b.Length; }
            else { var b = new float[rowCount]; await rg.ReadAsync<float>(field, b.AsMemory()).ConfigureAwait(false); _ = b.Length; }
        }
        else if (t == typeof(double))
        {
            if (nullable) { var b = new double?[rowCount]; await rg.ReadAsync<double>(field, b.AsMemory()).ConfigureAwait(false); _ = b.Length; }
            else { var b = new double[rowCount]; await rg.ReadAsync<double>(field, b.AsMemory()).ConfigureAwait(false); _ = b.Length; }
        }
        else if (t == typeof(bool))
        {
            if (nullable) { var b = new bool?[rowCount]; await rg.ReadAsync<bool>(field, b.AsMemory()).ConfigureAwait(false); _ = b.Length; }
            else { var b = new bool[rowCount]; await rg.ReadAsync<bool>(field, b.AsMemory()).ConfigureAwait(false); _ = b.Length; }
        }
        else if (t == typeof(string) || t == typeof(byte[]))
        {
            // Parquet.Net 6 has no byte[] read overload; binary columns must be read as UTF-8 strings.
            // Comparable to ParquetSharp's byte[] decode for benchmarking purposes.
            var b = new string[rowCount];
            await rg.ReadAsync(field, b.AsMemory()!).ConfigureAwait(false);
            _ = b.Length;
        }
        else if (t == typeof(DateTime))
        {
            if (nullable) { var b = new DateTime?[rowCount]; await rg.ReadAsync<DateTime>(field, b.AsMemory()).ConfigureAwait(false); _ = b.Length; }
            else { var b = new DateTime[rowCount]; await rg.ReadAsync<DateTime>(field, b.AsMemory()).ConfigureAwait(false); _ = b.Length; }
        }
        else if (t == typeof(DateTimeOffset))
        {
            if (nullable) { var b = new DateTimeOffset?[rowCount]; await rg.ReadAsync<DateTimeOffset>(field, b.AsMemory()).ConfigureAwait(false); _ = b.Length; }
            else { var b = new DateTimeOffset[rowCount]; await rg.ReadAsync<DateTimeOffset>(field, b.AsMemory()).ConfigureAwait(false); _ = b.Length; }
        }
        else if (t == typeof(decimal))
        {
            if (nullable) { var b = new decimal?[rowCount]; await rg.ReadAsync<decimal>(field, b.AsMemory()).ConfigureAwait(false); _ = b.Length; }
            else { var b = new decimal[rowCount]; await rg.ReadAsync<decimal>(field, b.AsMemory()).ConfigureAwait(false); _ = b.Length; }
        }
        else
        {
            throw new NotSupportedException(
                $"Parquet.Net 6 read benchmark does not support field {field.Name} of type {t}.");
        }
    }

    /// <summary>
    /// Reads a column and returns its data as an <see cref="Array"/>, in the densely-scattered
    /// shape that Parquet.Net 5's <c>DataColumn.Data</c> exposed for cross-reader value comparison.
    /// Returns null for types not handled by the comparison harness.
    /// </summary>
    public static async Task<Array?> ReadColumnAsArrayV6Async(
        ParquetRowGroupReader rg, DataField field, int rowCount)
    {
        Type t = field.ClrType;
        bool nullable = field.IsNullable;

        if (t == typeof(int))
        {
            if (nullable) { var b = new int?[rowCount]; await rg.ReadAsync<int>(field, b.AsMemory()).ConfigureAwait(false); return b; }
            { var b = new int[rowCount]; await rg.ReadAsync<int>(field, b.AsMemory()).ConfigureAwait(false); return b; }
        }
        if (t == typeof(long))
        {
            if (nullable) { var b = new long?[rowCount]; await rg.ReadAsync<long>(field, b.AsMemory()).ConfigureAwait(false); return b; }
            { var b = new long[rowCount]; await rg.ReadAsync<long>(field, b.AsMemory()).ConfigureAwait(false); return b; }
        }
        if (t == typeof(float))
        {
            if (nullable) { var b = new float?[rowCount]; await rg.ReadAsync<float>(field, b.AsMemory()).ConfigureAwait(false); return b; }
            { var b = new float[rowCount]; await rg.ReadAsync<float>(field, b.AsMemory()).ConfigureAwait(false); return b; }
        }
        if (t == typeof(double))
        {
            if (nullable) { var b = new double?[rowCount]; await rg.ReadAsync<double>(field, b.AsMemory()).ConfigureAwait(false); return b; }
            { var b = new double[rowCount]; await rg.ReadAsync<double>(field, b.AsMemory()).ConfigureAwait(false); return b; }
        }
        if (t == typeof(bool))
        {
            if (nullable) { var b = new bool?[rowCount]; await rg.ReadAsync<bool>(field, b.AsMemory()).ConfigureAwait(false); return b; }
            { var b = new bool[rowCount]; await rg.ReadAsync<bool>(field, b.AsMemory()).ConfigureAwait(false); return b; }
        }
        if (t == typeof(DateTime))
        {
            if (nullable) { var b = new DateTime?[rowCount]; await rg.ReadAsync<DateTime>(field, b.AsMemory()).ConfigureAwait(false); return b; }
            { var b = new DateTime[rowCount]; await rg.ReadAsync<DateTime>(field, b.AsMemory()).ConfigureAwait(false); return b; }
        }
        if (t == typeof(DateTimeOffset))
        {
            if (nullable) { var b = new DateTimeOffset?[rowCount]; await rg.ReadAsync<DateTimeOffset>(field, b.AsMemory()).ConfigureAwait(false); return b; }
            { var b = new DateTimeOffset[rowCount]; await rg.ReadAsync<DateTimeOffset>(field, b.AsMemory()).ConfigureAwait(false); return b; }
        }
        return null; // string / byte[] / unsupported — comparison harness skips these
    }
}
#endif
