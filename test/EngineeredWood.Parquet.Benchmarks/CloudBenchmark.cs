// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Azure.Storage.Blobs;
using EngineeredWood.IO;
using EngineeredWood.IO.Azure;
using EngineeredWood.Parquet;

namespace EngineeredWood.Benchmarks;

/// <summary>
/// Interactive cloud benchmark comparing EngineeredWood, ParquetSharp, and Parquet.Net
/// reading from Azure Blob Storage.
/// </summary>
public static class CloudBenchmark
{
    public static async Task RunAsync()
    {
        Console.WriteLine("=== Cloud Parquet Read Benchmark ===\n");

        Console.Write("Azure Blob URL (e.g. https://account.blob.core.windows.net/container/file.parquet): ");
        string blobUrl = Console.ReadLine()?.Trim() ?? "";
        if (string.IsNullOrEmpty(blobUrl))
        {
            Console.WriteLine("No URL provided. Exiting.");
            return;
        }

        Console.Write("Account key (leave blank for anonymous access): ");
        string accountKey = Console.ReadLine()?.Trim() ?? "";

        var blobUri = new Uri(blobUrl);
        BlobClient blobClient;

        if (!string.IsNullOrEmpty(accountKey))
        {
            // Extract account name from URL
            string accountName = blobUri.Host.Split('.')[0];
            var credential = new Azure.Storage.StorageSharedKeyCredential(accountName, accountKey);
            blobClient = new BlobClient(blobUri, credential);
        }
        else
        {
            blobClient = new BlobClient(blobUri);
        }

        // Verify connectivity and get file size
        Console.Write("\nConnecting... ");
        long fileSize;
        try
        {
            var props = await blobClient.GetPropertiesAsync();
            fileSize = props.Value.ContentLength;
            Console.WriteLine($"OK ({fileSize / 1024.0 / 1024.0:F1} MB)");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"FAILED: {ex.Message}");
            return;
        }

        // Discover row groups via EngineeredWood (first read also warms DNS/TLS)
        Console.Write("Reading metadata... ");
        int rowGroupCount;
        int totalColumns;
        long firstRowGroupRows;
        using (var ewFile = new AzureBlobRandomAccessFile(blobClient, fileSize))
        using (var reader = new ParquetFileReader(ewFile))
        {
            var meta = await reader.ReadMetadataAsync();
            rowGroupCount = meta.RowGroups.Count;
            totalColumns = meta.RowGroups[0].Columns.Count;
            firstRowGroupRows = meta.RowGroups[0].NumRows;
        }
        Console.WriteLine($"{rowGroupCount} row group(s), {totalColumns} column(s), " +
                          $"{firstRowGroupRows:N0} rows in RG0");

        Console.Write("\nRow group index to read [0]: ");
        string rgInput = Console.ReadLine()?.Trim() ?? "";
        int rowGroupIndex = string.IsNullOrEmpty(rgInput) ? 0 : int.Parse(rgInput);

        Console.Write("Number of iterations [3]: ");
        string iterInput = Console.ReadLine()?.Trim() ?? "";
        int iterations = string.IsNullOrEmpty(iterInput) ? 3 : int.Parse(iterInput);

        Console.WriteLine($"\nBenchmarking row group {rowGroupIndex} × {iterations} iterations...\n");
        Console.WriteLine($"{"Method",-52} {"Mean",10} {"Min",10} {"Max",10}");
        Console.WriteLine(new string('-', 84));

        // --- EngineeredWood ---
        await RunEW(blobClient, fileSize, rowGroupIndex, iterations);

        // --- ParquetSharp (via Azure BlobClient.OpenRead stream) ---
        await RunParquetSharp(blobClient, rowGroupIndex, iterations);

        // --- Parquet.Net (via Azure BlobClient.OpenRead stream) ---
        try
        {
            await RunParquetNet(blobClient, rowGroupIndex, iterations);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"{"Parquet.Net",-52} FAILED: {ex.GetType().Name}: {ex.Message}");
        }

        Console.WriteLine();

        // --- Optional cross-reader validation ---
        Console.Write("Validate values across readers? [y/N]: ");
        string validateInput = Console.ReadLine()?.Trim() ?? "";
        if (validateInput.Equals("y", StringComparison.OrdinalIgnoreCase))
        {
            await ValidateAcrossReaders(blobClient, fileSize, rowGroupIndex);
        }
    }

    private static async Task RunEW(
        BlobClient blobClient, long fileSize,
        int rowGroupIndex, int iterations)
    {
        var times = new double[iterations];
        var sw = new Stopwatch();

        for (int i = 0; i < iterations; i++)
        {
            sw.Restart();
            await using IRandomAccessFile file = new AzureBlobRandomAccessFile(blobClient, fileSize);
            using (var reader = new ParquetFileReader(file))
            {
                using var batch = await reader.ReadRowGroupAsync(rowGroupIndex);
            }
            sw.Stop();
            times[i] = sw.Elapsed.TotalMilliseconds;
            Console.Write(".");
        }

        PrintResult("EngineeredWood", times);
    }

    private static async Task RunParquetSharp(
        BlobClient blobClient, int rowGroupIndex, int iterations)
    {
        var times = new double[iterations];
        var sw = new Stopwatch();

        for (int i = 0; i < iterations; i++)
        {
            sw.Restart();
#if NET8_0_OR_GREATER
            await using var stream = await blobClient.OpenReadAsync(
                new Azure.Storage.Blobs.Models.BlobOpenReadOptions(allowModifications: false));
#else
            using var stream = await blobClient.OpenReadAsync(
                new Azure.Storage.Blobs.Models.BlobOpenReadOptions(allowModifications: false));
#endif

            using var reader = new ParquetSharp.ParquetFileReader(stream, leaveOpen: false);
            using var rowGroup = reader.RowGroup(rowGroupIndex);
            int numColumns = rowGroup.MetaData.NumColumns;
            long numRows = rowGroup.MetaData.NumRows;

            for (int c = 0; c < numColumns; c++)
            {
                using var col = rowGroup.Column(c);
                ReadParquetSharpColumn(col, numRows);
            }

            sw.Stop();
            times[i] = sw.Elapsed.TotalMilliseconds;
            Console.Write(".");
        }

        PrintResult("ParquetSharp (stream)", times);
    }

    private static void ReadParquetSharpColumn(
        ParquetSharp.ColumnReader col, long numRows)
    {
        // Use the non-generic LogicalReader so ParquetSharp resolves the correct
        // element type (e.g. DateTimeNanos for TIMESTAMP columns) automatically.
        using var logicalReader = col.LogicalReader();
        logicalReader.Apply(new DrainVisitor(numRows));
    }

    /// <summary>
    /// Visitor that reads all values from a ParquetSharp LogicalColumnReader
    /// regardless of the element type. This avoids InvalidCastException when
    /// the column's logical type maps to a ParquetSharp-specific CLR type
    /// (e.g. DateTimeNanos, TimeSpanNanos).
    /// </summary>
    private sealed class DrainVisitor : ParquetSharp.ILogicalColumnReaderVisitor<bool>
    {
        private readonly long _numRows;
        public DrainVisitor(long numRows) => _numRows = numRows;

        public bool OnLogicalColumnReader<TElement>(
            ParquetSharp.LogicalColumnReader<TElement> columnReader)
        {
            var buffer = new TElement[_numRows];
            columnReader.ReadBatch(buffer);
            return true;
        }
    }

    private static async Task RunParquetNet(
        BlobClient blobClient, int rowGroupIndex, int iterations)
    {
        var times = new double[iterations];
        var sw = new Stopwatch();

        for (int i = 0; i < iterations; i++)
        {
            sw.Restart();
#if NET8_0_OR_GREATER
            await using var stream = await blobClient.OpenReadAsync(
                new Azure.Storage.Blobs.Models.BlobOpenReadOptions(allowModifications: false));
#else
            using var stream = await blobClient.OpenReadAsync(
                new Azure.Storage.Blobs.Models.BlobOpenReadOptions(allowModifications: false));
#endif

#if NET8_0_OR_GREATER
            await using var reader = await global::Parquet.ParquetReader.CreateAsync(stream);
            using var rg = reader.OpenRowGroupReader(rowGroupIndex);
            int rowCount = checked((int)reader.RowGroups[rowGroupIndex].RowCount);
            foreach (var field in reader.Schema.GetDataFields())
                await ParquetNetReadHelpers.DrainColumnV6Async(rg, field, rowCount);
#else
            using var reader = await global::Parquet.ParquetReader.CreateAsync(stream);
            using var rg = reader.OpenRowGroupReader(rowGroupIndex);
            foreach (var field in reader.Schema.GetDataFields())
                await rg.ReadColumnAsync(field);
#endif

            sw.Stop();
            times[i] = sw.Elapsed.TotalMilliseconds;
            Console.Write(".");
        }

        PrintResult("Parquet.Net (stream)", times);
    }

    private static void PrintResult(string label, double[] times)
    {
        System.Array.Sort(times);
        double mean = times.Average();
        double min = times[0];
        double max = times[^1];

        Console.WriteLine();
        Console.WriteLine($"{label,-52} {mean,8:F0} ms {min,8:F0} ms {max,8:F0} ms");
    }

    // ---------------------------------------------------------------
    // Cross-reader validation
    // ---------------------------------------------------------------

    private static async Task ValidateAcrossReaders(
        BlobClient blobClient, long fileSize, int rowGroupIndex)
    {
        Console.WriteLine("\nReading via EngineeredWood...");
        RecordBatch ewBatch;
        {
            await using IRandomAccessFile file = new AzureBlobRandomAccessFile(blobClient, fileSize);
            using var reader = new ParquetFileReader(file);
            ewBatch = await reader.ReadRowGroupAsync(rowGroupIndex);
        }

        Console.WriteLine("Reading via Parquet.Net...");
        PnReadResult[] pnetColumns;
        {
#if NET8_0_OR_GREATER
            await using var stream = await blobClient.OpenReadAsync(
                new Azure.Storage.Blobs.Models.BlobOpenReadOptions(allowModifications: false));
#else
            using var stream = await blobClient.OpenReadAsync(
                new Azure.Storage.Blobs.Models.BlobOpenReadOptions(allowModifications: false));
#endif
#if NET8_0_OR_GREATER
            await using var reader = await global::Parquet.ParquetReader.CreateAsync(stream);
#else
            using var reader = await global::Parquet.ParquetReader.CreateAsync(stream);
#endif
            using var rg = reader.OpenRowGroupReader(rowGroupIndex);
            var fields = reader.Schema.GetDataFields();
            pnetColumns = new PnReadResult[fields.Length];
            int pnRowCount = checked((int)reader.RowGroups[rowGroupIndex].RowCount);
            for (int i = 0; i < fields.Length; i++)
            {
#if NET8_0_OR_GREATER
                var arr = await ParquetNetReadHelpers.ReadColumnAsArrayV6Async(rg, fields[i], pnRowCount);
                pnetColumns[i] = arr is null
                    ? new PnReadResult(System.Array.Empty<int>(), null)
                    : new PnReadResult(arr, null);
#else
                var col = await rg.ReadColumnAsync(fields[i]);
                pnetColumns[i] = new PnReadResult(col.Data, col.DefinitionLevels);
#endif
            }
        }

        Console.WriteLine("Reading via ParquetSharp...");
        System.Array[] psColumns;
        string[] psTypeNames;
        {
#if NET8_0_OR_GREATER
            await using var stream = await blobClient.OpenReadAsync(
                new Azure.Storage.Blobs.Models.BlobOpenReadOptions(allowModifications: false));
#else
            using var stream = await blobClient.OpenReadAsync(
                new Azure.Storage.Blobs.Models.BlobOpenReadOptions(allowModifications: false));
#endif
            using var reader = new ParquetSharp.ParquetFileReader(stream, leaveOpen: false);
            using var rowGroup = reader.RowGroup(rowGroupIndex);
            int numCols = rowGroup.MetaData.NumColumns;
            psColumns = new System.Array[numCols];
            psTypeNames = new string[numCols];
            for (int c = 0; c < numCols; c++)
            {
                using var col = rowGroup.Column(c);
                using var logicalReader = col.LogicalReader();
                var (arr, typeName) = logicalReader.Apply(
                    new CaptureVisitor(rowGroup.MetaData.NumRows));
                psColumns[c] = arr;
                psTypeNames[c] = typeName;
            }
        }

        int numColumns = Math.Min(ewBatch.ColumnCount, pnetColumns.Length);
        numColumns = Math.Min(numColumns, psColumns.Length);
        int rowCount = ewBatch.Length;

        Console.WriteLine($"\nComparing {numColumns} columns, {rowCount} rows...\n");

        int passed = 0, skipped = 0, failed = 0;

        for (int c = 0; c < numColumns; c++)
        {
            var ewCol = ewBatch.Column(c);
            var pnetCol = pnetColumns[c];
            var psArr = psColumns[c];
            string colName = ewBatch.Schema.GetFieldByIndex(c).Name;

            var result = CompareColumn(ewCol, pnetCol, psArr, psTypeNames[c], rowCount, colName);
            switch (result)
            {
                case CompareResult.Pass: passed++; break;
                case CompareResult.Skip: skipped++; break;
                case CompareResult.Fail: failed++; break;
            }
        }

        Console.WriteLine($"\n  Passed: {passed}, Skipped: {skipped}, Failed: {failed}");

        ewBatch.Dispose();
    }

    private enum CompareResult { Pass, Skip, Fail }

    private static CompareResult CompareColumn(
        IArrowArray ewCol, PnReadResult pnetCol,
        System.Array psArr, string psTypeName, int rowCount, string colName)
    {
        // Extract EW values as object?[] for scalar types
        object?[]? ewValues = ExtractArrowValues(ewCol, rowCount);
        if (ewValues == null)
        {
            Console.WriteLine($"  [{colName}] SKIP (non-scalar Arrow type: {ewCol.Data.DataType})");
            return CompareResult.Skip;
        }

        // Extract Parquet.Net values
        object?[]? pnetValues = ExtractParquetNetValues(pnetCol, rowCount);
        if (pnetValues == null)
        {
            Console.WriteLine($"  [{colName}] SKIP (non-scalar Parquet.Net type: {pnetCol.Data.GetType().Name})");
            return CompareResult.Skip;
        }

        // Extract ParquetSharp values
        object?[]? psValues = ExtractParquetSharpValues(psArr, psTypeName, rowCount);
        if (psValues == null)
        {
            Console.WriteLine($"  [{colName}] SKIP (non-scalar ParquetSharp type: {psTypeName})");
            return CompareResult.Skip;
        }

        // Compare EW vs Parquet.Net
        int ewVsPnet = CompareArrays(ewValues, pnetValues, rowCount);
        int ewVsPs = CompareArrays(ewValues, psValues, rowCount);

        if (ewVsPnet == 0 && ewVsPs == 0)
        {
            Console.WriteLine($"  [{colName}] PASS");
            return CompareResult.Pass;
        }
        else
        {
            Console.WriteLine($"  [{colName}] FAIL — mismatches: EW↔Parquet.Net={ewVsPnet}, EW↔ParquetSharp={ewVsPs}");

            // Show first mismatch detail
            if (ewVsPnet > 0)
                ShowFirstMismatch(ewValues, pnetValues, rowCount, "EW", "Parquet.Net");
            if (ewVsPs > 0)
                ShowFirstMismatch(ewValues, psValues, rowCount, "EW", "ParquetSharp");

            return CompareResult.Fail;
        }
    }

    private static int CompareArrays(object?[] a, object?[] b, int count)
    {
        int mismatches = 0;
        for (int i = 0; i < count; i++)
        {
            if (!ValuesEqual(a[i], b[i]))
                mismatches++;
        }
        return mismatches;
    }

    private static bool ValuesEqual(object? a, object? b)
    {
        if (a is null && b is null) return true;
        if (a is null || b is null) return false;
        if (a.Equals(b)) return true;
        // Handle float/double near-equality
        if (a is double da && b is double db)
            return Math.Abs(da - db) < 1e-10 || da.Equals(db);
        if (a is float fa && b is float fb)
            return Math.Abs(fa - fb) < 1e-6 || fa.Equals(fb);
        // Handle cross-type temporal comparisons (long µs ↔ DateTime/DateTimeOffset)
        if (IsTemporal(a) || IsTemporal(b))
        {
            long? aUs = ToEpochMicroseconds(a);
            long? bUs = ToEpochMicroseconds(b);
            if (aUs.HasValue && bUs.HasValue)
                return aUs.Value == bUs.Value;
        }
        return false;
    }

    private static bool IsTemporal(object? value) =>
        value is DateTime or DateTimeOffset;

    private static readonly DateTimeOffset Epoch = new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static long? ToEpochMicroseconds(object? value) => value switch
    {
        DateTimeOffset dto => (dto - Epoch).Ticks / (TimeSpan.TicksPerMillisecond / 1000),
        DateTime dt => (new DateTimeOffset(dt, TimeSpan.Zero) - Epoch).Ticks
                       / (TimeSpan.TicksPerMillisecond / 1000),
        long l => l, // Arrow TimestampArray stores epoch µs as long
        _ => null,
    };

    private static void ShowFirstMismatch(
        object?[] a, object?[] b, int count, string nameA, string nameB)
    {
        for (int i = 0; i < count; i++)
        {
            if (!ValuesEqual(a[i], b[i]))
            {
                Console.WriteLine($"    Row {i}: {nameA}={a[i] ?? "null"}, {nameB}={b[i] ?? "null"}");
                return;
            }
        }
    }

    // --- Value extraction helpers ---

    private static object?[]? ExtractArrowValues(IArrowArray col, int count)
    {
        return col switch
        {
            Int32Array a => ExtractPrimitive(a, count, i => a.IsNull(i) ? null : (object?)a.GetValue(i)),
            Int64Array a => ExtractPrimitive(a, count, i => a.IsNull(i) ? null : (object?)a.GetValue(i)),
            FloatArray a => ExtractPrimitive(a, count, i => a.IsNull(i) ? null : (object?)a.GetValue(i)),
            DoubleArray a => ExtractPrimitive(a, count, i => a.IsNull(i) ? null : (object?)a.GetValue(i)),
            BooleanArray a => ExtractPrimitive(a, count, i => a.IsNull(i) ? null : (object?)a.GetValue(i)),
            Int8Array a => ExtractPrimitive(a, count, i => a.IsNull(i) ? null : (object?)a.GetValue(i)),
            Int16Array a => ExtractPrimitive(a, count, i => a.IsNull(i) ? null : (object?)a.GetValue(i)),
            UInt8Array a => ExtractPrimitive(a, count, i => a.IsNull(i) ? null : (object?)a.GetValue(i)),
            UInt16Array a => ExtractPrimitive(a, count, i => a.IsNull(i) ? null : (object?)a.GetValue(i)),
            UInt32Array a => ExtractPrimitive(a, count, i => a.IsNull(i) ? null : (object?)a.GetValue(i)),
            UInt64Array a => ExtractPrimitive(a, count, i => a.IsNull(i) ? null : (object?)a.GetValue(i)),
            Date32Array a => ExtractPrimitive(a, count, i => a.IsNull(i) ? null : (object?)a.GetValue(i)),
            TimestampArray a => ExtractPrimitive(a, count, i => a.IsNull(i) ? null : (object?)a.GetValue(i)),
            _ => null, // non-scalar (string, binary, struct, list, etc.)
        };
    }

    private static object?[] ExtractPrimitive<T>(T _, int count, Func<int, object?> getter)
    {
        var values = new object?[count];
        for (int i = 0; i < count; i++)
            values[i] = getter(i);
        return values;
    }

    private static object?[]? ExtractParquetNetValues(
        PnReadResult col, int count)
    {
        var data = col.Data;
        if (data is int[] ints) return BoxArray(ints, col.DefinitionLevels, count);
        if (data is int?[] nints) return BoxNullable(nints, count);
        if (data is long[] longs) return BoxArray(longs, col.DefinitionLevels, count);
        if (data is long?[] nlongs) return BoxNullable(nlongs, count);
        if (data is float[] floats) return BoxArray(floats, col.DefinitionLevels, count);
        if (data is float?[] nfloats) return BoxNullable(nfloats, count);
        if (data is double[] doubles) return BoxArray(doubles, col.DefinitionLevels, count);
        if (data is double?[] ndoubles) return BoxNullable(ndoubles, count);
        if (data is bool[] bools) return BoxArray(bools, col.DefinitionLevels, count);
        if (data is bool?[] nbools) return BoxNullable(nbools, count);
        if (data is DateTime[] dts) return BoxArray(dts, col.DefinitionLevels, count);
        if (data is DateTime?[] ndts) return BoxNullable(ndts, count);
        if (data is DateTimeOffset[] dtos) return BoxArray(dtos, col.DefinitionLevels, count);
        if (data is DateTimeOffset?[] ndtos) return BoxNullable(ndtos, count);
        return null;
    }

    /// <summary>
    /// Unified shape for Parquet.Net read results across v5 (DataColumn) and v6 (typed ReadAsync).
    /// On v6, nullable columns are scattered into <c>T?[]</c> directly so DefinitionLevels is null.
    /// On v5, nullable required columns return densely-packed <c>T[]</c> plus def levels.
    /// </summary>
    private sealed class PnReadResult
    {
        public PnReadResult(System.Array data, int[]? defLevels)
        {
            Data = data;
            DefinitionLevels = defLevels;
        }
        public System.Array Data { get; }
        public int[]? DefinitionLevels { get; }
    }

    private static object?[] BoxArray<T>(T[] data, int[]? defLevels, int count)
        where T : struct
    {
        var result = new object?[count];
        // If no def levels, all values are present
        if (defLevels == null)
        {
            for (int i = 0; i < count; i++)
                result[i] = i < data.Length ? data[i] : null;
        }
        else
        {
            // Parquet.Net 5 packs non-null values densely; use defLevel to scatter
            int valueIdx = 0;
            for (int i = 0; i < count; i++)
            {
                if (defLevels[i] > 0 && valueIdx < data.Length)
                    result[i] = data[valueIdx++];
                // else null
            }
        }
        return result;
    }

    private static object?[] BoxNullable<T>(T?[] data, int count) where T : struct
    {
        var result = new object?[count];
        for (int i = 0; i < count && i < data.Length; i++)
            result[i] = data[i].HasValue ? (object?)data[i]!.Value : null;
        return result;
    }

    private static object?[]? ExtractParquetSharpValues(
        System.Array arr, string typeName, int count)
    {
        // ParquetSharp returns typed arrays via the visitor. Map common types.
        if (arr is int[] ints) return ints.Take(count).Select(x => (object?)x).ToArray();
        if (arr is int?[] nints) return nints.Take(count).Select(x => x.HasValue ? (object?)x.Value : null).ToArray();
        if (arr is long[] longs) return longs.Take(count).Select(x => (object?)x).ToArray();
        if (arr is long?[] nlongs) return nlongs.Take(count).Select(x => x.HasValue ? (object?)x.Value : null).ToArray();
        if (arr is float[] floats) return floats.Take(count).Select(x => (object?)x).ToArray();
        if (arr is float?[] nfloats) return nfloats.Take(count).Select(x => x.HasValue ? (object?)x.Value : null).ToArray();
        if (arr is double[] doubles) return doubles.Take(count).Select(x => (object?)x).ToArray();
        if (arr is double?[] ndoubles) return ndoubles.Take(count).Select(x => x.HasValue ? (object?)x.Value : null).ToArray();
        if (arr is bool[] bools) return bools.Take(count).Select(x => (object?)x).ToArray();
        if (arr is bool?[] nbools) return nbools.Take(count).Select(x => x.HasValue ? (object?)x.Value : null).ToArray();

        // ParquetSharp timestamp types — extract underlying long value for comparison
        if (typeName.Contains("DateTimeNanos") || typeName.Contains("TimeSpanNanos"))
        {
            // DateTimeNanos stores nanoseconds; convert to µs to match Arrow's TimestampArray
            return ExtractViaLongField(arr, count, nanosToMicros: true);
        }

        return null; // non-scalar or unrecognised type
    }

    private static object?[]? ExtractViaLongField(
        System.Array arr, int count, bool nanosToMicros = false)
    {
        // ParquetSharp's DateTimeNanos/TimeSpanNanos wrap a long. Try to extract it.
        var elemType = arr.GetType().GetElementType();
        if (elemType == null) return null;

        var result = new object?[count];
        bool isNullable = Nullable.GetUnderlyingType(elemType) != null;
        var innerType = Nullable.GetUnderlyingType(elemType) ?? elemType;

        // Look for a field or property that gives us the raw ticks/nanos value
        var ticksProp = innerType.GetProperty("Ticks")
                     ?? innerType.GetProperty("Value");
        var ticksField = ticksProp == null
            ? innerType.GetFields().FirstOrDefault(f => f.FieldType == typeof(long))
            : null;

        if (ticksProp == null && ticksField == null)
            return null;

        for (int i = 0; i < count; i++)
        {
            var val = arr.GetValue(i);
            if (val == null)
            {
                result[i] = null;
            }
            else
            {
                object inner = isNullable
                    ? Convert.ChangeType(val, innerType)!
                    : val;
                object? raw = ticksProp != null
                    ? ticksProp.GetValue(inner)
                    : ticksField!.GetValue(inner);
                if (nanosToMicros && raw is long nanos)
                    result[i] = nanos / 1000L;
                else
                    result[i] = raw;
            }
        }
        return result;
    }

    /// <summary>
    /// Visitor that captures the read values into an Array for later comparison.
    /// </summary>
    private sealed class CaptureVisitor
        : ParquetSharp.ILogicalColumnReaderVisitor<(System.Array, string)>
    {
        private readonly long _numRows;
        public CaptureVisitor(long numRows) => _numRows = numRows;

        public (System.Array, string) OnLogicalColumnReader<TElement>(
            ParquetSharp.LogicalColumnReader<TElement> columnReader)
        {
            var buffer = new TElement[_numRows];
            columnReader.ReadBatch(buffer);
            return (buffer, typeof(TElement).Name);
        }
    }
}
