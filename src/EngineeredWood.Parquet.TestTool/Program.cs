// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Diagnostics;
using System.Security.Cryptography;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

if (args.Length == 0)
{
    PrintUsage();
    return 1;
}

return args[0] switch
{
    "create_test_file" => await CreateTestFile(args.Skip(1).ToArray()),
    "read_test_file" => await ReadTestFile(args.Skip(1).ToArray()),
    "strip_path_in_schema" => StripPathInSchema(args.Skip(1).ToArray()),
    _ => PrintUsage(),
};

static int PrintUsage()
{
    Console.Error.WriteLine("""
        Usage:
          ew-test-tool create_test_file <path> --size <bytes>
            Creates a Parquet file with a single row group of approximately <bytes>
            uncompressed data. Includes columns exercising every encoding path
            (DeltaBinaryPacked, ByteStreamSplit, Rle, DeltaLengthByteArray,
            DeltaByteArray, RleDictionary, Plain) with monotonic sequences for
            validation. Supports KB/MB/GB suffixes.

          ew-test-tool read_test_file <path> [--batch-rows <n>] [--batch-bytes <n>] [--validate]
            Reads the file one RecordBatch at a time and reports peak memory.
            --validate checks every encoding-exercise column for correct values.

          ew-test-tool strip_path_in_schema <input> <output>
            Reads <input>, decodes its footer, and writes <output> with the same
            row group data and a re-encoded footer that omits the path_in_schema
            field from every column chunk's metadata. The body bytes (data pages,
            dictionary pages, bloom filter blocks, page indexes) are copied
            verbatim, so file offsets remain valid. Useful for testing other
            Parquet readers' tolerance of files written without path_in_schema.
        """);
    return 1;
}

// ---------------------------------------------------------------------------
// create_test_file
// ---------------------------------------------------------------------------

static async Task<int> CreateTestFile(string[] args)
{
    if (args.Length < 3 || args[1] != "--size")
    {
        Console.Error.WriteLine("Usage: create_test_file <path> --size <bytes>");
        return 1;
    }

    string path = args[0];
    long targetBytes = ParseSize(args[2]);

    // Schema designed to exercise every distinct encoding/page-format combination
    // the writer produces, while keeping values cheaply validatable (monotonic
    // sequences that can be checked with a running counter).
    //
    // Encoding path              Column                Arrow type          Parquet encoding (V2)
    // ──────────────────────────  ────────────────────  ──────────────────  ──────────────────────────
    // DeltaBinaryPacked          id          (req)      Int64               DeltaBinaryPacked
    // DeltaBinaryPacked          seq_i32     (req)      Int32               DeltaBinaryPacked
    // DeltaBinaryPacked+null     seq_i64n    (opt)      Int64?              DeltaBinaryPacked + def levels
    // ByteStreamSplit             seq_f32     (req)      Float               ByteStreamSplit
    // ByteStreamSplit             seq_f64     (req)      Double              ByteStreamSplit
    // RLE Boolean                 seq_bool    (req)      Boolean             Rle
    // DeltaLengthByteArray        seq_str     (req)      String              DeltaLengthByteArray
    // DeltaByteArray (binary)     seq_bin     (req)      Binary              DeltaByteArray
    // RleDictionary               seq_dict    (req)      String (dict)       RleDictionary
    // FixedLenByteArray (plain)   seq_fsb     (req)      FixedSizeBinary(4)  Plain
    //
    // The "bulk" columns (id + 5 random Int64s) provide the majority of the file
    // size. The encoding-exercise columns are small per-row but ensure that every
    // decode path is split across batch boundaries.

    // Approximate row size for the bulk portion (determines row count).
    const int numBulkInt64 = 6; // id + v0..v4
    const int approxRowBytes = numBulkInt64 * 8 + 4 + 4 + 8 + 8 + 1 + 20 + 20 + 10 + 4;
    // ≈ 48 (bulk Int64) + 4 (Int32) + 4 (Float) + 8 (Double) + 8 (Int64?) + 1 (Bool) + ~54 (strings/binary/fsb) ≈ 127
    int totalRows = (int)Math.Min(targetBytes / approxRowBytes, int.MaxValue);
    if (totalRows < 1) totalRows = 1;

    Console.WriteLine($"Target size:  {FormatBytes(targetBytes)}");
    Console.WriteLine($"Rows:         {totalRows:N0}");
    Console.WriteLine($"Output:       {path}");

    var schema = new Apache.Arrow.Schema.Builder()
        // Bulk columns (DeltaBinaryPacked, provide file size)
        .Field(new Field("id", Int64Type.Default, nullable: false))
        .Field(new Field("v0", Int64Type.Default, nullable: false))
        .Field(new Field("v1", Int64Type.Default, nullable: false))
        .Field(new Field("v2", Int64Type.Default, nullable: false))
        .Field(new Field("v3", Int64Type.Default, nullable: false))
        .Field(new Field("v4", Int64Type.Default, nullable: false))
        // Encoding-exercise columns (monotonic sequences)
        .Field(new Field("seq_i32", Int32Type.Default, nullable: false))
        .Field(new Field("seq_i64n", Int64Type.Default, nullable: true))
        .Field(new Field("seq_f32", FloatType.Default, nullable: false))
        .Field(new Field("seq_f64", DoubleType.Default, nullable: false))
        .Field(new Field("seq_bool", BooleanType.Default, nullable: false))
        .Field(new Field("seq_str", StringType.Default, nullable: false))
        .Field(new Field("seq_bin", BinaryType.Default, nullable: false))
        .Field(new Field("seq_dict", StringType.Default, nullable: false))
        .Field(new Field("seq_fsb", new FixedSizeBinaryType(4), nullable: false))
        .Build();

    var writeOptions = new ParquetWriteOptions
    {
        Compression = CompressionCodec.Snappy,
        RowGroupMaxRows = totalRows,
        DataPageSize = 64 * 1024, // 64 KB pages → many pages even for small test files
        DictionaryEnabled = true,
        // seq_bin uses DeltaByteArray; seq_str uses DeltaLengthByteArray (default).
        // Override seq_bin's encoding via ColumnEncodings.
        ColumnEncodings = new Dictionary<string, ByteArrayEncoding>
        {
            ["seq_bin"] = ByteArrayEncoding.DeltaByteArray,
        },
    };

    var sw = Stopwatch.StartNew();

    Console.Write("  Building batch in memory...");
    var batch = GenerateRandomBatch(schema, totalRows, startId: 0);
    Console.WriteLine($" done ({sw.Elapsed.TotalSeconds:F1}s, {FormatBytes(GC.GetTotalMemory(false))} heap)");

    Console.Write("  Writing...");
    await using (var file = new LocalSequentialFile(path))
    await using (var writer = new ParquetFileWriter(file, ownsFile: false, writeOptions))
    {
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
    }
    Console.WriteLine(" done");

    var fileInfo = new FileInfo(path);
    Console.WriteLine($"File size:    {FormatBytes(fileInfo.Length)}");
    Console.WriteLine($"Elapsed:      {sw.Elapsed.TotalSeconds:F1}s");
    return 0;
}

static RecordBatch GenerateRandomBatch(Apache.Arrow.Schema schema, int rowCount, long startId)
{
    var arrays = new List<IArrowArray>();

    // --- id (sequential Int64, also serves as the master row counter) ---
    var ids = new long[rowCount];
    for (int i = 0; i < rowCount; i++)
        ids[i] = startId + i;
    arrays.Add(new Int64Array.Builder().AppendRange(ids).Build());

    // --- v0..v4 (random Int64 bulk columns) ---
    for (int c = 0; c < 5; c++)
    {
        var values = new long[rowCount];
#if NET8_0_OR_GREATER
        RandomNumberGenerator.Fill(
            System.Runtime.InteropServices.MemoryMarshal.AsBytes(values.AsSpan()));
#else
        var rngBytes = new byte[rowCount * 8];
        using (var rng = RandomNumberGenerator.Create())
            rng.GetBytes(rngBytes);
        Buffer.BlockCopy(rngBytes, 0, values, 0, rngBytes.Length);
#endif
        arrays.Add(new Int64Array.Builder().AppendRange(values).Build());
    }

    // --- seq_i32: Int32, monotonic (DeltaBinaryPacked) ---
    {
        var b = new Int32Array.Builder();
        for (int i = 0; i < rowCount; i++) b.Append(i);
        arrays.Add(b.Build());
    }

    // --- seq_i64n: Int64?, monotonic with every 7th null (DeltaBinaryPacked + def levels) ---
    {
        var b = new Int64Array.Builder();
        for (int i = 0; i < rowCount; i++)
        {
            if (i % 7 == 3)
                b.AppendNull();
            else
                b.Append(i);
        }
        arrays.Add(b.Build());
    }

    // --- seq_f32: Float, monotonic (ByteStreamSplit) ---
    {
        var b = new FloatArray.Builder();
        for (int i = 0; i < rowCount; i++) b.Append((float)i);
        arrays.Add(b.Build());
    }

    // --- seq_f64: Double, monotonic (ByteStreamSplit) ---
    {
        var b = new DoubleArray.Builder();
        for (int i = 0; i < rowCount; i++) b.Append((double)i);
        arrays.Add(b.Build());
    }

    // --- seq_bool: Boolean, alternating (Rle) ---
    {
        var b = new BooleanArray.Builder();
        for (int i = 0; i < rowCount; i++) b.Append(i % 2 == 0);
        arrays.Add(b.Build());
    }

    // --- seq_str: String, monotonic formatted (DeltaLengthByteArray) ---
    {
        var b = new StringArray.Builder();
        for (int i = 0; i < rowCount; i++) b.Append(i.ToString());
        arrays.Add(b.Build());
    }

    // --- seq_bin: Binary, monotonic 4-byte LE (DeltaByteArray via ColumnEncodings override) ---
    {
        var b = new BinaryArray.Builder();
        Span<byte> buf = stackalloc byte[4];
        for (int i = 0; i < rowCount; i++)
        {
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(buf, i);
            b.Append(buf);
        }
        arrays.Add(b.Build());
    }

    // --- seq_dict: String, low-cardinality (RleDictionary — writer auto-dicts) ---
    {
        string[] categories = ["alpha", "beta", "gamma", "delta", "epsilon",
                               "zeta", "eta", "theta", "iota", "kappa"];
        var b = new StringArray.Builder();
        for (int i = 0; i < rowCount; i++) b.Append(categories[i % categories.Length]);
        arrays.Add(b.Build());
    }

    // --- seq_fsb: FixedSizeBinary(4), monotonic 4-byte LE (Plain for FLBA) ---
    {
        byte[] fsbData = new byte[rowCount * 4];
        for (int i = 0; i < rowCount; i++)
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(
                fsbData.AsSpan(i * 4, 4), i);
        var fsbType = new FixedSizeBinaryType(4);
        var arrayData = new ArrayData(fsbType, rowCount, nullCount: 0, offset: 0,
            [ArrowBuffer.Empty, new ArrowBuffer(fsbData)]);
        arrays.Add(new FixedSizeBinaryArray(arrayData));
    }

    return new RecordBatch(schema, arrays, rowCount);
}

// ---------------------------------------------------------------------------
// read_test_file
// ---------------------------------------------------------------------------

static async Task<int> ReadTestFile(string[] args)
{
    if (args.Length < 1)
    {
        Console.Error.WriteLine("Usage: read_test_file <path> [--batch-rows <n>] [--batch-bytes <n>]");
        return 1;
    }

    string path = args[0];
    int? batchRows = null;
    long? batchBytes = null;
    bool validate = false;

    for (int i = 1; i < args.Length; i++)
    {
        if (args[i] == "--batch-rows" && i + 1 < args.Length)
            batchRows = int.Parse(args[++i]);
        else if (args[i] == "--batch-bytes" && i + 1 < args.Length)
            batchBytes = ParseSize(args[++i]);
        else if (args[i] == "--validate")
            validate = true;
    }

    if (batchRows is null && batchBytes is null)
    {
        Console.Error.WriteLine("Specify at least one of --batch-rows or --batch-bytes.");
        return 1;
    }

    var readOptions = new ParquetReadOptions
    {
        BatchSize = batchRows,
        MaxBatchByteSize = batchBytes,
    };

    Console.WriteLine($"File:         {path}");
    Console.WriteLine($"BatchSize:    {(batchRows.HasValue ? $"{batchRows:N0} rows" : "(none)")}");
    Console.WriteLine($"MaxBytes:     {(batchBytes.HasValue ? FormatBytes(batchBytes.Value) : "(none)")}");

    // Force a GC before we start to get a clean baseline.
#if NET8_0_OR_GREATER
    GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
#else
    GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
#endif
    long baselineMemory = GC.GetTotalMemory(forceFullCollection: true);

    await using var file = new LocalRandomAccessFile(path);
    await using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

    var metadata = await reader.ReadMetadataAsync();
    Console.WriteLine($"Row groups:   {metadata.RowGroups.Count}");
    Console.WriteLine($"Total rows:   {metadata.NumRows:N0}");
    Console.WriteLine();

    var sw = Stopwatch.StartNew();
    long totalRowsRead = 0;
    int batchCount = 0;
    long peakMemory = 0;
    long peakDelta = 0;

    await foreach (var batch in reader.ReadAllAsync())
    {
        batchCount++;
        totalRowsRead += batch.Length;

        // Sample memory after each batch is alive.
        long currentMemory = GC.GetTotalMemory(forceFullCollection: false);
        if (currentMemory > peakMemory)
            peakMemory = currentMemory;

        long delta = currentMemory - baselineMemory;
        if (delta > peakDelta)
            peakDelta = delta;

        if (batchCount % 10 == 0 || batchCount <= 3)
        {
            Console.WriteLine(
                $"  Batch {batchCount,5}: {batch.Length,10:N0} rows  |  " +
                $"total {totalRowsRead,12:N0}  |  " +
                $"mem {FormatBytes(currentMemory),10}  |  " +
                $"Δ {FormatBytes(delta),10}");
        }

        if (validate)
        {
            int batchStartRow = (int)(totalRowsRead - batch.Length);
            ValidateBatch(batch, batchStartRow);
        }

        // Dispose arrays explicitly to free native buffers promptly.
        for (int c = 0; c < batch.ColumnCount; c++)
            batch.Column(c).Dispose();
    }

    // Final GC to measure steady-state.
#if NET8_0_OR_GREATER
    GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
#else
    GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
#endif
    long finalMemory = GC.GetTotalMemory(forceFullCollection: true);

    Console.WriteLine();
    if (validate)
        Console.WriteLine($"Validation:   PASSED ({totalRowsRead:N0} rows)");
    Console.WriteLine($"Batches read: {batchCount:N0}");
    Console.WriteLine($"Total rows:   {totalRowsRead:N0}");
    Console.WriteLine($"Elapsed:      {sw.Elapsed.TotalSeconds:F1}s");
    Console.WriteLine($"Baseline mem: {FormatBytes(baselineMemory)}");
    Console.WriteLine($"Peak mem:     {FormatBytes(peakMemory)}");
    Console.WriteLine($"Peak Δ:       {FormatBytes(peakDelta)}");
    Console.WriteLine($"Final mem:    {FormatBytes(finalMemory)}");
    return 0;
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

static void ValidateBatch(RecordBatch batch, int globalRowStart)
{
    var schema = batch.Schema;
    int len = (int)batch.Length;

    // Find columns by name — gracefully skip if the file wasn't created by this tool.
    int Idx(string name) => schema.GetFieldIndex(name);

    // seq_i32: Int32, monotonic (DeltaBinaryPacked)
    if (Idx("seq_i32") >= 0)
    {
        var col = (Int32Array)batch.Column(Idx("seq_i32"));
        for (int i = 0; i < len; i++)
            Check(col.GetValue(i) == globalRowStart + i,
                $"seq_i32[{globalRowStart + i}]: expected {globalRowStart + i}, got {col.GetValue(i)}");
    }

    // seq_i64n: Int64?, monotonic with null at i%7==3 (DeltaBinaryPacked + def levels)
    if (Idx("seq_i64n") >= 0)
    {
        var col = (Int64Array)batch.Column(Idx("seq_i64n"));
        for (int i = 0; i < len; i++)
        {
            int g = globalRowStart + i;
            if (g % 7 == 3)
                Check(col.IsNull(i), $"seq_i64n[{g}]: expected null");
            else
                Check(col.GetValue(i) == g, $"seq_i64n[{g}]: expected {g}, got {col.GetValue(i)}");
        }
    }

    // seq_f32: Float, monotonic (ByteStreamSplit)
    if (Idx("seq_f32") >= 0)
    {
        var col = (FloatArray)batch.Column(Idx("seq_f32"));
        for (int i = 0; i < len; i++)
            Check(col.GetValue(i) == (float)(globalRowStart + i),
                $"seq_f32[{globalRowStart + i}]: expected {(float)(globalRowStart + i)}, got {col.GetValue(i)}");
    }

    // seq_f64: Double, monotonic (ByteStreamSplit)
    if (Idx("seq_f64") >= 0)
    {
        var col = (DoubleArray)batch.Column(Idx("seq_f64"));
        for (int i = 0; i < len; i++)
            Check(col.GetValue(i) == (double)(globalRowStart + i),
                $"seq_f64[{globalRowStart + i}]: expected {(double)(globalRowStart + i)}, got {col.GetValue(i)}");
    }

    // seq_bool: Boolean, alternating (Rle)
    if (Idx("seq_bool") >= 0)
    {
        var col = (BooleanArray)batch.Column(Idx("seq_bool"));
        for (int i = 0; i < len; i++)
            Check(col.GetValue(i) == ((globalRowStart + i) % 2 == 0),
                $"seq_bool[{globalRowStart + i}]: expected {(globalRowStart + i) % 2 == 0}");
    }

    // seq_str: String, monotonic (DeltaLengthByteArray)
    if (Idx("seq_str") >= 0)
    {
        var col = (StringArray)batch.Column(Idx("seq_str"));
        for (int i = 0; i < len; i++)
        {
            string expected = (globalRowStart + i).ToString();
            Check(col.GetString(i) == expected,
                $"seq_str[{globalRowStart + i}]: expected \"{expected}\", got \"{col.GetString(i)}\"");
        }
    }

    // seq_bin: Binary, 4-byte LE monotonic (DeltaByteArray)
    if (Idx("seq_bin") >= 0)
    {
        var col = (BinaryArray)batch.Column(Idx("seq_bin"));
        for (int i = 0; i < len; i++)
        {
            int g = globalRowStart + i;
            var span = col.GetBytes(i);
            int actual = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(span);
            Check(actual == g, $"seq_bin[{g}]: expected {g}, got {actual}");
        }
    }

    // seq_dict: String, low-cardinality repeating (RleDictionary)
    if (Idx("seq_dict") >= 0)
    {
        string[] categories = ["alpha", "beta", "gamma", "delta", "epsilon",
                               "zeta", "eta", "theta", "iota", "kappa"];
        var col = (StringArray)batch.Column(Idx("seq_dict"));
        for (int i = 0; i < len; i++)
        {
            string expected = categories[(globalRowStart + i) % categories.Length];
            Check(col.GetString(i) == expected,
                $"seq_dict[{globalRowStart + i}]: expected \"{expected}\", got \"{col.GetString(i)}\"");
        }
    }

    // seq_fsb: FixedSizeBinary(4), 4-byte LE monotonic (Plain)
    if (Idx("seq_fsb") >= 0)
    {
        var col = (FixedSizeBinaryArray)batch.Column(Idx("seq_fsb"));
        for (int i = 0; i < len; i++)
        {
            int g = globalRowStart + i;
            var span = col.GetBytes(i);
            int actual = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(span);
            Check(actual == g, $"seq_fsb[{g}]: expected {g}, got {actual}");
        }
    }

    // id: Int64, sequential (should match globalRowStart + i)
    if (Idx("id") >= 0)
    {
        var col = (Int64Array)batch.Column(Idx("id"));
        for (int i = 0; i < len; i++)
            Check(col.GetValue(i) == globalRowStart + i,
                $"id[{globalRowStart + i}]: expected {globalRowStart + i}, got {col.GetValue(i)}");
    }
}

static void Check(bool condition, string message)
{
    if (!condition)
        throw new InvalidDataException($"Validation failed: {message}");
}

// ---------------------------------------------------------------------------
// strip_path_in_schema
// ---------------------------------------------------------------------------

static int StripPathInSchema(string[] args)
{
    if (args.Length < 2)
    {
        Console.Error.WriteLine("Usage: strip_path_in_schema <input> <output>");
        return 1;
    }

    string inputPath = args[0];
    string outputPath = args[1];

    if (!File.Exists(inputPath))
    {
        Console.Error.WriteLine($"Input file not found: {inputPath}");
        return 1;
    }

    byte[] sourceBytes = File.ReadAllBytes(inputPath);

    // Parquet layout: PAR1 <data> <footer> <footer_length:4 LE> PAR1
    if (sourceBytes.Length < 12 ||
        sourceBytes[0] != 'P' || sourceBytes[1] != 'A' || sourceBytes[2] != 'R' || sourceBytes[3] != '1' ||
        sourceBytes[^4] != 'P' || sourceBytes[^3] != 'A' || sourceBytes[^2] != 'R' || sourceBytes[^1] != '1')
    {
        Console.Error.WriteLine("Input is not a Parquet file (missing PAR1 magic).");
        return 1;
    }

    int footerLength = BinaryPrimitives.ReadInt32LittleEndian(sourceBytes.AsSpan(sourceBytes.Length - 8, 4));
    int footerStart = sourceBytes.Length - 8 - footerLength;
    if (footerStart < 4 || footerLength <= 0)
    {
        Console.Error.WriteLine($"Invalid footer length: {footerLength}");
        return 1;
    }

    var footerSpan = sourceBytes.AsSpan(footerStart, footerLength);
    var metadata = MetadataDecoder.DecodeFileMetaData(footerSpan);

    // Re-encode the footer with path_in_schema omitted. The decoder has already
    // populated PathInSchema (if present in the source), but the encoder will
    // skip the field when writePathInSchema = false.
    byte[] newFooter = MetadataEncoder.EncodeFileMetaData(metadata, writePathInSchema: false);

    // Sanity check: the new footer must be no larger than the old one, otherwise
    // any absolute offsets recorded in column chunks (DataPageOffset,
    // DictionaryPageOffset, BloomFilterOffset, IndexPageOffset) would still point
    // to the right bytes — those offsets are absolute from the start of the file
    // and refer to data that lives *before* the footer, so footer growth/shrink
    // is fine. We assert nothing here.
    int columnsTouched = 0;
    foreach (var rg in metadata.RowGroups)
        foreach (var c in rg.Columns)
            if (c.MetaData?.PathInSchema is not null) columnsTouched++;

    using (var output = File.Create(outputPath))
    {
        // Body bytes [0 .. footerStart) — preserved verbatim, including data
        // pages, dictionary pages, bloom filters, page indexes, and the leading
        // PAR1 magic.
        output.Write(sourceBytes, 0, footerStart);

        // New footer
        output.Write(newFooter, 0, newFooter.Length);

        // Footer length (4-byte LE)
        byte[] lenBytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(lenBytes, newFooter.Length);
        output.Write(lenBytes, 0, lenBytes.Length);

        // Trailing PAR1 magic
        byte[] par1 = [(byte)'P', (byte)'A', (byte)'R', (byte)'1'];
        output.Write(par1, 0, par1.Length);
    }

    long oldSize = sourceBytes.LongLength;
    long newSize = new FileInfo(outputPath).Length;
    Console.WriteLine($"Input:               {inputPath}");
    Console.WriteLine($"Output:              {outputPath}");
    Console.WriteLine($"Row groups:          {metadata.RowGroups.Count}");
    Console.WriteLine($"Columns stripped:    {columnsTouched}");
    Console.WriteLine($"Old footer size:     {FormatBytes(footerLength)}");
    Console.WriteLine($"New footer size:     {FormatBytes(newFooter.Length)}");
    Console.WriteLine($"File size:           {FormatBytes(oldSize)} → {FormatBytes(newSize)}");
    return 0;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static long ParseSize(string s)
{
    s = s.Trim();
    long multiplier = 1;
    if (s.EndsWith("GB", StringComparison.OrdinalIgnoreCase))
    {
        multiplier = 1L << 30;
        s = s.Substring(0, s.Length - 2);
    }
    else if (s.EndsWith("MB", StringComparison.OrdinalIgnoreCase))
    {
        multiplier = 1L << 20;
        s = s.Substring(0, s.Length - 2);
    }
    else if (s.EndsWith("KB", StringComparison.OrdinalIgnoreCase))
    {
        multiplier = 1L << 10;
        s = s.Substring(0, s.Length - 2);
    }
    return (long)(double.Parse(s.Trim()) * multiplier);
}

static string FormatBytes(long bytes)
{
    return bytes switch
    {
        >= 1L << 30 => $"{bytes / (double)(1L << 30):F2} GB",
        >= 1L << 20 => $"{bytes / (double)(1L << 20):F1} MB",
        >= 1L << 10 => $"{bytes / (double)(1L << 10):F1} KB",
        _ => $"{bytes} B",
    };
}
