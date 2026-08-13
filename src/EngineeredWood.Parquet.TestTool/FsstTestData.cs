// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#pragma warning disable EWPARQUET0003 // Generating FSST reference data is the point of this command.

using System.Buffers.Binary;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Parquet.TestTool;

/// <summary>
/// Generates a single Parquet file exercising the FSST encoding's corner cases, for use as
/// reference data by other implementations.
/// </summary>
/// <remarks>
/// <para>Modelled on the ALP test file in
/// <see href="https://github.com/apache/parquet-testing/pull/119">parquet-testing#119</see>:
/// one small file rather than a large corpus, every FSST column duplicated by a
/// conventionally-encoded column holding the same values, so a reader can bit-compare the two
/// without needing any expected data alongside the file.</para>
/// <para>The corpus is synthetic and generated from a fixed seed, so the file is reproducible
/// and carries no third-party licensing. It is shaped like the data FSST targets — URLs, log
/// lines, paths — because a symbol table trained on genuinely random bytes would be
/// meaningless.</para>
/// </remarks>
internal static class FsstTestData
{
    /// <summary>Fixed so the file is byte-reproducible from this source.</summary>
    private const int Seed = 20260813;

    /// <summary>Small enough to force many data pages per column chunk (§1.4 coverage).</summary>
    private const int DataPageSize = 2048;

    private sealed record Section(string Name, string?[] Strings, byte[]?[] Binaries, string Rationale);

    public static async Task<int> Create(string[] args)
    {
        if (args.Length < 1)
        {
            Console.Error.WriteLine(
                "Usage: create_fsst_test_file <path> [--compression <uncompressed|snappy|zstd|gzip>]");
            return 1;
        }

        string path = args[0];
        var codec = CompressionCodec.Uncompressed;

        for (int i = 1; i < args.Length; i++)
        {
            if (args[i] == "--compression" && i + 1 < args.Length)
            {
                string name = args[++i];
                if (!Enum.TryParse(name, ignoreCase: true, out codec))
                {
                    Console.Error.WriteLine($"Unknown compression codec '{name}'.");
                    return 1;
                }
            }
            else
            {
                Console.Error.WriteLine($"Unexpected argument '{args[i]}'.");
                return 1;
            }
        }

        var sections = BuildCorpus();
        var schema = BuildSchema();

        var options = ParquetWriteOptions.Default with
        {
            DataPageVersion = DataPageVersion.V2,   // §1.3: FSST is V2-only
            DictionaryEnabled = false,              // a dictionary would pre-empt FSST
            Compression = codec,
            DataPageSize = DataPageSize,
            ByteArrayEncoding = ByteArrayEncoding.DeltaLengthByteArray,
            ColumnEncodings = new Dictionary<string, ByteArrayEncoding>
            {
                ["string_fsst"] = ByteArrayEncoding.Fsst,
                ["string_fsst16"] = ByteArrayEncoding.Fsst16,
                ["binary_fsst"] = ByteArrayEncoding.Fsst,
                ["binary_fsst16"] = ByteArrayEncoding.Fsst16,
            },
        };

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            foreach (var section in sections)
                await writer.WriteRowGroupAsync(BuildBatch(schema, section));

            await writer.CloseAsync();
        }

        Console.WriteLine($"Wrote {path} ({new FileInfo(path).Length} bytes), " +
                          $"{sections.Length} row groups, {sections.Sum(s => s.Strings.Length)} rows, " +
                          $"compression {codec}.");
        Console.WriteLine();

        for (int i = 0; i < sections.Length; i++)
            Console.WriteLine($"  row group {i}: {sections[i].Name} " +
                              $"({sections[i].Strings.Length} rows) — {sections[i].Rationale}");
        Console.WriteLine();

        return await VerifyAndDescribe(path, sections);
    }

    // -----------------------------------------------------------------------
    // Corpus
    // -----------------------------------------------------------------------

    /// <summary>
    /// One row group per section. Each trains its own symbol table (§1.4), so the sections are
    /// also what gives the file more than one table to check.
    /// </summary>
    private static Section[] BuildCorpus()
    {
        var rng = new Random(Seed);

        return
        [
            UrlSection(rng),
            NullsAndEmptiesSection(rng),
            EscapeSection(rng),
            LongValueSection(rng),
        ];
    }

    /// <summary>The happy path: high-cardinality machine-generated text, FSST's target.</summary>
    private static Section UrlSection(Random rng)
    {
        string[] hosts = ["example.com", "cdn.example.com", "api.example.org"];
        string[] paths = ["orders", "customers", "invoices", "sessions"];

        var strings = new string?[400];
        for (int i = 0; i < strings.Length; i++)
        {
            strings[i] = $"https://{hosts[rng.Next(hosts.Length)]}/{paths[rng.Next(paths.Length)]}" +
                         $"/{i}/items/{rng.Next(1000)}?src=catalog&page={rng.Next(50)}";
        }

        return new Section(
            "urls", strings, Utf8(strings),
            "happy path — high-cardinality URLs across many pages sharing one symbol table");
    }

    /// <summary>Nulls and empty strings, which the offset array has to represent exactly.</summary>
    private static Section NullsAndEmptiesSection(Random rng)
    {
        var strings = new string?[240];
        for (int i = 0; i < strings.Length; i++)
        {
            strings[i] = (i % 7, i % 5) switch
            {
                (0, _) => null,
                (_, 0) => string.Empty,
                _ => $"2026-08-13T{i % 24:D2}:{i % 60:D2}:00Z svc-{rng.Next(9)} request {i} ok",
            };
        }

        return new Section(
            "nulls_and_empties", strings, Utf8(strings),
            "NULLs and empty strings — zero-length values, and def levels alongside FSST pages");
    }

    /// <summary>
    /// Values holding bytes the symbol table cannot cover, which the encoder has to emit as
    /// escapes (§5.2). The string column reaches them through multi-byte UTF-8; the binary
    /// column uses raw high bytes directly.
    /// </summary>
    private static Section EscapeSection(Random rng)
    {
        var strings = new string?[200];
        var binaries = new byte[]?[200];

        for (int i = 0; i < strings.Length; i++)
        {
            // The rare bytes have to be genuinely rare. An earlier version put a multi-byte
            // code point in every value; the trainer simply learned it as a symbol and the
            // column came out with no escapes at all. Only every ninth value carries one, so
            // there is no training gain in covering it and the encoder must escape it.
            string tail = i % 9 == 0 ? "→文" : $"{rng.Next(100)}";
            strings[i] = $"session-{i}-user{rng.Next(50)}-region-eu-west-{tail}";

            var raw = new byte[28];
            System.Text.Encoding.UTF8.GetBytes($"blob-{i:D4}-region-eu-west-").CopyTo(raw, 0);
            for (int b = 24; b < raw.Length; b++)
                raw[b] = i % 9 == 0 ? (byte)(0xF0 + rng.Next(0x10)) : (byte)('a' + rng.Next(26));
            binaries[i] = raw;
        }

        return new Section(
            "escapes", strings, binaries,
            "bytes absent from the symbol table — forces escape sequences in the code stream");
    }

    /// <summary>
    /// Values long enough that few fit a page, which is where a PLAIN offset array can beat a
    /// DELTA_BINARY_PACKED one and the header's offset_encoding byte earns its keep (§4.4).
    /// </summary>
    private static Section LongValueSection(Random rng)
    {
        var strings = new string?[16];
        for (int i = 0; i < strings.Length; i++)
        {
            var sb = new StringBuilder();
            sb.Append($"/srv/data/warehouse/partition={i}/");
            while (sb.Length < 400)
                sb.Append($"segment-{rng.Next(10000)}/part-{rng.Next(10000)}.parquet;");
            strings[i] = sb.ToString();
        }

        return new Section(
            "long_values", strings, Utf8(strings),
            "values of ~400 bytes — few per page, so the offset array encoding choice differs");
    }

    private static byte[]?[] Utf8(string?[] values)
    {
        var result = new byte[]?[values.Length];
        for (int i = 0; i < values.Length; i++)
            result[i] = values[i] is null ? null : System.Text.Encoding.UTF8.GetBytes(values[i]!);
        return result;
    }

    // -----------------------------------------------------------------------
    // Arrow plumbing
    // -----------------------------------------------------------------------

    private static Apache.Arrow.Schema BuildSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("string_reference", StringType.Default, nullable: true))
            .Field(new Field("string_fsst", StringType.Default, nullable: true))
            .Field(new Field("string_fsst16", StringType.Default, nullable: true))
            .Field(new Field("binary_reference", BinaryType.Default, nullable: true))
            .Field(new Field("binary_fsst", BinaryType.Default, nullable: true))
            .Field(new Field("binary_fsst16", BinaryType.Default, nullable: true))
            .Build();

    private static RecordBatch BuildBatch(Apache.Arrow.Schema schema, Section section)
    {
        IArrowArray Strings()
        {
            var builder = new StringArray.Builder();
            foreach (string? v in section.Strings)
            {
                if (v is null) builder.AppendNull();
                else builder.Append(v);
            }
            return builder.Build();
        }

        IArrowArray Binaries()
        {
            var builder = new BinaryArray.Builder();
            foreach (byte[]? v in section.Binaries)
            {
                if (v is null) builder.AppendNull();
                else builder.Append(v.AsSpan());
            }
            return builder.Build();
        }

        return new RecordBatch(
            schema,
            [Strings(), Strings(), Strings(), Binaries(), Binaries(), Binaries()],
            section.Strings.Length);
    }

    // -----------------------------------------------------------------------
    // Verification and description
    // -----------------------------------------------------------------------

    /// <summary>
    /// Reads the file back, checks every FSST column against its reference column, and reports
    /// what the file actually contains — the writer chooses the offset encoding per page and
    /// the trainer chooses the symbol table, so neither can be described without looking.
    /// </summary>
    private static async Task<int> VerifyAndDescribe(string path, Section[] sections)
    {
        byte[] raw = File.ReadAllBytes(path);
        int failures = 0;

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();

        for (int rg = 0; rg < meta.RowGroups.Count; rg++)
        {
            Console.WriteLine($"Row group {rg} — {sections[rg].Name}");

            var batch = await reader.ReadRowGroupAsync(rg);
            failures += CompareColumns(batch, rg);

            foreach (var column in meta.RowGroups[rg].Columns)
            {
                var columnMeta = column.MetaData!;
                string name = string.Join(".", columnMeta.PathInSchema ?? []);
                if (columnMeta.SymbolTablePageOffset is not long tableOffset)
                {
                    // §7.5: a chunk FSST could not shrink falls back and writes no symbol table
                    // page. Worth saying out loud — silently omitting the column would read as
                    // a bug in this summary rather than as the writer declining.
                    if (name.Contains("fsst"))
                        Console.WriteLine(
                            $"  {name,-17} FSST declined this chunk (§7.5 fallback) — encodings " +
                            $"[{string.Join(" ", columnMeta.Encodings)}]");
                    continue;
                }

                var table = ReadSymbolTable(raw, tableOffset, columnMeta);
                var pages = WalkDataPages(raw, columnMeta, table);

                Console.WriteLine(
                    $"  {name,-17} {DescribeTable(table)}, {pages.Count} data pages" +
                    $"{DescribeOffsetEncodings(pages)}{DescribeEscapes(pages)}");
            }

            Console.WriteLine();
        }

        Console.WriteLine(failures == 0
            ? "OK — every FSST column decodes bit-identically to its reference column."
            : $"FAILED — {failures} mismatches against the reference columns.");
        return failures == 0 ? 0 : 1;
    }

    private static int CompareColumns(RecordBatch batch, int rowGroup)
    {
        int failures = 0;

        failures += Compare<StringArray>(batch, "string_reference", "string_fsst", rowGroup);
        failures += Compare<StringArray>(batch, "string_reference", "string_fsst16", rowGroup);
        failures += Compare<BinaryArray>(batch, "binary_reference", "binary_fsst", rowGroup);
        failures += Compare<BinaryArray>(batch, "binary_reference", "binary_fsst16", rowGroup);

        return failures;
    }

    private static int Compare<T>(RecordBatch batch, string referenceName, string fsstName, int rowGroup)
        where T : IArrowArray
    {
        var schema = batch.Schema;
        var reference = (T)batch.Column(schema.GetFieldIndex(referenceName));
        var actual = (T)batch.Column(schema.GetFieldIndex(fsstName));

        for (int i = 0; i < reference.Length; i++)
        {
            bool same = (reference, actual) switch
            {
                (StringArray r, StringArray a) => r.GetString(i) == a.GetString(i),
                (BinaryArray r, BinaryArray a) =>
                    r.IsNull(i) == a.IsNull(i) &&
                    (r.IsNull(i) || r.GetBytes(i).SequenceEqual(a.GetBytes(i))),
                _ => false,
            };

            if (!same)
            {
                Console.Error.WriteLine(
                    $"  MISMATCH row group {rowGroup}, {fsstName} row {i} differs from {referenceName}.");
                return 1;
            }
        }

        return 0;
    }

    private static FsstSymbolTable ReadSymbolTable(
        byte[] raw, long offset, Metadata.ColumnMetaData columnMeta)
    {
        var header = PageHeaderDecoder.Decode(raw.AsSpan((int)offset), out int headerBytes);
        var body = raw.AsSpan((int)offset + headerBytes, header.CompressedPageSize);
        return FsstPageDecoder.ReadSymbolTablePage(header, body, columnMeta);
    }

    private sealed record PageInfo(byte OffsetEncoding, int Escapes);

    /// <summary>
    /// Walks a chunk's data pages to recover what the writer actually chose per page — the
    /// offset encoding is picked per page and the escapes depend on the trained table, so
    /// neither can be reported without reading the bytes back.
    /// </summary>
    private static List<PageInfo> WalkDataPages(
        byte[] raw, Metadata.ColumnMetaData columnMeta, FsstSymbolTable table)
    {
        var pages = new List<PageInfo>();
        int position = (int)columnMeta.DataPageOffset;
        long seen = 0;

        while (seen < columnMeta.NumValues)
        {
            var header = PageHeaderDecoder.Decode(raw.AsSpan(position), out int headerBytes);
            var v2 = header.DataPageHeaderV2;
            if (v2 is null)
                break;

            // In a V2 page the levels are never compressed; only the values section is.
            int levels = v2.DefinitionLevelsByteLength + v2.RepetitionLevelsByteLength;
            var values = raw.AsSpan(
                position + headerBytes + levels, header.CompressedPageSize - levels);

            if (v2.IsCompressed && columnMeta.Codec != CompressionCodec.Uncompressed)
            {
                var decompressed = new byte[header.UncompressedPageSize - levels];
                int written = Decompressor.Decompress(columnMeta.Codec, values, decompressed);
                pages.Add(ReadFsstPage(decompressed.AsSpan(0, written), table));
            }
            else
            {
                pages.Add(ReadFsstPage(values, table));
            }

            seen += v2.NumValues;
            position += headerBytes + header.CompressedPageSize;
        }

        return pages;
    }

    private static PageInfo ReadFsstPage(ReadOnlySpan<byte> body, FsstSymbolTable table)
    {
        byte offsetEncoding = body[0];
        int offsetArrayLength = BinaryPrimitives.ReadInt32LittleEndian(body.Slice(5, 4));
        var data = body.Slice(FsstPageEncoder.HeaderSize + offsetArrayLength);

        // Counting escapes is the same walk a decoder makes, and is what proves the file
        // actually exercises them rather than merely being intended to.
        int escapes = 0;
        if (table is FsstSymbolTable16)
        {
            for (int i = 0; i + 1 < data.Length; i += 2)
            {
                if (BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(i, 2)) == FsstSymbolTable16.EscapeCode)
                {
                    escapes++;
                    i += 2;
                }
            }
        }
        else
        {
            for (int i = 0; i < data.Length; i++)
            {
                if (data[i] == FsstSymbolTable8.EscapeCode)
                {
                    escapes++;
                    i++;
                }
            }
        }

        return new PageInfo(offsetEncoding, escapes);
    }

    private static string DescribeTable(FsstSymbolTable table)
    {
        byte[] body = table.Serialize();
        int headerSize = table is FsstSymbolTable16
            ? FsstSymbolTable16.BodyHeaderSize
            : FsstSymbolTable8.BodyHeaderSize;

        int longest = 0;
        int slots = table is FsstSymbolTable16 ? FsstSymbolTable16.MaxSymbolLength : FsstSymbolTable8.MaxSymbolLength;
        for (int length = 1; length <= slots; length++)
        {
            int count = table is FsstSymbolTable16
                ? BinaryPrimitives.ReadUInt16LittleEndian(body.AsSpan(2 + ((length - 1) * 2), 2))
                : body[1 + (length - 1)];
            if (count > 0)
                longest = length;
        }

        string type = table is FsstSymbolTable16 ? "FSST_16" : "FSST";
        return $"{type,-7} table of {table.SymbolCount,4} symbols " +
               $"(longest {longest,2}, body {headerSize + body.Length - headerSize} bytes)";
    }

    private static string DescribeOffsetEncodings(List<PageInfo> pages)
    {
        if (pages.Count == 0)
            return string.Empty;

        int plain = pages.Count(p => p.OffsetEncoding == 0);
        int delta = pages.Count - plain;
        return $", offsets {plain} PLAIN / {delta} DELTA";
    }

    private static string DescribeEscapes(List<PageInfo> pages)
    {
        if (pages.Count == 0)
            return string.Empty;

        int escapes = pages.Sum(p => p.Escapes);
        return escapes > 0 ? $", {escapes} escapes" : ", no escapes";
    }
}
