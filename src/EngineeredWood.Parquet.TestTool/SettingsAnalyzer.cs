// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.TestTool;

internal static class SettingsAnalyzer
{
    private const int MaxPageHeaderSize = 64 * 1024;

    public static async Task<int> Run(string[] args)
    {
        if (args.Length == 0)
        {
            Console.Error.WriteLine("Usage: analyze_settings <file|directory|glob> [...]");
            return 1;
        }

        IReadOnlyList<string> paths;
        try
        {
            paths = ExpandPaths(args);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException)
        {
            Console.Error.WriteLine($"Unable to enumerate input files: {ex.Message}");
            return 1;
        }

        if (paths.Count == 0)
        {
            Console.Error.WriteLine("No Parquet files matched the supplied paths.");
            return 1;
        }

        var files = new List<FileObservation>(paths.Count);
        foreach (string path in paths)
        {
            try
            {
                files.Add(await InspectFile(path).ConfigureAwait(false));
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ParquetFormatException)
            {
                Console.Error.WriteLine($"{path}: {ex.Message}");
                return 1;
            }
        }

        PrintReport(files);
        return 0;
    }

    private static IReadOnlyList<string> ExpandPaths(IEnumerable<string> inputs)
    {
        var paths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (string input in inputs)
        {
            string fullPath = Path.GetFullPath(input);
            if (File.Exists(fullPath))
            {
                paths.Add(fullPath);
                continue;
            }

            if (Directory.Exists(fullPath))
            {
                foreach (string path in Directory.EnumerateFiles(fullPath, "*.parquet", SearchOption.AllDirectories))
                    paths.Add(Path.GetFullPath(path));
                continue;
            }

            string? directory = Path.GetDirectoryName(fullPath);
            string pattern = Path.GetFileName(fullPath);
            if (directory is null || !Directory.Exists(directory) ||
                pattern.IndexOfAny(['*', '?']) < 0)
            {
                throw new FileNotFoundException($"Input path does not exist: {input}", input);
            }

            foreach (string path in Directory.EnumerateFiles(directory, pattern, SearchOption.TopDirectoryOnly))
                paths.Add(Path.GetFullPath(path));
        }

        var result = paths.ToList();
        result.Sort(StringComparer.OrdinalIgnoreCase);
        return result;
    }

    private static async Task<FileObservation> InspectFile(string path)
    {
        var info = new FileInfo(path);
        await using var randomAccessFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(randomAccessFile, ownsFile: false);
        FileMetaData metadata = await reader.ReadMetadataAsync().ConfigureAwait(false);

        var pages = new List<PageObservation>();
        int externalChunks = 0;
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        byte[] headerBuffer = new byte[MaxPageHeaderSize];

        for (int rowGroupIndex = 0; rowGroupIndex < metadata.RowGroups.Count; rowGroupIndex++)
        {
            RowGroup rowGroup = metadata.RowGroups[rowGroupIndex];
            for (int columnIndex = 0; columnIndex < rowGroup.Columns.Count; columnIndex++)
            {
                ColumnChunk chunk = rowGroup.Columns[columnIndex];
                if (chunk.FilePath is not null || chunk.MetaData is not { } column)
                {
                    externalChunks++;
                    continue;
                }

                long start = FirstPageOffset(column);
                long end = checked(start + column.TotalCompressedSize);
                string columnName = column.PathInSchema is { Count: > 0 }
                    ? string.Join(".", column.PathInSchema)
                    : $"column[{columnIndex}]";

                while (start < end)
                {
                    stream.Position = start;
                    long remaining = end - start;
                    int requested = (int)Math.Min(headerBuffer.Length, remaining);
                    int read = ReadAtMost(stream, headerBuffer, requested);
                    if (read == 0)
                        throw new EndOfStreamException($"Unexpected end of file at offset {start}.");

                    PageHeader header;
                    int headerSize;
                    try
                    {
                        header = PageHeaderDecoder.Decode(headerBuffer.AsSpan(0, read), out headerSize);
                    }
                    catch (Exception ex) when (ex is ParquetFormatException or IndexOutOfRangeException)
                    {
                        throw new ParquetFormatException(
                            $"Column '{columnName}' in row group {rowGroupIndex} has an unreadable page header at offset {start}.",
                            ex);
                    }

                    long pageSize = checked((long)headerSize + header.CompressedPageSize);
                    if (header.CompressedPageSize < 0 || pageSize > remaining)
                    {
                        throw new ParquetFormatException(
                            $"Column '{columnName}' in row group {rowGroupIndex} has a page at offset {start} " +
                            $"whose {pageSize:N0} bytes exceed the column chunk boundary.");
                    }

                    int? valueCount = header.Type switch
                    {
                        PageType.DataPage => header.DataPageHeader?.NumValues,
                        PageType.DataPageV2 => header.DataPageHeaderV2?.NumValues,
                        PageType.DictionaryPage => header.DictionaryPageHeader?.NumValues,
                        _ => null,
                    };
                    int? rowCount = header.Type == PageType.DataPageV2
                        ? header.DataPageHeaderV2?.NumRows
                        : null;
                    Encoding? encoding = header.Type switch
                    {
                        PageType.DataPage => header.DataPageHeader?.Encoding,
                        PageType.DataPageV2 => header.DataPageHeaderV2?.Encoding,
                        _ => null,
                    };

                    pages.Add(new PageObservation(
                        header.Type,
                        header.UncompressedPageSize,
                        header.CompressedPageSize,
                        valueCount,
                        rowCount,
                        encoding,
                        header.Crc.HasValue));
                    start += pageSize;
                }
            }
        }

        return new FileObservation(path, info.Length, metadata, pages, externalChunks);
    }

    private static long FirstPageOffset(ColumnMetaData column)
    {
        long offset = column.DataPageOffset;
        if (column.DictionaryPageOffset is { } dictionaryOffset)
            offset = Math.Min(offset, dictionaryOffset);
        if (column.SymbolTablePageOffset is { } symbolTableOffset)
            offset = Math.Min(offset, symbolTableOffset);
        return offset;
    }

    private static int ReadAtMost(FileStream stream, byte[] buffer, int count)
    {
        int total = 0;
        while (total < count)
        {
            int read = stream.Read(buffer, total, count - total);
            if (read == 0)
                break;
            total += read;
        }
        return total;
    }

    private static void PrintReport(IReadOnlyList<FileObservation> files)
    {
        long[] fileSizes = files.Select(file => file.Length).ToArray();
        FileMetaData[] metadata = files.Select(file => file.Metadata).ToArray();
        RowGroup[] rowGroups = metadata.SelectMany(item => item.RowGroups).ToArray();
        RowGroup[] boundaryRowGroups = metadata
            .SelectMany(item => item.RowGroups.Take(Math.Max(0, item.RowGroups.Count - 1)))
            .ToArray();
        PageObservation[] pages = files.SelectMany(file => file.Pages).ToArray();
        PageObservation[] dataPages = pages
            .Where(page => page.Type is PageType.DataPage or PageType.DataPageV2)
            .ToArray();
        PageObservation[] dictionaryPages = pages
            .Where(page => page.Type == PageType.DictionaryPage)
            .ToArray();
        ColumnChunk[] chunks = rowGroups.SelectMany(group => group.Columns).ToArray();
        ColumnMetaData[] columns = chunks
            .Where(chunk => chunk.MetaData is not null)
            .Select(chunk => chunk.MetaData!)
            .ToArray();

        Console.WriteLine($"Analyzed {files.Count:N0} file(s), {rowGroups.Length:N0} row group(s), " +
            $"{dataPages.Length:N0} data page(s)");
        Console.WriteLine();
        Console.WriteLine("Observed");
        Console.WriteLine($"  Created by                 {FormatCounts(metadata
            .Select(item => item.CreatedBy ?? "(not recorded)"))}");
        Console.WriteLine($"  Parquet format versions    {FormatCounts(metadata.Select(item => item.Version))}");
        PrintRange("File size", fileSizes, bytes: true);
        PrintRange("Rows per file", metadata.Select(item => item.NumRows));
        PrintRange("Row groups per file", metadata.Select(item => (long)item.RowGroups.Count));
        PrintRange("Rows per row group", rowGroups.Select(group => group.NumRows));
        PrintRange("Row group bytes (raw)", rowGroups.Select(group => group.TotalByteSize), bytes: true);
        PrintRange("Row group bytes (compressed)", rowGroups.Select(group =>
            group.TotalCompressedSize ?? group.Columns.Sum(column => column.MetaData?.TotalCompressedSize ?? 0)),
            bytes: true);
        PrintRange("Data page bytes (raw)", dataPages.Select(page => (long)page.UncompressedSize), bytes: true);
        PrintRange("Data page bytes (compressed)", dataPages.Select(page => (long)page.CompressedSize), bytes: true);
        PrintRange("Values per data page", dataPages.Where(page => page.ValueCount.HasValue)
            .Select(page => (long)page.ValueCount!.Value));
        PrintRange("Rows per V2 data page", dataPages.Where(page => page.RowCount.HasValue)
            .Select(page => (long)page.RowCount!.Value));
        PrintRange("Dictionary page bytes (raw)", dictionaryPages.Select(page => (long)page.UncompressedSize),
            bytes: true);
        PrintRange("Dictionary entries", dictionaryPages.Where(page => page.ValueCount.HasValue)
            .Select(page => (long)page.ValueCount!.Value));

        Console.WriteLine($"  Data page versions:       {FormatCounts(dataPages.Select(page => page.Type))}");
        Console.WriteLine($"  Compression codecs:       {FormatCounts(columns.Select(column => column.Codec))}");
        Console.WriteLine($"  Data encodings:           {FormatCounts(dataPages
            .Where(page => page.Encoding.HasValue)
            .Select(page => page.Encoding!.Value))}");
        Console.WriteLine($"  Dictionary pages:         {dictionaryPages.Length:N0} across " +
            $"{columns.Count(column => column.DictionaryPageOffset.HasValue):N0}/{columns.Length:N0} column chunks");
        Console.WriteLine($"  Page checksums:           {pages.Count(page => page.HasChecksum):N0}/{pages.Length:N0} pages");
        Console.WriteLine($"  Column indexes:           {chunks.Count(chunk => chunk.ColumnIndexOffset.HasValue):N0}/{chunks.Length:N0} chunks");
        Console.WriteLine($"  Offset indexes:           {chunks.Count(chunk => chunk.OffsetIndexOffset.HasValue):N0}/{chunks.Length:N0} chunks");
        Console.WriteLine($"  Bloom filters:            {columns.Count(column => column.BloomFilterOffset.HasValue):N0}/{columns.Length:N0} chunks");
        int externalChunks = files.Sum(file => file.ExternalChunks);
        if (externalChunks > 0)
            Console.WriteLine($"  External/unread chunks:   {externalChunks:N0}");

        Console.WriteLine();
        Console.WriteLine("Likely writer settings");
        PrintBoundaryCandidate(
            "RowGroupMaxRows",
            boundaryRowGroups.Select(group => group.NumRows).ToArray(),
            "No non-final row groups expose a row-count boundary.");
        PrintBoundaryCandidate(
            "RowGroupMaxBytes",
            boundaryRowGroups.Select(group => group.TotalByteSize).ToArray(),
            "No non-final row groups expose a byte-size boundary.",
            bytes: true);
        PrintPageCandidate("DataPageSize", dataPages.Select(page => (long)page.UncompressedSize).ToArray());
        PrintPageRowCandidate(dataPages
            .Where(page => page.RowCount.HasValue)
            .Select(page => (long)page.RowCount!.Value)
            .ToArray());
        PrintDictionaryCandidate(
            "DictionaryPageSizeLimit",
            dictionaryPages.Select(page => (long)page.UncompressedSize).ToArray());
        PrintFileSizeCandidate(fileSizes);

        Console.WriteLine();
        Console.WriteLine("Notes");
        Console.WriteLine("  Page and dictionary limits are not stored in Parquet metadata. Page payload sizes are");
        Console.WriteLine("  encoded sizes, while many writers apply their limits to buffered/plain values.");
        Console.WriteLine("  Maximum file size is normally a dataset-writer setting; each Parquet file only reveals");
        Console.WriteLine("  its final size. Compression levels cannot be recovered from the codec identifier.");
    }

    private static void PrintRange(string label, IEnumerable<long> source, bool bytes = false)
    {
        long[] values = source.OrderBy(value => value).ToArray();
        if (values.Length == 0)
        {
            Console.WriteLine($"  {label,-26} none");
            return;
        }

        string minimum = bytes ? FormatBytes(values[0]) : $"{values[0]:N0}";
        string median = bytes ? FormatBytes(Percentile(values, 50)) : $"{Percentile(values, 50):N0}";
        string maximum = bytes ? FormatBytes(values[^1]) : $"{values[^1]:N0}";
        Console.WriteLine($"  {label,-26} {minimum} / {median} / {maximum} (min / median / max)");
    }

    private static void PrintBoundaryCandidate(
        string name,
        long[] values,
        string unavailable,
        bool bytes = false)
    {
        if (values.Length == 0)
        {
            Console.WriteLine($"  {name,-26} unknown (low confidence: {unavailable})");
            return;
        }

        var mode = values.GroupBy(value => value)
            .OrderByDescending(group => group.Count())
            .ThenByDescending(group => group.Key)
            .First();
        string confidence = mode.Count() == values.Length && values.Length > 1 ? "high" : "medium";
        string formatted = bytes ? FormatBytes(mode.Key) : $"{mode.Key:N0}";
        Console.WriteLine($"  {name,-26} about {formatted} ({confidence} confidence; " +
            $"{mode.Count():N0}/{values.Length:N0} non-final row groups)");
    }

    private static void PrintPageCandidate(string name, long[] values)
    {
        if (values.Length == 0)
        {
            Console.WriteLine($"  {name,-26} unknown (no applicable pages)");
            return;
        }

        long maximum = values.Max();
        long conventionalLimit = NearestPowerOfTwo(maximum);
        Console.WriteLine($"  {name,-26} at least {FormatBytes(maximum)} observed; " +
            $"{FormatBytes(conventionalLimit)} is a plausible limit (low confidence)");
    }

    private static void PrintPageRowCandidate(long[] values)
    {
        if (values.Length == 0)
        {
            Console.WriteLine($"  {"DataPageRowCountLimit",-26} unknown " +
                "(V1 page headers do not record row counts)");
            return;
        }

        long maximum = values.Max();
        Console.WriteLine($"  {"DataPageRowCountLimit",-26} at least {maximum:N0} rows observed " +
            "(low confidence; final and byte-limited pages may be smaller)");
    }

    private static void PrintDictionaryCandidate(string name, long[] values)
    {
        if (values.Length == 0)
        {
            Console.WriteLine($"  {name,-26} unknown (no dictionary pages)");
            return;
        }

        Console.WriteLine($"  {name,-26} greater than or equal to {FormatBytes(values.Max())} " +
            "(low confidence; a completed dictionary may be far below the configured limit)");
    }

    private static void PrintFileSizeCandidate(long[] fileSizes)
    {
        long maximum = fileSizes.Max();
        if (fileSizes.Length < 3)
        {
            Console.WriteLine($"  {"MaximumFileSize",-26} at least {FormatBytes(maximum)} " +
                "(low confidence; fewer than three files)");
            return;
        }

        long median = Percentile(fileSizes.OrderBy(value => value).ToArray(), 50);
        int clustered = fileSizes.Count(value => Math.Abs(value - median) <= median / 10);
        if (clustered * 4 >= fileSizes.Length * 3)
        {
            Console.WriteLine($"  {"MaximumFileSize",-26} about {FormatBytes(median)} " +
                $"(medium confidence; {clustered:N0}/{fileSizes.Length:N0} files within 10%)");
        }
        else
        {
            Console.WriteLine($"  {"MaximumFileSize",-26} at least {FormatBytes(maximum)} " +
                "(low confidence; file sizes do not cluster)");
        }
    }

    private static string FormatCounts<T>(IEnumerable<T> source) where T : notnull
    {
        string[] values = source.GroupBy(value => value)
            .OrderByDescending(group => group.Count())
            .ThenBy(group => group.Key)
            .Select(group => $"{group.Key} ({group.Count():N0})")
            .ToArray();
        return values.Length == 0 ? "none" : string.Join(", ", values);
    }

    private static long Percentile(long[] sortedValues, int percentile)
    {
        int index = (int)((long)(sortedValues.Length - 1) * percentile / 100);
        return sortedValues[index];
    }

    private static long NearestPowerOfTwo(long value)
    {
        if (value <= 1)
            return 1;

        long upper = 1;
        while (upper < value && upper <= long.MaxValue / 2)
            upper <<= 1;
        long lower = upper >> 1;
        return value - lower <= upper - value ? lower : upper;
    }

    private static string FormatBytes(long bytes)
    {
        string[] suffixes = ["B", "KB", "MB", "GB", "TB"];
        double value = bytes;
        int suffix = 0;
        while (Math.Abs(value) >= 1024 && suffix < suffixes.Length - 1)
        {
            value /= 1024;
            suffix++;
        }
        return suffix == 0 ? $"{bytes:N0} B" : $"{value:N2} {suffixes[suffix]}";
    }

    private sealed record FileObservation(
        string Path,
        long Length,
        FileMetaData Metadata,
        IReadOnlyList<PageObservation> Pages,
        int ExternalChunks);

    private readonly record struct PageObservation(
        PageType Type,
        int UncompressedSize,
        int CompressedSize,
        int? ValueCount,
        int? RowCount,
        Encoding? Encoding,
        bool HasChecksum);
}
