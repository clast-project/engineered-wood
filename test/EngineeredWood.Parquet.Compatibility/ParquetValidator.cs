// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using Encoding = EngineeredWood.Parquet.Encoding;

namespace EngineeredWood.Compatibility;

enum ValidationResult
{
    Pass,
    Fail,
    Skip,
}

record ValidationReport(FileEntry Entry, ValidationResult Result, string Detail, TimeSpan Elapsed);

static class ParquetValidator
{
    public static async Task<ValidationReport> ValidateAsync(FileEntry entry, string localPath)
    {
        var sw = Stopwatch.StartNew();

        if (entry.Expected == ExpectedOutcome.Skip)
            return new ValidationReport(entry, ValidationResult.Skip, entry.SkipReason ?? "skipped", sw.Elapsed);

        try
        {
            await using var file = new LocalRandomAccessFile(localPath);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            var metadata = await reader.ReadMetadataAsync().ConfigureAwait(false);

            if (metadata.RowGroups.Count == 0)
                return new ValidationReport(entry, ValidationResult.Pass, "0 row groups", sw.Elapsed);

            // Pre-check: detect unsupported codecs and encodings from first row group
            var rg = metadata.RowGroups[0];
            foreach (var col in rg.Columns)
            {
                if (col.MetaData == null)
                    return new ValidationReport(entry, ValidationResult.Skip, "missing column metadata", sw.Elapsed);

                if (col.MetaData.Codec != CompressionCodec.Uncompressed &&
                    col.MetaData.Codec != CompressionCodec.Snappy &&
                    col.MetaData.Codec != CompressionCodec.Gzip &&
                    col.MetaData.Codec != CompressionCodec.Brotli &&
                    col.MetaData.Codec != CompressionCodec.Lz4Hadoop &&
                    col.MetaData.Codec != CompressionCodec.Zstd &&
                    col.MetaData.Codec != CompressionCodec.Lz4)
                {
                    return new ValidationReport(entry, ValidationResult.Skip,
                        $"unsupported codec {col.MetaData.Codec}", sw.Elapsed);
                }

                foreach (var enc in col.MetaData.Encodings)
                {
                    if (enc != Encoding.Plain &&
                        enc != Encoding.PlainDictionary &&
                        enc != Encoding.RleDictionary &&
                        enc != Encoding.Rle &&
                        enc != Encoding.BitPacked &&
                        enc != Encoding.DeltaBinaryPacked &&
                        enc != Encoding.DeltaLengthByteArray &&
                        enc != Encoding.DeltaByteArray &&
                        enc != Encoding.ByteStreamSplit)
                    {
                        return new ValidationReport(entry, ValidationResult.Skip,
                            $"unsupported encoding {enc}", sw.Elapsed);
                    }
                }
            }

            // Read ALL row groups
            for (int i = 0; i < metadata.RowGroups.Count; i++)
            {
                var batch = await reader.ReadRowGroupAsync(i).ConfigureAwait(false);
                if (batch.Length < 0)
                    return new ValidationReport(entry, ValidationResult.Fail,
                        $"row group {i}: negative batch length", sw.Elapsed);
            }

            int totalRows = metadata.RowGroups.Sum(rg => checked((int)rg.NumRows));
            return new ValidationReport(entry, ValidationResult.Pass,
                $"{metadata.RowGroups.Count} rg, {totalRows} rows, {metadata.Schema.Count - 1} cols", sw.Elapsed);
        }
        catch (NotSupportedException ex)
        {
            return new ValidationReport(entry, ValidationResult.Skip, ex.Message, sw.Elapsed);
        }
        catch (AggregateException ex) when (ex.InnerExceptions.All(e => e is NotSupportedException))
        {
            return new ValidationReport(entry, ValidationResult.Skip,
                ex.InnerExceptions[0].Message, sw.Elapsed);
        }
        catch (Exception ex)
        {
            return new ValidationReport(entry, ValidationResult.Fail,
                $"{ex.GetType().Name}: {ex.Message}", sw.Elapsed);
        }
    }
}
