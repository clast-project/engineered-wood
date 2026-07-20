// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

public class PageChecksumTests
{
    [Fact]
    public async Task RoundTrip_WriteWithCrc_ReadWithValidation()
    {
        var path = Path.GetTempFileName();
        try
        {
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, false))
                .Field(new Apache.Arrow.Field("name", Apache.Arrow.Types.StringType.Default, false))
                .Build();

            var ids = new Apache.Arrow.Int32Array.Builder();
            var names = new Apache.Arrow.StringArray.Builder();
            for (int i = 0; i < 200; i++)
            {
                ids.Append(i);
                names.Append($"name_{i}");
            }

            var batch = new Apache.Arrow.RecordBatch(schema, new Apache.Arrow.IArrowArray[]
            {
                ids.Build(), names.Build()
            }, 200);

            // Write with CRC enabled.
            await using (var file = new LocalSequentialFile(path))
            await using (var writer = new ParquetFileWriter(file, ownsFile: false, new ParquetWriteOptions
            {
                PageChecksumEnabled = true,
            }))
            {
                await writer.WriteRowGroupAsync(batch);
            }

            // Read with CRC validation enabled — should succeed.
            await using var readFile = new LocalRandomAccessFile(path);
            await using var reader = new ParquetFileReader(readFile, ownsFile: false, new ParquetReadOptions
            {
                PageChecksumValidation = true,
            });

            var result = await reader.ReadRowGroupAsync(0);
            Assert.Equal(200, result.Length);

            // Verify data is correct.
            var idCol = (Apache.Arrow.Int32Array)result.Column(0);
            Assert.Equal(0, idCol.GetValue(0));
            Assert.Equal(199, idCol.GetValue(199));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task TamperDetection_FlippedBit_ThrowsOnValidation()
    {
        var path = Path.GetTempFileName();
        try
        {
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("x", Apache.Arrow.Types.Int32Type.Default, false))
                .Build();

            var values = new Apache.Arrow.Int32Array.Builder().AppendRange(Enumerable.Range(0, 100)).Build();
            var batch = new Apache.Arrow.RecordBatch(schema,
                new Apache.Arrow.IArrowArray[] { values }, 100);

            // Write with CRC.
            await using (var file = new LocalSequentialFile(path))
            await using (var writer = new ParquetFileWriter(file, ownsFile: false, new ParquetWriteOptions
            {
                PageChecksumEnabled = true,
            }))
            {
                await writer.WriteRowGroupAsync(batch);
            }

            // Tamper with a byte that's definitely in compressed page data.
            // The footer is at the end: [data...][footer][4-byte length][PAR1].
            // We tamper just before the footer — this is page data for the last column.
            var fileBytes = File.ReadAllBytes(path);
            int footerLen = BitConverter.ToInt32(fileBytes, fileBytes.Length - 8);
            int tamperOffset = fileBytes.Length - 8 - footerLen - 5; // 5 bytes before footer
            fileBytes[tamperOffset] ^= 0xFF; // flip all bits for maximum disruption
            File.WriteAllBytes(path, fileBytes);

            // Read with CRC validation — should throw (possibly wrapped in AggregateException
            // from Parallel.For).
            await using var readFile = new LocalRandomAccessFile(path);
            await using var reader = new ParquetFileReader(readFile, ownsFile: false, new ParquetReadOptions
            {
                PageChecksumValidation = true,
            });

            var ex = await Assert.ThrowsAnyAsync<Exception>(
                () => reader.ReadRowGroupAsync(0).AsTask());

            // Unwrap AggregateException if thrown from Parallel.For.
            var inner = ex is AggregateException agg ? agg.InnerException! : ex;
            Assert.IsType<ParquetFormatException>(inner);
            Assert.Contains("CRC", inner.Message);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task BackwardCompat_NoCrc_ValidationEnabled_Succeeds()
    {
        // Existing test files don't have CRC. With validation enabled,
        // reading should still succeed (CRC is optional — no CRC = no validation).
        await using var file = new LocalRandomAccessFile(
            TestData.GetPath("alltypes_plain.parquet"));
        await using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions
        {
            PageChecksumValidation = true,
        });

        var result = await reader.ReadRowGroupAsync(0);
        Assert.Equal(8, result.Length);
    }

    [Fact]
    public async Task WriteWithoutCrc_ReadWithValidation_Succeeds()
    {
        // Files written without CRC should read fine even with validation enabled.
        var path = Path.GetTempFileName();
        try
        {
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("x", Apache.Arrow.Types.Int32Type.Default, false))
                .Build();

            var values = new Apache.Arrow.Int32Array.Builder().AppendRange(Enumerable.Range(0, 50)).Build();
            var batch = new Apache.Arrow.RecordBatch(schema,
                new Apache.Arrow.IArrowArray[] { values }, 50);

            // Write WITHOUT CRC (default).
            await using (var file = new LocalSequentialFile(path))
            await using (var writer = new ParquetFileWriter(file, ownsFile: false))
            {
                await writer.WriteRowGroupAsync(batch);
            }

            // Read WITH validation — should succeed (no CRC to validate).
            await using var readFile = new LocalRandomAccessFile(path);
            await using var reader = new ParquetFileReader(readFile, ownsFile: false, new ParquetReadOptions
            {
                PageChecksumValidation = true,
            });

            var result = await reader.ReadRowGroupAsync(0);
            Assert.Equal(50, result.Length);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task CrossValidation_ParquetSharpReadsFileWithCrc()
    {
        // Verify that files with CRC don't break third-party readers.
        var path = Path.GetTempFileName();
        try
        {
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, false))
                .Build();

            var values = new Apache.Arrow.Int32Array.Builder().AppendRange(Enumerable.Range(0, 100)).Build();
            var batch = new Apache.Arrow.RecordBatch(schema,
                new Apache.Arrow.IArrowArray[] { values }, 100);

            await using (var file = new LocalSequentialFile(path))
            await using (var writer = new ParquetFileWriter(file, ownsFile: false, new ParquetWriteOptions
            {
                PageChecksumEnabled = true,
            }))
            {
                await writer.WriteRowGroupAsync(batch);
            }

            // ParquetSharp should read the file without errors.
            using var reader = new ParquetSharp.ParquetFileReader(path);
            Assert.Equal(100, reader.FileMetaData.NumRows);

            using var rg = reader.RowGroup(0);
            using var col = rg.Column(0).LogicalReader<int>();
            var buffer = new int[100];
            col.ReadBatch(buffer);
            Assert.Equal(0, buffer[0]);
            Assert.Equal(99, buffer[99]);
        }
        finally
        {
            File.Delete(path);
        }
    }
}
