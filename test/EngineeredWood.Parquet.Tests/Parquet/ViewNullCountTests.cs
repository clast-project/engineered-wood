// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// #786: a view-typed column's null count matches its rows. DELTA_BYTE_ARRAY, DELTA_LENGTH_BYTE_ARRAY and FSST pages
/// counted every value twice in view output (<c>ColumnBuildState.AddByteArrayValues</c>), and the build derives the null
/// count from that count, so it came out negative. Found, and fixed, on <c>feature/row-level-filtering</c> (#785), where
/// this test is <c>KeepRowsTests.AViewColumnsNullCountMatchesItsRows</c>; ported to <c>main</c> ahead of that branch.
/// </summary>
public class ViewNullCountTests
{
    [Fact]
    public async Task AViewColumnsNullCountMatchesItsRows()
    {
        int columns = 0;
        var failures = new List<string>();
        foreach (var path in TestData.GetAllParquetFiles())
        {
            string name = Path.GetFileName(path);
            if (name.Contains("encrypt", StringComparison.OrdinalIgnoreCase)
                || name.Contains("malformed", StringComparison.OrdinalIgnoreCase)
                || name == "large_string_map.brotli.parquet"
                || name == "fixed_length_byte_array.parquet")
            {
                continue;
            }

            RecordBatch batch;
            try
            {
                await using var file = new LocalRandomAccessFile(path);
                using var reader = new ParquetFileReader(file, ownsFile: false,
                    new ParquetReadOptions { ByteArrayOutput = ByteArrayOutputKind.ViewType });
                if ((await reader.ReadMetadataAsync()).RowGroups.Count == 0)
                    continue;
                batch = await reader.ReadRowGroupAsync(0);
            }
            catch (Exception e) when (e is not Xunit.Sdk.XunitException)
            {
                continue;
            }

            using (batch)
            {
                for (int c = 0; c < batch.ColumnCount; c++)
                {
                    if (batch.Column(c) is not BinaryViewArray views)
                        continue;
                    columns++;
                    int nulls = Enumerable.Range(0, views.Length).Count(views.IsNull);
                    if (views.NullCount != nulls)
                        failures.Add($"{name}.{batch.Schema.GetFieldByIndex(c).Name}: NullCount {views.NullCount}, {nulls} null rows");
                }
            }
        }

        Assert.True(columns > 5, $"only {columns} view columns were read");
        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
    }
}
