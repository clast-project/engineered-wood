// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.DeletionVectors;

namespace EngineeredWood.DeltaLake.Tests;

public class DeletionVectorFilterTests
{
    [Fact]
    public void Filter_RemovesDeletedRows()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var ids = new Int64Array.Builder()
            .Append(10).Append(20).Append(30).Append(40).Append(50).Build();
        var batch = new RecordBatch(schema, [ids], 5);

        // Delete rows 1 and 3 (values 20 and 40)
        var deletedRows = new HashSet<long> { 1, 3 };
        var filtered = DeletionVectorFilter.Filter(batch, deletedRows, batchStartRow: 0);

        Assert.Equal(3, filtered.Length);
        var result = (Int64Array)filtered.Column(0);
        Assert.Equal(10L, result.GetValue(0));
        Assert.Equal(30L, result.GetValue(1));
        Assert.Equal(50L, result.GetValue(2));
    }

    [Fact]
    public void Filter_WithOffset()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var ids = new Int64Array.Builder()
            .Append(100).Append(200).Append(300).Build();
        var batch = new RecordBatch(schema, [ids], 3);

        // Delete absolute row 11 — this batch starts at row 10, so relative row 1 (value 200)
        var deletedRows = new HashSet<long> { 11 };
        var filtered = DeletionVectorFilter.Filter(batch, deletedRows, batchStartRow: 10);

        Assert.Equal(2, filtered.Length);
        var result = (Int64Array)filtered.Column(0);
        Assert.Equal(100L, result.GetValue(0));
        Assert.Equal(300L, result.GetValue(1));
    }

    [Fact]
    public void Filter_NoDeletedRows_ReturnsSameBatch()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var batch = new RecordBatch(schema, [ids], 2);

        var deletedRows = new HashSet<long>();
        var filtered = DeletionVectorFilter.Filter(batch, deletedRows, batchStartRow: 0);

        Assert.Same(batch, filtered); // Should return the same instance
    }

    [Fact]
    public void Filter_AllRowsDeleted_ReturnsEmpty()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var batch = new RecordBatch(schema, [ids], 2);

        var deletedRows = new HashSet<long> { 0, 1 };
        var filtered = DeletionVectorFilter.Filter(batch, deletedRows, batchStartRow: 0);

        Assert.Equal(0, filtered.Length);
    }

    [Fact]
    public void Filter_DeletedRowsOutOfRange_IgnoredSafely()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var batch = new RecordBatch(schema, [ids], 3);

        // Deleted rows are way beyond this batch
        var deletedRows = new HashSet<long> { 1000, 2000 };
        var filtered = DeletionVectorFilter.Filter(batch, deletedRows, batchStartRow: 0);

        Assert.Same(batch, filtered);
    }

    [Fact]
    public void Filter_MultipleColumns()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("name", StringType.Default, true))
            .Build();

        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var names = new StringArray.Builder().Append("a").Append("b").Append("c").Build();
        var batch = new RecordBatch(schema, [ids, names], 3);

        var deletedRows = new HashSet<long> { 1 };
        var filtered = DeletionVectorFilter.Filter(batch, deletedRows, batchStartRow: 0);

        Assert.Equal(2, filtered.Length);
        Assert.Equal(1L, ((Int64Array)filtered.Column(0)).GetValue(0));
        Assert.Equal(3L, ((Int64Array)filtered.Column(0)).GetValue(1));
        Assert.Equal("a", ((StringArray)filtered.Column(1)).GetString(0));
        Assert.Equal("c", ((StringArray)filtered.Column(1)).GetString(1));
    }
}
