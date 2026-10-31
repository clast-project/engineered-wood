// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using DeltaLake.Interfaces;
using DeltaLake.Table;
using EngineeredWood.Arrow;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions.Arrow;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using DdSaveMode = DeltaLake.Table.SaveMode;
using EwDeltaTable = EngineeredWood.DeltaLake.Table.DeltaTable;

namespace DeltaDotnetComparison;

/// <summary>Which engine wrote a fixture table. Every reader is run against tables from both.</summary>
public enum TableWriter
{
    EngineeredWood,
    DeltaDotnet,
}

/// <summary>
/// The SQL text of the operations both engines run. delta-dotnet takes SQL strings; EngineeredWood is handed
/// the SAME text through <see cref="SparkSqlParser"/>, so the two always evaluate one predicate.
/// </summary>
internal static class Sql
{
    /// <summary>One file in twenty, by id range: what file statistics can prune.</summary>
    public const string SelectiveRange = "id >= 500000 AND id < 510000";

    /// <summary>One row in sixteen, spread through every file: nothing can be pruned.</summary>
    public const string OneCategory = "category = 'cat_03'";

    public const string UpdateAssignmentColumn = "amount";
    public const string UpdateAssignment = "amount * 2";
}

/// <summary>EngineeredWood operations, shaped as the benchmarks call them.</summary>
internal static class Ew
{
    private static readonly ArrowRowEvaluator Evaluator = new(new EngineeredWood.Expressions.Arrow.Spark.SparkFunctionRegistry());

    public static LocalTableFileSystem Fs(string path) => new(path);

    public static async Task<long> CreateAndWriteAsync(
        string path, IReadOnlyList<RecordBatch> batches, IReadOnlyDictionary<string, string>? configuration = null)
    {
        await using var table = await EwDeltaTable.CreateAsync(Fs(path), Data.Schema, configuration: configuration);
        return await table.WriteAsync(batches);
    }

    public static async Task<long> AppendAsync(string path, IReadOnlyList<RecordBatch> batches)
    {
        await using var table = await EwDeltaTable.OpenAsync(Fs(path));
        return await table.WriteAsync(batches);
    }

    public static async Task<long> CountAsync(IAsyncEnumerable<RecordBatch> batches)
    {
        long rows = 0;
        await foreach (var batch in batches)
        {
            rows += batch.Length;
            batch.Dispose();
        }
        return rows;
    }

    /// <summary>
    /// <see cref="DeltaReadOptions.Filter"/> only PRUNES (files, row groups, pages); surviving batches still
    /// hold non-matching rows. A query engine returns exactly the matching rows, so for a like-for-like
    /// comparison this applies the predicate per row afterwards, as a caller of EW would have to.
    /// </summary>
    public static async Task<long> FilteredCountAsync(
        EwDeltaTable table, string predicateSql, IReadOnlyList<string> columns, bool exact)
    {
        var predicate = SparkSqlParser.ParsePredicate(predicateSql);
        long rows = 0;
        // The row filter needs the predicate's own column even when the query does not project it; the
        // SQL engine reads it for the same reason.
        var read = exact ? columns.Union(PredicateColumns(predicateSql)).ToList() : columns;
        var options = new DeltaReadOptions { Columns = read, Filter = predicate };
        await foreach (var batch in table.ReadAsync(options))
        {
            if (!exact)
            {
                rows += batch.Length;
                batch.Dispose();
                continue;
            }

            var indices = new List<int>();
            using (var mask = Evaluator.EvaluatePredicate(predicate, batch))
            {
                for (int i = 0; i < mask.Length; i++)
                {
                    if (mask.GetValue(i) == true)
                        indices.Add(i);
                }
            }

            // Gather only the projected columns: the predicate's own column was read to evaluate the filter,
            // but the query does not return it, and the SQL engine does not materialize it either.
            var projected = Project(batch, columns);
            using var filtered = ArrowCompute.Take(projected, projected.Schema, indices);
            rows += filtered.Length;
            batch.Dispose(); // owns the arrays `projected` shares, so `projected` is not disposed separately
        }
        return rows;
    }

    private static RecordBatch Project(RecordBatch batch, IReadOnlyList<string> columns)
    {
        if (batch.ColumnCount == columns.Count)
            return batch;
        var fields = new List<Field>(columns.Count);
        var arrays = new IArrowArray[columns.Count];
        for (int c = 0; c < columns.Count; c++)
        {
            int index = batch.Schema.GetFieldIndex(columns[c]);
            fields.Add(batch.Schema.FieldsList[index]);
            arrays[c] = batch.Column(index);
        }
        return new RecordBatch(new Schema(fields, batch.Schema.Metadata), arrays, batch.Length);
    }

    /// <summary>
    /// Predicate DELETE through <see cref="EwDeltaTable.DeleteAsync(Expressions.Predicate, CancellationToken)"/>.
    /// A delete that removes PART of a file writes a deletion vector, so this needs a table with
    /// <c>delta.enableDeletionVectors</c>; on any other table it refuses.
    /// </summary>
    public static async Task<long> DeleteAsync(string path, string predicateSql)
    {
        await using var table = await EwDeltaTable.OpenAsync(Fs(path));
        var (deleted, _) = await table.DeleteAsync(SparkSqlParser.ParsePredicate(predicateSql));
        return deleted;
    }

    /// <summary>
    /// Predicate DELETE as copy-on-write, which is what delta-rs does. EW has no predicate overload for it:
    /// a host scans with <see cref="DeltaRowMetadata.Locator"/>, evaluates the predicate itself, and hands
    /// the matching positions to <see cref="EwDeltaTable.DeleteRowsAsync"/> with
    /// <see cref="RowDeleteMode.CopyOnWrite"/>. That whole sequence is what is measured.
    /// </summary>
    public static async Task<long> DeleteCopyOnWriteAsync(string path, string predicateSql)
    {
        await using var table = await EwDeltaTable.OpenAsync(Fs(path));
        var predicate = SparkSqlParser.ParsePredicate(predicateSql);
        var options = new DeltaReadOptions
        {
            Columns = PredicateColumns(predicateSql),
            Filter = predicate,
            Metadata = DeltaRowMetadata.Locator,
        };
        string pathColumn = options.MetadataPrefix + RowSelection.FilePathColumnSuffix;
        string indexColumn = options.MetadataPrefix + RowSelection.RowIndexColumnSuffix;

        var positions = new Dictionary<string, List<long>>(StringComparer.Ordinal);
        await foreach (var batch in table.ReadAsync(options))
        {
            using var mask = Evaluator.EvaluatePredicate(predicate, batch);
            var paths =(StringArray)batch.Column(batch.Schema.GetFieldIndex(pathColumn));
            var indexes = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(indexColumn));
            for (int i = 0; i < batch.Length; i++)
            {
                if (mask.GetValue(i) != true)
                    continue;
                string file = paths.GetString(i);
                if (!positions.TryGetValue(file, out var list))
                    positions[file] = list = [];
                list.Add(indexes.GetValue(i)!.Value);
            }
            batch.Dispose();
        }

        var selection = RowSelection.ByPath(
            positions.ToDictionary(p => p.Key, p => (IReadOnlyCollection<long>)p.Value, StringComparer.Ordinal));
        var (deleted, _) = await table.DeleteRowsAsync(selection, RowDeleteMode.CopyOnWrite);
        return deleted;
    }

    /// <summary>The benchmark predicates each test one column; read only that one to find the rows.</summary>
    private static IReadOnlyList<string> PredicateColumns(string predicateSql) =>
        [predicateSql.TrimStart().Split(' ')[0]];

    /// <summary><c>UPDATE … SET amount = amount * 2 WHERE …</c>, the assignment evaluated by EW's own evaluator.</summary>
    public static async Task<long> UpdateAsync(string path, string predicateSql)
    {
        await using var table = await EwDeltaTable.OpenAsync(Fs(path));
        var assignment = SparkSqlParser.ParseExpression(Sql.UpdateAssignment);
        var (updated, _) = await table.UpdateAsync(SparkSqlParser.ParsePredicate(predicateSql), batch =>
        {
            int column = batch.Schema.GetFieldIndex(Sql.UpdateAssignmentColumn);
            var value = Evaluator.EvaluateExpression(assignment, batch, batch.Schema.FieldsList[column].DataType);
            var columns = new IArrowArray[batch.ColumnCount];
            for (int c = 0; c < columns.Length; c++)
                columns[c] = c == column ? value : batch.Column(c);
            return new RecordBatch(batch.Schema, columns, batch.Length);
        });
        return updated;
    }

    /// <summary>Row count and sums, read back by EW: what the verify pass compares across engines.</summary>
    public static async Task<Fingerprint> FingerprintAsync(string path)
    {
        await using var table = await EwDeltaTable.OpenAsync(Fs(path));
        var fp = new Fingerprint();
        await foreach (var batch in table.ReadAllAsync())
        {
            fp.Add(batch);
            batch.Dispose();
        }
        return fp;
    }
}

/// <summary>delta-dotnet operations, shaped as the benchmarks call them.</summary>
internal static class Dd
{
    /// <summary>
    /// One engine (one tokio runtime) per process. Its construction is not benchmarked: it is a one-time host
    /// cost with no EngineeredWood counterpart.
    /// </summary>
    public static readonly DeltaEngine Engine = new(EngineOptions.Default);

    /// <summary>delta-dotnet's own default for <see cref="InsertOptions.MaxRowsPerGroup"/>.</summary>
    public const ulong DefaultMaxRowsPerGroup = 100;

    public static async Task<ITable> OpenAsync(string path, ulong? version = null) =>
        await Engine.LoadTableAsync(
            new TableOptions { TableLocation = path, Version = version }, CancellationToken.None);

    public static async Task CreateAndInsertAsync(
        string path,
        IReadOnlyList<RecordBatch> batches,
        IReadOnlyDictionary<string, string>? configuration = null,
        ulong maxRowsPerGroup = DefaultMaxRowsPerGroup)
    {
        var create = new TableCreateOptions(path, Data.Schema)
        {
            SaveMode = DdSaveMode.ErrorIfExists,
            Configuration = configuration?.ToDictionary(p => p.Key, p => p.Value),
        };
        using var table = await Engine.CreateTableAsync(create, CancellationToken.None);
        await InsertAsync(table, batches, maxRowsPerGroup);
    }

    public static async Task AppendAsync(
        string path, IReadOnlyList<RecordBatch> batches, ulong maxRowsPerGroup = DefaultMaxRowsPerGroup)
    {
        using var table = await OpenAsync(path);
        await InsertAsync(table, batches, maxRowsPerGroup);
    }

    public static Task InsertAsync(ITable table, IReadOnlyList<RecordBatch> batches, ulong maxRowsPerGroup) =>
        table.InsertAsync(
            batches.ToArray(),
            Data.Schema,
            new InsertOptions { SaveMode = DdSaveMode.Append, MaxRowsPerGroup = maxRowsPerGroup },
            CancellationToken.None);

    public static async Task<long> CountAsync(IAsyncEnumerable<RecordBatch> batches)
    {
        long rows = 0;
        await foreach (var batch in batches)
        {
            rows += batch.Length;
            batch.Dispose();
        }
        return rows;
    }

    public static async Task<long> DeleteAsync(string path, string predicateSql)
    {
        using var table = await OpenAsync(path);
        await table.DeleteAsync(predicateSql, CancellationToken.None);
        return (long)(table.Version() ?? 0);
    }

    public static async Task<long> UpdateAsync(string path, string predicateSql)
    {
        using var table = await OpenAsync(path);
        await table.UpdateAsync(
            $"UPDATE deltatable SET {Sql.UpdateAssignmentColumn} = {Sql.UpdateAssignment} WHERE {predicateSql}",
            CancellationToken.None);
        return (long)(table.Version() ?? 0);
    }

    /// <summary>The same fingerprint, read back through delta-rs's DataFusion scan (<c>QueryAsync</c>).</summary>
    public static async Task<Fingerprint> FingerprintViaQueryAsync(string path)
    {
        using var table = await OpenAsync(path);
        var fp = new Fingerprint();
        await foreach (var batch in table.QueryAsync(
            new SelectQuery("SELECT id, amount FROM deltatable"), CancellationToken.None))
        {
            fp.Add(batch);
            batch.Dispose();
        }
        return fp;
    }

    /// <summary>The same fingerprint as <see cref="Ew.FingerprintAsync"/>, read back by delta-dotnet's kernel scan.</summary>
    public static async Task<Fingerprint> FingerprintAsync(string path)
    {
        using var table = await OpenAsync(path);
        using var owned = await table.ReadAsArrowTableAsync(CancellationToken.None);
        var fp = new Fingerprint();
        var t = owned.Table;
        var schema = t.Schema;
        // Apache.Arrow.Table exposes chunked columns; rebuild per-chunk batches by column name.
        int chunks = t.Column(0).Data.ArrayCount;
        for (int k = 0; k < chunks; k++)
        {
            var arrays = new IArrowArray[schema.FieldsList.Count];
            for (int c = 0; c < arrays.Length; c++)
                arrays[c] = t.Column(c).Data.Array(k);
            fp.Add(new RecordBatch(schema, arrays, arrays[0].Length));
        }
        return fp;
    }
}

/// <summary>An order-independent summary of a table's contents.</summary>
internal sealed class Fingerprint
{
    public long Rows { get; private set; }
    public long IdSum { get; private set; }
    public decimal AmountSum { get; private set; }

    public void Add(RecordBatch batch)
    {
        Rows += batch.Length;
        var ids = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
        var amounts = (DoubleArray)batch.Column(batch.Schema.GetFieldIndex("amount"));
        for (int i = 0; i < batch.Length; i++)
        {
            IdSum += ids.GetValue(i) ?? 0;
            // Amounts are cents/100, so rounding to the cent makes the sum exact and order-independent.
            AmountSum += Math.Round((decimal)(amounts.GetValue(i) ?? 0), 2);
        }
    }

    public override string ToString() => $"rows={Rows} idSum={IdSum} amountSum={AmountSum}";

    public override bool Equals(object? obj) =>
        obj is Fingerprint o && o.Rows == Rows && o.IdSum == IdSum && o.AmountSum == AmountSum;

    public override int GetHashCode() => HashCode.Combine(Rows, IdSum, AmountSum);
}
